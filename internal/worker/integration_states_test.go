package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
)

var integrationTables = []string{
	"user_profiles", "addresses", "contact_methods", "object_metadata", "storage_object_identities",
	"deletion_requests", "deletion_batches", "deletion_batch_items", "deletion_outbox", "deletion_audit_log",
}

func (h *harness) snapshot() map[string]string {
	h.t.Helper()
	state := make(map[string]string, len(integrationTables))
	for _, table := range integrationTables {
		var rows string
		if err := h.owner.QueryRow(h.ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb)::text
			FROM `+table+` r`).Scan(&rows); err != nil {
			h.t.Fatal(err)
		}
		state[table] = rows
	}
	return state
}

func (h *harness) assertSnapshot(want map[string]string) {
	h.t.Helper()
	got := h.snapshot()
	for table, rows := range want {
		if got[table] != rows {
			h.t.Errorf("%s changed unexpectedly:\nbefore: %s\nafter:  %s", table, rows, got[table])
		}
	}
}

func (h *harness) assertRows(want map[string]int) {
	h.t.Helper()
	for _, table := range integrationTables {
		if got := h.count(`SELECT count(*) FROM ` + table); got != want[table] {
			h.t.Errorf("%s rows=%d, want %d", table, got, want[table])
		}
	}
}

func (h *harness) assertUnresolved(m Message, state, code string, attempts int) {
	h.t.Helper()
	h.assertRows(map[string]int{
		"user_profiles": 1, "addresses": 1, "contact_methods": 1, "object_metadata": 1,
		"storage_object_identities": 1, "deletion_requests": 1, "deletion_batches": 1,
		"deletion_batch_items": 1, "deletion_outbox": 1, "deletion_audit_log": 1,
	})
	if h.count(`SELECT count(*) FROM deletion_requests WHERE request_id=$1 AND state='PENDING'
		AND user_id=$2 AND completed_at IS NULL AND finalizer_generation=0`, m.RequestID, m.UserID) != 1 ||
		h.count(`SELECT count(*) FROM user_profiles WHERE id=$1 AND status='PENDING_DELETION'`, m.UserID) != 1 ||
		h.count(`SELECT count(*) FROM object_metadata WHERE user_id=$1 AND status='PENDING_DELETION'`, m.UserID) != 1 {
		h.t.Error("unresolved request or subject was completed or scrubbed")
	}
	batchState := state
	if state == "INVALID" {
		batchState = "BLOCKED"
	}
	if h.count(`SELECT count(*) FROM deletion_batches WHERE request_id=$1 AND state=$2 AND deleted_count=0
		AND completed_at IS NULL AND lease_token IS NULL AND lease_until IS NULL`, m.RequestID, batchState) != 1 ||
		h.count(`SELECT count(*) FROM deletion_batch_items WHERE state=$1 AND error_code IS NOT DISTINCT FROM NULLIF($2,'') AND attempts=$3
			AND bucket IS NOT NULL AND object_key=prefix||filename`, state, code, attempts) != 1 {
		h.t.Error("missing durable error state, attempts, snapshot, or released lease")
	}
	if h.count(`SELECT count(*) FROM deletion_audit_log WHERE stage='INITIATED' AND items_affected=0
		AND initiator_user_hash=$1`, m.InitiatorUserHash) != 1 {
		h.t.Error("failure incorrectly changed completion audit evidence")
	}
}

func (h *harness) failWrites(table, operation, predicate string) {
	h.t.Helper()
	h.exec(`CREATE FUNCTION integration_fail_write() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected integration failure'; END; $$`)
	h.exec(`CREATE TRIGGER integration_fail BEFORE ` + operation + ` ON ` + table +
		` FOR EACH ROW ` + predicate + ` EXECUTE FUNCTION integration_fail_write()`)
}

func (h *harness) allowWrites(table string) {
	h.t.Helper()
	h.exec(`DROP TRIGGER integration_fail ON ` + table + `; DROP FUNCTION integration_fail_write()`)
}

func testIntegrationStates(t *testing.T, adminURL string) {
	t.Run("initiation-is-atomic", func(t *testing.T) {
		for _, tc := range []struct{ table, operation string }{
			{"deletion_requests", "INSERT"}, {"user_profiles", "UPDATE"},
			{"deletion_audit_log", "INSERT"}, {"deletion_batches", "INSERT"},
			{"deletion_batch_items", "INSERT"}, {"object_metadata", "UPDATE"}, {"deletion_outbox", "INSERT"},
		} {
			t.Run(tc.table, func(t *testing.T) {
				h := newHarness(t, adminURL)
				m := h.seed(2)
				before := h.snapshot()
				h.failWrites(tc.table, tc.operation, "")
				err := h.store.Initiate(m.Context(h.ctx), m)
				var pgErr interface{ SQLState() string }
				if !errors.As(err, &pgErr) || pgErr.SQLState() != "P0001" {
					t.Fatalf("expected injected database failure, got %v", err)
				}
				h.assertSnapshot(before)
				h.allowWrites(tc.table)
				if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
					t.Fatal("retry after rollback", err)
				}
				h.finish(m)
			})
		}
	})
	t.Run("commit-failure-rolls-back-entire-initiation", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(1)
		before := h.snapshot()
		h.exec(`CREATE FUNCTION integration_fail_write() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'injected deferred failure'; END; $$;
			CREATE CONSTRAINT TRIGGER integration_fail AFTER INSERT ON deletion_outbox
			DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION integration_fail_write()`)
		err := h.store.Initiate(m.Context(h.ctx), m)
		if err == nil || !strings.Contains(err.Error(), "commit database transaction") {
			t.Fatalf("expected commit failure, got %v", err)
		}
		h.assertSnapshot(before)
		h.allowWrites("deletion_outbox")
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		h.finish(m)
	})
	t.Run("rejected-commands-do-not-mutate-tables", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(1)
		other := h.seed(0)
		purge := h.command(m.RequestID, purgeAction)
		for _, tc := range []struct {
			name string
			call func() error
			want error
		}{
			{"missing-subject", func() error { return h.store.Initiate(h.ctx, testMessage(initiateAction)) }, pgx.ErrNoRows},
			{"request-rebound-to-another-subject", func() error {
				conflict := m
				conflict.UserID = other.UserID
				return h.store.Initiate(h.ctx, conflict)
			}, errInvalid},
			{"request-rebound-to-another-actor", func() error {
				conflict := m
				conflict.InitiatorUserHash = userHash("other", "actor")
				return h.store.Initiate(h.ctx, conflict)
			}, errInvalid},
			{"second-request-for-pending-subject", func() error {
				conflict := m
				conflict.RequestID = newUUID()
				return h.store.Initiate(h.ctx, conflict)
			}, errPending},
			{"unknown-request", func() error {
				unknown := purge
				unknown.RequestID = newUUID()
				return h.svc.Purge(h.ctx, unknown)
			}, pgx.ErrNoRows},
			{"unknown-batch", func() error {
				unknown := purge
				unknown.BatchID = newUUID()
				return h.svc.Purge(h.ctx, unknown)
			}, pgx.ErrNoRows},
			{"missing-cursor", func() error {
				invalid := purge
				invalid.StartKey = nil
				return h.svc.Purge(h.ctx, invalid)
			}, errInvalid},
			{"wrong-cursor", func() error {
				invalid, cursor := purge, int64(1)
				invalid.StartKey = &cursor
				return h.svc.Purge(h.ctx, invalid)
			}, errInvalid},
			{"premature-finalizer", func() error {
				early := testMessage(finalizeAction)
				early.RequestID = m.RequestID
				return h.store.Finalize(h.ctx, early)
			}, errPending},
		} {
			t.Run(tc.name, func(t *testing.T) {
				before := h.snapshot()
				if err := tc.call(); !errors.Is(err, tc.want) {
					t.Fatalf("error=%v, want %v", err, tc.want)
				}
				h.assertSnapshot(before)
				if len(h.s3.calls) != 0 || len(h.queue.snapshot()) != 0 {
					t.Fatal("rejected command performed external work")
				}
			})
		}
	})
	t.Run("invalid-envelopes-create-no-workflow-rows", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(1)
		valid, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		cases := []struct {
			name string
			body []byte
		}{
			{"malformed-json", []byte("{")},
			{"trailing-json", append(append([]byte(nil), valid...), []byte(` {}`)...)},
			{"unknown-field", []byte(strings.TrimSuffix(string(valid), "}") + `,"object_key":"untrusted"}`)},
			{"invalid-utf8", []byte{0xff}},
			{"oversized-message", []byte(strings.Repeat(" ", 1024*1024+1))},
		}
		for _, tc := range []struct {
			name   string
			mutate func(*Message)
		}{
			{"wrong-version", func(m *Message) { m.Version = 2 }},
			{"invalid-message-id", func(m *Message) { m.MessageID = "not-a-uuid" }},
			{"invalid-request-id", func(m *Message) { m.RequestID = "not-a-uuid" }},
			{"missing-actor", func(m *Message) { m.InitiatorUserHash = "" }},
			{"missing-trace", func(m *Message) { m.TraceParent = "" }},
			{"invalid-tracestate", func(m *Message) { m.TraceState = "invalid state" }},
			{"missing-subject", func(m *Message) { m.UserID = " \t " }},
			{"null-in-subject", func(m *Message) { m.UserID = "subject\x00" }},
			{"unknown-action", func(m *Message) { m.Action = "DELETE_EVERYTHING" }},
			{"unexpected-batch-id", func(m *Message) { m.BatchID = newUUID() }},
		} {
			invalid := m
			tc.mutate(&invalid)
			body, err := json.Marshal(invalid)
			if err != nil {
				t.Fatal(err)
			}
			cases = append(cases, struct {
				name string
				body []byte
			}{tc.name, body})
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				before := h.snapshot()
				if err := h.svc.Handle(h.ctx, "db", tc.body); !errors.Is(err, errInvalid) {
					t.Fatalf("malformed envelope error=%v", err)
				}
				h.assertSnapshot(before)
				if len(h.queue.snapshot()) != 0 || len(h.s3.calls) != 0 {
					t.Fatal("invalid envelope reached AWS")
				}
			})
		}
	})
	t.Run("s3-errors-retain-canonical-evidence", func(t *testing.T) {
		for _, tc := range []struct {
			name, state, code string
			setup             func(*fakeS3)
		}{
			{"transport", "PENDING", "operation_failed", func(f *fakeS3) {
				f.delete = func(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
					return nil, errors.New("sensitive transport details must not be persisted")
				}
			}},
			{"deadline", "PENDING", "deadline_exceeded", func(f *fakeS3) { f.check = context.DeadlineExceeded }},
			{"missing-bucket", "BLOCKED", "NoSuchBucket", func(f *fakeS3) {
				f.check = fmt.Errorf("wrapped AWS failure: %w", &s3types.NoSuchBucket{})
			}},
			{"versioning-enabled", "BLOCKED", "unsupported_bucket_versioning", func(f *fakeS3) {
				f.version = s3types.BucketVersioningStatusEnabled
			}},
			{"versioning-suspended", "BLOCKED", "unsupported_bucket_versioning", func(f *fakeS3) {
				f.version = s3types.BucketVersioningStatusSuspended
			}},
			{"mfa-delete", "BLOCKED", "unsupported_bucket_versioning", func(f *fakeS3) {
				f.versioning = func(*s3.GetBucketVersioningInput) (*s3.GetBucketVersioningOutput, error) {
					return &s3.GetBucketVersioningOutput{MFADelete: s3types.MFADeleteStatusEnabled}, nil
				}
			}},
			{"nil-versioning-response", "PENDING", "operation_failed", func(f *fakeS3) {
				f.versioning = func(*s3.GetBucketVersioningInput) (*s3.GetBucketVersioningOutput, error) { return nil, nil }
			}},
			{"nil-delete-response", "PENDING", "invalid_s3_response", func(f *fakeS3) {
				f.delete = func(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) { return nil, nil }
			}},
			{"unconfirmed-delete", "PENDING", "missing_confirmation", func(f *fakeS3) {
				f.delete = func(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) { return &s3.DeleteObjectsOutput{}, nil }
			}},
			{"unexpected-confirmation", "PENDING", "missing_confirmation", func(f *fakeS3) {
				f.delete = func(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
					return &s3.DeleteObjectsOutput{Deleted: []s3types.DeletedObject{{Key: aws.String("unrequested")}}}, nil
				}
			}},
			{"unexpected-error-overrides-confirmation", "PENDING", "unexpected_s3_response", func(f *fakeS3) {
				f.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
					out := confirmed(in)
					out.Errors = []s3types.Error{{Key: aws.String("unrequested"), Code: aws.String("AccessDenied")}}
					return out, nil
				}
			}},
			{"permanent-error-overrides-confirmation", "BLOCKED", "AccessDenied", func(f *fakeS3) {
				f.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
					out := confirmed(in)
					out.Errors = []s3types.Error{{Key: in.Delete.Objects[0].Key, Code: aws.String("AccessDenied")}}
					return out, nil
				}
			}},
			{"unsafe-error-code-is-sanitized", "PENDING", "s3_error", func(f *fakeS3) {
				f.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
					return &s3.DeleteObjectsOutput{Errors: []s3types.Error{{
						Key: in.Delete.Objects[0].Key, Code: aws.String("contains secret/path\n"),
					}}}, nil
				}
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				h := newHarness(t, adminURL)
				m := h.initiate(1)
				purge := h.command(m.RequestID, purgeAction)
				tc.setup(h.s3)
				err := h.svc.Purge(m.Context(h.ctx), purge)
				if tc.state == "PENDING" && !errors.Is(err, errPending) || tc.state == "BLOCKED" && err != nil {
					t.Fatalf("unexpected failure disposition: %v", err)
				}
				h.assertUnresolved(m, tc.state, tc.code, 1)
				if tc.state == "BLOCKED" {
					before := h.snapshot()
					calls := len(h.s3.calls)
					if err := h.svc.Purge(m.Context(h.ctx), purge); err != nil {
						t.Fatal(err)
					}
					h.assertSnapshot(before)
					if len(h.s3.calls) != calls {
						t.Fatal("permanent failure was retried")
					}
				} else {
					h.s3.check, h.s3.versioning, h.s3.delete = nil, nil, nil
					h.finish(m)
					h.assertRows(map[string]int{
						"storage_object_identities": 1, "deletion_requests": 1, "deletion_batches": 1,
						"deletion_batch_items": 1, "deletion_outbox": 2, "deletion_audit_log": 3,
					})
				}
			})
		}
	})
	testIntegrationCheckpoints(t, adminURL)
	testIntegrationBoundaries(t, adminURL)
	testIntegrationRecoveryStates(t, adminURL)
}

func testIntegrationCheckpoints(t *testing.T, adminURL string) {
	t.Run("checkpoint-failure-after-external-delete", func(t *testing.T) {
		for _, tc := range []struct{ table, operation, predicate string }{
			{"object_metadata", "DELETE", ""},
			{"deletion_batch_items", "UPDATE", ""},
			{"deletion_batches", "UPDATE", "WHEN (NEW.deleted_count > OLD.deleted_count)"},
		} {
			t.Run(tc.table, func(t *testing.T) {
				h := newHarness(t, adminURL)
				m := h.initiate(1)
				purge := h.command(m.RequestID, purgeAction)
				before := h.snapshot()
				delete(before, "deletion_batches")
				h.failWrites(tc.table, tc.operation, tc.predicate)
				if err := h.svc.Purge(m.Context(h.ctx), purge); err == nil {
					t.Fatal("checkpoint failure was suppressed")
				}
				if len(h.s3.calls) != 1 {
					t.Fatal("failure must occur after the external delete")
				}
				h.assertSnapshot(before)
				if h.count(`SELECT count(*) FROM deletion_batches WHERE state='PENDING' AND attempts=1
					AND deleted_count=0 AND lease_token IS NOT NULL AND lease_until>now()`) != 1 {
					t.Fatal("failed checkpoint lost the recoverable claim")
				}
				h.allowWrites(tc.table)
				h.exec(`UPDATE deletion_batches SET lease_until=now()-interval '1 second'`)
				h.finish(m)
				if len(h.s3.calls) != 2 || h.count(`SELECT items_affected FROM deletion_audit_log WHERE stage='BATCH_COMPLETED'`) != 1 {
					t.Fatal("external retry did not preserve exactly one audited effect")
				}
			})
		}
	})
	t.Run("batch-completion-is-atomic", func(t *testing.T) {
		for _, tc := range []struct{ table, predicate string }{
			{"deletion_audit_log", "WHEN (NEW.stage='BATCH_COMPLETED')"},
			{"deletion_outbox", "WHEN (NEW.destination='db')"},
		} {
			t.Run(tc.table, func(t *testing.T) {
				h := newHarness(t, adminURL)
				m := h.initiate(1)
				purge := h.command(m.RequestID, purgeAction)
				h.failWrites(tc.table, "INSERT", tc.predicate)
				if err := h.svc.Purge(m.Context(h.ctx), purge); err == nil {
					t.Fatal("completion transaction failure was suppressed")
				}
				h.assertRows(map[string]int{
					"user_profiles": 1, "addresses": 1, "contact_methods": 1, "storage_object_identities": 1,
					"deletion_requests": 1, "deletion_batches": 1, "deletion_batch_items": 1,
					"deletion_outbox": 1, "deletion_audit_log": 1,
				})
				if h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='DELETED' AND bucket IS NULL
					AND object_key IS NULL AND prefix IS NULL AND filename IS NULL AND error_code IS NULL`) != 1 ||
					h.count(`SELECT count(*) FROM deletion_batches WHERE state='PENDING' AND deleted_count=1
						AND completed_at IS NULL AND lease_token IS NOT NULL`) != 1 ||
					h.count(`SELECT count(*) FROM deletion_requests WHERE state='PENDING' AND finalizer_generation=0`) != 1 {
					t.Fatal("committed checkpoint was lost or partial completion escaped its transaction")
				}
				h.allowWrites(tc.table)
				h.exec(`UPDATE deletion_batches SET lease_until=now()-interval '1 second'`)
				h.finish(m)
				if len(h.s3.calls) != 1 || h.count(`SELECT count(*) FROM deletion_audit_log`) != 3 {
					t.Fatal("completion retry repeated checkpointed S3 work or audit effects")
				}
			})
		}
	})
	t.Run("finalization-is-atomic", func(t *testing.T) {
		for _, tc := range []struct{ table, operation, predicate string }{
			{"addresses", "DELETE", ""},
			{"user_profiles", "DELETE", ""},
			{"deletion_requests", "UPDATE", "WHEN (NEW.state='COMPLETED')"},
			{"deletion_audit_log", "INSERT", "WHEN (NEW.stage='COMPLETED')"},
		} {
			t.Run(tc.table, func(t *testing.T) {
				h := newHarness(t, adminURL)
				m := h.initiate(0)
				finalizer := h.command(m.RequestID, finalizeAction)
				before := h.snapshot()
				h.failWrites(tc.table, tc.operation, tc.predicate)
				if err := h.store.Finalize(m.Context(h.ctx), finalizer); err == nil {
					t.Fatal("relational deletion failure was suppressed")
				}
				h.assertSnapshot(before)
				h.allowWrites(tc.table)
				h.finish(m)
				h.assertRows(map[string]int{"deletion_requests": 1, "deletion_outbox": 1, "deletion_audit_log": 2})
			})
		}
	})
	t.Run("finalizer-continuation-failure-rolls-back-chunk", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(0)
		h.exec(`INSERT INTO contact_methods(user_id,value) SELECT $1,'contact' FROM generate_series(1,500)`, m.UserID)
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		before := h.snapshot()
		h.failWrites("deletion_outbox", "INSERT", "")
		if err := h.store.Finalize(m.Context(h.ctx), h.command(m.RequestID, finalizeAction)); err == nil {
			t.Fatal("lost continuation was acknowledged")
		}
		h.assertSnapshot(before)
		h.allowWrites("deletion_outbox")
		h.finish(m)
		if h.count(`SELECT items_affected FROM deletion_audit_log WHERE stage='COMPLETED'`) != 503 {
			t.Fatal("rolled-back chunk inflated the completion count")
		}
	})
	t.Run("next-page-failure-rolls-back-acquisition", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(501)
		before := h.snapshot()
		h.failWrites("deletion_outbox", "INSERT", "")
		if err := h.svc.Purge(m.Context(h.ctx), h.command(m.RequestID, purgeAction)); err == nil {
			t.Fatal("failed next-page preparation was acknowledged")
		}
		h.assertSnapshot(before)
		if len(h.s3.calls) != 0 {
			t.Fatal("S3 ran before the next page and lease were committed")
		}
		h.allowWrites("deletion_outbox")
		h.finish(m)
	})
}

func testIntegrationBoundaries(t *testing.T, adminURL string) {
	t.Run("storage-validation-records-exact-errors", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(0)
		cases := []struct{ bucket, prefix, filename, code string }{
			{"test-bucket", "", "root.jpg", ""},
			{"test-bucket", "", strings.Repeat("a", 1024), ""},
			{"test-bucket", "", strings.Repeat("\u96ea", 341) + "a", ""},
			{"test-bucket", "../folder/", "literal%2Fy+.jpg", ""},
			{"test-bucket", "", "", "invalid_key"},
			{"test-bucket", "", strings.Repeat("b", 1025), "invalid_key"},
			{"test-bucket", "", strings.Repeat("\u96ea", 342), "invalid_key"},
			{"test-bucket", "", "control\x01.jpg", "unsupported_key_character"},
			{"test-bucket", "", "nested/name.jpg", "inconsistent_storage_identity"},
			{"test-bucket", "no-trailing-slash", "photo.jpg", "inconsistent_storage_identity"},
			{"test-bucket", "empty-filename/", "", "inconsistent_storage_identity"},
			{"Bad-Bucket", "", "bad-bucket.jpg", "invalid_bucket"},
			{"not-authorized", "", "unauthorized.jpg", "unauthorized_bucket"},
		}
		for _, tc := range cases {
			h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
				VALUES($1,$2,$3,$4,'original','image/jpeg',1)`, m.UserID, tc.bucket, tc.prefix, tc.filename)
		}
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		h.s3.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			if len(in.Delete.Objects) != 4 || aws.ToBool(in.Delete.Quiet) {
				t.Error("valid boundary keys must be sent with explicit confirmations")
			}
			for i, object := range in.Delete.Objects {
				if i >= 4 || aws.ToString(object.Key) != cases[i].prefix+cases[i].filename {
					t.Error("S3 key was changed, decoded, normalized, or invalid")
				}
			}
			return confirmed(in), nil
		}
		if err := h.svc.Purge(m.Context(h.ctx), h.command(m.RequestID, purgeAction)); err != nil {
			t.Fatal(err)
		}
		h.assertRows(map[string]int{
			"user_profiles": 1, "addresses": 1, "contact_methods": 1, "object_metadata": len(cases) - 4,
			"storage_object_identities": len(cases), "deletion_requests": 1, "deletion_batches": 1,
			"deletion_batch_items": len(cases), "deletion_outbox": 1, "deletion_audit_log": 1,
		})
		for _, tc := range cases[4:] {
			if h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='INVALID' AND attempts=1
				AND bucket=$1 AND prefix=$2 AND filename=$3 AND object_key=$2||$3 AND error_code=$4`,
				tc.bucket, tc.prefix, tc.filename, tc.code) != 1 {
				t.Errorf("missing INVALID evidence for %q: %s", tc.filename, tc.code)
			}
		}
		if h.count(`SELECT count(*) FROM deletion_batches WHERE state='BLOCKED' AND deleted_count=4 AND completed_at IS NULL`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='DELETED' AND bucket IS NULL AND object_key IS NULL
				AND prefix IS NULL AND filename IS NULL AND error_code IS NULL AND attempts=1`) != 4 {
			t.Fatal("invalid files blocked valid work or were certified deleted")
		}
	})
	t.Run("mixed-results-retry-only-transient-items", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(3)
		purge := h.command(m.RequestID, purgeAction)
		h.s3.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			return &s3.DeleteObjectsOutput{
				Deleted: []s3types.DeletedObject{{Key: in.Delete.Objects[0].Key}},
				Errors: []s3types.Error{
					{Key: in.Delete.Objects[1].Key, Code: aws.String("AccessDenied")},
					{Key: in.Delete.Objects[2].Key, Code: aws.String("SlowDown")},
				},
			}, nil
		}
		if err := h.svc.Purge(m.Context(h.ctx), purge); !errors.Is(err, errPending) {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='PENDING' AND error_code='SlowDown' AND attempts=1`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='BLOCKED' AND error_code='AccessDenied' AND attempts=1`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='DELETED' AND error_code IS NULL AND attempts=1`) != 1 {
			t.Fatal("mixed response was not durably partitioned")
		}
		h.s3.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			if len(in.Delete.Objects) != 1 || !strings.HasSuffix(aws.ToString(in.Delete.Objects[0].Key), "-3.jpg") {
				t.Error("retry included permanently failed or already deleted items")
			}
			return confirmed(in), nil
		}
		if err := h.svc.Purge(m.Context(h.ctx), purge); err != nil {
			t.Fatal(err)
		}
		h.assertRows(map[string]int{
			"user_profiles": 1, "addresses": 1, "contact_methods": 1, "object_metadata": 1,
			"storage_object_identities": 3, "deletion_requests": 1, "deletion_batches": 1,
			"deletion_batch_items": 3, "deletion_outbox": 1, "deletion_audit_log": 1,
		})
		if h.count(`SELECT count(*) FROM deletion_batches WHERE state='BLOCKED' AND deleted_count=2 AND lease_token IS NULL`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='DELETED' AND attempts=2`) != 1 {
			t.Fatal("retry lost permanent failure or accumulated progress")
		}
		before := h.snapshot()
		early := testMessage(finalizeAction)
		early.RequestID = m.RequestID
		if err := h.store.Finalize(m.Context(h.ctx), early); !errors.Is(err, errPending) {
			t.Fatal("blocked batch allowed finalization", err)
		}
		h.assertSnapshot(before)
	})
	t.Run("interleaved-subjects-and-cursor-gaps", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m, other := h.seed(0), h.seed(0)
		h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
			SELECT CASE WHEN n%2=0 THEN $1 ELSE $2 END,'test-bucket','p/',n||'.jpg','original','image/jpeg',1
			FROM generate_series(1,1002) n`, m.UserID, other.UserID)
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		h.finish(m)
		h.assertRows(map[string]int{
			"user_profiles": 1, "addresses": 1, "contact_methods": 1, "object_metadata": 501,
			"storage_object_identities": 1002, "deletion_requests": 1, "deletion_batches": 2,
			"deletion_batch_items": 501, "deletion_outbox": 3, "deletion_audit_log": 4,
		})
		if h.count(`SELECT count(*) FROM object_metadata WHERE user_id=$1 AND status='ACTIVE'`, other.UserID) != 501 ||
			h.count(`SELECT count(*) FROM user_profiles WHERE id=$1 AND status='ACTIVE'`, other.UserID) != 1 ||
			h.count(`SELECT count(*) FROM deletion_batch_items WHERE object_id%2<>0`) != 0 ||
			h.count(`SELECT count(*) FROM deletion_batches WHERE start_key=1000 AND next_key IS NULL AND deleted_count=1`) != 1 {
			t.Fatal("pagination skipped a gapped ID or crossed subject boundaries")
		}
	})
	t.Run("relational-chunk-boundaries", func(t *testing.T) {
		for _, tc := range []struct{ contacts, addresses int }{{0, 0}, {0, 500}, {499, 1}, {500, 1}, {0, 501}, {251, 250}} {
			t.Run(fmt.Sprintf("%d-contacts-%d-addresses", tc.contacts, tc.addresses), func(t *testing.T) {
				h := newHarness(t, adminURL)
				m := h.seed(0)
				h.exec(`DELETE FROM addresses; DELETE FROM contact_methods`)
				h.exec(`INSERT INTO contact_methods(user_id,value) SELECT $1,'contact' FROM generate_series(1,$2::integer)`, m.UserID, tc.contacts)
				h.exec(`INSERT INTO addresses(user_id,address_line_1,city,state,zip_code)
					SELECT $1,'address','city','state','code' FROM generate_series(1,$2::integer)`, m.UserID, tc.addresses)
				if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
					t.Fatal(err)
				}
				first := h.command(m.RequestID, finalizeAction)
				future := first
				future.Generation++
				beforeFuture := h.snapshot()
				if err := h.store.Finalize(m.Context(h.ctx), future); !errors.Is(err, errPending) {
					t.Fatal("future finalizer generation was accepted", err)
				}
				h.assertSnapshot(beforeFuture)
				if err := h.store.Finalize(m.Context(h.ctx), first); err != nil {
					t.Fatal(err)
				}
				total := tc.contacts + tc.addresses
				if h.count(`SELECT (SELECT count(*) FROM addresses)+(SELECT count(*) FROM contact_methods)`) != max(0, total-pageSize) {
					t.Fatal("finalizer did not obey the shared 500-child budget")
				}
				before := h.snapshot()
				if err := h.store.Finalize(m.Context(h.ctx), first); err != nil {
					t.Fatal(err)
				}
				h.assertSnapshot(before)
				h.finish(m)
				commands := max(1, (total+499)/500)
				h.assertRows(map[string]int{"deletion_requests": 1, "deletion_outbox": commands, "deletion_audit_log": 2})
				if h.count(`SELECT items_affected FROM deletion_audit_log WHERE stage='COMPLETED'`) != total+1 ||
					h.count(`SELECT count(*) FROM deletion_requests WHERE state='COMPLETED' AND user_id IS NULL
						AND completed_at IS NOT NULL AND db_deleted_count=$1`, total+1) != 1 {
					t.Fatal("completion counts or request scrubbing are incorrect")
				}
			})
		}

	})
}

func testIntegrationRecoveryStates(t *testing.T, adminURL string) {
	t.Run("expired-outbox-owner-cannot-finish-new-claim", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(1)
		first, err := h.svc.claimDispatch(h.ctx, "s3")
		if err != nil || first == nil {
			t.Fatalf("first claim: %+v %v", first, err)
		}
		h.exec(`UPDATE deletion_outbox SET lease_until=now()-interval '1 second'`)
		second, err := h.svc.claimDispatch(h.ctx, "s3")
		if err != nil || second == nil || first.token == second.token || first.id != second.id {
			t.Fatalf("reclaim: %+v %v", second, err)
		}
		before := h.snapshot()
		if err := h.svc.finishDispatch(h.ctx, *first, nil); err != nil {
			t.Fatal(err)
		}
		h.assertSnapshot(before)
		if err := h.svc.finishDispatch(h.ctx, *second, errors.New("private transport details")); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NULL AND attempts=2
					AND lease_token IS NULL AND lease_until IS NULL AND last_error_code='operation_failed'
					AND available_at>now()`) != 1 {
			t.Fatal("send failure did not persist sanitized error and retry schedule")
		}
		if next, err := h.svc.claimDispatch(h.ctx, "s3"); err != nil || next != nil {
			t.Fatalf("retry was eligible before backoff expired: %+v %v", next, err)
		}
		h.exec(`UPDATE deletion_outbox SET available_at=now()`)
		third, err := h.svc.claimDispatch(h.ctx, "s3")
		if err != nil || third == nil {
			t.Fatalf("third claim: %+v %v", third, err)
		}
		if err := h.svc.finishDispatch(h.ctx, *third, nil); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NOT NULL AND attempts=3
					AND lease_token IS NULL AND lease_until IS NULL AND last_error_code IS NULL`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_batches WHERE state='PENDING' AND deleted_count=0`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_audit_log WHERE stage='INITIATED'`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_requests WHERE request_id=$1 AND state='PENDING'`, m.RequestID) != 1 {
			t.Fatal("publication was mistaken for business completion")
		}
	})
	t.Run("reconciliation-failure-isolates-requests", func(t *testing.T) {
		h := newHarness(t, adminURL)
		bad, good := h.initiate(1), h.initiate(1)
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.exec(`UPDATE deletion_requests SET next_reconcile_at=now()-interval '1 minute';
					UPDATE deletion_batches SET last_dispatched_at=now()-interval '16 minutes'`)
		h.failWrites("deletion_outbox", "INSERT", fmt.Sprintf("WHEN (NEW.request_id='%s'::uuid)", bad.RequestID))
		if err := h.svc.Reconcile(h.ctx); err == nil {
			t.Fatal("recovery publication failure was suppressed")
		}
		h.assertRows(map[string]int{
			"user_profiles": 2, "addresses": 2, "contact_methods": 2, "object_metadata": 2,
			"storage_object_identities": 2, "deletion_requests": 2, "deletion_batches": 2,
			"deletion_batch_items": 2, "deletion_outbox": 3, "deletion_audit_log": 2,
		})
		if h.count(`SELECT count(*) FROM deletion_batches WHERE request_id=$1 AND recovery_count=0 AND state='PENDING'`, bad.RequestID) != 1 ||
			h.count(`SELECT count(*) FROM deletion_batches WHERE request_id=$1 AND recovery_count=1 AND state='PENDING'`, good.RequestID) != 1 ||
			h.count(`SELECT count(*) FROM deletion_outbox WHERE request_id=$1 AND sent_at IS NULL`, good.RequestID) != 1 ||
			h.count(`SELECT count(*) FROM deletion_outbox WHERE request_id=$1 AND sent_at IS NULL`, bad.RequestID) != 0 ||
			h.count(`SELECT count(*) FROM deletion_audit_log WHERE stage<>'INITIATED'`) != 0 {
			t.Fatal("failed recovery partially committed or blocked an unrelated request")
		}
		h.allowWrites("deletion_outbox")
		h.exec(`UPDATE deletion_requests SET next_reconcile_at=now()-interval '1 minute' WHERE request_id=$1`, bad.RequestID)
		if err := h.svc.Reconcile(h.ctx); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NULL`) != 2 {
			t.Fatal("recovery retry lost failed work or duplicated healthy work")
		}
	})
	t.Run("recovery-exhaustion-retains-unresolved-rows", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(1)
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.exec(`UPDATE deletion_requests SET next_reconcile_at=now()-interval '1 minute';
					UPDATE deletion_batches SET recovery_count=5,last_dispatched_at=now()-interval '16 minutes',
						lease_token=gen_random_uuid(),lease_until=now()-interval '1 second'`)
		if err := h.svc.Reconcile(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.assertUnresolved(m, "BLOCKED", "retry_exhausted", 0)
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NULL`) != 0 ||
			h.count(`SELECT recovery_count FROM deletion_batches`) != maxAttempts {
			t.Fatal("exhausted work was republished or its recovery evidence reset")
		}
	})
	t.Run("finalizer-recovery-is-bounded", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(0)
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.exec(`UPDATE deletion_requests SET next_reconcile_at=now()-interval '1 minute',
					finalizer_dispatched_at=now()-interval '16 minutes',recovery_count=5`)
		if err := h.svc.Reconcile(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.assertRows(map[string]int{
			"user_profiles": 1, "addresses": 1, "contact_methods": 1,
			"deletion_requests": 1, "deletion_outbox": 1, "deletion_audit_log": 1,
		})
		if h.count(`SELECT count(*) FROM deletion_requests WHERE request_id=$1 AND recovery_count=5
					AND state='PENDING' AND completed_at IS NULL AND user_id=$2 AND finalizer_generation=1`, m.RequestID, m.UserID) != 1 {
			t.Fatal("exhausted finalizer lost its durable pending state")
		}
	})
	t.Run("concurrent-replays-have-single-effects", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(1)
		runTwice := func(call func() error) {
			t.Helper()
			done := make(chan error, 2)
			for range 2 {
				go func() { done <- call() }()
			}
			for range 2 {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
		}
		runTwice(func() error { return h.store.Initiate(m.Context(h.ctx), m) })
		if err := h.svc.Purge(m.Context(h.ctx), h.command(m.RequestID, purgeAction)); err != nil {
			t.Fatal(err)
		}
		finalizer := h.command(m.RequestID, finalizeAction)
		runTwice(func() error { return h.store.Finalize(m.Context(h.ctx), finalizer) })
		h.assertRows(map[string]int{
			"storage_object_identities": 1, "deletion_requests": 1, "deletion_batches": 1,
			"deletion_batch_items": 1, "deletion_outbox": 2, "deletion_audit_log": 3,
		})
		if h.count(`SELECT items_affected FROM deletion_audit_log WHERE stage='COMPLETED'`) != 3 ||
			h.count(`SELECT items_affected FROM deletion_audit_log WHERE stage='BATCH_COMPLETED'`) != 1 {
			t.Fatal("concurrent replay duplicated counts")
		}
	})
	t.Run("unexpected-active-subject-blocks-deletion", func(t *testing.T) {
		for _, n := range []int{0, 1} {
			t.Run(fmt.Sprintf("%d-objects", n), func(t *testing.T) {
				h := newHarness(t, adminURL)
				m := h.initiate(n)
				h.exec(`UPDATE user_profiles SET status='ACTIVE' WHERE id=$1`, m.UserID)
				before := h.snapshot()
				var err error
				if n == 0 {
					err = h.store.Finalize(m.Context(h.ctx), h.command(m.RequestID, finalizeAction))
				} else {
					err = h.svc.Purge(m.Context(h.ctx), h.command(m.RequestID, purgeAction))
				}
				if !errors.Is(err, errPending) {
					t.Fatalf("unsafe subject lifecycle error=%v", err)
				}
				h.assertSnapshot(before)
				if len(h.s3.calls) != 0 {
					t.Fatal("unexpectedly active subject reached S3")
				}
			})
		}
	})
	t.Run("lambda-acks-commit-despite-publication-error", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(1)
		h.queue.send = func(*sqs.SendMessageInput) error { return errors.New("private queue transport failure") }
		body, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(events.SQSEvent{Records: []events.SQSMessage{{MessageId: "committed", Body: string(body)}}})
		if err != nil {
			t.Fatal(err)
		}
		out, err := h.svc.LambdaHandler("db")(h.ctx, raw)
		if err != nil || len(out.BatchItemFailures) != 0 {
			t.Fatalf("recoverable publication NACKed a committed command: %+v %v", out, err)
		}
		h.assertUnresolved(m, "PENDING", "", 0)
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NULL AND attempts=1
			AND last_error_code='operation_failed' AND available_at>now() AND lease_token IS NULL`) != 1 {
			t.Fatal("committed outbox did not retain the publication failure")
		}
		h.queue.send = nil
		h.exec(`UPDATE deletion_outbox SET available_at=now()`)
		if _, err := h.svc.LambdaHandler("reconciler")(h.ctx, json.RawMessage(`{"action":"RECONCILE"}`)); err != nil {
			t.Fatal(err)
		}
		if len(h.queue.snapshot()) != 1 || h.count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NOT NULL
			AND attempts=2 AND last_error_code IS NULL AND lease_token IS NULL`) != 1 {
			t.Fatal("scheduled maintenance did not recover publication")
		}
		h.assertRows(map[string]int{
			"user_profiles": 1, "addresses": 1, "contact_methods": 1, "object_metadata": 1,
			"storage_object_identities": 1, "deletion_requests": 1, "deletion_batches": 1,
			"deletion_batch_items": 1, "deletion_outbox": 1, "deletion_audit_log": 1,
		})
	})
	t.Run("lambda-mixed-errors-persist-only-valid-work", func(t *testing.T) {
		h := newHarness(t, adminURL)
		valid, missing, wrongQueue := h.seed(1), testMessage(initiateAction), testMessage(purgeAction)
		encode := func(m Message) string {
			t.Helper()
			body, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			return string(body)
		}
		raw, err := json.Marshal(events.SQSEvent{Records: []events.SQSMessage{
			{MessageId: "malformed", Body: `{"action":"INITIATE_DELETION"}`},
			{MessageId: "valid", Body: encode(valid)},
			{MessageId: "missing", Body: encode(missing)},
			{MessageId: "wrong-queue", Body: encode(wrongQueue)},
		}})
		if err != nil {
			t.Fatal(err)
		}
		response, err := h.svc.LambdaHandler("db")(h.ctx, raw)
		if err != nil || len(response.BatchItemFailures) != 3 {
			t.Fatalf("response=%+v error=%v", response, err)
		}
		for i, id := range []string{"malformed", "missing", "wrong-queue"} {
			if response.BatchItemFailures[i].ItemIdentifier != id {
				t.Errorf("failure %d=%q, want %q", i, response.BatchItemFailures[i].ItemIdentifier, id)
			}
		}
		h.assertRows(map[string]int{
			"user_profiles": 1, "addresses": 1, "contact_methods": 1, "object_metadata": 1,
			"storage_object_identities": 1, "deletion_requests": 1, "deletion_batches": 1,
			"deletion_batch_items": 1, "deletion_outbox": 1, "deletion_audit_log": 1,
		})
		if h.count(`SELECT count(*) FROM deletion_requests WHERE request_id=$1 AND state='PENDING'`, valid.RequestID) != 1 ||
			h.count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NOT NULL`) != 1 || len(h.queue.snapshot()) != 1 {
			t.Fatal("failed messages created workflow rows or blocked valid publication")
		}
	})
}
