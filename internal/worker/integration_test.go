package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type fakeS3 struct {
	mu         sync.Mutex
	calls      []string
	version    s3types.BucketVersioningStatus
	check      error
	versioning func(*s3.GetBucketVersioningInput) (*s3.GetBucketVersioningOutput, error)
	delete     func(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
}

func (f *fakeS3) GetBucketVersioning(_ context.Context, in *s3.GetBucketVersioningInput, _ ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	if f.versioning != nil {
		return f.versioning(in)
	}
	return &s3.GetBucketVersioningOutput{Status: f.version}, f.check
}

func confirmed(in *s3.DeleteObjectsInput) *s3.DeleteObjectsOutput {
	out := &s3.DeleteObjectsOutput{}
	for _, o := range in.Delete.Objects {
		out.Deleted = append(out.Deleted, s3types.DeletedObject{Key: o.Key})
	}
	return out
}

func (f *fakeS3) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	f.mu.Lock()
	f.calls = append(f.calls, aws.ToString(in.Bucket))
	f.mu.Unlock()
	if f.delete != nil {
		return f.delete(in)
	}
	return confirmed(in), nil
}

type sentMessage struct{ destination, body string }
type fakeQueue struct {
	mu       sync.Mutex
	messages []sentMessage
	send     func(*sqs.SendMessageInput) error
}

func (f *fakeQueue) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if f.send != nil {
		if err := f.send(in); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	f.messages = append(f.messages, sentMessage{aws.ToString(in.QueueUrl), aws.ToString(in.MessageBody)})
	f.mu.Unlock()
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeQueue) snapshot() []sentMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sentMessage(nil), f.messages...)
}

type harness struct {
	t     *testing.T
	ctx   context.Context
	owner *pgx.Conn
	store *Store
	svc   Service
	s3    *fakeS3
	queue *fakeQueue
}

func newHarness(t *testing.T, adminURL string) *harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	name := "deletion_test_" + strings.ReplaceAll(newUUID(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
		_ = admin.Close(ctx)
		cancel()
		t.Fatal(err)
	}
	h := &harness{t: t, ctx: ctx, s3: &fakeS3{}, queue: &fakeQueue{}}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		defer cancel()
		if h.store != nil {
			h.store.Close()
		}
		if h.owner != nil {
			_ = h.owner.Close(cleanup)
		}
		if _, err := admin.Exec(cleanup, `DROP DATABASE `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`); err != nil {
			t.Error(err)
		}
		_ = admin.Close(cleanup)
	})
	ownerURL, err := url.Parse(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	ownerURL.Path = "/" + name
	h.owner, err = pgx.Connect(ctx, ownerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"001_init.sql", "002_outbox_retention.sql"} {
		schema, err := os.ReadFile("../../migrations/" + file)
		if err != nil {
			t.Fatal(err)
		}
		h.exec(string(schema))
	}
	workerURL := *ownerURL
	workerURL.User = url.UserPassword("deletion_worker", "integration-worker-password")
	h.store, err = NewStore(ctx, workerURL.String(), strings.Repeat("integration-salt", 3))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.CheckPrivileges(ctx); err != nil {
		t.Fatal(err)
	}
	h.svc = Service{Store: h.store, S3: h.s3, Queue: h.queue, DBQueue: "db", S3Queue: "s3",
		Buckets: map[string]struct{}{"test-bucket": {}, "second-bucket": {}}}
	return h
}

func (h *harness) exec(query string, args ...any) {
	h.t.Helper()
	if _, err := h.owner.Exec(h.ctx, query, args...); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) count(query string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.owner.QueryRow(h.ctx, query, args...).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

func (h *harness) seed(n int) Message {
	h.t.Helper()
	m := testMessage(initiateAction)
	h.exec(`INSERT INTO user_profiles(id) VALUES($1)`, m.UserID)
	h.exec(`INSERT INTO addresses(user_id,address_line_1,city,state,zip_code) VALUES($1,'synthetic address','city','state','code')`, m.UserID)
	h.exec(`INSERT INTO contact_methods(user_id,value) VALUES($1,'synthetic contact')`, m.UserID)
	h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
		SELECT $1,'test-bucket','p/',$1||'-'||n||'.jpg','original.jpg','image/jpeg',1 FROM generate_series(1,$2::integer) n`, m.UserID, n)
	return m
}

func (h *harness) initiate(n int) Message {
	h.t.Helper()
	m := h.seed(n)
	if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
		h.t.Fatal(err)
	}
	return m
}

func (h *harness) command(requestID, action string) Message {
	h.t.Helper()
	var body []byte
	if err := h.owner.QueryRow(h.ctx, `SELECT payload FROM deletion_outbox WHERE request_id=$1 AND payload->>'action'=$2
		ORDER BY (payload->>'generation')::integer DESC NULLS LAST,created_at DESC,id DESC LIMIT 1`, requestID, action).Scan(&body); err != nil {
		h.t.Fatal(err)
	}
	m, err := decodeMessage(body)
	if err != nil {
		h.t.Fatal(err)
	}
	return m
}

func (h *harness) finish(m Message) {
	h.t.Helper()
	for range 30 {
		if h.count(`SELECT count(*) FROM deletion_requests WHERE request_id=$1 AND state='COMPLETED'`, m.RequestID) == 1 {
			return
		}
		var id string
		var start int64
		err := h.owner.QueryRow(h.ctx, `SELECT batch_id::text,start_key FROM deletion_batches
			WHERE request_id=$1 AND state='PENDING' ORDER BY start_key LIMIT 1`, m.RequestID).Scan(&id, &start)
		if err == nil {
			purge := testMessage(purgeAction)
			purge.RequestID, purge.BatchID, purge.StartKey = m.RequestID, id, &start
			if err := h.svc.Purge(m.Context(h.ctx), purge); err != nil {
				h.t.Fatal(err)
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			h.t.Fatal(err)
		}
		if err := h.store.Finalize(m.Context(h.ctx), h.command(m.RequestID, finalizeAction)); err != nil {
			h.t.Fatal(err)
		}
	}
	h.t.Fatal("workflow did not finish in bounded number of steps")
}

func TestIntegration(t *testing.T) {
	adminURL := os.Getenv("TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("TEST_DATABASE_URL must point to a disposable PostgreSQL admin connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	var roleExists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='deletion_worker')`).Scan(&roleExists); err != nil {
		t.Fatal(err)
	}
	if !roleExists {
		if _, err := admin.Exec(ctx, `CREATE ROLE deletion_worker LOGIN PASSWORD 'integration-worker-password'`); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if !roleExists {
			if _, err := admin.Exec(cleanup, `DROP ROLE deletion_worker`); err != nil {
				t.Error(err)
			}
		}
		_ = admin.Close(cleanup)
	})
	for _, n := range []int{0, 1, 499, 500, 501, 1001} {
		t.Run(fmt.Sprintf("pagination-%d", n), func(t *testing.T) {
			h := newHarness(t, adminURL)
			m := h.initiate(n)
			if got := h.count(`SELECT count(*) FROM deletion_batches WHERE request_id=$1`, m.RequestID); got != min(n, 1) {
				t.Fatalf("initial batches=%d; preparation must be bounded", got)
			}
			if n > 0 && h.count(`SELECT count(*) FROM deletion_outbox WHERE destination='db'`) != 0 {
				t.Fatal("premature finalizer")
			}
			h.finish(m)
			h.finish(m)
			if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
				t.Fatal("initiation replay failed", err)
			}
			if h.count(`SELECT count(*) FROM user_profiles WHERE id=$1`, m.UserID) != 0 ||
				h.count(`SELECT count(*) FROM deletion_batches WHERE state<>'COMPLETED'`) != 0 {
				t.Fatal("unfinished data retained")
			}
			if got := h.count(`SELECT count(*) FROM deletion_audit_log`); got != (n+499)/500+2 {
				t.Fatalf("audit entries=%d", got)
			}
			if got := h.count(`SELECT COALESCE(sum(items_affected),0) FROM deletion_audit_log WHERE stage='BATCH_COMPLETED'`); got != n {
				t.Fatalf("deleted object count=%d", got)
			}
			if h.count(`SELECT count(*) FROM deletion_audit_log WHERE initiator_user_hash<>$1`, m.InitiatorUserHash) != 0 ||
				h.count(`SELECT count(*) FROM deletion_batch_items WHERE bucket IS NOT NULL OR object_key IS NOT NULL`) != 0 {
				t.Fatal("actor lost or completed snapshots not scrubbed")
			}
			if h.count(`SELECT count(*) FROM storage_object_identities`) != n {
				t.Fatal("identity retirement history lost")
			}
		})
	}
	t.Run("bucket-grouping-and-identity", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(0)
		for _, bucket := range []string{"test-bucket", "second-bucket"} {
			h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
				VALUES($1,$2,'same/','name.jpg','original.jpg','image/jpeg',1)`, m.UserID, bucket)
		}
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		h.finish(m)
		if len(h.s3.calls) != 2 || h.s3.calls[0] == h.s3.calls[1] {
			t.Fatal("bulk requests not grouped by bucket")
		}
		other := h.seed(0)
		_, err := h.owner.Exec(h.ctx, `INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
			VALUES($1,'test-bucket','same/','name.jpg','original.jpg','image/jpeg',1)`, other.UserID)
		if err == nil {
			t.Fatal("reused a completed storage identity")
		}
	})
	t.Run("last-page-cannot-finalize-before-earlier-page", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(501)
		first := h.command(m.RequestID, purgeAction)
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var once sync.Once
		h.s3.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			early := false
			once.Do(func() { early = true; close(entered) })
			if early {
				<-release
			}
			return confirmed(in), nil
		}
		done := make(chan error, 1)
		go func() { done <- h.svc.Purge(m.Context(h.ctx), first) }()
		select {
		case <-entered:
		case <-h.ctx.Done():
			t.Fatal("first page did not reach S3")
		}
		last := h.command(m.RequestID, purgeAction)
		if last.BatchID == first.BatchID || last.StartKey == nil || *last.StartKey == 0 {
			t.Fatal("second page was not durably prepared")
		}
		if err := h.svc.Purge(m.Context(h.ctx), last); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_requests WHERE enumeration_complete`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_outbox WHERE destination='db'`) != 0 {
			t.Fatal("end of pagination incorrectly certified outstanding work")
		}
		unblock()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		h.finish(m)
	})
	t.Run("bucket-checkpoint-survives-later-cancellation", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(0)
		for _, bucket := range []string{"test-bucket", "second-bucket"} {
			h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
				VALUES($1,$2,'','photo.jpg','original','image/jpeg',1)`, m.UserID, bucket)
		}
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		workCtx, cancel := context.WithCancel(m.Context(h.ctx))
		defer cancel()
		calls := 0
		h.s3.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			calls++
			if calls == 2 {
				cancel()
				return nil, context.Canceled
			}
			return confirmed(in), nil
		}
		if err := h.svc.Purge(workCtx, h.command(m.RequestID, purgeAction)); err == nil {
			t.Fatal("cancellation was ignored")
		}
		if h.count(`SELECT count(*) FROM object_metadata`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='DELETED'`) != 1 {
			t.Fatal("successful earlier bucket checkpoint rolled back")
		}
		h.exec(`UPDATE deletion_batches SET lease_until=now()-interval '1 second'`)
		h.s3.delete = nil
		h.finish(m)
		if h.count(`SELECT items_affected FROM deletion_audit_log WHERE stage='BATCH_COMPLETED'`) != 2 {
			t.Fatal("checkpoint recovery lost accumulated counts")
		}
	})
	t.Run("bucket-work-is-bounded-with-durable-continuation", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(0)
		for i := range 11 {
			bucket := fmt.Sprintf("allowed-bucket-%02d", i)
			h.svc.Buckets[bucket] = struct{}{}
			h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
				VALUES($1,$2,'','photo.jpg','original','image/jpeg',1)`, m.UserID, bucket)
		}
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		if err := h.svc.Purge(m.Context(h.ctx), h.command(m.RequestID, purgeAction)); err != nil {
			t.Fatal("normal bounded continuation treated as delivery failure", err)
		}
		if len(h.s3.calls) != 10 || h.count(`SELECT count(*) FROM object_metadata`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_outbox WHERE destination='s3'`) != 2 {
			t.Fatal("unattempted group was not durably continued")
		}
		h.finish(m)
	})
	t.Run("invalid-items-are-durable-and-do-not-stop-valid-work", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(1)
		for i, bucket := range []string{"Bad-Bucket", "not-authorized", "test-bucket"} {
			file := fmt.Sprintf("invalid-%d.jpg", i)
			if i == 2 {
				file = strings.Repeat("a", 1025)
			}
			h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
				VALUES($1,$2,'',$3,'original','image/jpeg',1)`, m.UserID, bucket, file)
		}
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		if err := h.svc.Purge(m.Context(h.ctx), h.command(m.RequestID, purgeAction)); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='INVALID' AND error_code IS NOT NULL`) != 3 ||
			h.count(`SELECT count(*) FROM object_metadata`) != 3 || len(h.s3.calls) != 1 {
			t.Fatal("invalid files lost or valid work not completed")
		}
		if h.count(`SELECT count(*) FROM deletion_batches WHERE state='BLOCKED'`) != 1 ||
			h.count(`SELECT count(*) FROM deletion_outbox WHERE destination='db'`) != 0 {
			t.Fatal("unresolved files incorrectly certified complete")
		}
	})
	t.Run("partial-confirmations-and-retries", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(3)
		purge := h.command(m.RequestID, purgeAction)
		h.s3.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			probe, stop := context.WithTimeout(h.ctx, time.Second)
			defer stop()
			if _, err := h.owner.Exec(probe, `UPDATE user_profiles SET status=status WHERE id=$1`, m.UserID); err != nil {
				return nil, fmt.Errorf("S3 call held database lock: %w", err)
			}
			return &s3.DeleteObjectsOutput{
				Deleted: []s3types.DeletedObject{{Key: in.Delete.Objects[0].Key}, {Key: in.Delete.Objects[1].Key}},
				Errors:  []s3types.Error{{Key: in.Delete.Objects[1].Key, Code: aws.String("SlowDown")}},
			}, nil
		}
		if err := h.svc.Purge(m.Context(h.ctx), purge); !errors.Is(err, errPending) {
			t.Fatal("partial result did not remain pending", err)
		}
		if h.count(`SELECT count(*) FROM object_metadata`) != 2 {
			t.Fatal("unconfirmed metadata removed")
		}
		h.s3.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			if len(in.Delete.Objects) != 2 {
				t.Error("retry should include only unfinished items")
			}
			return confirmed(in), nil
		}
		h.finish(m)
		if h.count(`SELECT items_affected FROM deletion_audit_log WHERE stage='BATCH_COMPLETED'`) != 3 {
			t.Fatal("retry overcounted or lost confirmations")
		}
	})
	t.Run("expired-worker-and-key-reuse", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(1)
		purge := h.command(m.RequestID, purgeAction)
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var once sync.Once
		h.s3.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			first := false
			once.Do(func() { first = true; close(entered) })
			if first {
				<-release
			}
			return confirmed(in), nil
		}
		done := make(chan error, 1)
		go func() { done <- h.svc.Purge(m.Context(h.ctx), purge) }()
		select {
		case <-entered:
		case <-h.ctx.Done():
			t.Fatal("first worker did not reach S3")
		}
		if err := h.svc.Purge(m.Context(h.ctx), purge); !errors.Is(err, errPending) {
			t.Fatal("active lease was bypassed", err)
		}
		h.exec(`UPDATE deletion_batches SET lease_until=now()-interval '1 second'`)
		if err := h.svc.Purge(m.Context(h.ctx), purge); err != nil {
			t.Fatal(err)
		}
		other := h.seed(0)
		_, err := h.owner.Exec(h.ctx, `INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
			VALUES($1,'test-bucket','p/',$2,'original','image/jpeg',1)`, other.UserID, m.UserID+"-1.jpg")
		if err == nil {
			t.Fatal("retired key was reused while stale S3 work remained in flight")
		}
		h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
			VALUES($1,'test-bucket','p/',$2,'original','image/jpeg',1)`, other.UserID, newUUID()+".jpg")
		unblock()
		if err := <-done; !errors.Is(err, errPending) {
			t.Fatal("stale worker recorded results with expired claim", err)
		}
		if h.count(`SELECT count(*) FROM object_metadata WHERE user_id=$1`, other.UserID) != 1 ||
			h.count(`SELECT count(*) FROM deletion_audit_log WHERE stage='BATCH_COMPLETED'`) != 1 {
			t.Fatal("stale work damaged new generation or duplicated audit")
		}
	})
	t.Run("bounded-finalization-and-ownership", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.seed(0)
		h.exec(`INSERT INTO contact_methods(user_id,value) SELECT $1,'contact' FROM generate_series(1,1000)`, m.UserID)
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		wrong := m
		wrong.InitiatorUserHash = userHash("other", "actor")
		if err := h.store.Initiate(m.Context(h.ctx), wrong); !errors.Is(err, errInvalid) {
			t.Fatal("actor mismatch accepted")
		}
		finalizer := h.command(m.RequestID, finalizeAction)
		if err := h.store.Finalize(m.Context(h.ctx), finalizer); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM contact_methods`) != 501 || h.count(`SELECT count(*) FROM user_profiles`) != 1 {
			t.Fatal("finalization did not stop at 500 child rows")
		}
		if err := h.store.Finalize(m.Context(h.ctx), finalizer); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM contact_methods`) != 501 {
			t.Fatal("stale generation deleted another chunk")
		}
		h.finish(m)
		if h.count(`SELECT items_affected FROM deletion_audit_log WHERE stage='COMPLETED'`) != 1003 {
			t.Fatal("incorrect accumulated relational count")
		}
	})
	t.Run("transport-retries-and-batch-command-validation", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(1)
		purge := h.command(m.RequestID, purgeAction)
		wrong := purge
		wrong.InitiatorUserHash = userHash("other", "actor")
		if err := h.svc.Purge(m.Context(h.ctx), wrong); !errors.Is(err, errInvalid) {
			t.Fatal("wrong actor accepted")
		}
		wrong = purge
		cursor := int64(9999)
		wrong.StartKey = &cursor
		if err := h.svc.Purge(m.Context(h.ctx), wrong); !errors.Is(err, errInvalid) {
			t.Fatal("untrusted cursor accepted")
		}
		early := testMessage(finalizeAction)
		early.RequestID = m.RequestID
		if err := h.store.Finalize(m.Context(h.ctx), early); !errors.Is(err, errPending) {
			t.Fatal("early finalization accepted")
		}
		h.s3.delete = func(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			return nil, errors.New("injected transport failure")
		}
		for i := 1; i <= maxAttempts; i++ {
			err := h.svc.Purge(m.Context(h.ctx), purge)
			if i < maxAttempts && !errors.Is(err, errPending) || i == maxAttempts && err != nil {
				t.Fatalf("attempt %d: %v", i, err)
			}
		}
		if h.count(`SELECT count(*) FROM deletion_batch_items WHERE state='BLOCKED' AND attempts=5`) != 1 ||
			h.count(`SELECT count(*) FROM object_metadata`) != 1 {
			t.Fatal("failed work was lost or retried without a bound")
		}
		if err := h.svc.Purge(m.Context(h.ctx), purge); err != nil || len(h.s3.calls) != 5 {
			t.Fatal("blocked batch was retried automatically", err)
		}
	})
	t.Run("outbox-leases-failure-isolation-and-replay", func(t *testing.T) {
		h := newHarness(t, adminURL)
		h.initiate(1)
		zero := h.initiate(0)
		h.queue.send = func(in *sqs.SendMessageInput) error {
			if aws.ToString(in.QueueUrl) == "db" {
				return errors.New("injected send failure")
			}
			return nil
		}
		if err := h.svc.Flush(h.ctx); err == nil {
			t.Fatal("publication error suppressed")
		}
		if len(h.queue.snapshot()) != 1 || h.queue.snapshot()[0].destination != "s3" {
			t.Fatal("failing destination blocked another destination")
		}
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE destination='db' AND sent_at IS NULL AND attempts=1 AND available_at>now()`) != 1 {
			t.Fatal("backoff or durable retry missing")
		}
		h.queue.send = nil
		h.exec(`UPDATE deletion_outbox SET available_at=now() WHERE sent_at IS NULL`)
		h.exec(`CREATE FUNCTION fail_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
			IF NEW.sent_at IS NOT NULL THEN RAISE EXCEPTION 'injected mark failure'; END IF; RETURN NEW; END; $$;
			CREATE TRIGGER fail_mark BEFORE UPDATE ON deletion_outbox FOR EACH ROW EXECUTE FUNCTION fail_mark()`)
		if err := h.svc.Flush(h.ctx); err == nil {
			t.Fatal("mark failure not surfaced")
		}
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NULL AND lease_token IS NOT NULL`) != 1 {
			t.Fatal("failed mark lost dispatch")
		}
		h.exec(`DROP TRIGGER fail_mark ON deletion_outbox; DROP FUNCTION fail_mark();
			UPDATE deletion_outbox SET lease_until=now()-interval '1 second' WHERE sent_at IS NULL`)
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		messages := h.queue.snapshot()
		if len(messages) != 3 {
			t.Fatal("send-before-mark recovery did not replay")
		}
		var first, second Message
		_ = json.Unmarshal([]byte(messages[1].body), &first)
		_ = json.Unmarshal([]byte(messages[2].body), &second)
		if first.MessageID != second.MessageID || first.RequestID != zero.RequestID {
			t.Fatal("logical command identity changed during replay")
		}
	})
	t.Run("concurrent-dispatchers-claim-once", func(t *testing.T) {
		h := newHarness(t, adminURL)
		h.initiate(0)
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var once sync.Once
		h.queue.send = func(*sqs.SendMessageInput) error {
			once.Do(func() { close(entered) })
			<-release
			return nil
		}
		done := make(chan error, 1)
		go func() { done <- h.svc.Flush(h.ctx) }()
		select {
		case <-entered:
		case <-h.ctx.Done():
			t.Fatal("dispatcher did not reach SQS")
		}
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		unblock()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if len(h.queue.snapshot()) != 1 {
			t.Fatal("concurrent dispatchers duplicated an actively claimed row")
		}
	})
	t.Run("reconciler-recovers-published-unfinished-work", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(1)
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.exec(`UPDATE deletion_requests SET next_reconcile_at=now()-interval '1 minute';
			UPDATE deletion_batches SET last_dispatched_at=now()-interval '16 minutes'`)
		if err := h.svc.Reconcile(h.ctx); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE batch_id IS NOT NULL AND sent_at IS NULL`) != 1 {
			t.Fatal("published but uncompleted operation not recovered")
		}
		h.exec(`UPDATE deletion_requests SET next_reconcile_at=now()-interval '1 minute'`)
		if err := h.svc.Reconcile(h.ctx); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE batch_id IS NOT NULL AND sent_at IS NULL`) != 1 {
			t.Fatal("reconciliation duplicated an outstanding dispatch")
		}
		h.finish(m)
	})
	t.Run("reconciler-finalizer-and-exhausted-page", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(0)
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.exec(`UPDATE deletion_requests SET next_reconcile_at=now()-interval '1 minute',
			finalizer_dispatched_at=now()-interval '16 minutes'`)
		if err := h.svc.Reconcile(h.ctx); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE request_id=$1 AND sent_at IS NULL`, m.RequestID) != 1 {
			t.Fatal("lost finalizer not recovered")
		}
		large := h.initiate(501)
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.exec(`UPDATE deletion_requests SET next_reconcile_at=now()-interval '1 minute' WHERE request_id=$1`, large.RequestID)
		h.exec(`UPDATE deletion_batches SET recovery_count=5,last_dispatched_at=now()-interval '16 minutes' WHERE request_id=$1`, large.RequestID)
		if err := h.svc.Reconcile(h.ctx); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_batches WHERE request_id=$1`, large.RequestID) != 2 ||
			h.count(`SELECT count(*) FROM deletion_batches WHERE request_id=$1 AND state='BLOCKED'`, large.RequestID) != 1 {
			t.Fatal("exhausted page stopped enumeration or was retried forever")
		}
	})
	t.Run("reconciler-respects-active-processing-lease", func(t *testing.T) {
		h := newHarness(t, adminURL)
		h.initiate(1)
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.exec(`UPDATE deletion_requests SET next_reconcile_at=now()-interval '1 minute';
			UPDATE deletion_batches SET last_dispatched_at=now()-interval '16 minutes',
				lease_token=gen_random_uuid(),lease_until=now()+interval '1 minute'`)
		if err := h.svc.Reconcile(h.ctx); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NULL`) != 0 {
			t.Fatal("reconciler bypassed active worker lease")
		}
	})
	t.Run("bounded-dispatch-and-retention", func(t *testing.T) {
		h := newHarness(t, adminURL)
		for range 21 {
			h.initiate(0)
		}
		if err := h.svc.Flush(h.ctx); err != nil {
			t.Fatal(err)
		}
		if len(h.queue.snapshot()) != 10 {
			t.Fatal("dispatch did not honor per-destination bound")
		}
		h.exec(`UPDATE deletion_outbox SET sent_at=now()-interval '31 days'`)
		h.exec(`INSERT INTO deletion_outbox(id,request_id,destination,payload,sent_at)
			SELECT gen_random_uuid(),request_id,destination,payload,now()-interval '31 days'
			FROM deletion_outbox CROSS JOIN generate_series(1,24)`)
		if err := h.svc.pruneOutbox(h.ctx); err != nil {
			t.Fatal(err)
		}
		if h.count(`SELECT count(*) FROM deletion_outbox`) != 25 {
			t.Fatal("retention cleanup was not bounded to 500 rows")
		}
	})
	t.Run("guards-and-append-only-ledger", func(t *testing.T) {
		h := newHarness(t, adminURL)
		m := h.initiate(1)
		for _, query := range []string{
			`UPDATE deletion_audit_log SET stage=stage`, `DELETE FROM deletion_audit_log`, `TRUNCATE deletion_audit_log`,
			`DELETE FROM storage_object_identities`, `UPDATE storage_object_identities SET identity_hash=identity_hash`,
		} {
			if _, err := h.owner.Exec(h.ctx, query); err == nil {
				t.Fatalf("append-only guard bypassed: %s", query)
			}
		}
		for _, statement := range []string{
			`INSERT INTO contact_methods(user_id,value) VALUES($1,'late')`,
			`INSERT INTO addresses(user_id,address_line_1,city,state,zip_code) VALUES($1,'late','city','state','code')`,
			`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
				VALUES($1,'test-bucket','','late.jpg','original','image/jpeg',1)`,
		} {
			_, err := h.owner.Exec(h.ctx, statement, m.UserID)
			var pgErr interface{ SQLState() string }
			if !errors.As(err, &pgErr) || pgErr.SQLState() != "P0001" || !strings.Contains(err.Error(), "unavailable") {
				t.Fatalf("expected write guard, got %v", err)
			}
		}
		if _, err := h.owner.Exec(h.ctx, `UPDATE object_metadata SET bucket='second-bucket' WHERE user_id=$1`, m.UserID); err == nil {
			t.Fatal("storage identity was mutable")
		}
		for _, query := range []string{
			`UPDATE deletion_audit_log SET stage=stage`, `TRUNCATE deletion_audit_log`,
			`ALTER TABLE deletion_audit_log DISABLE TRIGGER ALL`, `DELETE FROM storage_object_identities`,
		} {
			if _, err := h.store.pool.Exec(h.ctx, query); err == nil {
				t.Fatal("runtime bypassed restricted grants")
			}
		}
	})
	t.Run("traced-envelope-and-lambda-partial-response", func(t *testing.T) {
		h := newHarness(t, adminURL)
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		previous := otel.GetTracerProvider()
		otel.SetTracerProvider(provider)
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(previous) })
		m := h.seed(0)
		body, _ := json.Marshal(m)
		raw, _ := json.Marshal(events.SQSEvent{Records: []events.SQSMessage{
			{MessageId: "good", Body: string(body)}, {MessageId: "bad", Body: `{"action":"INITIATE_DELETION"}`},
		}})
		response, err := h.svc.LambdaHandler("db")(h.ctx, raw)
		if err != nil || len(response.BatchItemFailures) != 1 || response.BatchItemFailures[0].ItemIdentifier != "bad" {
			t.Fatalf("wrong partial response: %+v %v", response, err)
		}
		messages := h.queue.snapshot()
		if len(messages) != 1 {
			t.Fatal("finalizer not published")
		}
		child, err := decodeMessage([]byte(messages[0].body))
		if err != nil || child.InitiatorUserHash != m.InitiatorUserHash {
			t.Fatal("strict actor envelope not preserved", err)
		}
		parentSC := trace.SpanContextFromContext(m.Context(h.ctx))
		childSC := trace.SpanContextFromContext(child.Context(h.ctx))
		if parentSC.TraceID() != childSC.TraceID() || parentSC.SpanID() == childSC.SpanID() {
			t.Fatal("trace did not propagate with fresh producer span")
		}
		found := false
		for _, span := range exporter.GetSpans() {
			if span.Name == "sqs.publish" && span.SpanContext.SpanID() == childSC.SpanID() {
				found = true
			}
		}
		if !found {
			t.Fatal("outgoing message did not carry its producer span context")
		}
	})
	t.Run("state-contracts", func(t *testing.T) {
		testIntegrationStates(t, adminURL)
	})
	if endpoint := os.Getenv("TEST_AWS_ENDPOINT_URL"); endpoint != "" {
		t.Run("real-localstack-round-trip", func(t *testing.T) {
			testLocalStack(t, adminURL, endpoint)
		})
	}
}

func testLocalStack(t *testing.T, adminURL, endpoint string) {
	h := newHarness(t, adminURL)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	cfg, err := config.LoadDefaultConfig(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	objects := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
	queue := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
	bucket := "deletion-test-" + newUUID()
	if _, err := objects.CreateBucket(h.ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		list, err := objects.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if err == nil {
			for _, o := range list.Contents {
				_, err = objects.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: o.Key})
				if err != nil {
					t.Error(err)
				}
			}
		}
		if _, err := objects.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Error(err)
		}
	})
	createQueue := func(suffix string) string {
		out, err := queue.CreateQueue(h.ctx, &sqs.CreateQueueInput{QueueName: aws.String(bucket + suffix)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := queue.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl}); err != nil {
				t.Error(err)
			}
		})
		return aws.ToString(out.QueueUrl)
	}
	h.svc.DBQueue, h.svc.S3Queue = createQueue("-db"), createQueue("-s3")
	h.svc.Queue, h.svc.S3 = queue, objects
	h.svc.Buckets = map[string]struct{}{bucket: {}}
	m := h.seed(0)
	key := "p/" + newUUID() + ".jpg"
	h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
		VALUES($1,$2,'p/',$3,'original','image/jpeg',7)`, m.UserID, bucket, strings.TrimPrefix(key, "p/"))
	h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
		VALUES($1,$2,'absent/',$3,'original','image/jpeg',0)`, m.UserID, bucket, newUUID()+".jpg")
	if _, err := objects.PutObject(h.ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("private")}); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(m)
	if _, err := queue.SendMessage(h.ctx, &sqs.SendMessageInput{QueueUrl: aws.String(h.svc.DBQueue), MessageBody: aws.String(string(body))}); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct{ queue, kind string }{
		{h.svc.DBQueue, "db"}, {h.svc.S3Queue, "s3"}, {h.svc.DBQueue, "db"},
	} {
		received, err := queue.ReceiveMessage(h.ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(step.queue), MaxNumberOfMessages: 1, WaitTimeSeconds: 10})
		if err != nil || len(received.Messages) != 1 {
			t.Fatalf("receive %s: %+v %v", step.kind, received, err)
		}
		record := received.Messages[0]
		event, _ := json.Marshal(events.SQSEvent{Records: []events.SQSMessage{{MessageId: aws.ToString(record.MessageId), Body: aws.ToString(record.Body)}}})
		out, err := h.svc.LambdaHandler(step.kind)(h.ctx, event)
		if err != nil || len(out.BatchItemFailures) != 0 {
			t.Fatalf("handle %s: %+v %v", step.kind, out, err)
		}
		if _, err := queue.DeleteMessage(h.ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(step.queue), ReceiptHandle: record.ReceiptHandle}); err != nil {
			t.Fatal(err)
		}
	}
	list, err := objects.ListObjectsV2(h.ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil || len(list.Contents) != 0 || h.count(`SELECT count(*) FROM user_profiles`) != 0 ||
		h.count(`SELECT count(*) FROM deletion_audit_log`) != 3 ||
		h.count(`SELECT items_affected FROM deletion_audit_log WHERE stage='BATCH_COMPLETED'`) != 2 {
		t.Fatal("round-trip did not delete tracked data with audit evidence", err)
	}
	t.Run("missing-bucket-retains-unresolved-evidence", func(t *testing.T) {
		h := newHarness(t, adminURL)
		missing := "missing-" + newUUID()
		h.svc.S3 = objects
		h.svc.Buckets[missing] = struct{}{}
		m := h.seed(0)
		h.exec(`INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
			VALUES($1,$2,'','missing.jpg','original','image/jpeg',1)`, m.UserID, missing)
		if err := h.store.Initiate(m.Context(h.ctx), m); err != nil {
			t.Fatal(err)
		}
		if err := h.svc.Purge(m.Context(h.ctx), h.command(m.RequestID, purgeAction)); err != nil {
			t.Fatal(err)
		}
		h.assertUnresolved(m, "BLOCKED", "NoSuchBucket", 1)
	})
	for _, status := range []s3types.BucketVersioningStatus{s3types.BucketVersioningStatusEnabled, s3types.BucketVersioningStatusSuspended} {
		if _, err := objects.PutBucketVersioning(h.ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(bucket),
			VersioningConfiguration: &s3types.VersioningConfiguration{Status: status}}); err != nil {
			t.Fatal(err)
		}
		if err := h.svc.CheckBucket(h.ctx, bucket); err == nil {
			t.Fatal("unsafe versioned bucket accepted")
		}
	}
}
