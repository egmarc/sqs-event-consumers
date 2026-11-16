package worker

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
)

type fakeS3 struct {
	calls   int
	version s3types.BucketVersioningStatus
	delete  func(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
}

func (f *fakeS3) GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	return &s3.GetBucketVersioningOutput{Status: f.version}, nil
}
func (f *fakeS3) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	f.calls++
	if f.delete != nil {
		return f.delete(in)
	}
	out := &s3.DeleteObjectsOutput{}
	for _, o := range in.Delete.Objects {
		out.Deleted = append(out.Deleted, s3types.DeletedObject{Key: o.Key})
	}
	return out, nil
}

type fakeQueue struct {
	fail   bool
	bodies []string
}

func (f *fakeQueue) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if f.fail {
		return nil, errors.New("injected send failure")
	}
	f.bodies = append(f.bodies, aws.ToString(in.MessageBody))
	return &sqs.SendMessageOutput{}, nil
}

func TestIntegration(t *testing.T) {
	adminURL := os.Getenv("TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("TEST_DATABASE_URL is not set (must be a disposable PostgreSQL admin connection)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	dbName := "deletion_test_" + strings.ReplaceAll(newUUID(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err := admin.Exec(context.Background(), `DROP DATABASE `+pgx.Identifier{dbName}.Sanitize()+` WITH (FORCE)`)
		if err != nil {
			t.Error(err)
		}
	}()
	if _, err = admin.Exec(ctx, `DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='deletion_worker') THEN CREATE ROLE deletion_worker LOGIN PASSWORD 'integration-worker-password'; END IF; END $$`); err != nil {
		t.Fatal(err)
	}
	ownerURL, err := url.Parse(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	ownerURL.Path = "/" + dbName
	owner, err := pgx.Connect(ctx, ownerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx)
	schema, err := os.ReadFile("../../migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = owner.Exec(ctx, string(schema)); err != nil {
		t.Fatal(err)
	}
	workerURL := *ownerURL
	workerURL.User = url.UserPassword("deletion_worker", "integration-worker-password")
	store := Store{URL: workerURL.String(), Salt: strings.Repeat("integration-salt", 3)}
	if err = store.CheckPrivileges(ctx); err != nil {
		t.Fatal(err)
	}
	seed := func(user string, n int) {
		t.Helper()
		if _, err := owner.Exec(ctx, `INSERT INTO users(id) VALUES($1)`, user); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.Exec(ctx, `INSERT INTO address(user_id,address) VALUES($1,'private address')`, user); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.Exec(ctx, `INSERT INTO contact_methods(user_id,value) VALUES($1,'private contact')`, user); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.Exec(ctx, `INSERT INTO object_storage(user_id,object_key) SELECT $1,$1||'/'||n FROM generate_series(1,$2::integer) n`, user, n); err != nil {
			t.Fatal(err)
		}
	}
	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		if err := owner.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	batch := func(request string) string {
		t.Helper()
		var id string
		if err := owner.QueryRow(ctx, `SELECT batch_id::text FROM deletion_batches WHERE request_id=$1 ORDER BY batch_id LIMIT 1`, request).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	t.Run("chunking-idempotency-ownership", func(t *testing.T) {
		seed("chunk-user", 1001)
		m := Message{Type: "INITIATE_DELETION", RequestID: newUUID(), UserID: "chunk-user"}
		if err := store.Initiate(ctx, m); err != nil {
			t.Fatal(err)
		}
		if err := store.Initiate(ctx, m); err != nil {
			t.Fatal(err)
		}
		if n := count(`SELECT count(*) FROM deletion_batches WHERE request_id=$1`, m.RequestID); n != 2 {
			t.Fatalf("batches=%d", n)
		}
		if n := count(`SELECT max(cardinality(keys)) FROM deletion_batches WHERE request_id=$1`, m.RequestID); n != 1000 {
			t.Fatal("wrong chunk size")
		}
		if n := count(`SELECT count(*) FROM deletion_audit_log WHERE deletion_request_id=$1`, m.RequestID); n != 1 {
			t.Fatal("duplicate initiation ledger")
		}
		m.UserID = "other-user"
		if err := store.Initiate(ctx, m); err == nil {
			t.Fatal("request ownership bypass")
		}
		if err := store.Finalize(ctx, Message{RequestID: m.RequestID, UserID: "chunk-user"}); err == nil {
			t.Fatal("premature finalization")
		}
	})
	t.Run("row-lock-and-multibatch-single-finalizer", func(t *testing.T) {
		seed("locked-user", 0)
		tx, err := owner.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SELECT 1 FROM users WHERE id='locked-user' FOR UPDATE`); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- store.Initiate(ctx, Message{RequestID: newUUID(), UserID: "locked-user"}) }()
		select {
		case err := <-done:
			t.Fatalf("initiation bypassed row lock: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		var request string
		if err := owner.QueryRow(ctx, `SELECT request_id::text FROM deletion_requests WHERE user_id='chunk-user'`).Scan(&request); err != nil {
			t.Fatal(err)
		}
		rows, err := owner.Query(ctx, `SELECT batch_id::text FROM deletion_batches WHERE request_id=$1`, request)
		if err != nil {
			t.Fatal(err)
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		done = make(chan error, len(ids))
		for _, id := range ids {
			go func(id string) {
				svc := Service{Store: store, S3: &fakeS3{}, Bucket: "test"}
				done <- svc.Purge(ctx, Message{RequestID: request, BatchID: id})
			}(id)
		}
		for range ids {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		if n := count(`SELECT count(*) FROM deletion_outbox WHERE payload->>'deletion_request_id'=$1 AND destination='db'`, request); n != 1 {
			t.Fatal("multiple finalizers queued")
		}
		if n := count(`SELECT sum(items_affected) FROM deletion_audit_log WHERE deletion_request_id=$1 AND stage='BATCH_COMPLETED'`, request); n != 1001 {
			t.Fatal("inaccurate batch total")
		}
	})
	t.Run("partial-confirmations-canonical-keys-counts", func(t *testing.T) {
		seed("partial-user", 3)
		seed("unrelated-user", 1)
		m := Message{RequestID: newUUID(), UserID: "partial-user"}
		if err := store.Initiate(ctx, m); err != nil {
			t.Fatal(err)
		}
		fake := &fakeS3{}
		fake.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			if aws.ToBool(in.Delete.Quiet) || len(in.Delete.Objects) != 3 {
				t.Fatal("wrong deletion request")
			}
			for _, o := range in.Delete.Objects {
				if !strings.HasPrefix(aws.ToString(o.Key), "partial-user/") {
					t.Fatal("noncanonical key")
				}
			}
			return &s3.DeleteObjectsOutput{
				Deleted: []s3types.DeletedObject{{Key: aws.String("partial-user/1")}, {Key: aws.String("unrelated-user/1")}, {Key: aws.String("partial-user/2")}},
				Errors:  []s3types.Error{{Key: aws.String("partial-user/2"), Code: aws.String("AccessDenied")}},
			}, nil
		}
		svc := Service{Store: store, S3: fake, Bucket: "test", Queue: &fakeQueue{}}
		purge := Message{RequestID: m.RequestID, BatchID: batch(m.RequestID)}
		untrusted := fmt.Sprintf(`{"action":"PURGE_S3_BATCH","deletion_request_id":%q,"batch_id":%q,"user_id":"unrelated-user","keys":["unrelated-user/1"],"bucket":"attacker-bucket"}`, m.RequestID, purge.BatchID)
		if err := svc.Handle(ctx, "s3", []byte(untrusted)); !errors.Is(err, errPending) {
			t.Fatalf("expected partial retry, got %v", err)
		}
		if n := count(`SELECT count(*) FROM object_storage WHERE user_id='partial-user'`); n != 2 {
			t.Fatalf("remaining=%d", n)
		}
		if n := count(`SELECT count(*) FROM object_storage WHERE user_id='unrelated-user'`); n != 1 {
			t.Fatal("deleted other user's metadata")
		}
		if n := count(`SELECT count(*) FROM deletion_audit_log WHERE deletion_request_id=$1 AND stage='BATCH_COMPLETED'`, m.RequestID); n != 0 {
			t.Fatal("premature completion")
		}
		if _, err := owner.Exec(ctx, `INSERT INTO object_storage(user_id,object_key) VALUES('unrelated-user','partial-user/1')`); err == nil {
			t.Fatal("pending canonical key reused")
		}
		fake.delete = nil
		if err := svc.Purge(ctx, purge); err != nil {
			t.Fatal(err)
		}
		if err := svc.Purge(ctx, purge); err != nil {
			t.Fatal(err)
		}
		if fake.calls != 2 {
			t.Fatal("completed batch replay made S3 call")
		}
		if n := count(`SELECT items_affected FROM deletion_audit_log WHERE batch_id=$1`, purge.BatchID); n != 3 {
			t.Fatalf("unique deleted count=%d", n)
		}
		if err := store.Finalize(ctx, m); err != nil {
			t.Fatal(err)
		}
		if err := store.Finalize(ctx, m); err != nil {
			t.Fatal(err)
		}
		if n := count(`SELECT count(*) FROM deletion_audit_log WHERE deletion_request_id=$1 AND stage='COMPLETED'`, m.RequestID); n != 1 {
			t.Fatal("duplicate completion")
		}
		if n := count(`SELECT count(*) FROM users WHERE id='partial-user'`); n != 0 {
			t.Fatal("user retained")
		}
		if n := count(`SELECT count(*) FROM (
			SELECT row_to_json(r)::text AS data FROM deletion_requests r WHERE request_id=$1
			UNION ALL SELECT row_to_json(b)::text FROM deletion_batches b WHERE request_id=$1
			UNION ALL SELECT row_to_json(o)::text FROM deletion_outbox o WHERE payload->>'deletion_request_id'=$1::text
			UNION ALL SELECT row_to_json(a)::text FROM deletion_audit_log a WHERE deletion_request_id=$1
		) retained WHERE data LIKE '%partial-user%'`, m.RequestID); n != 0 {
			t.Fatal("raw user or filename retained")
		}
		if err := store.Finalize(ctx, Message{RequestID: m.RequestID}); err != nil {
			t.Fatal("request-only finalizer replay failed", err)
		}
		if n := count(`SELECT items_affected FROM deletion_audit_log WHERE deletion_request_id=$1 AND stage='COMPLETED'`, m.RequestID); n != 3 {
			t.Fatal("incorrect relational deletion count")
		}
	})
	t.Run("transport-missing-confirmation-unexpected-error", func(t *testing.T) {
		seed("retry-user", 2)
		m := Message{RequestID: newUUID(), UserID: "retry-user"}
		if err := store.Initiate(ctx, m); err != nil {
			t.Fatal(err)
		}
		fake := &fakeS3{delete: func(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			return nil, errors.New("injected transport failure")
		}}
		svc := Service{Store: store, S3: fake, Bucket: "test"}
		purge := Message{RequestID: m.RequestID, BatchID: batch(m.RequestID)}
		if err := svc.Purge(ctx, purge); err == nil {
			t.Fatal("transport failure accepted")
		}
		if n := count(`SELECT count(*) FROM object_storage WHERE user_id='retry-user'`); n != 2 {
			t.Fatal("transport failure removed metadata")
		}
		fake.delete = func(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			return &s3.DeleteObjectsOutput{Deleted: []s3types.DeletedObject{{Key: aws.String("retry-user/1")}}}, nil
		}
		if err := svc.Purge(ctx, purge); !errors.Is(err, errPending) {
			t.Fatal("missing confirmation accepted", err)
		}
		fake.delete = func(in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			out := &s3.DeleteObjectsOutput{Errors: []s3types.Error{{Key: aws.String("unexpected-key")}}}
			for _, o := range in.Delete.Objects {
				out.Deleted = append(out.Deleted, s3types.DeletedObject{Key: o.Key})
			}
			return out, nil
		}
		if err := svc.Purge(ctx, purge); !errors.Is(err, errPending) {
			t.Fatal("unexpected error accepted", err)
		}
		fake.delete = nil
		if err := svc.Purge(ctx, purge); err != nil {
			t.Fatal(err)
		}
		if n := count(`SELECT items_affected FROM deletion_audit_log WHERE batch_id=$1`, purge.BatchID); n != 2 {
			t.Fatal("retry overcounted")
		}
	})
	t.Run("outbox-send-failure-and-replay", func(t *testing.T) {
		seed("outbox-user", 0)
		queue := &fakeQueue{fail: true}
		svc := Service{Store: store, Queue: queue}
		m := Message{Type: "INITIATE_DELETION", RequestID: newUUID(), UserID: "outbox-user"}
		body := fmt.Sprintf(`{"action":"INITIATE_DELETION","deletion_request_id":%q,"user_id":%q}`, m.RequestID, m.UserID)
		if err := svc.Handle(ctx, "db", []byte(body)); err == nil {
			t.Fatal("send failure acknowledged")
		}
		if n := count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NULL`); n == 0 {
			t.Fatal("outbox message lost")
		}
		queue.fail = false
		if _, err := svc.LambdaHandler("db")(ctx, []byte(`{"action":"FLUSH_OUTBOX"}`)); err != nil {
			t.Fatal(err)
		}
		if n := count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NULL`); n != 0 {
			t.Fatal("outbox not drained")
		}
		before := len(queue.bodies)
		if err := svc.Flush(ctx); err != nil || len(queue.bodies) != before {
			t.Fatal("sent messages replayed")
		}
		if err := store.Finalize(ctx, m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("outbox-send-before-mark-replays-after-rollback", func(t *testing.T) {
		seed("outbox-rollback-user", 0)
		m := Message{RequestID: newUUID(), UserID: "outbox-rollback-user"}
		if err := store.Initiate(ctx, m); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.Exec(ctx, `CREATE FUNCTION fail_outbox_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected mark failure'; END; $$;
			CREATE TRIGGER fail_mark BEFORE UPDATE ON deletion_outbox FOR EACH STATEMENT EXECUTE FUNCTION fail_outbox_mark()`); err != nil {
			t.Fatal(err)
		}
		queue := &fakeQueue{}
		svc := Service{Store: store, Queue: queue}
		if err := svc.Flush(ctx); err == nil {
			t.Fatal("mark failure accepted")
		}
		if len(queue.bodies) != 1 {
			t.Fatal("send did not precede mark")
		}
		first := queue.bodies[0]
		if n := count(`SELECT count(*) FROM deletion_outbox WHERE sent_at IS NULL`); n == 0 {
			t.Fatal("rollback lost dispatch")
		}
		if _, err := owner.Exec(ctx, `DROP TRIGGER fail_mark ON deletion_outbox; DROP FUNCTION fail_outbox_mark()`); err != nil {
			t.Fatal(err)
		}
		if err := svc.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if len(queue.bodies) < 2 || queue.bodies[1] != first {
			t.Fatal("uncommitted dispatch was not safely replayed")
		}
	})
	t.Run("concurrent-initiation-and-finalization", func(t *testing.T) {
		seed("concurrent-user", 0)
		m := Message{RequestID: newUUID(), UserID: "concurrent-user"}
		run := func(fn func() error) {
			var wg sync.WaitGroup
			errs := make(chan error, 8)
			for range 8 {
				wg.Add(1)
				go func() { defer wg.Done(); errs <- fn() }()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		run(func() error { return store.Initiate(ctx, m) })
		if n := count(`SELECT count(*) FROM deletion_outbox WHERE payload->>'deletion_request_id'=$1`, m.RequestID); n != 1 {
			t.Fatal("duplicate finalizer")
		}
		run(func() error { return store.Finalize(ctx, m) })
		if n := count(`SELECT count(*) FROM deletion_audit_log WHERE deletion_request_id=$1`, m.RequestID); n != 2 {
			t.Fatal("duplicate ledger events")
		}
	})
	t.Run("append-only-privileges-and-pending-writes", func(t *testing.T) {
		for _, query := range []string{`UPDATE deletion_audit_log SET stage=stage`, `DELETE FROM deletion_audit_log`, `TRUNCATE deletion_audit_log`} {
			if _, err := owner.Exec(ctx, query); err == nil {
				t.Fatal("owner bypassed trigger")
			}
		}
		worker, err := pgx.Connect(ctx, store.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer worker.Close(ctx)
		for _, query := range []string{`UPDATE deletion_audit_log SET stage=stage`, `DELETE FROM deletion_audit_log`, `TRUNCATE deletion_audit_log`, `ALTER TABLE deletion_audit_log DISABLE TRIGGER ALL`} {
			if _, err := worker.Exec(ctx, query); err == nil {
				t.Fatal("worker bypassed ledger restrictions")
			}
		}
		if _, err := owner.Exec(ctx, `INSERT INTO object_storage(user_id,object_key) VALUES('chunk-user','late-key')`); err == nil {
			t.Fatal("pending user accepted metadata")
		}
		unsafe := store
		unsafe.URL = ownerURL.String()
		if err := unsafe.CheckPrivileges(ctx); err == nil {
			t.Fatal("accepted schema owner")
		}
	})
	if endpoint := os.Getenv("TEST_AWS_ENDPOINT_URL"); endpoint != "" {
		t.Run("real-localstack-round-trip", func(t *testing.T) {
			t.Setenv("AWS_ACCESS_KEY_ID", "test")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
			t.Setenv("AWS_REGION", "us-east-1")
			cfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
			queue := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
			name := "deletion-test-" + newUUID()
			if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
				t.Fatal(err)
			}
			defer s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
			dbQ, err := queue.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name + "-db")})
			if err != nil {
				t.Fatal(err)
			}
			defer queue.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: dbQ.QueueUrl})
			s3Q, err := queue.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name + "-s3")})
			if err != nil {
				t.Fatal(err)
			}
			defer queue.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: s3Q.QueueUrl})
			seed("real-user", 2)
			for i := 1; i <= 2; i++ {
				if _, err := s3Client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(name), Key: aws.String(fmt.Sprintf("real-user/%d", i)), Body: strings.NewReader("private")}); err != nil {
					t.Fatal(err)
				}
			}
			svc := Service{Store: store, S3: s3Client, Queue: queue, Bucket: name, DBQueue: aws.ToString(dbQ.QueueUrl), S3Queue: aws.ToString(s3Q.QueueUrl)}
			m := Message{RequestID: newUUID(), UserID: "real-user"}
			if err := store.Initiate(ctx, m); err != nil {
				t.Fatal(err)
			}
			// Earlier cases intentionally leave messages pending; all are safe to publish.
			if err := svc.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			received, err := queue.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: s3Q.QueueUrl, MaxNumberOfMessages: 10})
			if err != nil || len(received.Messages) == 0 {
				t.Fatal("outbox did not reach SQS", err)
			}
			if err := svc.Purge(ctx, Message{RequestID: m.RequestID, BatchID: batch(m.RequestID)}); err != nil {
				t.Fatal(err)
			}
			if err := svc.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := store.Finalize(ctx, m); err != nil {
				t.Fatal(err)
			}
			objects, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(name)})
			if err != nil || len(objects.Contents) != 0 {
				t.Fatal("S3 objects remain", err)
			}
			if _, err := s3Client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(name), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
				t.Fatal(err)
			}
			if err := svc.CheckBucket(ctx); err == nil {
				t.Fatal("versioned bucket accepted")
			}
			if _, err := s3Client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(name), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusSuspended}}); err != nil {
				t.Fatal(err)
			}
			if err := svc.CheckBucket(ctx); err == nil {
				t.Fatal("suspended bucket accepted")
			}
		})
	}
}
