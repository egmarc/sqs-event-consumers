# SQS event consumers

Go monorepo for two AWS Lambda consumers implementing a data-deletion workflow:

```text
API → DB SQS → db-worker → transactional outbox → S3 SQS → s3-worker
                  ↑                                      │
                  └──────── finalization via DB SQS ──────┘
                      PostgreSQL append-only audit ledger
```

Both standard queues have independent dead-letter queues. PostgreSQL is the source
of truth for request ownership, batches, progress, and dispatch. SQS delivery is
at-least-once; effects and audit entries are idempotent, not “exactly once.”

## Layout

- `cmd/db-worker`, `cmd/s3-worker`: Lambda entry points and local polling mode.
- `internal/worker`: shared orchestration, PostgreSQL, AWS adapters, and colocated
  Go unit/integration tests.
- `migrations/001_init.sql`: example application tables, orchestration tables,
  append-only `deletion_audit_log`, write guards, and restricted runtime grants.
- `local`: development-only PostgreSQL/LocalStack initialization.
- `Dockerfile`, `docker-compose.yml`: local workers, PostgreSQL, and LocalStack.
- `template.yaml`: SAM deployment for Lambda, SQS/DLQs, replay schedule, and alarms.

The application tables are an example, not a migration for an existing product.
Adapt foreign keys and deletion order to your actual schema before deployment.

## Workflow and safety

1. Send `INITIATE_DELETION` with a generated UUID `deletion_request_id` and
   `user_id` to the DB queue. The worker locks the user, changes their status to
   `PENDING_DELETION`, and records `INITIATED` with a salted SHA-256 identifier.
2. In the same transaction, it snapshots `object_storage` into batches of at most
   1,000 keys and writes dispatch records to an outbox. S3 messages reference
   persisted batch IDs instead of carrying filenames. Workers never trust
   caller-supplied keys or a caller-supplied bucket.
3. The S3 worker loads the canonical batch and calls `DeleteObjects` with
   `Quiet=false`. Only positively confirmed `Deleted` keys lose their metadata;
   errors and missing confirmations cause a retry. Counts reflect distinct
   metadata rows removed, including across partial retries.
4. A completed batch records `BATCH_COMPLETED`. Serialized completion checks and
   a durable finalizer flag enqueue one finalization once there are no objects
   **and** no unfinished batches. A zero-file user takes this path immediately.
5. Finalization checks readiness again, then atomically deletes
   `contact_methods → address → users`, clears transient identifying data, and
   appends `COMPLETED`. Replayed messages cannot certify an unfinished deletion.

Outbox sending and marking are deliberately ordered send-before-mark, with AWS
calls outside database transactions; a crash or concurrent flush can duplicate
a message but cannot silently lose a committed dispatch. Consumers are
idempotent. Sent outbox rows are pruned after 30 days. A one-minute DB-worker
schedule replays unsent entries even after the inbound message reaches its DLQ.
Local workers also replay periodically. Each worker process uses a PostgreSQL
pool capped at two connections. Database locks serialize work for the same
user; different users can progress concurrently.

### Compliance boundaries

- The ledger contains request/batch UUIDs, a salted user-ID hash, stages, counts,
  and timestamps—not names, emails, user IDs, or filenames. Completed
  orchestration records also scrub raw identifiers and key snapshots.
- Salted hashes are **pseudonymous**, not a blanket guarantee of legal
  anonymization. Protect the salt, keep it stable across retries, and define a
  reviewed retention and rotation policy. Queue messages/DLQs still contain the
  initiating user ID; restrict access, avoid payload logging, and handle their
  retention as personal data.
- The runtime must connect as the non-owner `deletion_worker` role. Startup
  rejects unsafe privileges. SQL triggers reject ledger updates, deletes, and
  truncation. A database owner/superuser can still bypass these controls; use
  separate administrator credentials and export to an immutable compliance
  store (for example S3 Object Lock) if your threat model requires it.
- Only **never-versioned S3 buckets** are supported. Workers fail closed for
  enabled or suspended versioning: deleting a key is not proof that historical
  versions were erased. Do not enable versioning during processing. Replicas,
  Object Lock, backups, logs, caches, search indexes, and external systems need
  separate deletion/retention policies; `COMPLETED` certifies only this schema
  and configured bucket, not an organization's entire data footprint.
- Setting user status is not itself session revocation. Your API/auth layer
  must reject pending users, revoke sessions, and prevent uploads, including
  previously issued upload URLs/in-flight writes. Database triggers prevent new
  tracked relational/object metadata writes after initiation; they cannot stop
  an independent S3 uploader. Quiesce uploads before beginning deletion.
- Treat the queues as privileged commands: only a trusted, authorized producer
  should be able to initiate deletion. UUIDs are idempotency keys, not auth.

## Run locally

Requirements: Docker with Compose. Local credentials and audit salt below are
deliberately public development values in `.env.example`; copy that file to
`.env` to start, and never reuse those values outside this stack. Compose reads
`.env` automatically, and `.env` is ignored by Git.

```sh
cp .env.example .env
docker compose up --build -d
docker compose ps
docker compose logs db-worker s3-worker
```

PostgreSQL and AWS resources initialize automatically on first startup.
Workers run as a non-root user and poll their respective queues. LocalStack is
bound only to `127.0.0.1:4566`; PostgreSQL is not exposed to the host.

Seed a synthetic user, relational data, and one object:

```sh
docker compose exec -T postgres psql -U postgres -d deletion <<'SQL'
INSERT INTO users(id) VALUES ('demo-user');
INSERT INTO address(user_id, address) VALUES ('demo-user', 'synthetic address');
INSERT INTO contact_methods(user_id, value) VALUES ('demo-user', 'synthetic contact');
INSERT INTO object_storage(user_id, object_key) VALUES ('demo-user', 'demo/file.txt');
SQL
docker compose exec -T localstack sh -c 'printf synthetic >/tmp/demo-object'
docker compose exec -T localstack awslocal s3api put-object \
  --bucket deletion-objects --key demo/file.txt --body /tmp/demo-object
```

Initiate deletion (use a new UUID for each new request):

```sh
docker compose exec -T localstack awslocal sqs send-message \
  --queue-url http://localstack:4566/queue/us-east-1/000000000000/db-deletion-queue \
  --message-body '{"action":"INITIATE_DELETION","deletion_request_id":"9ccb8491-74ed-4bd0-8ca0-4d462c3733ac","user_id":"demo-user"}'
```

After processing, inspect proof without selecting identifying application data:

```sh
docker compose exec -T postgres psql -U postgres -d deletion \
  -c "SELECT deletion_request_id, user_id_hash, stage, items_affected, created_at FROM deletion_audit_log ORDER BY created_at;"
docker compose exec -T postgres psql -U postgres -d deletion \
  -c "SELECT EXISTS(SELECT 1 FROM users WHERE id='demo-user') AS user_remains;"
docker compose exec -T localstack awslocal s3api list-objects-v2 --bucket deletion-objects
```

Resending the same initiation is safe. For a zero-file example, seed another user
without object metadata and send a different request UUID. Inspect both DLQs
when processing fails; do not treat queue acknowledgement as deletion proof.

```sh
docker compose down -v
```

This destroys local data. Run it when resetting the example schema; initialization
scripts do not migrate an existing PostgreSQL volume.

## Development

Use Go 1.25 or newer:

```sh
go mod download
make test
make vet
make build
sam validate --lint
```

Go tests live beside their packages, not in separate top-level unit directories.
The fast default test run runs unit tests and skips the service-dependent
integration test. CI runs both the default suite and the integration suite
against disposable PostgreSQL and LocalStack services:

```sh
docker compose -f docker-compose.integration.yml up -d --wait postgres localstack
TEST_DATABASE_URL="postgres://postgres:integration-owner@127.0.0.1:5432/postgres?sslmode=disable" \
TEST_AWS_ENDPOINT_URL=http://127.0.0.1:4566 \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
go test -count=1 -run '^TestIntegration$' -v ./internal/worker
docker compose -f docker-compose.integration.yml down --volumes --remove-orphans
```

The LocalStack round-trip test sends and consumes real SQS messages, runs each
worker handler through initiation, S3 batch deletion, and database finalization,
and verifies the PostgreSQL, audit-ledger, and S3 results. The service-dependent
integration test is opt-in outside CI:

```sh
TEST_DATABASE_URL="$DISPOSABLE_POSTGRES_ADMIN_URL" \
TEST_AWS_ENDPOINT_URL=http://localhost:4566 \
go test -count=1 -run TestIntegration -v ./internal/worker
```

Use an isolated, **disposable PostgreSQL server** with an administrator URL:
tests create/drop a temporary database and create a cluster-wide
`deletion_worker` login with a fixed test password if one does not already
exist. A test-created role is dropped afterward; an existing role is left
untouched and must accept that password. Do not point them at your application
or production cluster. Omit `TEST_AWS_ENDPOINT_URL` to test database
behavior with fake AWS clients only; set it to a disposable LocalStack endpoint
to include real SQS/S3 round trips. Integration AWS resources use unique names.

### Configuration

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | PostgreSQL URI or pgx connection string for `deletion_worker`; require TLS in production |
| `AUDIT_SALT` | Stable secret, at least 32 bytes, used for SHA-256 user pseudonyms |
| `S3_BUCKET` | One trusted, never-versioned bucket |
| `DB_QUEUE_URL`, `S3_QUEUE_URL` | Respective queue URLs |
| `AWS_REGION` | AWS SDK region |
| `APP_MODE` | `local` for polling; unset or `lambda` for Lambda runtime |
| `AWS_ENDPOINT_URL` | LocalStack endpoint; leave unset in AWS |

AWS credentials use the SDK default provider chain (execution roles in AWS).
Do not put production credentials or salts in this repository.
For local development, set Compose inputs in the ignored `.env` file; the
committed `.env.example` contains disposable local-only defaults. For AWS,
`template.yaml` maps protected SAM/CloudFormation parameters into Lambda
environment variables; `DatabaseURL` and `AuditSalt` are `NoEcho`. Use separate
secret values and deployment parameters for sandbox and production. Lambda
environment variables are configured on the function, rather than in ECS task
definitions.

## Deploy to AWS

1. Provision PostgreSQL and a dedicated never-versioned bucket. Create a
   non-owner login named `deletion_worker` through your secret-management
   process, **before** applying `migrations/001_init.sql` and
   `migrations/002_outbox_retention.sql` in order as a separate schema owner.
   The migration grants the runtime only its required table/sequence access.
   Never run workers as the migration owner.
2. Build with `sam build` (Go 1.25+ and Make), then `sam deploy --guided`.
   Supply the restricted database connection string and secret salt through a
   protected deployment process. Both parameters are `NoEcho`; avoid shell
   history and committing `samconfig.toml` containing secrets.
3. Supply private subnet/security-group IDs with database access and NAT or
   appropriate SQS/S3 endpoints. Keep PostgreSQL private and require TLS.
4. Supply `AlarmTopicARN` for an existing SNS topic with confirmed
   on-call/compliance subscriptions; both DLQ alarms publish to it. Verify the
   topic policy permits CloudWatch alarm delivery. Grant your trusted API
   producer permission to send only to the DB queue. Monitor outbox age and
   pending requests as well as Lambda failures and DLQ depth.

The SAM template sets DB reserved concurrency to 5 (SQS maximum 2, leaving
capacity for replay) and S3 reserved concurrency to 75 (SQS maximum 50).
Both functions time out after 60 seconds; queues have 360-second visibility,
partial batch failure responses, five attempts before redrive, one-day source
retention, and fourteen-day DLQ retention. Tune these limits and database
capacity with measured load; this starter does not promise a particular deletion
throughput or certify legal compliance.

Optional container deployment targets are `db-lambda` and `s3-lambda` in the
Dockerfile; Compose uses its `local` target.
