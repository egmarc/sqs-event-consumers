# SQS event consumers

Go monorepo with two SQS consumers and a scheduled recovery Lambda:

```text
API -> DB SQS -> db-worker -> PostgreSQL outbox -> S3 SQS -> s3-worker
                    ^                                         |
                    +----------- finalization via DB SQS ------+
                                  ^
                 EventBridge Scheduler -> reconciler
```

PostgreSQL owns workflow state and the append-only audit ledger. Each standard
queue has its own DLQ. Delivery is at-least-once; database effects and audit
events are idempotent. There is no distributed ACID transaction spanning
PostgreSQL, SQS, and S3. Transactions atomically persist local effects and outgoing
intent; an outbox and reconciliation recover interruptions.

## Layout

- `cmd/db-worker`: `INITIATE_DELETION` and `FINALIZE_DB_DELETION`.
- `cmd/s3-worker`: `PURGE_S3_BATCH`.
- `cmd/reconciler`: scheduled recovery, bounded dispatch, retention, health metrics.
- `internal/worker`: shared contracts, validation, PostgreSQL, AWS, and tracing.
- `migrations`: disposable example schema and runtime grants.
- `template.yaml`: SAM zip deployments, scoped IAM, Scheduler, queues, and alarms.
- `telemetry/collector.yaml`: ADOT Collector OTLP-to-X-Ray configuration.
- `local`, Compose files, and `Dockerfile`: isolated local infrastructure.

This is an example application schema, not an upgrade migration for an existing
product. These schema changes require recreating disposable local/test databases.
Adapt foreign keys and deletion order before integrating an existing schema.

## Workflow

1. The authorized API sends `INITIATE_DELETION`. A transaction locks the user,
   marks `user_profiles.status=PENDING_DELETION`, records the initiator and
   `INITIATED` audit event, and snapshots **at most 500** objects into a persisted
   batch and per-object items. Its outgoing command is written in the same
   transaction. A user with no object metadata goes directly to finalization.
2. Pagination uses immutable `object_id` keyset cursors and the
   `(user_id, object_id)` index, never `OFFSET`. `object_metadata.object_key` is
   generated from `prefix || filename`; `(bucket, object_key)` is unique.
   The owning application chooses names, normally GUIDs plus extensions.
3. Before deleting a batch, the S3 worker obtains a two-minute recoverable lease
   and durably prepares at most the next 500-item page. Enumeration can continue
   even when an earlier page has unresolved files. Duplicate cursor commands
   cannot create duplicate pages. A message's `start_key` must match its
   canonical stored batch; caller-supplied filenames and buckets are rejected.
4. The worker validates each object and groups supported targets by bucket.
   Each invocation handles at most ten bucket groups, with bounded AWS calls
   and database checkpoints after each group. Successful checkpoints survive
   later invocation failures. Unattempted groups receive a durable continuation
   rather than consuming the SQS failure budget for normal bounded work.
   Only explicit `Deleted` confirmations remove
   metadata. An error overrides a contradictory confirmation; missing
   confirmations and transport failures remain retryable.
5. Invalid/unauthorized objects become durable `INVALID` items. Permanent S3
   failures become `BLOCKED`; retryable per-item failures have five attempts.
   Valid files continue, including files in other buckets/pages. Unresolved
   items retain their snapshot and sanitized error code; they **never** count
   as completed. Retries target only unfinished items.
6. Finalization requires **enumeration complete, no unfinished/blocked batches,
   and no remaining object metadata**. A null next cursor alone is not proof:
   other batches may still be executing or failed.
7. DB finalization removes at most 500 child rows per invocation, in
   `contact_methods -> addresses` order. A durable generation number deduplicates
   continuation messages. The final transaction deletes `user_profiles`, scrubs
   the request's user ID, and atomically records `COMPLETED` and accumulated
   relational deletion counts.

Batch snapshots scrub bucket/key/name fields after confirmed deletion. Audit
counts reflect distinct metadata rows actually removed, not retry attempts.
Request ownership and initiator hashes must match on replays.

### Object identity and application responsibilities

The deletion workers do not choose filenames, buckets, or prefixes. The
application must allocate new physical keys for new uploads/ownership
generations and must not overwrite previously allocated addresses.

The schema makes ownership, object ID, bucket, prefix, and filename immutable.
A database trigger atomically registers each allocated `(bucket, key)`
fingerprint in `storage_object_identities`; the append-only registry outlives
deleted metadata. Reusing the address is rejected even after deletion completes.
This prevents a stale in-flight S3 delete from damaging a later tracked object.
Moves/replacements require new storage identities, not updates to old locations.
The registry stores SHA-256 fingerprints, not raw filenames, but fingerprints
remain pseudonymous retention data, not a legal anonymization guarantee.

Your uploader must enforce the same rule for S3 writes, including presigned
URLs. Database constraints cannot stop an independent S3 uploader from
overwriting an old key. Quiesce in-flight uploads before initiating deletion.

The API/auth layer owns user-state transitions, session revocation, and
preventing reactivation/new writes during deletion. These consumers do not
implement that lifecycle. Existing database guards reject tracked child writes
for pending users; workers also refuse to operate on an unexpectedly active
user. A violated lifecycle contract blocks progress and requires investigation.

### Authorized buckets and validation

`ALLOWED_S3_BUCKETS` is a comma-separated allowlist. SAM derives both this value
and bucket-scoped S3 IAM resources from the same `AllowedBuckets` parameter.
DB and reconciler roles do not need S3 access. A valid-looking bucket name in
metadata does not authorize deletion.

Typed `Message`, `Object`, and `Settings` contracts have explicit `Validate`
methods. Validation includes action-specific fields, UUIDs, initiator hash,
W3C trace context, bucket naming/authorization, nonempty keys up to 1,024 UTF-8
bytes, XML-compatible characters, and prefix/filename consistency. Keys are not
URL-decoded or path-normalized. No GUID filename policy is imposed by consumers.
An empty prefix supports a root-level object.

Only **never-versioned general-purpose S3 buckets** are supported. Each targeted
bucket is checked before deletion; enabled or suspended versioning fails closed
for its files without stopping other buckets. Do not enable versioning while
processing. Historical versions, Object Lock, replicas, backups, logs, caches,
search indexes, and external services need separate policies. `COMPLETED`
certifies only the tracked schema and supported authorized storage targets.

## Message envelope and tracing

Legacy messages without the full envelope are rejected. `action`, not `type`,
is the command discriminator. Unknown fields and trailing JSON are rejected.

```json
{
  "version": 1,
  "message_id": "bc8d9832-7b6f-4f3e-b723-ce29d63b2c85",
  "action": "INITIATE_DELETION",
  "deletion_request_id": "9ccb8491-74ed-4bd0-8ca0-4d462c3733ac",
  "user_id": "demo-user",
  "initiator_user_hash": "<64 lowercase hexadecimal characters>",
  "traceparent": "<API producer's W3C version-00 traceparent>",
  "tracestate": ""
}
```

The API must derive the initiator hash from the authenticated actor, not trust
an arbitrary hash supplied by its caller. Use a reviewed, stable pseudonymization
scheme. The actor is distinct from the user being deleted. Queues are privileged
commands; only trusted producers should have permission to publish. UUIDs and
hashes are not authentication.

Internal purge messages omit `user_id` and include persisted `batch_id` and a
nonnegative `start_key` (zero for the first page). Finalizer messages instead
carry a positive `generation`. The initiator hash is preserved across every
dispatch and included in all audit events.

OpenTelemetry extracts W3C context from the envelope, creates a fresh consumer
span per attempt, and creates a fresh producer span on each actual SQS
publication. The outgoing `traceparent` carries that producer span ID; it
implicitly supplies the next consumer's parent. A separate `parent_span_id`
field would duplicate W3C information and is not needed. Trace and initiator
context persist in the outbox so delayed publication and reconciliation preserve
the business trace. Request IDs remain the idempotency keys, not trace/span IDs.

AWS zip deployments export OTLP/HTTP to a regional ADOT Collector Lambda
extension, which exports to X-Ray. SAM requires a compatible x86_64
`ADOTCollectorLayerARN`, packages `collector.yaml`, and configures
`OPENTELEMETRY_COLLECTOR_CONFIG_URI`. Each invocation flushes application spans
with a bounded cleanup deadline. VPC egress/endpoints must permit X-Ray export.
Local Compose generates/propagates span IDs but disables export.

Logs are structured JSON with request/message/batch IDs, trace/span IDs,
initiator hash, attempts, counts, age, and sanitized error codes. They do not
log message payloads, user IDs, original filenames, or S3 keys. Operational
traces can be sampled and must not substitute for the durable audit ledger.

## Dispatch, reconciliation, and operations

The outbox publishes **before** marking sent. A crash in between can duplicate
a command but cannot silently lose its committed intent. Short
`FOR UPDATE SKIP LOCKED` claims use one-minute leases; AWS calls happen outside
transactions. Every flush handles at most ten commands per destination and ten
seconds total, with per-send deadlines and exponential backoff plus jitter.
Expired claims are recoverable and a failed destination does not block the
other queue. Committed work is acknowledged even if an unrelated dispatch
fails: that failure is logged and scheduled maintenance retries it.

EventBridge Scheduler invokes `reconciler` every minute. It considers at most
25 stale requests and 20 stale batches per request per run. It distinguishes:

| State | Durable evidence |
| --- | --- |
| Published | Outbox `sent_at` |
| S3 operation completed | Batch `state=COMPLETED`, confirmed per-file outcomes |
| Deletion completed | Request `state=COMPLETED` and append-only completion event |

After a 15-minute inactivity window, reconciliation can republish unfinished
business commands even when their previous messages were sent or reached a DLQ.
It respects processing leases and existing unsent dispatches. Recovery is
transactionally deduplicated and capped at five no-progress recoveries; it does
not indefinitely redrive bad work. Confirmed progress resets recovery budgets.
A finalizer's boolean "already queued" flag is not used as permanent evidence
that the business operation ran.

Maintenance removes at most 500 sent outbox rows older than 30 days using a
partial retention index. It does not prune unfinished work, identity history,
or compliance evidence. CloudWatch alarms cover DLQs, reconciler failures,
unresolved work, outbox age, pending-request age, and a missing reconciler
heartbeat. Embedded health metrics
and alarms are scoped to the CloudFormation stack. Configure an SNS topic with
confirmed responders and tune `PendingAgeAlarmSeconds` to your deletion SLO.

Investigate `deletion_batch_items.state IN ('INVALID','BLOCKED')` and
`error_code`, the associated request/batch, both DLQs, and trace-correlated logs.
Correct authorization/configuration or perform reviewed storage remediation.
An authorized administrator can return corrected items and batches to
`PENDING` and schedule reconciliation; immutable identities cannot be redirected.
Do not mark unresolved items `DELETED` merely to advance the workflow.
Administrative exceptions require separately reviewed audit evidence. There is
deliberately no generic "skip errors and declare success" command.

The append-only ledger records subject/initiator pseudonyms, stages, counts,
trace IDs, and timestamps, not raw identifying data. Startup requires the
non-owner `deletion_worker` role and rejects unsafe privileges. Owners and
superusers can still bypass database controls; separate administrator access
and export to immutable compliance storage if required. This provides evidence
for a reviewed control, not automatic SOC 2 certification. Queue/DLQ initiation
messages still contain user IDs: restrict access and govern their retention.

## Run locally

Requires Docker with Compose. Copy the disposable development settings:

```sh
cp .env.example .env
docker compose up --build -d
docker compose ps
docker compose logs db-worker s3-worker reconciler
```

For an existing local setup, these disposable schema changes require recreating
its PostgreSQL volume. `docker compose down -v` **destroys this stack's local
data**; run it deliberately, not against shared infrastructure. Initialization
scripts do not migrate an existing volume. `.env` is ignored and must never
contain values reused in production. Existing local `.env` files may use
`S3_BUCKET` as a Compose-only fallback; new setups should set
`ALLOWED_S3_BUCKETS`. `S3_BUCKET` in the example identifies its demonstration
bucket, not a worker target override.

Seed synthetic data using the updated schema:

```sh
docker compose exec -T postgres psql -U postgres -d deletion <<'SQL'
INSERT INTO user_profiles(id) VALUES ('demo-user');
INSERT INTO addresses(user_id,address_line_1,city,state,zip_code)
VALUES ('demo-user','synthetic address','city','state','code');
INSERT INTO contact_methods(user_id,value) VALUES ('demo-user','synthetic contact');
INSERT INTO object_metadata(user_id,bucket,prefix,filename,original_filename,content_type,size)
VALUES ('demo-user','deletion-objects','demo/',
        '11111111-1111-4111-8111-111111111111.jpg','beach.jpg','image/jpeg',9);
SQL
docker compose exec -T localstack sh -c 'printf synthetic >/tmp/demo-object'
docker compose exec -T localstack awslocal s3api put-object \
  --bucket deletion-objects --key demo/11111111-1111-4111-8111-111111111111.jpg \
  --body /tmp/demo-object
```

Use a fresh GUID when reseeding; retired storage addresses cannot be reused.

Send a **local-only synthetic** API envelope with fresh tracing identifiers:

```sh
docker compose exec -T localstack python - <<'PY'
import hashlib, json, os, secrets, time, uuid
import boto3
client = boto3.client("sqs", endpoint_url="http://localhost:4566",
                      region_name=os.environ["AWS_REGION"],
                      aws_access_key_id="test", aws_secret_access_key="test")
queue = client.get_queue_url(QueueName=os.environ["DB_QUEUE_NAME"])["QueueUrl"]
trace_id = int(time.time()).to_bytes(4, "big").hex() + secrets.token_hex(12)
message = {
    "version": 1, "message_id": str(uuid.uuid4()), "action": "INITIATE_DELETION",
    "deletion_request_id": str(uuid.uuid4()), "user_id": "demo-user",
    "initiator_user_hash": hashlib.sha256(b"local-only-demo-actor").hexdigest(),
    "traceparent": f"00-{trace_id}-{secrets.token_hex(8)}-01"
}
client.send_message(QueueUrl=queue, MessageBody=json.dumps(message))
PY
```

Inspect audit stages and request/batch/item states; queue acknowledgement alone
is not deletion proof:

```sh
docker compose exec -T postgres psql -U postgres -d deletion \
  -c "SELECT deletion_request_id,initiator_user_hash,stage,items_affected FROM deletion_audit_log ORDER BY created_at;"
docker compose exec -T postgres psql -U postgres -d deletion \
  -c "SELECT request_id,state,enumeration_complete FROM deletion_requests;"
```

## Development

Use Go 1.25 or newer:

```sh
go mod download
make test
make vet
make build
sam validate --lint
```

The default test suite does not require services. Integration subtests create
independent temporary databases and cover 500-item boundaries, bucket grouping,
immutable identities, expired-worker races, invalid files, partial confirmations,
bounded finalization, outbox claims/backoff, recovery, audit restrictions, W3C
parentage, and Lambda partial responses. Fault-injection tests verify rollback
across every workflow table, including deferred commit failures, S3 checkpoint
failures, completion-audit failures, and lost finalizer continuations. Error
cases assert retained metadata, item states and sanitized codes, attempt counts,
leases, retry schedules, and the absence of false completion evidence.
Additional cases cover malformed envelopes, UTF-8 key-size boundaries, mixed
permanent/transient failures, interleaved users, stale generations, concurrent
replays, and publication failures after a business commit.

The optional LocalStack test exercises both actual queues through the Lambda
SQS adapter, idempotent deletion of already-absent keys, and durable unresolved
evidence for a missing bucket.

```sh
docker compose -f docker-compose.integration.yml up -d --wait postgres localstack
TEST_DATABASE_URL="$DISPOSABLE_POSTGRES_ADMIN_URL" \
TEST_AWS_ENDPOINT_URL=http://127.0.0.1:4566 \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
go test -race -count=1 -run '^TestIntegration$' -v ./internal/worker
docker compose -f docker-compose.integration.yml down --volumes --remove-orphans
```

Use an isolated **disposable PostgreSQL server**. Tests create/drop databases and,
if absent, a cluster-wide `deletion_worker` login with a fixed test password.
A pre-existing role must accept the test password and is left untouched.
Never use an application/production cluster. Omit `TEST_AWS_ENDPOINT_URL` to
exercise PostgreSQL with fake AWS clients only.

### Configuration and deployment

| Setting | Purpose |
| --- | --- |
| `DATABASE_URL` | Restricted PostgreSQL connection; require TLS in production |
| `AUDIT_SALT` | Stable secret of at least 32 bytes for subject pseudonyms |
| `ALLOWED_S3_BUCKETS` | Authorized, never-versioned bucket names |
| `DB_QUEUE_URL`, `S3_QUEUE_URL` | Respective queue URLs |
| `AWS_REGION` | AWS SDK and ADOT region |
| `APP_MODE` | `local` for polling; unset or `lambda` for Lambda |
| `AWS_ENDPOINT_URL` | LocalStack only; rejected outside local mode |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Local collector endpoint in SAM; explicit collector endpoint for other deployments |
| `OTEL_SDK_DISABLED` | `true` disables export/sampling, not trace-context propagation |
| `WORKFLOW_NAME` | Stack name for deployment-scoped health metrics |

Provision PostgreSQL and authorized buckets separately. Create the non-owner
`deletion_worker` login through your secret-management process **before** applying
both migrations as a separate schema owner. Keep the audit salt stable and
define reviewed retention/rotation policies.

Build using `sam build`, then `sam deploy --guided`. Supply `AllowedBuckets`,
a regional compatible `ADOTCollectorLayerARN`, private subnet/security-group
IDs, database credentials, audit salt, and the alarm SNS topic. Database URL and
salt are `NoEcho`; do not commit them in deployment configuration or shell
history. Use the official [ADOT Go Lambda documentation](https://aws-otel.github.io/docs/getting-started/lambda/lambda-go/)
to select a regional collector extension with the `awsxray` exporter. Private
networking needs PostgreSQL access plus SQS/S3/X-Ray egress or appropriate
endpoints. Grant the trusted API producer send access only to the DB queue.

Functions time out after 60 seconds with 512 MB memory. Database initialization
and safety checks share a 30-second startup deadline; rollback, acknowledgements,
telemetry flush, and shutdown have bounded cleanup contexts. Each process caps
its database pool at two connections. DB SQS concurrency is 2 (reserved 5), S3
SQS concurrency is 50 (reserved 75), and reconciler reserved concurrency is 1.
Queues use 360-second visibility, five receives before redrive, one-day source
retention, fourteen-day DLQ retention, and partial batch responses. Physical AWS
queue names are generated by CloudFormation; consume the stack queue URL outputs.
Measure throughput and database/collector capacity before changing these limits.

Docker targets `db-lambda`, `s3-lambda`, and `reconciler-lambda` build optional
custom-runtime images. The SAM zip deployment includes the ADOT layer; these
standalone images do not. For image deployments supply a collector endpoint or
package a compatible collector extension yourself. Compose uses the non-root
`local` target and disables span export.
