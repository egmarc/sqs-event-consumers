package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type dispatch struct {
	id, token string
	message   Message
	attempt   int
	created   time.Time
}

func (s Service) claimDispatch(ctx context.Context, destination string) (*dispatch, error) {
	var claimed *dispatch
	err := s.Store.transaction(ctx, func(tx pgx.Tx) error {
		var id string
		var payload []byte
		var attempt int
		var created time.Time
		err := tx.QueryRow(ctx, `SELECT id::text,payload,attempts,created_at FROM deletion_outbox
			WHERE destination=$1 AND sent_at IS NULL AND available_at<=now()
			AND (lease_until IS NULL OR lease_until<now())
			ORDER BY available_at,created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`, destination).
			Scan(&id, &payload, &attempt, &created)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		var m Message
		if err := json.Unmarshal(payload, &m); err != nil {
			return fmt.Errorf("decode persisted outbox message %s: %w", id, err)
		}
		if err := m.Validate(); err != nil {
			return fmt.Errorf("validate persisted outbox message %s: %w", id, err)
		}
		token := newUUID()
		if _, err := tx.Exec(ctx, `UPDATE deletion_outbox SET lease_token=$2,lease_until=now()+interval '1 minute',
			attempts=attempts+1 WHERE id=$1`, id, token); err != nil {
			return err
		}
		claimed = &dispatch{id, token, m, attempt + 1, created}
		return nil
	})
	return claimed, err
}

func (s Service) finishDispatch(ctx context.Context, d dispatch, sendErr error) error {
	return s.Store.transaction(ctx, func(tx pgx.Tx) error {
		if sendErr != nil {
			_, err := tx.Exec(ctx, `UPDATE deletion_outbox SET lease_token=NULL,lease_until=NULL,
				available_at=now()+$3::double precision*interval '1 second',last_error_code=$4
				WHERE id=$1 AND lease_token=$2`, d.id, d.token, retryDelay(d.attempt).Seconds(), errorCode(sendErr))
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE deletion_outbox SET sent_at=now(),lease_token=NULL,lease_until=NULL,last_error_code=NULL
			WHERE id=$1 AND lease_token=$2 AND sent_at IS NULL`, d.id, d.token)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		if d.message.BatchID != "" {
			_, err = tx.Exec(ctx, `UPDATE deletion_batches SET last_dispatched_at=now() WHERE batch_id=$1`, d.message.BatchID)
		} else {
			_, err = tx.Exec(ctx, `UPDATE deletion_requests SET finalizer_dispatched_at=now()
				WHERE request_id=$1 AND finalizer_generation=$2`, d.message.RequestID, d.message.Generation)
		}
		return err
	})
}

// Flush bounds dispatch independently of queue depth and isolates both destinations.
func (s Service) Flush(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var failures []error
	for range 10 {
		found := false
		for _, destination := range []string{"db", "s3"} {
			if ctx.Err() != nil {
				return errors.Join(append(failures, ctx.Err())...)
			}
			d, err := s.claimDispatch(ctx, destination)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			if d == nil {
				continue
			}
			found = true
			url := s.DBQueue
			if destination == "s3" {
				url = s.S3Queue
			}
			sendCtx, sendCancel := context.WithTimeout(d.message.Context(ctx), 2*time.Second)
			sendCtx, span := otel.Tracer("deletion-worker").Start(sendCtx, "sqs.publish", trace.WithSpanKind(trace.SpanKindProducer))
			carrier := propagation.MapCarrier{}
			propagation.TraceContext{}.Inject(sendCtx, carrier)
			m := d.message
			m.TraceParent, m.TraceState = carrier.Get("traceparent"), carrier.Get("tracestate")
			body, encodeErr := json.Marshal(m)
			var sendErr error
			if encodeErr != nil {
				sendErr = encodeErr
			} else {
				_, sendErr = s.Queue.SendMessage(sendCtx, &sqs.SendMessageInput{
					QueueUrl: aws.String(url), MessageBody: aws.String(string(body)),
				})
			}
			if sendErr != nil {
				span.SetStatus(codes.Error, errorCode(sendErr))
				failures = append(failures, fmt.Errorf("publish command %s: %w", d.id, sendErr))
			}
			logEvent(sendCtx, "outbox_dispatch", &m, "attempt", d.attempt,
				"age_seconds", time.Since(d.created).Seconds(), "error_code", codeOrEmpty(sendErr))
			span.End()
			sendCancel()
			if err := s.finishDispatch(ctx, *d, sendErr); err != nil {
				failures = append(failures, fmt.Errorf("finish dispatch %s: %w", d.id, err))
			}
		}
		if !found {
			break
		}
	}
	return errors.Join(failures...)
}

func codeOrEmpty(err error) string {
	if err == nil {
		return ""
	}
	return errorCode(err)
}

func retryDelay(attempt int) time.Duration {
	// UUID entropy supplies bounded jitter without sharing mutable PRNG state.
	id := newUUID()
	jitter := time.Duration(id[0]%5) * time.Second
	return time.Duration(1<<min(max(attempt, 1), 8))*time.Second + jitter
}
