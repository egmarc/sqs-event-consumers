package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
)

func (s Service) Reconcile(ctx context.Context) error {
	var ids []string
	err := s.Store.transaction(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `UPDATE deletion_requests SET next_reconcile_at=now()+interval '15 minutes'
			WHERE request_id IN (SELECT request_id FROM deletion_requests
				WHERE state='PENDING' AND next_reconcile_at<=now()
				ORDER BY next_reconcile_at,request_id LIMIT 25 FOR UPDATE SKIP LOCKED)
			RETURNING request_id::text`)
		if err != nil {
			return err
		}
		ids, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	if err != nil {
		return fmt.Errorf("claim stale requests: %w", err)
	}
	var failures []error
	for _, id := range ids {
		if err := s.reconcileRequest(ctx, id); err != nil {
			logEvent(ctx, "reconcile_failed", nil, "request_id", id, "error_code", errorCode(err))
			failures = append(failures, err)
		}
	}
	if err := s.pruneOutbox(ctx); err != nil {
		failures = append(failures, err)
	}
	if err := s.reportHealth(ctx); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (s Service) reportHealth(ctx context.Context) error {
	var pending, outbox float64
	err := s.Store.pool.QueryRow(ctx, `SELECT
		COALESCE(EXTRACT(EPOCH FROM now()-(SELECT created_at FROM deletion_requests
			WHERE state='PENDING' ORDER BY created_at LIMIT 1)),0),
		COALESCE(EXTRACT(EPOCH FROM now()-(SELECT created_at FROM deletion_outbox
			WHERE sent_at IS NULL ORDER BY created_at LIMIT 1)),0)`).Scan(&pending, &outbox)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "workflow_health",
		"_aws", map[string]any{
			"Timestamp": time.Now().UnixMilli(),
			"CloudWatchMetrics": []any{map[string]any{
				"Namespace": "DeletionWorkflow", "Dimensions": [][]string{{"Workflow"}},
				"Metrics": []any{
					map[string]string{"Name": "OldestPendingAgeSeconds", "Unit": "Seconds"},
					map[string]string{"Name": "OldestUnsentAgeSeconds", "Unit": "Seconds"},
				},
			}},
		},
		"Workflow", os.Getenv("WORKFLOW_NAME"), "OldestPendingAgeSeconds", pending, "OldestUnsentAgeSeconds", outbox)
	return nil
}

func (s Service) reconcileRequest(ctx context.Context, id string) error {
	return s.Store.transaction(ctx, func(tx pgx.Tx) error {
		r, err := loadRequest(ctx, tx, id, "")
		if err != nil || r.state != "PENDING" {
			return err
		}
		ctx, span := otel.Tracer("deletion-worker").Start(r.message(initiateAction).Context(ctx), "deletion.reconcile")
		defer span.End()
		if r.user == nil {
			return errInvalid
		}
		var status string
		if err := tx.QueryRow(ctx, `SELECT status FROM user_profiles WHERE id=$1 FOR UPDATE`, *r.user).Scan(&status); err != nil {
			return err
		}
		if status != "PENDING_DELETION" {
			logEvent(ctx, "request_blocked", nil, "request_id", r.id, "error_code", "subject_not_pending")
			return nil
		}
		type staleBatch struct {
			id       string
			start    int64
			attempts int
			recovery int
		}
		rows, err := tx.Query(ctx, `SELECT batch_id::text,start_key,attempts,recovery_count FROM deletion_batches b
			WHERE request_id=$1 AND state='PENDING' AND last_dispatched_at<now()-interval '15 minutes'
			AND (lease_until IS NULL OR lease_until<now())
			AND NOT EXISTS(SELECT 1 FROM deletion_outbox o WHERE o.batch_id=b.batch_id AND sent_at IS NULL)
			ORDER BY start_key LIMIT 20 FOR UPDATE`, r.id)
		if err != nil {
			return err
		}
		batches, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (staleBatch, error) {
			var b staleBatch
			err := row.Scan(&b.id, &b.start, &b.attempts, &b.recovery)
			return b, err
		})
		if err != nil {
			return err
		}
		for _, b := range batches {
			if b.recovery >= maxAttempts || b.attempts >= maxAttempts {
				if _, err := tx.Exec(ctx, `UPDATE deletion_batches SET state='BLOCKED',lease_token=NULL,lease_until=NULL WHERE batch_id=$1`, b.id); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `UPDATE deletion_batch_items SET state='BLOCKED',error_code='retry_exhausted',updated_at=now()
					WHERE batch_id=$1 AND state='PENDING'`, b.id); err != nil {
					return err
				}
				logEvent(ctx, "request_blocked", nil, "request_id", r.id, "batch_id", b.id, "error_code", "retry_exhausted")
				continue
			}
			m := r.message(purgeAction)
			m.BatchID, m.StartKey = b.id, &b.start
			if err := enqueue(ctx, tx, "s3", m); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE deletion_batches SET recovery_count=recovery_count+1,last_dispatched_at=now()
				WHERE batch_id=$1`, b.id); err != nil {
				return err
			}
			logEvent(ctx, "recovery_scheduled", &m, "attempt", b.recovery+1)
		}
		// Exhaustion of one page must not prevent enumeration of later pages.
		if !r.enumerated {
			if err := createPage(ctx, tx, &r, r.cursor); err != nil {
				return err
			}
		}
		if err := scheduleFinalizer(ctx, tx, &r); err != nil {
			return err
		}
		var ready bool
		var recovery int
		if err := tx.QueryRow(ctx, `SELECT finalizer_generation>0 AND
			finalizer_dispatched_at<now()-interval '15 minutes' AND
			NOT EXISTS(SELECT 1 FROM deletion_outbox WHERE request_id=$1 AND destination='db' AND sent_at IS NULL),
			recovery_count FROM deletion_requests WHERE request_id=$1`, r.id).Scan(&ready, &recovery); err != nil {
			return err
		}
		if ready {
			if recovery >= maxAttempts {
				logEvent(ctx, "request_blocked", nil, "request_id", r.id, "error_code", "finalizer_retry_exhausted")
			} else {
				if err := enqueueFinalizer(ctx, tx, r); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `UPDATE deletion_requests SET recovery_count=recovery_count+1,
					finalizer_dispatched_at=now() WHERE request_id=$1`, r.id); err != nil {
					return err
				}
			}
		}
		var blocked bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM deletion_batches WHERE request_id=$1 AND state='BLOCKED')`, r.id).Scan(&blocked); err != nil {
			return err
		}
		if blocked {
			logEvent(ctx, "request_blocked", nil, "request_id", r.id, "error_code", "unresolved_items")
		}
		return nil
	})
}

func (s Service) pruneOutbox(ctx context.Context) error {
	return s.Store.transaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `DELETE FROM deletion_outbox WHERE id IN
			(SELECT id FROM deletion_outbox WHERE sent_at<now()-interval '30 days'
			ORDER BY sent_at,id LIMIT 500 FOR UPDATE SKIP LOCKED)`)
		return err
	})
}
