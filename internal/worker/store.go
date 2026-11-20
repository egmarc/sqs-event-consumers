package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Store struct {
	salt string
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, url, salt string) (*Store, error) {
	if len(salt) < 32 {
		return nil, &ValidationError{Code: "invalid_audit_salt"}
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database configuration: %w", err)
	}
	config.MaxConns = 2
	config.ConnConfig.ConnectTimeout = 10 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return &Store{salt: salt, pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) transaction(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin database transaction: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tx.Rollback(cleanup); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			logEvent(ctx, "rollback_failed", nil, "error_code", errorCode(err))
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit database transaction: %w", err)
	}
	return nil
}

func (s *Store) CheckPrivileges(ctx context.Context) error {
	var unsafe bool
	err := s.pool.QueryRow(ctx, `SELECT current_user <> 'deletion_worker'
		OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
		OR has_schema_privilege(current_user, 'public', 'CREATE')
		OR has_table_privilege(current_user, 'deletion_audit_log', 'UPDATE,DELETE,TRUNCATE')
		OR has_table_privilege(current_user, 'storage_object_identities', 'UPDATE,DELETE,TRUNCATE')
		OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname='public' AND pg_has_role(current_user,c.relowner,'MEMBER'))
		FROM pg_roles r WHERE r.rolname=current_user`).Scan(&unsafe)
	if err != nil {
		return fmt.Errorf("check runtime privileges: %w", err)
	}
	if unsafe {
		return &ValidationError{Code: "unsafe_database_role"}
	}
	return nil
}

type request struct {
	id, hash, actor, state, traceparent, tracestate string
	user                                            *string
	enumerated                                      bool
	cursor                                          int64
	generation                                      int
}

func (r request) message(action string) Message {
	return Message{Version: 1, MessageID: newUUID(), Action: action, RequestID: r.id,
		InitiatorUserHash: r.actor, TraceParent: r.traceparent, TraceState: r.tracestate}
}

func lockUser(ctx context.Context, tx pgx.Tx, hash string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, hash)
	return err
}

func loadRequest(ctx context.Context, tx pgx.Tx, id, actor string) (request, error) {
	var r request
	if err := tx.QueryRow(ctx, `SELECT user_hash FROM deletion_requests WHERE request_id=$1`, id).Scan(&r.hash); err != nil {
		return r, fmt.Errorf("find deletion request: %w", err)
	}
	if err := lockUser(ctx, tx, r.hash); err != nil {
		return r, err
	}
	err := tx.QueryRow(ctx, `SELECT request_id::text,user_hash,initiator_user_hash,state,user_id,
		traceparent,tracestate,enumeration_complete,cursor,finalizer_generation
		FROM deletion_requests WHERE request_id=$1 FOR UPDATE`, id).
		Scan(&r.id, &r.hash, &r.actor, &r.state, &r.user, &r.traceparent, &r.tracestate, &r.enumerated, &r.cursor, &r.generation)
	if err == nil && actor != "" && actor != r.actor {
		return r, errInvalid
	}
	return r, err
}

func enqueue(ctx context.Context, tx pgx.Tx, destination string, m Message) error {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		ctx = m.Context(ctx)
	}
	ctx, span := otel.Tracer("deletion-worker").Start(ctx, "outbox.enqueue")
	defer span.End()
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	m.TraceParent, m.TraceState = carrier.Get("traceparent"), carrier.Get("tracestate")
	if err := m.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode outbox command: %w", err)
	}
	var batch any
	if m.BatchID != "" {
		batch = m.BatchID
	}
	_, err = tx.Exec(ctx, `INSERT INTO deletion_outbox(id,request_id,batch_id,destination,payload)
		VALUES($1,$2,$3,$4,$5)`, m.MessageID, m.RequestID, batch, destination, body)
	return err
}

func ledger(ctx context.Context, tx pgx.Tx, r request, event string, batch any, count int64) error {
	resource := "database"
	if batch != nil {
		resource = "s3"
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		sc = trace.SpanContextFromContext(r.message(initiateAction).Context(ctx))
	}
	_, err := tx.Exec(ctx, `INSERT INTO deletion_audit_log
		(audit_id,deletion_request_id,user_id_hash,initiator_user_hash,trace_id,stage,resource_type,batch_id,items_affected)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, newUUID(), r.id, r.hash, r.actor, sc.TraceID().String(), event, resource, batch, count)
	return err
}

func (s *Store) Initiate(ctx context.Context, m Message) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if m.Action != initiateAction {
		return errInvalid
	}
	hash := userHash(s.salt, m.UserID)
	return s.transaction(ctx, func(tx pgx.Tx) error {
		if err := lockUser(ctx, tx, hash); err != nil {
			return err
		}
		var owner, actor string
		err := tx.QueryRow(ctx, `SELECT user_hash,initiator_user_hash FROM deletion_requests WHERE request_id=$1`, m.RequestID).Scan(&owner, &actor)
		if err == nil {
			if owner != hash || actor != m.InitiatorUserHash {
				return errInvalid
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var state string
		if err := tx.QueryRow(ctx, `SELECT status FROM user_profiles WHERE id=$1 FOR UPDATE`, m.UserID).Scan(&state); err != nil {
			return fmt.Errorf("lock deletion subject: %w", err)
		}
		if state != "ACTIVE" {
			return errPending
		}
		_, err = tx.Exec(ctx, `INSERT INTO deletion_requests
			(request_id,user_hash,initiator_user_hash,user_id,traceparent,tracestate) VALUES($1,$2,$3,$4,$5,$6)`,
			m.RequestID, hash, m.InitiatorUserHash, m.UserID, m.TraceParent, m.TraceState)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE user_profiles SET status='PENDING_DELETION' WHERE id=$1`, m.UserID); err != nil {
			return err
		}
		r := request{id: m.RequestID, hash: hash, actor: m.InitiatorUserHash, user: &m.UserID,
			state: "PENDING", traceparent: m.TraceParent, tracestate: m.TraceState}
		if err := ledger(ctx, tx, r, "INITIATED", nil, 0); err != nil {
			return err
		}
		return createPage(ctx, tx, &r, 0)
	})
}

func createPage(ctx context.Context, tx pgx.Tx, r *request, after int64) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM deletion_batches WHERE request_id=$1 AND start_key=$2)`, r.id, after).Scan(&exists); err != nil {
		return err
	}
	if exists || r.enumerated {
		return scheduleFinalizer(ctx, tx, r)
	}
	if r.user == nil || after != r.cursor {
		return errInvalid
	}
	rows, err := tx.Query(ctx, `SELECT object_id,bucket,object_key,prefix,filename FROM object_metadata
		WHERE user_id=$1 AND object_id>$2 ORDER BY object_id LIMIT 500`, *r.user, after)
	if err != nil {
		return err
	}
	objects, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Object, error) {
		var o Object
		err := row.Scan(&o.ID, &o.Bucket, &o.Key, &o.Prefix, &o.Filename)
		return o, err
	})
	if err != nil {
		return err
	}
	var next *int64
	if len(objects) > 0 {
		r.cursor = objects[len(objects)-1].ID
		var more bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM object_metadata WHERE user_id=$1 AND object_id>$2)`, *r.user, r.cursor).Scan(&more); err != nil {
			return err
		}
		if more {
			value := r.cursor
			next = &value
		}
		id := newUUID()
		if _, err := tx.Exec(ctx, `INSERT INTO deletion_batches(batch_id,request_id,start_key,next_key) VALUES($1,$2,$3,$4)`,
			id, r.id, after, next); err != nil {
			return err
		}
		values := make([][]any, 0, len(objects))
		ids := make([]int64, 0, len(objects))
		for _, o := range objects {
			values = append(values, []any{id, o.ID, o.Bucket, o.Key, o.Prefix, o.Filename})
			ids = append(ids, o.ID)
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"deletion_batch_items"},
			[]string{"batch_id", "object_id", "bucket", "object_key", "prefix", "filename"}, pgx.CopyFromRows(values)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE object_metadata SET status='PENDING_DELETION' WHERE user_id=$1 AND object_id=ANY($2)`, *r.user, ids); err != nil {
			return err
		}
		m := r.message(purgeAction)
		m.BatchID, m.StartKey = id, &after
		if err := enqueue(ctx, tx, "s3", m); err != nil {
			return err
		}
	}
	r.enumerated = next == nil
	if _, err := tx.Exec(ctx, `UPDATE deletion_requests SET cursor=$2,enumeration_complete=$3,last_progress_at=now(),
		recovery_count=0,next_reconcile_at=now()+interval '15 minutes' WHERE request_id=$1`, r.id, r.cursor, r.enumerated); err != nil {
		return err
	}
	return scheduleFinalizer(ctx, tx, r)
}

func scheduleFinalizer(ctx context.Context, tx pgx.Tx, r *request) error {
	if !r.enumerated || r.generation != 0 || r.state != "PENDING" || r.user == nil {
		return nil
	}
	var blocked bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM object_metadata WHERE user_id=$1)
		OR EXISTS(SELECT 1 FROM deletion_batches WHERE request_id=$2 AND state<>'COMPLETED')`, *r.user, r.id).Scan(&blocked); err != nil {
		return err
	}
	if blocked {
		return nil
	}
	r.generation = 1
	if _, err := tx.Exec(ctx, `UPDATE deletion_requests SET finalizer_generation=1,finalizer_dispatched_at=now() WHERE request_id=$1`, r.id); err != nil {
		return err
	}
	return enqueueFinalizer(ctx, tx, *r)
}

func enqueueFinalizer(ctx context.Context, tx pgx.Tx, r request) error {
	m := r.message(finalizeAction)
	m.Generation = r.generation
	return enqueue(ctx, tx, "db", m)
}

func (s *Store) Finalize(ctx context.Context, m Message) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if m.Action != finalizeAction {
		return errInvalid
	}
	return s.transaction(ctx, func(tx pgx.Tx) error {
		r, err := loadRequest(ctx, tx, m.RequestID, m.InitiatorUserHash)
		if err != nil {
			return err
		}
		if r.state == "COMPLETED" || m.Generation < r.generation {
			return nil
		}
		if r.user == nil || m.Generation != r.generation || !r.enumerated {
			return errPending
		}
		var state string
		if err := tx.QueryRow(ctx, `SELECT status FROM user_profiles WHERE id=$1 FOR UPDATE`, *r.user).Scan(&state); err != nil {
			return err
		}
		if state != "PENDING_DELETION" {
			return errPending
		}
		var blocked bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM object_metadata WHERE user_id=$1)
			OR EXISTS(SELECT 1 FROM deletion_batches WHERE request_id=$2 AND state<>'COMPLETED')`, *r.user, r.id).Scan(&blocked); err != nil {
			return err
		}
		if blocked {
			return errPending
		}
		var removed int64
		for _, table := range []string{"contact_methods", "addresses"} {
			tag, err := tx.Exec(ctx, `DELETE FROM `+table+` WHERE id IN (SELECT id FROM `+table+
				` WHERE user_id=$1 ORDER BY id LIMIT $2)`, *r.user, pageSize-removed)
			if err != nil {
				return err
			}
			removed += tag.RowsAffected()
		}
		var more bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM contact_methods WHERE user_id=$1)
			OR EXISTS(SELECT 1 FROM addresses WHERE user_id=$1)`, *r.user).Scan(&more); err != nil {
			return err
		}
		if more {
			r.generation++
			if _, err := tx.Exec(ctx, `UPDATE deletion_requests SET db_deleted_count=db_deleted_count+$2,
				finalizer_generation=$3,finalizer_dispatched_at=now(),last_progress_at=now(),recovery_count=0,
				next_reconcile_at=now()+interval '15 minutes' WHERE request_id=$1`, r.id, removed, r.generation); err != nil {
				return err
			}
			return enqueueFinalizer(ctx, tx, r)
		}
		tag, err := tx.Exec(ctx, `DELETE FROM user_profiles WHERE id=$1`, *r.user)
		if err != nil {
			return err
		}
		removed += tag.RowsAffected()
		var total int64
		if err := tx.QueryRow(ctx, `UPDATE deletion_requests SET state='COMPLETED',user_id=NULL,completed_at=now(),
			last_progress_at=now(),db_deleted_count=db_deleted_count+$2 WHERE request_id=$1 RETURNING db_deleted_count`, r.id, removed).Scan(&total); err != nil {
			return err
		}
		return ledger(ctx, tx, r, "COMPLETED", nil, total)
	})
}
