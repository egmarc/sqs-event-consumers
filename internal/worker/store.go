package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	URL  string
	Salt string
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, url, salt string) (*Store, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database configuration: %w", err)
	}
	config.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return &Store{URL: url, Salt: salt, pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) transaction(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin database transaction: %w", err)
	}
	defer tx.Rollback(context.Background())
	if err = fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit database transaction: %w", err)
	}
	return nil
}

// CheckPrivileges refuses roles capable of bypassing the append-only ledger.
func (s *Store) CheckPrivileges(ctx context.Context) error {
	return s.transaction(ctx, func(tx pgx.Tx) error {
		var unsafe bool
		err := tx.QueryRow(ctx, `SELECT current_user <> 'deletion_worker'
			OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
			OR has_schema_privilege(current_user, 'public', 'CREATE')
			OR has_table_privilege(current_user, 'deletion_audit_log', 'UPDATE,DELETE,TRUNCATE')
			OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
				WHERE n.nspname='public' AND pg_has_role(current_user,c.relowner,'MEMBER'))
			FROM pg_roles r WHERE r.rolname=current_user`).Scan(&unsafe)
		if err != nil {
			return err
		}
		if unsafe {
			return errors.New("unsafe database role")
		}
		return nil
	})
}

func enqueue(ctx context.Context, tx pgx.Tx, destination string, m Message) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO deletion_outbox(id,destination,payload) VALUES($1,$2,$3)`, newUUID(), destination, body)
	return err
}

func ledger(ctx context.Context, tx pgx.Tx, request, hash, event string, batch any, count int64) error {
	resource := "database"
	if batch != nil {
		resource = "s3"
	}
	_, err := tx.Exec(ctx, `INSERT INTO deletion_audit_log(audit_id,deletion_request_id,user_id_hash,stage,resource_type,batch_id,items_affected) VALUES($1,$2,$3,$4,$5,$6,$7)`,
		newUUID(), request, hash, event, resource, batch, count)
	return err
}

func (s *Store) Initiate(ctx context.Context, m Message) error {
	if !validUUID(m.RequestID) || m.UserID == "" {
		return errInvalid
	}
	hash := userHash(s.Salt, m.UserID)
	return s.transaction(ctx, func(tx pgx.Tx) error {
		// A transaction advisory lock also serializes retries after the user row is gone.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, hash); err != nil {
			return err
		}
		var owner string
		err := tx.QueryRow(ctx, `SELECT user_hash FROM deletion_requests WHERE request_id=$1`, m.RequestID).Scan(&owner)
		if err == nil {
			if owner != hash {
				return errInvalid
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var state string
		if err := tx.QueryRow(ctx, `SELECT status FROM users WHERE id=$1 FOR UPDATE`, m.UserID).Scan(&state); err != nil {
			return err
		}
		if state != "ACTIVE" {
			return errPending
		}
		if _, err := tx.Exec(ctx, `INSERT INTO deletion_requests(request_id,user_hash,user_id) VALUES($1,$2,$3)`, m.RequestID, hash, m.UserID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE users SET status='PENDING_DELETION' WHERE id=$1`, m.UserID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT object_key FROM object_storage WHERE user_id=$1 ORDER BY object_key`, m.UserID)
		if err != nil {
			return err
		}
		keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, group := range chunks(keys) {
			id := newUUID()
			if _, err := tx.Exec(ctx, `INSERT INTO deletion_batches(batch_id,request_id,keys) VALUES($1,$2,$3)`, id, m.RequestID, group); err != nil {
				return err
			}
			if err := enqueue(ctx, tx, "s3", Message{Type: "PURGE_S3_BATCH", RequestID: m.RequestID, BatchID: id}); err != nil {
				return err
			}
		}
		if err := ledger(ctx, tx, m.RequestID, hash, "INITIATED", nil, 0); err != nil {
			return err
		}
		return scheduleFinalizer(ctx, tx, m.RequestID, m.UserID)
	})
}

func scheduleFinalizer(ctx context.Context, tx pgx.Tx, request, user string) error {
	var ready bool
	if err := tx.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM object_storage WHERE user_id=$1)
		AND NOT EXISTS(SELECT 1 FROM deletion_batches WHERE request_id=$2 AND NOT completed)`, user, request).Scan(&ready); err != nil {
		return err
	}
	if !ready {
		return nil
	}
	tag, err := tx.Exec(ctx, `UPDATE deletion_requests SET finalizer_enqueued=true WHERE request_id=$1 AND NOT finalizer_enqueued AND state='PENDING'`, request)
	if err != nil || tag.RowsAffected() == 0 {
		return err
	}
	return enqueue(ctx, tx, "db", Message{Type: "FINALIZE_DB_DELETION", RequestID: request})
}

func (s *Store) Finalize(ctx context.Context, m Message) error {
	if !validUUID(m.RequestID) {
		return errInvalid
	}
	return s.transaction(ctx, func(tx pgx.Tx) error {
		var hash string
		if err := tx.QueryRow(ctx, `SELECT user_hash FROM deletion_requests WHERE request_id=$1`, m.RequestID).Scan(&hash); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, hash); err != nil {
			return err
		}
		var owner, state string
		var user *string
		if err := tx.QueryRow(ctx, `SELECT user_hash,state,user_id FROM deletion_requests WHERE request_id=$1 FOR UPDATE`, m.RequestID).Scan(&owner, &state, &user); err != nil {
			return err
		}
		if m.UserID != "" && userHash(s.Salt, m.UserID) != owner {
			return errInvalid
		}
		if state == "COMPLETED" {
			return nil
		}
		if user == nil {
			return errInvalid
		}
		if err := tx.QueryRow(ctx, `SELECT status FROM users WHERE id=$1 FOR UPDATE`, *user).Scan(&state); err != nil {
			return err
		}
		if state != "PENDING_DELETION" {
			return errPending
		}
		var blocked bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM object_storage WHERE user_id=$1)
			OR EXISTS(SELECT 1 FROM deletion_batches WHERE request_id=$2 AND NOT completed)`, *user, m.RequestID).Scan(&blocked); err != nil {
			return err
		}
		if blocked {
			return errPending
		}
		var removed int64
		for _, query := range []string{
			`DELETE FROM contact_methods WHERE user_id=$1`,
			`DELETE FROM address WHERE user_id=$1`,
			`DELETE FROM users WHERE id=$1`,
		} {
			tag, err := tx.Exec(ctx, query, *user)
			if err != nil {
				return err
			}
			removed += tag.RowsAffected()
		}
		if _, err := tx.Exec(ctx, `UPDATE deletion_requests SET state='COMPLETED',user_id=NULL,completed_at=now() WHERE request_id=$1`, m.RequestID); err != nil {
			return err
		}
		return ledger(ctx, tx, m.RequestID, hash, "COMPLETED", nil, removed)
	})
}
