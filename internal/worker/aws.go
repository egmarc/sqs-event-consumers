package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
)

type S3API interface {
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
}

type QueueAPI interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type Service struct {
	Store                    *Store
	S3                       S3API
	Queue                    QueueAPI
	Bucket, DBQueue, S3Queue string
}

func (s Service) CheckBucket(ctx context.Context) error {
	versioning, err := s.S3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(s.Bucket)})
	if err != nil {
		return err
	}
	if versioning.Status != "" || versioning.MFADelete != "" {
		return errors.New("bucket must never have enabled versioning")
	}
	return nil
}

func (s Service) Purge(ctx context.Context, m Message) error {
	if !validUUID(m.RequestID) || !validUUID(m.BatchID) {
		return errInvalid
	}
	// Refuse suspended as well as enabled versioning: neither permanently erases versions.
	if err := s.CheckBucket(ctx); err != nil {
		return fmt.Errorf("check S3 bucket versioning: %w", err)
	}
	var user string
	var keys []string
	alreadyCompleted := false
	err := s.Store.transaction(ctx, func(tx pgx.Tx) error {
		var hash, state string
		var requestUser *string
		if err := tx.QueryRow(ctx, `SELECT user_hash,user_id,state FROM deletion_requests WHERE request_id=$1`, m.RequestID).Scan(&hash, &requestUser, &state); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, hash); err != nil {
			return err
		}
		var completed bool
		if err := tx.QueryRow(ctx, `SELECT keys,completed FROM deletion_batches WHERE batch_id=$1 AND request_id=$2 FOR UPDATE`, m.BatchID, m.RequestID).Scan(&keys, &completed); err != nil {
			return err
		}
		if completed {
			alreadyCompleted = true
			return nil
		}
		if state != "PENDING" || requestUser == nil {
			return errInvalid
		}
		var userState string
		if err := tx.QueryRow(ctx, `SELECT status FROM users WHERE id=$1 FOR UPDATE`, *requestUser).Scan(&userState); err != nil {
			return err
		}
		if userState != "PENDING_DELETION" {
			return errPending
		}
		keys = append([]string(nil), keys...)
		user = *requestUser
		return nil
	})
	if err != nil {
		return err
	}
	if alreadyCompleted {
		return nil
	}

	objects := make([]s3types.ObjectIdentifier, 0, len(keys))
	allowed := make(map[string]bool, len(keys))
	for _, key := range keys {
		objects = append(objects, s3types.ObjectIdentifier{Key: aws.String(key)})
		allowed[key] = true
	}
	result, err := s.S3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.Bucket), Delete: &s3types.Delete{Objects: objects, Quiet: aws.Bool(false)},
	})
	if err != nil {
		return fmt.Errorf("delete S3 batch %s: %w", m.BatchID, err)
	}
	confirmed := make(map[string]bool, len(result.Deleted))
	for _, deleted := range result.Deleted {
		key := aws.ToString(deleted.Key)
		if allowed[key] {
			confirmed[key] = true
		}
	}
	// An explicit error always overrides a contradictory Deleted entry.
	for _, failed := range result.Errors {
		delete(confirmed, aws.ToString(failed.Key))
	}
	confirmedKeys := make([]string, 0, len(confirmed))
	for key := range confirmed {
		confirmedKeys = append(confirmedKeys, key)
	}
	pending := len(result.Errors) > 0 || len(confirmed) != len(keys)
	err = s.Store.transaction(ctx, func(tx pgx.Tx) error {
		var requestHash, state string
		var requestUser *string
		if err := tx.QueryRow(ctx, `SELECT user_hash,user_id,state FROM deletion_requests WHERE request_id=$1`, m.RequestID).Scan(&requestHash, &requestUser, &state); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, requestHash); err != nil {
			return err
		}
		var completed bool
		if err := tx.QueryRow(ctx, `SELECT completed FROM deletion_batches WHERE batch_id=$1 AND request_id=$2 FOR UPDATE`, m.BatchID, m.RequestID).Scan(&completed); err != nil {
			return err
		}
		if completed {
			return nil
		}
		if state != "PENDING" || requestUser == nil || *requestUser != user {
			return errInvalid
		}
		var userState string
		if err := tx.QueryRow(ctx, `SELECT status FROM users WHERE id=$1 FOR UPDATE`, user).Scan(&userState); err != nil {
			return err
		}
		if userState != "PENDING_DELETION" {
			return errPending
		}
		tag, err := tx.Exec(ctx, `DELETE FROM object_storage WHERE user_id=$1 AND object_key=ANY($2::text[])`, user, confirmedKeys)
		if err != nil {
			return err
		}
		removed := tag.RowsAffected()
		// Keep the immutable canonical snapshot; accumulated counts only include
		// unique metadata rows actually removed, including across partial retries.
		if _, err := tx.Exec(ctx, `UPDATE deletion_batches SET deleted_count=deleted_count+$2,completed=$3,keys=CASE WHEN $3 THEN '{}'::text[] ELSE keys END WHERE batch_id=$1`, m.BatchID, removed, !pending); err != nil {
			return err
		}
		if pending {
			return nil
		}
		var total int64
		if err := tx.QueryRow(ctx, `SELECT deleted_count FROM deletion_batches WHERE batch_id=$1`, m.BatchID).Scan(&total); err != nil {
			return err
		}
		if err := ledger(ctx, tx, m.RequestID, requestHash, "BATCH_COMPLETED", m.BatchID, total); err != nil {
			return err
		}
		return scheduleFinalizer(ctx, tx, m.RequestID, user)
	})
	if err == nil && pending {
		return errPending
	}
	return err
}

func (s Service) Flush(ctx context.Context) error {
	if err := s.Store.transaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `DELETE FROM deletion_outbox WHERE sent_at < now() - interval '30 days'`)
		return err
	}); err != nil {
		return fmt.Errorf("prune sent outbox messages: %w", err)
	}

	for {
		found := false
		var id, destination string
		var payload []byte
		err := s.Store.transaction(ctx, func(tx pgx.Tx) error {
			err := tx.QueryRow(ctx, `SELECT id::text,destination,payload FROM deletion_outbox WHERE sent_at IS NULL ORDER BY created_at,id LIMIT 1`).Scan(&id, &destination, &payload)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			found = true
			return nil
		})
		if err != nil {
			return fmt.Errorf("read outbox message: %w", err)
		}
		if !found {
			return nil
		}
		url := s.DBQueue
		if destination == "s3" {
			url = s.S3Queue
		}
		if _, err := s.Queue.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(url), MessageBody: aws.String(string(payload))}); err != nil {
			return fmt.Errorf("send outbox message %s: %w", id, err)
		}
		if err := s.Store.transaction(ctx, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE deletion_outbox SET sent_at=now() WHERE id=$1 AND sent_at IS NULL`, id)
			return err
		}); err != nil {
			return fmt.Errorf("mark outbox message %s sent: %w", id, err)
		}
	}
}
