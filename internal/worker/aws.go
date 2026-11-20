package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

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
	Store            *Store
	S3               S3API
	Queue            QueueAPI
	Buckets          map[string]struct{}
	DBQueue, S3Queue string
}

func (s Service) CheckBucket(ctx context.Context, bucket string) error {
	if !validBucket(bucket) {
		return &ValidationError{Code: "invalid_bucket"}
	}
	if _, ok := s.Buckets[bucket]; !ok {
		return &ValidationError{Code: "unauthorized_bucket"}
	}
	versioning, err := s.S3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		return err
	}
	if versioning == nil {
		return errors.New("missing bucket versioning response")
	}
	if versioning.Status != "" || versioning.MFADelete != "" {
		return &ValidationError{Code: "unsupported_bucket_versioning"}
	}
	return nil
}

type batchWork struct {
	token   string
	attempt int
	objects []Object
}

func (s Service) acquireBatch(ctx context.Context, m Message) (*batchWork, error) {
	var work *batchWork
	err := s.Store.transaction(ctx, func(tx pgx.Tx) error {
		r, err := loadRequest(ctx, tx, m.RequestID, m.InitiatorUserHash)
		if err != nil {
			return err
		}
		var state string
		var start int64
		var next *int64
		var busy bool
		var attempt int
		if err := tx.QueryRow(ctx, `SELECT state,start_key,next_key,COALESCE(lease_until>now(),false),attempts
			FROM deletion_batches WHERE batch_id=$1 AND request_id=$2 FOR UPDATE`, m.BatchID, m.RequestID).
			Scan(&state, &start, &next, &busy, &attempt); err != nil {
			return err
		}
		if start != *m.StartKey {
			return errInvalid
		}
		if state == "COMPLETED" || state == "BLOCKED" {
			return nil
		}
		if busy {
			return errPending
		}
		if r.state != "PENDING" || r.user == nil {
			return errInvalid
		}
		var userState string
		if err := tx.QueryRow(ctx, `SELECT status FROM user_profiles WHERE id=$1 FOR UPDATE`, *r.user).Scan(&userState); err != nil {
			return err
		}
		if userState != "PENDING_DELETION" {
			return errPending
		}
		token := newUUID()
		if _, err := tx.Exec(ctx, `UPDATE deletion_batches SET lease_token=$2,lease_until=now()+interval '2 minutes',
			attempts=attempts+1 WHERE batch_id=$1`, m.BatchID, token); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT object_id,bucket,object_key,prefix,filename,attempts FROM deletion_batch_items
			WHERE batch_id=$1 AND state='PENDING' ORDER BY object_id`, m.BatchID)
		if err != nil {
			return err
		}
		objects, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Object, error) {
			var o Object
			err := row.Scan(&o.ID, &o.Bucket, &o.Key, &o.Prefix, &o.Filename, &o.Attempts)
			return o, err
		})
		if err != nil {
			return err
		}
		// Enumeration progresses independently of failures in this page.
		if next != nil {
			if err := createPage(ctx, tx, &r, *next); err != nil {
				return err
			}
		}
		work = &batchWork{token: token, attempt: attempt + 1, objects: objects}
		return nil
	})
	return work, err
}

type itemResult struct{ state, code string }

func (s Service) Purge(ctx context.Context, m Message) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if m.Action != purgeAction {
		return errInvalid
	}
	work, err := s.acquireBatch(ctx, m)
	if err != nil || work == nil {
		return err
	}
	results := make(map[int64]itemResult, len(work.objects))
	groups := make(map[string][]Object)
	for _, o := range work.objects {
		err := o.Validate()
		if err == nil {
			if _, ok := s.Buckets[o.Bucket]; !ok {
				err = &ValidationError{Code: "unauthorized_bucket"}
			}
		}
		if err != nil {
			results[o.ID] = itemResult{"INVALID", errorCode(err)}
			continue
		}
		groups[o.Bucket] = append(groups[o.Bucket], o)
	}
	buckets := make([]string, 0, len(groups))
	for bucket := range groups {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	if len(results) > 0 {
		if err := s.recordResults(ctx, m, *work, results, false); err != nil {
			return err
		}
	}
	for i, bucket := range buckets {
		if i >= 10 {
			break
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < 6*time.Second {
			break
		}
		objects := groups[bucket]
		results = make(map[int64]itemResult, len(objects))
		bucketCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		result, callErr := s.deleteGroup(bucketCtx, bucket, objects)
		cancel()
		if callErr != nil || result == nil {
			code := "invalid_s3_response"
			if callErr != nil {
				code = errorCode(callErr)
			}
			for _, o := range objects {
				results[o.ID] = failureResult(code, o.Attempts+1)
			}
		} else {
			results = interpretResults(objects, result)
		}
		if err := s.recordResults(ctx, m, *work, results, false); err != nil {
			return err
		}
	}
	return s.recordResults(ctx, m, *work, nil, true)
}

func (s Service) deleteGroup(ctx context.Context, bucket string, objects []Object) (*s3.DeleteObjectsOutput, error) {
	if err := s.CheckBucket(ctx, bucket); err != nil {
		return nil, err
	}
	input := make([]s3types.ObjectIdentifier, 0, len(objects))
	for _, o := range objects {
		input = append(input, s3types.ObjectIdentifier{Key: aws.String(o.Key)})
	}
	return s.S3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket), Delete: &s3types.Delete{Objects: input, Quiet: aws.Bool(false)},
	})
}

func interpretResults(objects []Object, result *s3.DeleteObjectsOutput) map[int64]itemResult {
	results := make(map[int64]itemResult, len(objects))
	allowed := make(map[string]bool, len(objects))
	confirmed := make(map[string]bool, len(objects))
	failed := make(map[string]string)
	for _, o := range objects {
		allowed[o.Key] = true
	}
	for _, deleted := range result.Deleted {
		key := aws.ToString(deleted.Key)
		if allowed[key] {
			confirmed[key] = true
		}
	}
	unexpectedError := false
	for _, item := range result.Errors {
		key := aws.ToString(item.Key)
		if !allowed[key] {
			unexpectedError = true
		}
		failed[key] = safeCode(aws.ToString(item.Code))
	}
	for _, o := range objects {
		switch {
		case unexpectedError:
			results[o.ID] = failureResult("unexpected_s3_response", o.Attempts+1)
		case failed[o.Key] != "":
			results[o.ID] = failureResult(failed[o.Key], o.Attempts+1)
		case confirmed[o.Key]:
			results[o.ID] = itemResult{state: "DELETED"}
		default:
			results[o.ID] = failureResult("missing_confirmation", o.Attempts+1)
		}
	}
	return results
}

func failureResult(code string, attempt int) itemResult {
	state := "PENDING"
	switch code {
	case "AccessDenied", "InvalidAccessKeyId", "NoSuchBucket", "invalid_bucket", "unauthorized_bucket", "unsupported_bucket_versioning":
		state = "BLOCKED"
	}
	if attempt >= maxAttempts {
		state = "BLOCKED"
	}
	return itemResult{state, code}
}

func (s Service) recordResults(ctx context.Context, m Message, work batchWork, results map[int64]itemResult, final bool) error {
	pending := false
	err := s.Store.transaction(ctx, func(tx pgx.Tx) error {
		r, err := loadRequest(ctx, tx, m.RequestID, m.InitiatorUserHash)
		if err != nil {
			return err
		}
		var current bool
		if err := tx.QueryRow(ctx, `SELECT COALESCE(lease_token=$2::uuid,false) FROM deletion_batches
			WHERE batch_id=$1 FOR UPDATE`, m.BatchID, work.token).Scan(&current); err != nil {
			return err
		}
		if !current {
			return errPending
		}
		var userState string
		if r.user == nil {
			return errInvalid
		}
		if err := tx.QueryRow(ctx, `SELECT status FROM user_profiles WHERE id=$1 FOR UPDATE`, *r.user).Scan(&userState); err != nil {
			return err
		}
		if userState != "PENDING_DELETION" {
			return errPending
		}
		var removed int64
		for _, o := range work.objects {
			result, ok := results[o.ID]
			if !ok {
				continue
			}
			if result.state == "DELETED" {
				tag, err := tx.Exec(ctx, `DELETE FROM object_metadata WHERE user_id=$1 AND object_id=$2 AND bucket=$3 AND object_key=$4`,
					*r.user, o.ID, o.Bucket, o.Key)
				if err != nil {
					return err
				}
				removed += tag.RowsAffected()
				if _, err := tx.Exec(ctx, `UPDATE deletion_batch_items SET state='DELETED',bucket=NULL,object_key=NULL,attempts=attempts+1,
					prefix=NULL,filename=NULL,error_code=NULL,updated_at=now() WHERE batch_id=$1 AND object_id=$2`, m.BatchID, o.ID); err != nil {
					return err
				}
			} else {
				if _, err := tx.Exec(ctx, `UPDATE deletion_batch_items SET state=$3,error_code=$4,attempts=attempts+1,updated_at=now()
					WHERE batch_id=$1 AND object_id=$2`, m.BatchID, o.ID, result.state, result.code); err != nil {
					return err
				}
				logEvent(ctx, "item_unresolved", &m, "object_id", o.ID, "state", result.state,
					"error_code", result.code, "attempt", o.Attempts+1)
			}
		}
		var unresolved, unattempted bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM deletion_batch_items WHERE batch_id=$1 AND state='PENDING'),
			EXISTS(SELECT 1 FROM deletion_batch_items WHERE batch_id=$1 AND state<>'DELETED'),
			EXISTS(SELECT 1 FROM deletion_batch_items WHERE batch_id=$1 AND state='PENDING' AND attempts=0)`,
			m.BatchID).Scan(&pending, &unresolved, &unattempted); err != nil {
			return err
		}
		state := "PENDING"
		if final && !pending {
			state = "COMPLETED"
			if unresolved {
				state = "BLOCKED"
			}
		}
		var total int64
		if err := tx.QueryRow(ctx, `UPDATE deletion_batches SET state=$2,
			lease_token=CASE WHEN $4 THEN NULL ELSE lease_token END,lease_until=CASE WHEN $4 THEN NULL ELSE lease_until END,
			deleted_count=deleted_count+$3,last_progress_at=now(),recovery_count=CASE WHEN $3>0 THEN 0 ELSE recovery_count END,
			attempts=CASE WHEN $3>0 THEN 0 ELSE attempts END,
			completed_at=CASE WHEN $2='COMPLETED' THEN now() END WHERE batch_id=$1 RETURNING deleted_count`,
			m.BatchID, state, removed, final).Scan(&total); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE deletion_requests SET last_progress_at=now(),recovery_count=0,
			next_reconcile_at=now()+interval '15 minutes' WHERE request_id=$1`, r.id); err != nil {
			return err
		}
		logEvent(ctx, "batch_progress", &m, "state", state, "deleted_count", total, "attempt", work.attempt)
		if state == "COMPLETED" {
			if err := ledger(ctx, tx, r, "BATCH_COMPLETED", m.BatchID, total); err != nil {
				return err
			}
		}
		if final && pending && unattempted {
			next := r.message(purgeAction)
			next.BatchID, next.StartKey = m.BatchID, m.StartKey
			if err := enqueue(ctx, tx, "s3", next); err != nil {
				return err
			}
			// A bounded continuation is normal work, not an SQS delivery failure.
			pending = false
		}
		return scheduleFinalizer(ctx, tx, &r)
	})
	if err == nil && pending && final {
		return errPending
	}
	if err != nil {
		return fmt.Errorf("record S3 batch results: %w", err)
	}
	return nil
}
