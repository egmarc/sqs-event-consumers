package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func decodeMessage(body []byte) (Message, error) {
	var m Message
	if len(body) > 1024*1024 {
		return m, &ValidationError{Code: "message_too_large"}
	}
	if !utf8.Valid(body) {
		return m, &ValidationError{Code: "invalid_json_encoding"}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return m, &ValidationError{Code: "invalid_json_envelope"}
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return m, &ValidationError{Code: "trailing_json"}
	}
	return m, m.Validate()
}

func (s Service) Handle(ctx context.Context, kind string, body []byte) error {
	m, err := decodeMessage(body)
	if err != nil {
		logEvent(ctx, "message_rejected", nil, "error_code", errorCode(err))
		return err
	}
	ctx, span := otel.Tracer("deletion-worker").Start(m.Context(ctx), "sqs.process",
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(
			attribute.String("messaging.system", "aws_sqs"),
			attribute.String("messaging.message.id", m.MessageID),
			attribute.String("deletion.request_id", m.RequestID),
		))
	defer span.End()
	switch {
	case kind == "db" && m.Action == initiateAction:
		err = s.Store.Initiate(ctx, m)
	case kind == "db" && m.Action == finalizeAction:
		err = s.Store.Finalize(ctx, m)
	case kind == "s3" && m.Action == purgeAction:
		err = s.Purge(ctx, m)
	default:
		err = &ValidationError{Code: "action_queue_mismatch"}
	}
	if err != nil {
		span.SetStatus(codes.Error, errorCode(err))
	}
	logEvent(ctx, "message_processed", &m, "error_code", codeOrEmpty(err))
	// Dispatch is recoverable independently; unrelated publication must not fail this command.
	if flushErr := s.Flush(ctx); flushErr != nil {
		logEvent(ctx, "outbox_dispatch_failed", &m, "error_code", errorCode(flushErr))
	}
	return err
}

func (s Service) LambdaHandler(kind string) func(context.Context, json.RawMessage) (events.SQSEventResponse, error) {
	return func(ctx context.Context, raw json.RawMessage) (events.SQSEventResponse, error) {
		if flusher, ok := otel.GetTracerProvider().(interface{ ForceFlush(context.Context) error }); ok {
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := flusher.ForceFlush(cleanup); err != nil {
					logEvent(cleanup, "telemetry_failed", nil, "error_code", errorCode(err))
				}
			}()
		}
		if deadline, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline.Add(-8*time.Second))
			defer cancel()
		}
		var event struct {
			Action  string              `json:"action"`
			Records []events.SQSMessage `json:"Records"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return events.SQSEventResponse{}, errInvalid
		}
		if kind == "reconciler" {
			if event.Action != "RECONCILE" || len(event.Records) != 0 {
				return events.SQSEventResponse{}, errInvalid
			}
			reconcileErr := s.Reconcile(ctx)
			flushErr := s.Flush(ctx)
			return events.SQSEventResponse{}, errors.Join(reconcileErr, flushErr)
		}
		if len(event.Records) == 0 || event.Action != "" {
			return events.SQSEventResponse{}, errInvalid
		}
		response := events.SQSEventResponse{BatchItemFailures: []events.SQSBatchItemFailure{}}
		for _, record := range event.Records {
			if record.MessageId == "" {
				return events.SQSEventResponse{}, errInvalid
			}
			if err := s.Handle(ctx, kind, []byte(record.Body)); err != nil {
				logEvent(ctx, "queue_record_failed", nil, "sqs_message_id", record.MessageId, "error_code", errorCode(err))
				response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
			}
		}
		return response, nil
	}
}

func local(ctx context.Context, s Service, client *sqs.Client, kind string) error {
	if kind == "reconciler" {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			workCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
			if err := errors.Join(s.Reconcile(workCtx), s.Flush(workCtx)); err != nil && ctx.Err() == nil {
				logEvent(ctx, "maintenance_failed", nil, "error_code", errorCode(err))
			}
			cancel()
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}
	url := s.DBQueue
	if kind == "s3" {
		url = s.S3Queue
	}
	for ctx.Err() == nil {
		result, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 1, WaitTimeSeconds: 20, VisibilityTimeout: 360,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logEvent(ctx, "queue_receive_failed", nil, "error_code", errorCode(err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}
		for _, message := range result.Messages {
			workCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
			err := s.Handle(workCtx, kind, []byte(aws.ToString(message.Body)))
			cancel()
			if err != nil {
				continue
			}
			ackCtx, ackCancel := context.WithTimeout(ctx, 5*time.Second)
			_, err = client.DeleteMessage(ackCtx, &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: message.ReceiptHandle})
			ackCancel()
			if err != nil {
				logEvent(ctx, "queue_acknowledgement_failed", nil, "error_code", errorCode(err))
			}
		}
	}
	return nil
}

type Settings struct {
	DatabaseURL, AuditSalt, DBQueue, S3Queue, Mode, Endpoint string
	Buckets                                                  map[string]struct{}
}

func (c Settings) Validate() error {
	if c.DatabaseURL == "" || len(c.AuditSalt) < 32 || len(c.Buckets) == 0 ||
		(c.Mode != "" && c.Mode != "lambda" && c.Mode != "local") {
		return &ValidationError{Code: "invalid_configuration"}
	}
	for bucket := range c.Buckets {
		if !validBucket(bucket) {
			return &ValidationError{Code: "invalid_allowed_bucket"}
		}
	}
	if _, err := pgxpool.ParseConfig(c.DatabaseURL); err != nil {
		return &ValidationError{Code: "invalid_database_configuration"}
	}
	for _, raw := range []string{c.DBQueue, c.S3Queue} {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || u.User != nil ||
			(u.Scheme != "https" && !(c.Mode == "local" && u.Scheme == "http")) {
			return &ValidationError{Code: "invalid_queue_url"}
		}
	}
	if c.Endpoint != "" && c.Mode != "local" {
		return &ValidationError{Code: "local_endpoint_in_production"}
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return &ValidationError{Code: "invalid_local_endpoint"}
		}
	}
	return nil
}

func Run(kind string) error {
	if kind != "db" && kind != "s3" && kind != "reconciler" {
		return &ValidationError{Code: "invalid_worker_kind"}
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStartup()
	settings := Settings{
		DatabaseURL: os.Getenv("DATABASE_URL"), AuditSalt: os.Getenv("AUDIT_SALT"),
		DBQueue: os.Getenv("DB_QUEUE_URL"), S3Queue: os.Getenv("S3_QUEUE_URL"),
		Mode: os.Getenv("APP_MODE"), Endpoint: os.Getenv("AWS_ENDPOINT_URL"),
		Buckets: make(map[string]struct{}),
	}
	for _, bucket := range strings.Split(os.Getenv("ALLOWED_S3_BUCKETS"), ",") {
		if bucket = strings.TrimSpace(bucket); bucket != "" {
			settings.Buckets[bucket] = struct{}{}
		}
	}
	if err := settings.Validate(); err != nil {
		return err
	}
	provider, err := startTelemetry(startup, kind+"-worker", os.Getenv("OTEL_SDK_DISABLED") == "true")
	if err != nil {
		return fmt.Errorf("initialize telemetry: %s", errorCode(err))
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := provider.Shutdown(cleanup); err != nil {
			logEvent(cleanup, "telemetry_shutdown_failed", nil, "error_code", errorCode(err))
		}
	}()
	store, err := NewStore(startup, settings.DatabaseURL, settings.AuditSalt)
	if err != nil {
		return fmt.Errorf("initialize database: %s", errorCode(err))
	}
	defer store.Close()
	if err := store.CheckPrivileges(startup); err != nil {
		return fmt.Errorf("check runtime privileges: %s", errorCode(err))
	}
	cfg, err := config.LoadDefaultConfig(startup)
	if err != nil {
		return fmt.Errorf("initialize AWS configuration: %s", errorCode(err))
	}
	s := Service{Store: store, Buckets: settings.Buckets, DBQueue: settings.DBQueue, S3Queue: settings.S3Queue}
	s.S3 = s3.NewFromConfig(cfg, func(o *s3.Options) {
		if settings.Endpoint != "" {
			o.BaseEndpoint = aws.String(settings.Endpoint)
			o.UsePathStyle = true
		}
	})
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if settings.Endpoint != "" {
			o.BaseEndpoint = aws.String(settings.Endpoint)
		}
	})
	s.Queue = client
	cancelStartup()
	if settings.Mode == "local" {
		return local(ctx, s, client, kind)
	}
	lambda.StartWithOptions(s.LambdaHandler(kind), lambda.WithEnableSIGTERM(func() { cancel() }))
	return nil
}
