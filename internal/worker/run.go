package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func (s Service) Handle(ctx context.Context, kind string, body []byte) error {
	var m Message
	if err := json.Unmarshal(body, &m); err != nil {
		return errInvalid
	}
	var err error
	switch {
	case kind == "db" && m.Type == "INITIATE_DELETION":
		err = s.Store.Initiate(ctx, m)
	case kind == "db" && m.Type == "FINALIZE_DB_DELETION":
		err = s.Store.Finalize(ctx, m)
	case kind == "s3" && m.Type == "PURGE_S3_BATCH":
		err = s.Purge(ctx, m)
	default:
		err = errInvalid
	}
	flushErr := s.Flush(ctx)
	if err != nil {
		return err
	}
	return flushErr
}

func (s Service) LambdaHandler(kind string) func(context.Context, json.RawMessage) (events.SQSEventResponse, error) {
	return func(ctx context.Context, raw json.RawMessage) (events.SQSEventResponse, error) {
		var event struct {
			Action  string              `json:"action"`
			Source  string              `json:"source"`
			Records []events.SQSMessage `json:"Records"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return events.SQSEventResponse{}, errInvalid
		}
		if event.Action == "FLUSH_OUTBOX" || event.Source == "aws.events" {
			if err := s.Flush(ctx); err != nil {
				return events.SQSEventResponse{}, errors.New("outbox replay failed")
			}
			return events.SQSEventResponse{}, nil
		}
		if len(event.Records) == 0 {
			return events.SQSEventResponse{}, errInvalid
		}
		response := events.SQSEventResponse{BatchItemFailures: []events.SQSBatchItemFailure{}}
		for _, record := range event.Records {
			if err := s.Handle(ctx, kind, []byte(record.Body)); err != nil {
				log.Print("message processing failed")
				response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
			}
		}
		return response, nil
	}
}

func local(ctx context.Context, s Service, client *sqs.Client, kind string) {
	url := s.DBQueue
	if kind == "s3" {
		url = s.S3Queue
	}
	// A separate loop guarantees recovery even while long-polling an idle queue.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			flushCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
			if err := s.Flush(flushCtx); err != nil && ctx.Err() == nil {
				log.Print("outbox replay failed")
			}
			cancel()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() { <-done }()
	for ctx.Err() == nil {
		result, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 1, WaitTimeSeconds: 20, VisibilityTimeout: 360,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Print("queue receive failed")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		for _, message := range result.Messages {
			workCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			err := s.Handle(workCtx, kind, []byte(aws.ToString(message.Body)))
			cancel()
			if err != nil {
				log.Print("message processing failed")
				continue
			}
			if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: message.ReceiptHandle}); err != nil {
				log.Print("queue acknowledgement failed")
			}
		}
	}
}

func Run(kind string) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	s := Service{Store: Store{URL: os.Getenv("DATABASE_URL"), Salt: os.Getenv("AUDIT_SALT")},
		Bucket: os.Getenv("S3_BUCKET"), DBQueue: os.Getenv("DB_QUEUE_URL"), S3Queue: os.Getenv("S3_QUEUE_URL")}
	mode := os.Getenv("APP_MODE")
	if s.Store.URL == "" || len(s.Store.Salt) < 32 || s.Bucket == "" || s.DBQueue == "" || s.S3Queue == "" || (mode != "" && mode != "lambda" && mode != "local") {
		log.Print("invalid application configuration")
		os.Exit(1)
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Print("AWS configuration failed")
		os.Exit(1)
	}
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	s.S3 = s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	s.Queue = client
	startup, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	err = s.Store.CheckPrivileges(startup)
	if err == nil {
		err = s.CheckBucket(startup)
	}
	cancelStartup()
	if err != nil {
		log.Print("startup safety check failed")
		os.Exit(1)
	}
	if mode == "local" {
		local(ctx, s, client, kind)
		return
	}
	lambda.Start(s.LambdaHandler(kind))
}
