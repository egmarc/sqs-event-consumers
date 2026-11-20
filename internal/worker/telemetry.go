package worker

import (
	"context"
	"log/slog"
	"os"
	"regexp"

	"go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func startTelemetry(ctx context.Context, service string, disabled bool) (*sdktrace.TracerProvider, error) {
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", service),
			attribute.String("cloud.provider", "aws"),
			attribute.String("cloud.region", os.Getenv("AWS_REGION")),
		)),
		sdktrace.WithIDGenerator(xray.NewIDGenerator()),
	}
	if disabled {
		options = append(options, sdktrace.WithSampler(sdktrace.NeverSample()))
	} else {
		exporter, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(exporter),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())))
	}
	provider := sdktrace.NewTracerProvider(options...)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Error("telemetry_failed", "error_code", errorCode(err))
	}))
	return provider, nil
}

func logEvent(ctx context.Context, event string, m *Message, attrs ...any) {
	sc := trace.SpanContextFromContext(ctx)
	base := []any{"event", event, "workflow", os.Getenv("WORKFLOW_NAME"),
		"trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String()}
	if m != nil {
		base = append(base, "request_id", m.RequestID, "message_id", m.MessageID,
			"action", m.Action, "batch_id", m.BatchID, "initiator_user_hash", m.InitiatorUserHash)
	}
	slog.InfoContext(ctx, event, append(base, attrs...)...)
}

var safeCodePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,80}$`)

func safeCode(code string) string {
	if !safeCodePattern.MatchString(code) {
		return "s3_error"
	}
	return code
}
