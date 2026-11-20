package worker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func testMessage(action string) Message {
	start := int64(0)
	m := Message{
		Version: 1, MessageID: newUUID(), Action: action, RequestID: newUUID(),
		InitiatorUserHash: userHash("test-actor-salt", "authorized-api-user"),
		TraceParent:       "00-" + strings.ReplaceAll(newUUID(), "-", "") + "-1234567890abcdef-01",
	}
	switch action {
	case initiateAction:
		m.UserID = "u-" + newUUID()
	case purgeAction:
		m.BatchID, m.StartKey = newUUID(), &start
	case finalizeAction:
		m.Generation = 1
	}
	return m
}

func TestUUIDAndHash(t *testing.T) {
	seen := map[string]bool{}
	for range 100 {
		id := newUUID()
		if !validUUID(id) || id[14] != '4' || !strings.ContainsRune("89ab", rune(id[19])) || seen[id] {
			t.Fatalf("invalid or duplicate UUID: %s", id)
		}
		seen[id] = true
	}
	for _, bad := range []string{"", "x", "00000000-0000-0000-0000-000000000000", "zzzzzzzz-1234-1234-1234-123456789012"} {
		if validUUID(bad) {
			t.Fatalf("accepted invalid UUID %q", bad)
		}
	}
	hash := userHash("salt", "subject")
	if !validHash(hash) || hash != userHash("salt", "subject") ||
		hash == userHash("other", "subject") || hash == userHash("salt", "other") {
		t.Fatal("hash does not preserve pseudonym invariants")
	}
}

func TestMessageValidation(t *testing.T) {
	for _, action := range []string{initiateAction, purgeAction, finalizeAction} {
		if err := testMessage(action).Validate(); err != nil {
			t.Fatalf("%s: %v", action, err)
		}
	}
	cases := map[string]func(*Message){
		"version":       func(m *Message) { m.Version = 0 },
		"message-id":    func(m *Message) { m.MessageID = "invalid" },
		"request-id":    func(m *Message) { m.RequestID = "" },
		"actor":         func(m *Message) { m.InitiatorUserHash = "" },
		"zero-actor":    func(m *Message) { m.InitiatorUserHash = strings.Repeat("0", 64) },
		"upper-actor":   func(m *Message) { m.InitiatorUserHash = strings.Repeat("A", 64) },
		"trace":         func(m *Message) { m.TraceParent = "" },
		"zero-trace":    func(m *Message) { m.TraceParent = "00-" + strings.Repeat("0", 32) + "-1234567890abcdef-01" },
		"zero-span":     func(m *Message) { m.TraceParent = "00-" + strings.Repeat("1", 32) + "-0000000000000000-01" },
		"tracestate":    func(m *Message) { m.TraceState = "not valid" },
		"action":        func(m *Message) { m.Action = "DELETE_EVERYTHING" },
		"empty-user":    func(m *Message) { m.UserID = "" },
		"blank-user":    func(m *Message) { m.UserID = " \t" },
		"nul-user":      func(m *Message) { m.UserID = "bad\x00user" },
		"extra-batch":   func(m *Message) { m.BatchID = newUUID() },
		"extra-cursor":  func(m *Message) { v := int64(0); m.StartKey = &v },
		"extra-version": func(m *Message) { m.Generation = 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := testMessage(initiateAction)
			mutate(&m)
			if err := m.Validate(); !errors.Is(err, errInvalid) {
				t.Fatalf("got %v, want invalid request", err)
			}
		})
	}
	for _, mutate := range []func(*Message){
		func(m *Message) { m.StartKey = nil },
		func(m *Message) { v := int64(-1); m.StartKey = &v },
		func(m *Message) { m.BatchID = "" },
		func(m *Message) { m.UserID = "untrusted" },
	} {
		m := testMessage(purgeAction)
		mutate(&m)
		if err := m.Validate(); err == nil {
			t.Fatal("accepted invalid batch command")
		}
	}
	m := testMessage(finalizeAction)
	m.Generation = 0
	if err := m.Validate(); err == nil {
		t.Fatal("accepted unversioned finalizer")
	}
}

func TestObjectValidation(t *testing.T) {
	good := Object{Bucket: "allowed-bucket", Prefix: "summer/", Filename: "file.jpg", Key: "summer/file.jpg"}
	for _, o := range []Object{
		good,
		{Bucket: "allowed.bucket", Filename: "photo.jpg", Key: "photo.jpg"},
		{Bucket: "allowed-bucket", Filename: strings.Repeat("a", 1024), Key: strings.Repeat("a", 1024)},
		{Bucket: "allowed-bucket", Prefix: "a//../", Filename: "photo & \u96ea.jpg", Key: "a//../photo & \u96ea.jpg"},
	} {
		if err := o.Validate(); err != nil {
			t.Fatalf("rejected supported object: %v", err)
		}
	}
	cases := map[string]func(*Object){
		"empty-key":       func(o *Object) { o.Key = "" },
		"long-key":        func(o *Object) { o.Key = strings.Repeat("a", 1025) },
		"multibyte-long":  func(o *Object) { o.Key = strings.Repeat("\u96ea", 342) },
		"control":         func(o *Object) { o.Key = "bad\x01key" },
		"invalid-utf8":    func(o *Object) { o.Key = string([]byte{0xff}) },
		"invalid-xml":     func(o *Object) { o.Key = "\uffff" },
		"uppercase":       func(o *Object) { o.Bucket = "Bad-Bucket" },
		"short-bucket":    func(o *Object) { o.Bucket = "ab" },
		"ip-bucket":       func(o *Object) { o.Bucket = "127.0.0.1" },
		"reserved-prefix": func(o *Object) { o.Bucket = "xn--bucket" },
		"reserved-suffix": func(o *Object) { o.Bucket = "bucket--x-s3" },
		"dot":             func(o *Object) { o.Bucket = "bad..bucket" },
		"prefix":          func(o *Object) { o.Prefix = "summer" },
		"filename":        func(o *Object) { o.Filename = "dir/file.jpg" },
		"inconsistent":    func(o *Object) { o.Key = "other/file.jpg" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := good
			mutate(&o)
			if err := o.Validate(); !errors.Is(err, errInvalid) {
				t.Fatalf("got %v, want validation error", err)
			}
		})
	}
}

func TestEnvelopeAndSettings(t *testing.T) {
	for _, raw := range []string{`{}`, `bad`, `{"Records":[]}`, `{"action":"FLUSH_OUTBOX"}`} {
		if _, err := (Service{}).LambdaHandler("db")(context.Background(), json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted invalid event %q", raw)
		}
	}
	m := testMessage(initiateAction)
	body, _ := json.Marshal(m)
	for _, raw := range [][]byte{append(append([]byte(nil), body...), []byte(` {}`)...), []byte(`{"type":"INITIATE_DELETION"}`)} {
		if _, err := decodeMessage(raw); err == nil {
			t.Fatal("accepted invalid command")
		}
	}
	c := Settings{DatabaseURL: "postgres://localhost/example", AuditSalt: strings.Repeat("s", 32),
		DBQueue: "https://sqs.us-east-1.amazonaws.com/123/db", S3Queue: "https://sqs.us-east-1.amazonaws.com/123/s3",
		Buckets: map[string]struct{}{"allowed-bucket": {}}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Settings){
		func(c *Settings) { c.AuditSalt = "short" },
		func(c *Settings) { c.Buckets = nil },
		func(c *Settings) { c.Mode = "unknown" },
		func(c *Settings) { c.DBQueue = "http://localhost/queue" },
		func(c *Settings) { c.Endpoint = "http://localhost:4566" },
	} {
		bad := c
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatal("accepted invalid settings")
		}
	}
	if _, err := NewStore(context.Background(), "invalid", "short"); err == nil {
		t.Fatal("accepted weak audit configuration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewStore(ctx, "postgres://127.0.0.1:1/unreachable?sslmode=disable", c.AuditSalt); err == nil {
		t.Fatal("ignored canceled startup")
	}
}

func TestTraceContext(t *testing.T) {
	previous := otel.GetTracerProvider()
	provider, err := startTelemetry(context.Background(), "unit-test", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})
	m := testMessage(initiateAction)
	parent := trace.SpanContextFromContext(m.Context(context.Background()))
	ctx, span := otel.Tracer("test").Start(m.Context(context.Background()), "consumer")
	defer span.End()
	child := trace.SpanContextFromContext(ctx)
	if child.TraceID() != parent.TraceID() || child.SpanID() == parent.SpanID() || !child.IsValid() {
		t.Fatal("failed to preserve trace or generate a fresh span")
	}
	for _, attempt := range []int{0, 1, 5, 1000} {
		if d := retryDelay(attempt); d < time.Second || d > 5*time.Minute {
			t.Fatalf("unbounded retry delay: %v", d)
		}
	}

}

func TestOTLPExport(t *testing.T) {
	received := make(chan *collectortrace.ExportTraceServiceRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		request := &collectortrace.ExportTraceServiceRequest{}
		if err := proto.Unmarshal(body, request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- request
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", server.URL+"/v1/traces")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "")
	t.Setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "none")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_COMPRESSION", "none")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	previous := otel.GetTracerProvider()
	provider, err := startTelemetry(ctx, "export-test", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = provider.Shutdown(cleanup)
		otel.SetTracerProvider(previous)
	})
	m := testMessage(initiateAction)
	_, span := otel.Tracer("test").Start(m.Context(ctx), "exported-consumer")
	span.End()
	if err := provider.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-received:
		if len(request.ResourceSpans) != 1 || len(request.ResourceSpans[0].ScopeSpans) != 1 {
			t.Fatal("missing OTLP resource/scope")
		}
		spans := request.ResourceSpans[0].ScopeSpans[0].Spans
		if len(spans) != 1 || spans[0].Name != "exported-consumer" ||
			hex.EncodeToString(spans[0].TraceId) != strings.Split(m.TraceParent, "-")[1] ||
			hex.EncodeToString(spans[0].ParentSpanId) != "1234567890abcdef" {
			t.Fatal("exported trace lost W3C parentage")
		}
	case <-ctx.Done():
		t.Fatal("OTLP collector received no spans")
	}
}
