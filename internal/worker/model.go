package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"unicode/utf8"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var errInvalid = errors.New("invalid request")
var errPending = errors.New("deletion remains pending")

const (
	pageSize       = 500
	maxAttempts    = 5
	initiateAction = "INITIATE_DELETION"
	purgeAction    = "PURGE_S3_BATCH"
	finalizeAction = "FINALIZE_DB_DELETION"
)

type ValidationError struct{ Code string }

func (e *ValidationError) Error() string { return "validation failed: " + e.Code }
func (e *ValidationError) Unwrap() error { return errInvalid }

type Message struct {
	Version           int    `json:"version"`
	MessageID         string `json:"message_id"`
	Action            string `json:"action"`
	RequestID         string `json:"deletion_request_id"`
	InitiatorUserHash string `json:"initiator_user_hash"`
	TraceParent       string `json:"traceparent"`
	TraceState        string `json:"tracestate,omitempty"`
	UserID            string `json:"user_id,omitempty"`
	BatchID           string `json:"batch_id,omitempty"`
	StartKey          *int64 `json:"start_key,omitempty"`
	Generation        int    `json:"generation,omitempty"`
}

func (m Message) Validate() error {
	fail := func(code string) error { return &ValidationError{Code: code} }
	if m.Version != 1 || !validUUID(m.MessageID) || !validUUID(m.RequestID) {
		return fail("invalid_envelope")
	}
	if !validHash(m.InitiatorUserHash) {
		return fail("invalid_initiator_hash")
	}
	if len(m.TraceParent) != 55 || !strings.HasPrefix(m.TraceParent, "00-") ||
		strings.ToLower(m.TraceParent) != m.TraceParent || !trace.SpanContextFromContext(m.Context(context.Background())).IsValid() {
		return fail("invalid_traceparent")
	}
	if _, err := trace.ParseTraceState(m.TraceState); err != nil {
		return fail("invalid_tracestate")
	}
	switch m.Action {
	case initiateAction:
		if !utf8.ValidString(m.UserID) || strings.TrimSpace(m.UserID) == "" || strings.ContainsRune(m.UserID, 0) ||
			m.BatchID != "" || m.StartKey != nil || m.Generation != 0 {
			return fail("invalid_initiation")
		}
	case purgeAction:
		if !validUUID(m.BatchID) || m.StartKey == nil || *m.StartKey < 0 || m.UserID != "" || m.Generation != 0 {
			return fail("invalid_batch")
		}
	case finalizeAction:
		if m.Generation < 1 || m.BatchID != "" || m.StartKey != nil || m.UserID != "" {
			return fail("invalid_finalizer")
		}
	default:
		return fail("unknown_action")
	}
	return nil
}

func (m Message) Context(ctx context.Context) context.Context {
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{
		"traceparent": m.TraceParent, "tracestate": m.TraceState,
	})
}

type Object struct {
	ID       int64
	Bucket   string
	Key      string
	Prefix   string
	Filename string
	Attempts int
}

func (o Object) Validate() error {
	if !validBucket(o.Bucket) {
		return &ValidationError{Code: "invalid_bucket"}
	}
	if !utf8.ValidString(o.Key) || len(o.Key) == 0 || len(o.Key) > 1024 {
		return &ValidationError{Code: "invalid_key"}
	}
	for _, r := range o.Key {
		if !(r == 9 || r == 10 || r == 13 || r >= 0x20 && r <= 0xD7FF ||
			r >= 0xE000 && r <= 0xFFFD || r >= 0x10000 && r <= 0x10FFFF) {
			return &ValidationError{Code: "unsupported_key_character"}
		}
	}
	if o.Filename == "" || strings.Contains(o.Filename, "/") ||
		o.Prefix != "" && !strings.HasSuffix(o.Prefix, "/") || o.Key != o.Prefix+o.Filename {
		return &ValidationError{Code: "inconsistent_storage_identity"}
	}
	return nil
}

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func validBucket(s string) bool {
	if !bucketPattern.MatchString(s) || net.ParseIP(s) != nil || strings.Contains(s, "..") ||
		strings.Contains(s, ".-") || strings.Contains(s, "-.") {
		return false
	}
	for _, prefix := range []string{"xn--", "sthree-", "amzn-s3-demo-"} {
		if strings.HasPrefix(s, prefix) {
			return false
		}
	}
	for _, suffix := range []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"} {
		if strings.HasSuffix(s, suffix) {
			return false
		}
	}
	return true
}

func validHash(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && strings.ToLower(s) == s && s != strings.Repeat("0", 64)
}

func validUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil && len(b) == 16 && s != "00000000-0000-0000-0000-000000000000"
}

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("random source unavailable")
	}
	b[6] = b[6]&15 | 64
	b[8] = b[8]&63 | 128
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func userHash(salt, user string) string {
	h := sha256.New()
	h.Write([]byte(salt))
	h.Write([]byte{0})
	h.Write([]byte(user))
	return hex.EncodeToString(h.Sum(nil))
}

func errorCode(err error) string {
	var validation *ValidationError
	if errors.As(err, &validation) {
		return validation.Code
	}
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) {
		return safeCode(fmt.Sprintf("%.80s", coded.ErrorCode()))
	}
	var database interface{ SQLState() string }
	if errors.As(err, &database) {
		return "postgres_" + safeCode(database.SQLState())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, errPending) {
		return "pending"
	}
	if errors.Is(err, errInvalid) {
		return "invalid_request"
	}
	return "operation_failed"
}
