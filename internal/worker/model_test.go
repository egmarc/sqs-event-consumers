package worker

import (
	"context"
	"encoding/json"
	"testing"
)

func TestUUID(t *testing.T) {
	for range 100 {
		if id := newUUID(); !validUUID(id) {
			t.Fatal("generated invalid UUID")
		}
	}
	for _, bad := range []string{"", "x", "00000000-0000-0000-0000-000000000000", "12345678x1234-1234-1234-123456789012", "zzzzzzzz-1234-1234-1234-123456789012"} {
		if validUUID(bad) {
			t.Fatal("accepted invalid UUID")
		}
	}
}

func TestChunks(t *testing.T) {
	for _, n := range []int{0, 1, 999, 1000, 1001, 2500} {
		keys := make([]string, n)
		groups := chunks(keys)
		if len(groups) != (n+999)/1000 {
			t.Fatal("wrong chunk count")
		}
		total := 0
		for _, group := range groups {
			if len(group) > 1000 || len(group) == 0 {
				t.Fatal("invalid chunk")
			}
			total += len(group)
		}
		if total != n {
			t.Fatal("lost keys")
		}
	}
}

func TestHash(t *testing.T) {
	a := userHash("salt", "private@example.com")
	if len(a) != 64 || a == userHash("other", "private@example.com") || a == userHash("salt", "another") {
		t.Fatal("unsafe hash")
	}
}

func TestLambdaInvalidEnvelope(t *testing.T) {
	handler := (Service{}).LambdaHandler("db")
	for _, raw := range []string{`{}`, `bad`, `{"Records":[]}`} {
		if _, err := handler(context.Background(), json.RawMessage(raw)); err == nil {
			t.Fatal("accepted invalid event")
		}
	}
}
