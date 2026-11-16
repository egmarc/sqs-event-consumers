package worker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

var errInvalid = errors.New("invalid request")
var errPending = errors.New("deletion remains pending")

const chunkSize = 1000

type Message struct {
	Type      string `json:"action"`
	RequestID string `json:"deletion_request_id"`
	UserID    string `json:"user_id,omitempty"`
	BatchID   string `json:"batch_id,omitempty"`
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

func chunks(keys []string) [][]string {
	var result [][]string
	for len(keys) > 0 {
		n := min(len(keys), chunkSize)
		result = append(result, keys[:n])
		keys = keys[n:]
	}
	return result
}
