// Package signing authenticates exact webhook bytes. It does not deduplicate
// business effects; receivers must do that transactionally after verification.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var ErrInvalid = errors.New("invalid webhook authentication")

// Keys is implemented by config.Settings. Receivers should expose only their
// own destination key, rather than accepting another destination's signatures.
type Keys interface{ SigningKey(string) ([]byte, bool) }

// Auth uses Now once per call. A nil Now uses the current wall clock.
type Auth struct {
	Keys Keys
	Now  func() time.Time
}

func (a Auth) now() int64 {
	if a.Now != nil {
		return a.Now().Unix()
	}
	return time.Now().Unix()
}

// Sign generates new attempt headers, preserving the caller's operation/body.
func (a Auth) Sign(operation, ref string, body []byte) (http.Header, error) {
	if !uuid(operation) || !keyRef(ref) || a.Keys == nil {
		return nil, ErrInvalid
	}
	key, ok := a.Keys.SigningKey(ref)
	if !ok || len(key) != 32 {
		return nil, ErrInvalid
	}
	timestamp := strconv.FormatInt(a.now(), 10)
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	h.Set("Idempotency-Key", operation)
	h.Set("Webhook-Id", operation)
	h.Set("Webhook-Timestamp", timestamp)
	h.Set("Webhook-Key-Id", ref)
	h.Set("Webhook-Signature", "v1="+hex.EncodeToString(digest(key, timestamp, operation, body)))
	return h, nil
}

// Verify returns the authenticated operation UUID. All failures are generic;
// callers map ErrInvalid to HTTP 401 before any business work. Body limiting
// and strict envelope parsing are responsibilities of the receiver handler.
func (a Auth) Verify(h http.Header, body []byte) (string, error) {
	id, ok := single(h, "Webhook-Id", 36)
	if !ok || !uuid(id) {
		return "", ErrInvalid
	}
	idem, ok := single(h, "Idempotency-Key", 36)
	if !ok || idem != id {
		return "", ErrInvalid
	}
	ref, ok := single(h, "Webhook-Key-Id", 128)
	if !ok || !keyRef(ref) || a.Keys == nil {
		return "", ErrInvalid
	}
	timestamp, ok := single(h, "Webhook-Timestamp", 19)
	if !ok {
		return "", ErrInvalid
	}
	for _, c := range timestamp {
		if c < '0' || c > '9' {
			return "", ErrInvalid
		}
	}
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || strconv.FormatInt(seconds, 10) != timestamp || !within(a.now(), seconds) {
		return "", ErrInvalid
	}
	signature, ok := single(h, "Webhook-Signature", 67)
	if !ok || len(signature) != 67 || !strings.HasPrefix(signature, "v1=") {
		return "", ErrInvalid
	}
	for _, c := range signature[3:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", ErrInvalid
		}
	}
	supplied, err := hex.DecodeString(signature[3:])
	if err != nil {
		return "", ErrInvalid
	}
	key, ok := a.Keys.SigningKey(ref)
	if !ok || len(key) != 32 || !hmac.Equal(supplied, digest(key, timestamp, id, body)) {
		return "", ErrInvalid
	}
	return id, nil
}

func digest(key []byte, timestamp, id string, body []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(timestamp + "." + id + "."))
	mac.Write(body)
	return mac.Sum(nil)
}

// Ordered unsigned subtraction avoids signed overflow even at int64 extremes.
func within(now, timestamp int64) bool {
	if timestamp >= now {
		return uint64(timestamp)-uint64(now) <= 300
	}
	return uint64(now)-uint64(timestamp) <= 300
}

// Scan case-insensitively as Header maps may also be constructed directly.
func single(h http.Header, name string, max int) (string, bool) {
	var value string
	count := 0
	for k, values := range h {
		if strings.EqualFold(k, name) {
			for _, v := range values {
				count++
				value = v
			}
			if len(values) == 0 {
				return "", false
			}
		}
	}
	return value, count == 1 && len(value) > 0 && len(value) <= max
}
func keyRef(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}
func uuid(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
