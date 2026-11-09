package receiver

import (
	"net/http"
	"testing"
)

// This checks only the unsupported-Hijacker failure path. Actual response loss
// is verified through real HTTP/1.1 sockets plus PostgreSQL integration tests.
type noHijackWriter struct{ writes int }

func (w *noHijackWriter) Header() http.Header         { return make(http.Header) }
func (w *noHijackWriter) WriteHeader(int)             { w.writes++ }
func (w *noHijackWriter) Write(p []byte) (int, error) { w.writes++; return len(p), nil }
func TestDropHijackFailureAbortsWithoutResponse(t *testing.T) {
	w := &noHijackWriter{}
	r, _ := http.NewRequest("POST", "http://example.invalid/webhook", nil)
	defer func() {
		if got := recover(); got != http.ErrAbortHandler {
			t.Fatalf("did not abort unsupported hijack: %v", got)
		}
		if w.writes != 0 {
			t.Fatal("hijack failure substituted HTTP response")
		}
	}()
	dropCommittedResponse(w, r)
}
