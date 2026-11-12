package receiver

import (
	"log/slog"
	"net/http"
)

// Called only after the scenario transaction committed. Write no HTTP status:
// the peer must observe connection loss, not a fabricated failure response.
func dropCommittedResponse(w http.ResponseWriter, r *http.Request) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		slog.ErrorContext(r.Context(), "committed response drop failed", "error_code", "hijack_failed")
		// Abort the server response rather than silently substituting an ordinary
		// HTTP status or letting net/http automatically emit a successful 200.
		panic(http.ErrAbortHandler)
	}
	if err = conn.Close(); err != nil {
		slog.ErrorContext(r.Context(), "committed response close failed", "error_code", "close_failed")
		panic(http.ErrAbortHandler)
	}
}
