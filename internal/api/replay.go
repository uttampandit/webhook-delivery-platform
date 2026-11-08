package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

func (server *Server) replayDelivery(w http.ResponseWriter, r *http.Request, id string) {
	if !isValidUUIDParam(id) || strings.Count(id, "-") != 4 {
		writeError(w, r, codeInvalidRequest)
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || store.ValidateIdempotencyKey(keys[0]) != nil {
		writeError(w, r, codeInvalidRequest)
		return
	}
	body := http.MaxBytesReader(w, r.Body, store.MaxEventRequestBytes)
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, r, codeBodyTooLarge)
		} else {
			writeError(w, r, codeInvalidJSON)
		}
		return
	}
	if !json.Valid(raw) {
		writeError(w, r, codeInvalidJSON)
		return
	}
	trimmed := bytes.TrimSpace(raw)
	var fields map[string]json.RawMessage
	if len(trimmed) == 0 || trimmed[0] != '{' || json.Unmarshal(raw, &fields) != nil || len(fields) != 0 {
		writeError(w, r, codeInvalidRequest)
		return
	}
	if server.repository == nil {
		writeError(w, r, codeDependencyUnavailable)
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeError(w, r, codeInternalError)
		return
	}
	timeout := server.settings.DBStatementTimeout()
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	result, err := server.repository.Replay(ctx, principal.TenantID, id, keys[0], principal.Actor)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, r, codeNotFound)
		case errors.Is(err, store.ErrInvalidState):
			writeError(w, r, codeInvalidState)
		default:
			writeCreateEventError(w, r, err)
		}
		return
	}
	status := http.StatusAccepted
	if result.Deduplicated {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(result)
}
