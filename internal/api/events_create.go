package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

type createEventResponse struct {
	EventID      string    `json:"event_id"`
	Deduplicated bool      `json:"deduplicated"`
	CreatedAt    time.Time `json:"created_at"`
	DeliveryIDs  []string  `json:"delivery_ids"`
}

func (server *Server) createEvent(w http.ResponseWriter, r *http.Request) {
	if server.repository == nil {
		writeError(w, r, codeDependencyUnavailable)
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || store.ValidateIdempotencyKey(keys[0]) != nil {
		writeError(w, r, codeInvalidRequest)
		return
	}
	request, err := readValidatedEventRequest(w, r)
	if err != nil {
		writeCreateEventError(w, r, err)
		return
	}
	if !server.destinationsInRegistry(request) {
		writeCreateEventError(w, r, store.ErrUnknownDestination)
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
	accepted, err := server.repository.AcceptEvent(ctx, principal.TenantID, keys[0], request)
	if err != nil {
		writeCreateEventError(w, r, err)
		return
	}
	status := http.StatusAccepted
	if accepted.Deduplicated {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(createEventResponse{
		EventID: accepted.EventID, Deduplicated: accepted.Deduplicated,
		CreatedAt: accepted.CreatedAt.UTC(), DeliveryIDs: accepted.DeliveryIDs,
	})
}

func (server *Server) destinationsInRegistry(request store.ValidatedEventRequest) bool {
	configured := server.registry.Destinations()
	for _, requestedID := range request.DestinationIDs() {
		found := false
		for _, destination := range configured {
			if destination.TenantID == server.registry.Tenant().ID && destination.ID == requestedID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func writeCreateEventError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, errRequestBodyTooLarge), errors.Is(err, store.ErrOutgoingBodyTooLarge):
		writeError(w, r, codeBodyTooLarge)
	case errors.Is(err, store.ErrInvalidJSON):
		writeError(w, r, codeInvalidJSON)
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, r, codeInvalidRequest)
	case errors.Is(err, store.ErrUnknownDestination):
		writeError(w, r, codeUnknownDestination)
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeError(w, r, codeIdempotencyConflict)
	case errors.Is(err, store.ErrAmbiguousCommit), errors.Is(err, store.ErrStoreUnavailable):
		writeError(w, r, codeDependencyUnavailable)
	default:
		writeError(w, r, codeInternalError)
	}
}
