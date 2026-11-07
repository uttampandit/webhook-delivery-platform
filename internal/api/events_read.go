package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

// eventListDeliveryDTO is the compact delivery shape in the list response.
type eventListDeliveryDTO struct {
	ID                     string `json:"id"`
	DestinationCode        string `json:"destination_code"`
	Status                 string `json:"status"`
	HasUnconfirmedAttempts bool   `json:"has_unconfirmed_attempts"`
}

// eventListItemDTO is one event in the list response.
type eventListItemDTO struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	CreatedAt  time.Time              `json:"created_at"`
	Deliveries []eventListDeliveryDTO `json:"deliveries"`
}

// eventListResponse is the full list response body.
type eventListResponse struct {
	Items      []eventListItemDTO `json:"items"`
	NextCursor *string            `json:"next_cursor"`
}

// attemptDTO is one attempt in the detail response.
type attemptDTO struct {
	ID             string     `json:"id"`
	AttemptNo      int        `json:"attempt_no"`
	CycleNo        int        `json:"cycle_no"`
	CycleAttemptNo int        `json:"cycle_attempt_no"`
	Outcome        string     `json:"outcome"`
	StartedAt      time.Time  `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at"`
	HTTPStatus     *int       `json:"http_status"`
	ErrorCode      *string    `json:"error_code"`
	DurationMs     *int64     `json:"duration_ms"`
	RetryDelayMs   *int64     `json:"retry_delay_ms"`
}

// replayDTO is one replay in the detail response.
type replayDTO struct {
	ID        string    `json:"id"`
	FromCycle int       `json:"from_cycle"`
	ToCycle   int       `json:"to_cycle"`
	Actor     string    `json:"actor"`
	CreatedAt time.Time `json:"created_at"`
}

// eventDetailDeliveryDTO is the full delivery shape in the detail response.
type eventDetailDeliveryDTO struct {
	ID                     string       `json:"id"`
	OperationID            string       `json:"operation_id"`
	DestinationCode        string       `json:"destination_code"`
	Status                 string       `json:"status"`
	CycleNo                int          `json:"cycle_no"`
	CycleAttemptCount      int          `json:"cycle_attempt_count"`
	LifetimeAttemptCount   int          `json:"lifetime_attempt_count"`
	MaxAttempts            int          `json:"max_attempts"`
	NextAttemptAt          *time.Time   `json:"next_attempt_at"`
	LeaseUntil             *time.Time   `json:"lease_until"`
	ConfirmedAt            *time.Time   `json:"confirmed_at"`
	TerminalReason         *string      `json:"terminal_reason"`
	HasUnconfirmedAttempts bool         `json:"has_unconfirmed_attempts"`
	Attempts               []attemptDTO `json:"attempts"`
	Replays                []replayDTO  `json:"replays"`
}

// eventDetailResponse is the full event detail response body.
type eventDetailResponse struct {
	ID         string                   `json:"id"`
	Type       string                   `json:"type"`
	CreatedAt  time.Time                `json:"created_at"`
	Payload    json.RawMessage          `json:"payload"`
	Deliveries []eventDetailDeliveryDTO `json:"deliveries"`
}

// listEvents handles GET /v1/events with optional limit and cursor query params.
func (server *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	if server.repository == nil {
		writeError(w, r, codeDependencyUnavailable)
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeError(w, r, codeInternalError)
		return
	}

	// Parse limit.
	limit := store.DefaultPageLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > store.MaxPageLimit {
			writeError(w, r, codeInvalidRequest)
			return
		}
		limit = n
	}

	// Parse cursor.
	var cursor *store.EventCursor
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		if len(raw) > store.MaxCursorBytes {
			writeError(w, r, codeInvalidRequest)
			return
		}
		c, err := store.DecodeCursor(raw)
		if err != nil {
			writeError(w, r, codeInvalidRequest)
			return
		}
		cursor = &c
	}

	timeout := server.settings.DBStatementTimeout()
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	items, nextCursor, err := server.repository.ListEvents(ctx, principal.TenantID, limit, cursor)
	if err != nil {
		if errors.Is(err, store.ErrInvalidLimit) || errors.Is(err, store.ErrInvalidCursor) {
			writeError(w, r, codeInvalidRequest)
			return
		}
		writeError(w, r, codeDependencyUnavailable)
		return
	}

	dtoItems := make([]eventListItemDTO, 0, len(items))
	for _, item := range items {
		dtos := make([]eventListDeliveryDTO, 0, len(item.Deliveries))
		for _, d := range item.Deliveries {
			dtos = append(dtos, eventListDeliveryDTO{
				ID:                     d.ID,
				DestinationCode:        d.DestinationCode,
				Status:                 d.Status,
				HasUnconfirmedAttempts: d.HasUnconfirmedAttempts,
			})
		}
		dtoItems = append(dtoItems, eventListItemDTO{
			ID:         item.ID,
			Type:       item.Type,
			CreatedAt:  item.CreatedAt,
			Deliveries: dtos,
		})
	}

	var encodedCursor *string
	if nextCursor != nil {
		encoded, err := store.EncodeCursor(*nextCursor)
		if err != nil {
			writeError(w, r, codeInternalError)
			return
		}
		encodedCursor = &encoded
	}

	resp := eventListResponse{
		Items:      dtoItems,
		NextCursor: encodedCursor,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// getEvent handles GET /v1/events/{event_id}.
func (server *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	if server.repository == nil {
		writeError(w, r, codeDependencyUnavailable)
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeError(w, r, codeInternalError)
		return
	}

	// Extract event ID from path: /v1/events/{event_id}
	eventID := strings.TrimPrefix(r.URL.Path, "/v1/events/")
	if !isValidUUIDParam(eventID) {
		writeError(w, r, codeInvalidRequest)
		return
	}

	timeout := server.settings.DBStatementTimeout()
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	detail, err := server.repository.GetEvent(ctx, principal.TenantID, eventID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, r, codeNotFound)
			return
		}
		if errors.Is(err, store.ErrInvalidInput) {
			writeError(w, r, codeInvalidRequest)
			return
		}
		writeError(w, r, codeDependencyUnavailable)
		return
	}

	deliveries := make([]eventDetailDeliveryDTO, 0, len(detail.Deliveries))
	for _, d := range detail.Deliveries {
		attempts := make([]attemptDTO, 0, len(d.Attempts))
		for _, a := range d.Attempts {
			attempts = append(attempts, attemptDTO{
				ID:             a.ID,
				AttemptNo:      a.AttemptNo,
				CycleNo:        a.CycleNo,
				CycleAttemptNo: a.CycleAttemptNo,
				Outcome:        a.Outcome,
				StartedAt:      a.StartedAt,
				FinishedAt:     a.FinishedAt,
				HTTPStatus:     a.HTTPStatus,
				ErrorCode:      a.ErrorCode,
				DurationMs:     a.DurationMs,
				RetryDelayMs:   a.RetryDelayMs,
			})
		}
		replays := make([]replayDTO, 0, len(d.Replays))
		for _, rp := range d.Replays {
			replays = append(replays, replayDTO{
				ID:        rp.ID,
				FromCycle: rp.FromCycle,
				ToCycle:   rp.ToCycle,
				Actor:     rp.Actor,
				CreatedAt: rp.CreatedAt,
			})
		}
		deliveries = append(deliveries, eventDetailDeliveryDTO{
			ID:                     d.ID,
			OperationID:            d.OperationID,
			DestinationCode:        d.DestinationCode,
			Status:                 d.Status,
			CycleNo:                d.CycleNo,
			CycleAttemptCount:      d.CycleAttemptCount,
			LifetimeAttemptCount:   d.LifetimeAttemptCount,
			MaxAttempts:            d.MaxAttempts,
			NextAttemptAt:          d.NextAttemptAt,
			LeaseUntil:             d.LeaseUntil,
			ConfirmedAt:            d.ConfirmedAt,
			TerminalReason:         d.TerminalReason,
			HasUnconfirmedAttempts: d.HasUnconfirmedAttempts,
			Attempts:               attempts,
			Replays:                replays,
		})
	}

	resp := eventDetailResponse{
		ID:         detail.ID,
		Type:       detail.Type,
		CreatedAt:  detail.CreatedAt,
		Payload:    detail.Payload,
		Deliveries: deliveries,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// isValidUUIDParam checks that the path segment is a canonical lowercase UUID.
func isValidUUIDParam(value string) bool {
	if len(value) != 36 {
		return false
	}
	for _, c := range value {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || c == '-') {
			return false
		}
	}
	return value[8] == '-' && value[13] == '-' && value[18] == '-' && value[23] == '-'
}
