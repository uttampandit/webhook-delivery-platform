package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a tenant-scoped lookup finds no row.
var ErrNotFound = errors.New("not found")

// ErrInvalidCursor is returned when the cursor cannot be decoded or validated.
var ErrInvalidCursor = errors.New("invalid cursor")

// ErrInvalidLimit is returned when the requested page size is out of range.
var ErrInvalidLimit = errors.New("invalid limit")

const (
	DefaultPageLimit = 20
	MaxPageLimit     = 100
	MaxCursorBytes   = 512
)

// EventCursor encodes the timestamp+UUID position for keyset pagination.
type EventCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

// EncodeCursor base64url-encodes the cursor for transmission.
func EncodeCursor(c EventCursor) (string, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeCursor parses and validates a base64url cursor string.
// Returns ErrInvalidCursor for any malformed, oversized, or invalid value.
func DecodeCursor(encoded string) (EventCursor, error) {
	if len(encoded) == 0 || len(encoded) > MaxCursorBytes {
		return EventCursor{}, ErrInvalidCursor
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return EventCursor{}, ErrInvalidCursor
	}
	var c EventCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return EventCursor{}, ErrInvalidCursor
	}
	if !isCanonicalUUID(c.ID) {
		return EventCursor{}, ErrInvalidCursor
	}
	if c.CreatedAt.IsZero() {
		return EventCursor{}, ErrInvalidCursor
	}
	return c, nil
}

// DeliveryListItem is the compact delivery view returned in the event list.
type DeliveryListItem struct {
	ID                     string
	DestinationCode        string
	Status                 string
	HasUnconfirmedAttempts bool
}

// EventListItem is the compact event view returned by the list endpoint.
// Payloads and attempt history are not included (gate 4).
type EventListItem struct {
	ID         string
	Type       string
	CreatedAt  time.Time
	Deliveries []DeliveryListItem
}

// AttemptDetail is an attempt row returned on the event detail page.
type AttemptDetail struct {
	ID             string
	AttemptNo      int
	CycleNo        int
	CycleAttemptNo int
	Outcome        string
	StartedAt      time.Time
	FinishedAt     *time.Time
	HTTPStatus     *int
	ErrorCode      *string
	DurationMs     *int64
	RetryDelayMs   *int64
}

// ReplayDetail is a replay row returned on the event detail page.
type ReplayDetail struct {
	ID        string
	FromCycle int
	ToCycle   int
	Actor     string
	CreatedAt time.Time
}

// DeliveryDetail is the full delivery view returned on the event detail page.
type DeliveryDetail struct {
	ID                     string
	OperationID            string // same value as ID per spec
	DestinationCode        string
	Status                 string
	CycleNo                int
	CycleAttemptCount      int
	LifetimeAttemptCount   int
	MaxAttempts            int
	NextAttemptAt          *time.Time // non-nil only when pending
	LeaseUntil             *time.Time // non-nil only when in_flight
	ConfirmedAt            *time.Time
	TerminalReason         *string
	HasUnconfirmedAttempts bool
	Attempts               []AttemptDetail
	Replays                []ReplayDetail
}

// EventDetail is the full event view returned by the detail endpoint.
type EventDetail struct {
	ID         string
	Type       string
	CreatedAt  time.Time
	Payload    json.RawMessage // derived from wire_body's "data" field; never hashes or keys
	Deliveries []DeliveryDetail
}

// QueryRepository handles read-only tenant-scoped queries.
type QueryRepository struct {
	pool *pgxpool.Pool
}

// NewQueryRepository returns a QueryRepository backed by the given pool.
func NewQueryRepository(pool *pgxpool.Pool) *QueryRepository {
	return &QueryRepository{pool: pool}
}

// ListEvents returns a page of events for the tenant, ordered by
// (created_at DESC, id DESC). An empty cursor starts from the most recent.
// Payloads and attempt history are NOT fetched here (acceptance gate 4).
func (r *QueryRepository) ListEvents(ctx context.Context, tenantID string, limit int, cursor *EventCursor) ([]EventListItem, *EventCursor, error) {
	if r == nil || r.pool == nil {
		return nil, nil, ErrStoreUnavailable
	}
	if limit < 1 || limit > MaxPageLimit {
		return nil, nil, ErrInvalidLimit
	}
	tenantUUID, err := parseCanonicalUUID(tenantID)
	if err != nil {
		return nil, nil, ErrInvalidInput
	}

	// Fetch limit+1 rows to detect whether a next page exists.
	fetchLimit := limit + 1

	type rawEvent struct {
		id        string
		eventType string
		createdAt time.Time
	}

	var rawEvents []rawEvent

	if cursor == nil {
		rows, err := r.pool.Query(ctx, `
			SELECT id::text, event_type, created_at
			FROM events
			WHERE tenant_id = $1
			ORDER BY created_at DESC, id DESC
			LIMIT $2`,
			tenantUUID, fetchLimit)
		if err != nil {
			return nil, nil, ErrStoreUnavailable
		}
		defer rows.Close()
		for rows.Next() {
			var ev rawEvent
			if err := rows.Scan(&ev.id, &ev.eventType, &ev.createdAt); err != nil {
				return nil, nil, ErrStoreUnavailable
			}
			rawEvents = append(rawEvents, ev)
		}
		if err := rows.Err(); err != nil {
			return nil, nil, ErrStoreUnavailable
		}
	} else {
		cursorUUID, err := parseCanonicalUUID(cursor.ID)
		if err != nil {
			return nil, nil, ErrInvalidCursor
		}
		rows, err := r.pool.Query(ctx, `
			SELECT id::text, event_type, created_at
			FROM events
			WHERE tenant_id = $1
			  AND (created_at, id) < ($2, $3)
			ORDER BY created_at DESC, id DESC
			LIMIT $4`,
			tenantUUID, cursor.CreatedAt.UTC(), cursorUUID, fetchLimit)
		if err != nil {
			return nil, nil, ErrStoreUnavailable
		}
		defer rows.Close()
		for rows.Next() {
			var ev rawEvent
			if err := rows.Scan(&ev.id, &ev.eventType, &ev.createdAt); err != nil {
				return nil, nil, ErrStoreUnavailable
			}
			rawEvents = append(rawEvents, ev)
		}
		if err := rows.Err(); err != nil {
			return nil, nil, ErrStoreUnavailable
		}
	}

	var nextCursor *EventCursor
	if len(rawEvents) > limit {
		last := rawEvents[limit-1]
		rawEvents = rawEvents[:limit]
		nextCursor = &EventCursor{CreatedAt: last.createdAt.UTC(), ID: last.id}
	}

	if len(rawEvents) == 0 {
		return []EventListItem{}, nextCursor, nil
	}

	// Build a UUID slice and reverse-index for delivery lookup.
	eventUUIDs := make([]pgtype.UUID, 0, len(rawEvents))
	eventIndex := make(map[string]int, len(rawEvents))
	for i, ev := range rawEvents {
		uuid, err := parseCanonicalUUID(ev.id)
		if err != nil {
			return nil, nil, ErrStoreUnavailable
		}
		eventUUIDs = append(eventUUIDs, uuid)
		eventIndex[ev.id] = i
	}

	// Single query for delivery summaries; payloads and attempt rows excluded.
	deliveryRows, err := r.pool.Query(ctx, `
		SELECT
			d.id::text,
			d.event_id::text,
			dest.code,
			d.status,
			EXISTS (
				SELECT 1 FROM attempts a
				WHERE a.delivery_id = d.id
				  AND a.outcome IN ('running','unconfirmed')
			) AS has_unconfirmed_attempts
		FROM deliveries d
		JOIN destinations dest ON dest.id = d.destination_id AND dest.tenant_id = d.tenant_id
		WHERE d.tenant_id = $1
		  AND d.event_id = ANY($2)
		ORDER BY d.event_id, d.destination_id`,
		tenantUUID, eventUUIDs)
	if err != nil {
		return nil, nil, ErrStoreUnavailable
	}
	defer deliveryRows.Close()

	items := make([]EventListItem, len(rawEvents))
	for i, ev := range rawEvents {
		items[i] = EventListItem{
			ID:         ev.id,
			Type:       ev.eventType,
			CreatedAt:  ev.createdAt.UTC(),
			Deliveries: []DeliveryListItem{},
		}
	}

	for deliveryRows.Next() {
		var delivID, eventID, code, status string
		var hasUnconfirmed bool
		if err := deliveryRows.Scan(&delivID, &eventID, &code, &status, &hasUnconfirmed); err != nil {
			return nil, nil, ErrStoreUnavailable
		}
		idx, ok := eventIndex[eventID]
		if !ok {
			continue
		}
		items[idx].Deliveries = append(items[idx].Deliveries, DeliveryListItem{
			ID:                     delivID,
			DestinationCode:        code,
			Status:                 status,
			HasUnconfirmedAttempts: hasUnconfirmed,
		})
	}
	if err := deliveryRows.Err(); err != nil {
		return nil, nil, ErrStoreUnavailable
	}

	return items, nextCursor, nil
}

// GetEvent returns the full event detail including payload, deliveries,
// attempts, and replays. Returns ErrNotFound when the ID does not exist
// or belongs to a different tenant.
func (r *QueryRepository) GetEvent(ctx context.Context, tenantID, eventID string) (EventDetail, error) {
	if r == nil || r.pool == nil {
		return EventDetail{}, ErrStoreUnavailable
	}
	tenantUUID, err := parseCanonicalUUID(tenantID)
	if err != nil {
		return EventDetail{}, ErrInvalidInput
	}
	eventUUID, err := parseCanonicalUUID(eventID)
	if err != nil {
		return EventDetail{}, ErrInvalidInput
	}

	// Fetch event row including wire_body for payload extraction.
	var evType string
	var createdAt time.Time
	var wireBody []byte
	err = r.pool.QueryRow(ctx, `
		SELECT event_type, created_at, wire_body
		FROM events
		WHERE tenant_id = $1 AND id = $2`,
		tenantUUID, eventUUID).Scan(&evType, &createdAt, &wireBody)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return EventDetail{}, ErrNotFound
		}
		return EventDetail{}, ErrStoreUnavailable
	}

	payload, err := extractWireBodyData(wireBody)
	if err != nil {
		return EventDetail{}, ErrStoreUnavailable
	}

	// Fetch deliveries (tenant-scoped).
	deliveryRows, err := r.pool.Query(ctx, `
		SELECT
			d.id::text,
			dest.code,
			d.status,
			d.cycle_no,
			d.cycle_attempt_count,
			d.lifetime_attempt_count,
			d.max_attempts,
			CASE WHEN d.status = 'pending' THEN d.next_attempt_at ELSE NULL END,
			CASE WHEN d.status = 'in_flight' THEN d.lease_until ELSE NULL END,
			d.confirmed_at,
			d.terminal_reason,
			EXISTS (
				SELECT 1 FROM attempts a
				WHERE a.delivery_id = d.id
				  AND a.outcome IN ('running','unconfirmed')
			) AS has_unconfirmed_attempts
		FROM deliveries d
		JOIN destinations dest ON dest.id = d.destination_id AND dest.tenant_id = d.tenant_id
		WHERE d.tenant_id = $1 AND d.event_id = $2
		ORDER BY d.destination_id`,
		tenantUUID, eventUUID)
	if err != nil {
		return EventDetail{}, ErrStoreUnavailable
	}
	defer deliveryRows.Close()

	type rawDelivery struct {
		id                     string
		code                   string
		status                 string
		cycleNo                int
		cycleAttemptCount      int
		lifetimeAttemptCount   int
		maxAttempts            int
		nextAttemptAt          *time.Time
		leaseUntil             *time.Time
		confirmedAt            *time.Time
		terminalReason         *string
		hasUnconfirmedAttempts bool
	}

	rawDeliveries := make([]rawDelivery, 0, 2)
	deliveryUUIDs := make([]pgtype.UUID, 0, 2)
	delivIDIndex := make(map[string]int)

	for deliveryRows.Next() {
		var dr rawDelivery
		if err := deliveryRows.Scan(
			&dr.id, &dr.code, &dr.status,
			&dr.cycleNo, &dr.cycleAttemptCount, &dr.lifetimeAttemptCount, &dr.maxAttempts,
			&dr.nextAttemptAt, &dr.leaseUntil, &dr.confirmedAt, &dr.terminalReason,
			&dr.hasUnconfirmedAttempts,
		); err != nil {
			return EventDetail{}, ErrStoreUnavailable
		}
		delivIDIndex[dr.id] = len(rawDeliveries)
		rawDeliveries = append(rawDeliveries, dr)
		uuid, err := parseCanonicalUUID(dr.id)
		if err != nil {
			return EventDetail{}, ErrStoreUnavailable
		}
		deliveryUUIDs = append(deliveryUUIDs, uuid)
	}
	if err := deliveryRows.Err(); err != nil {
		return EventDetail{}, ErrStoreUnavailable
	}

	// Fetch attempts for all deliveries in a single query.
	attemptMap := make(map[string][]AttemptDetail, len(rawDeliveries))
	if len(deliveryUUIDs) > 0 {
		aRows, err := r.pool.Query(ctx, `
			SELECT
				id::text, delivery_id::text,
				attempt_no, cycle_no, cycle_attempt_no, outcome,
				started_at, finished_at, http_status, error_code, duration_ms, retry_delay_ms
			FROM attempts
			WHERE delivery_id = ANY($1)
			ORDER BY delivery_id, attempt_no`,
			deliveryUUIDs)
		if err != nil {
			return EventDetail{}, ErrStoreUnavailable
		}
		defer aRows.Close()

		for aRows.Next() {
			var at AttemptDetail
			var deliveryID string
			if err := aRows.Scan(
				&at.ID, &deliveryID,
				&at.AttemptNo, &at.CycleNo, &at.CycleAttemptNo, &at.Outcome,
				&at.StartedAt, &at.FinishedAt, &at.HTTPStatus, &at.ErrorCode, &at.DurationMs, &at.RetryDelayMs,
			); err != nil {
				return EventDetail{}, ErrStoreUnavailable
			}
			at.StartedAt = at.StartedAt.UTC()
			if at.FinishedAt != nil {
				utc := at.FinishedAt.UTC()
				at.FinishedAt = &utc
			}
			attemptMap[deliveryID] = append(attemptMap[deliveryID], at)
		}
		if err := aRows.Err(); err != nil {
			return EventDetail{}, ErrStoreUnavailable
		}
	}

	// Fetch replays for all deliveries in a single query.
	replayMap := make(map[string][]ReplayDetail, len(rawDeliveries))
	if len(deliveryUUIDs) > 0 {
		rRows, err := r.pool.Query(ctx, `
			SELECT
				id::text, delivery_id::text,
				from_cycle, to_cycle, actor, created_at
			FROM replays
			WHERE delivery_id = ANY($1)
			ORDER BY delivery_id, to_cycle`,
			deliveryUUIDs)
		if err != nil {
			return EventDetail{}, ErrStoreUnavailable
		}
		defer rRows.Close()

		for rRows.Next() {
			var rp ReplayDetail
			var deliveryID string
			if err := rRows.Scan(&rp.ID, &deliveryID, &rp.FromCycle, &rp.ToCycle, &rp.Actor, &rp.CreatedAt); err != nil {
				return EventDetail{}, ErrStoreUnavailable
			}
			rp.CreatedAt = rp.CreatedAt.UTC()
			replayMap[deliveryID] = append(replayMap[deliveryID], rp)
		}
		if err := rRows.Err(); err != nil {
			return EventDetail{}, ErrStoreUnavailable
		}
	}

	result := EventDetail{
		ID:         eventID,
		Type:       evType,
		CreatedAt:  createdAt.UTC(),
		Payload:    payload,
		Deliveries: make([]DeliveryDetail, 0, len(rawDeliveries)),
	}

	for _, dr := range rawDeliveries {
		dd := DeliveryDetail{
			ID:                     dr.id,
			OperationID:            dr.id,
			DestinationCode:        dr.code,
			Status:                 dr.status,
			CycleNo:                dr.cycleNo,
			CycleAttemptCount:      dr.cycleAttemptCount,
			LifetimeAttemptCount:   dr.lifetimeAttemptCount,
			MaxAttempts:            dr.maxAttempts,
			HasUnconfirmedAttempts: dr.hasUnconfirmedAttempts,
			Attempts:               attemptMap[dr.id],
			Replays:                replayMap[dr.id],
		}
		if dd.Attempts == nil {
			dd.Attempts = []AttemptDetail{}
		}
		if dd.Replays == nil {
			dd.Replays = []ReplayDetail{}
		}
		if dr.nextAttemptAt != nil {
			utc := dr.nextAttemptAt.UTC()
			dd.NextAttemptAt = &utc
		}
		if dr.leaseUntil != nil {
			utc := dr.leaseUntil.UTC()
			dd.LeaseUntil = &utc
		}
		if dr.confirmedAt != nil {
			utc := dr.confirmedAt.UTC()
			dd.ConfirmedAt = &utc
		}
		if dr.terminalReason != nil {
			s := *dr.terminalReason
			dd.TerminalReason = &s
		}
		result.Deliveries = append(result.Deliveries, dd)
	}

	return result, nil
}

// extractWireBodyData pulls the "data" field from the stored wire body JSON.
// The wire body has schema {"event_id":"...","type":"...","occurred_at":"...","data":{...}}.
// Never exposes the full wire body or hashes.
func extractWireBodyData(wireBody []byte) (json.RawMessage, error) {
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(wireBody, &envelope); err != nil {
		return nil, err
	}
	if envelope.Data == nil {
		return json.RawMessage("{}"), nil
	}
	return envelope.Data, nil
}
