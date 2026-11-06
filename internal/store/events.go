package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	MaxEventRequestBytes = 65536
	maxEventWireBytes    = 98304
	maxEventJSONDepth    = 32
)

var (
	ErrInvalidInput         = errors.New("invalid event input")
	ErrInvalidJSON          = errors.New("invalid event JSON")
	ErrUnknownDestination   = errors.New("unknown destination")
	ErrIdempotencyConflict  = errors.New("idempotency key conflicts with a different request")
	ErrStoreUnavailable     = errors.New("event store unavailable")
	ErrAmbiguousCommit      = errors.New("event acceptance commit outcome is unknown")
	ErrOutgoingBodyTooLarge = errors.New("outgoing event body exceeds limit")
)

// ValidatedEventRequest can only be populated by ValidateEventRequest. Its raw
// request bytes and parsed payload stay private so callers cannot mismatch the
// bytes used for idempotency with the event data sent to receivers.
type ValidatedEventRequest struct {
	valid          bool
	rawBody        []byte
	requestHash    [sha256.Size]byte
	eventType      string
	payload        json.RawMessage
	destinationIDs []string
}

// ValidateIdempotencyKey checks the stable key format before any database work.
func ValidateIdempotencyKey(key string) error {
	if len(key) < 1 || len(key) > 128 {
		return ErrInvalidInput
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return ErrInvalidInput
		}
	}
	return nil
}

// ValidateEventRequest parses the request without converting payload numbers
// through float64, and rejects duplicate keys before decoding the top-level DTO.
func ValidateEventRequest(raw []byte) (ValidatedEventRequest, error) {
	if len(raw) == 0 || !utf8.Valid(raw) {
		return ValidatedEventRequest{}, ErrInvalidJSON
	}
	if len(raw) > MaxEventRequestBytes {
		return ValidatedEventRequest{}, ErrInvalidInput
	}
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return ValidatedEventRequest{}, ErrInvalidJSON
	}

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return ValidatedEventRequest{}, ErrInvalidInput
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return ValidatedEventRequest{}, ErrInvalidJSON
	}
	if fields == nil || len(fields) != 3 {
		return ValidatedEventRequest{}, ErrInvalidInput
	}
	for name := range fields {
		if name != "type" && name != "payload" && name != "destination_ids" {
			return ValidatedEventRequest{}, ErrInvalidInput
		}
	}

	var eventType string
	if err := json.Unmarshal(fields["type"], &eventType); err != nil || !validEventType(eventType) {
		return ValidatedEventRequest{}, ErrInvalidInput
	}

	payload := bytes.TrimSpace(fields["payload"])
	if len(payload) == 0 || payload[0] != '{' || bytes.Equal(payload, []byte("null")) {
		return ValidatedEventRequest{}, ErrInvalidInput
	}

	var destinationIDs []string
	if err := json.Unmarshal(fields["destination_ids"], &destinationIDs); err != nil || len(destinationIDs) < 1 || len(destinationIDs) > 2 {
		return ValidatedEventRequest{}, ErrInvalidInput
	}
	seenDestinations := make(map[string]struct{}, len(destinationIDs))
	for _, id := range destinationIDs {
		if !isCanonicalUUID(id) {
			return ValidatedEventRequest{}, ErrInvalidInput
		}
		if _, exists := seenDestinations[id]; exists {
			return ValidatedEventRequest{}, ErrInvalidInput
		}
		seenDestinations[id] = struct{}{}
	}

	bodyCopy := append([]byte(nil), raw...)
	payloadCopy := append(json.RawMessage(nil), fields["payload"]...)
	destinationCopy := append([]string(nil), destinationIDs...)
	return ValidatedEventRequest{
		valid:          true,
		rawBody:        bodyCopy,
		requestHash:    sha256.Sum256(bodyCopy),
		eventType:      eventType,
		payload:        payloadCopy,
		destinationIDs: destinationCopy,
	}, nil
}

// DestinationIDs returns a copy of the selected UUIDs for tenant-registry
// authorization at the API boundary.
func (request ValidatedEventRequest) DestinationIDs() []string {
	return append([]string(nil), request.destinationIDs...)
}

// AcceptedEvent is the stable acceptance result returned for both new and
// duplicate requests. Delivery IDs are ordered by destination UUID.
type AcceptedEvent struct {
	EventID      string
	Deduplicated bool
	CreatedAt    time.Time
	DeliveryIDs  []string
}

// EventRepository persists accepted events and their immutable delivery
// snapshots in one short PostgreSQL transaction.
type EventRepository struct {
	pool *pgxpool.Pool
}

func NewEventRepository(pool *pgxpool.Pool) *EventRepository {
	return &EventRepository{pool: pool}
}

// AcceptEvent accepts a validated request under a tenant-scoped idempotency
// key. A conflict is read in a new READ COMMITTED statement after the insert.
func (repository *EventRepository) AcceptEvent(ctx context.Context, tenantID, key string, request ValidatedEventRequest) (AcceptedEvent, error) {
	if repository == nil || repository.pool == nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	if !request.valid || len(request.rawBody) == 0 || len(request.rawBody) > MaxEventRequestBytes ||
		!isCanonicalUUID(tenantID) || ValidateIdempotencyKey(key) != nil {
		return AcceptedEvent{}, ErrInvalidInput
	}

	tenantUUID, _ := parseCanonicalUUID(tenantID)
	eventID, eventUUID, err := newEventUUID()
	if err != nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	tx, err := repository.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	finished := false
	defer func() {
		if !finished {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tx.Rollback(rollbackCtx)
		}
	}()

	var createdAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&createdAt); err != nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	createdAt = createdAt.UTC()
	wireBody, err := marshalOutgoingEvent(eventID, request.eventType, createdAt, request.payload)
	if err != nil {
		return AcceptedEvent{}, ErrInvalidInput
	}
	if len(wireBody) > maxEventWireBytes {
		return AcceptedEvent{}, ErrOutgoingBodyTooLarge
	}

	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO events (
			id, tenant_id, idempotency_key, request_body, request_hash,
			event_type, wire_body, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
		RETURNING id::text`,
		eventUUID, tenantUUID, key, request.rawBody, request.requestHash[:], request.eventType, wireBody, createdAt,
	).Scan(&insertedID)
	if err == nil {
		eventID = insertedID
		destinations, err := loadDestinationSnapshots(ctx, tx, tenantUUID, request.destinationIDs)
		if err != nil {
			return AcceptedEvent{}, err
		}
		for _, destination := range destinations {
			_, deliveryUUID, err := newEventUUID()
			if err != nil {
				return AcceptedEvent{}, ErrStoreUnavailable
			}
			destinationUUID, err := parseCanonicalUUID(destination.id)
			if err != nil {
				return AcceptedEvent{}, ErrStoreUnavailable
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO deliveries (
					id, tenant_id, event_id, destination_id, url_snapshot, key_ref_snapshot,
					next_attempt_at, created_at, updated_at
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $7)`,
				deliveryUUID, tenantUUID, eventUUID, destinationUUID, destination.url, destination.keyRef, createdAt); err != nil {
				return AcceptedEvent{}, ErrStoreUnavailable
			}
		}
		result, err := readAcceptedEvent(ctx, tx, tenantID, eventID, false)
		if err != nil {
			return AcceptedEvent{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return AcceptedEvent{}, ErrAmbiguousCommit
		}
		finished = true
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return AcceptedEvent{}, ErrStoreUnavailable
	}

	// This separate SELECT gets a fresh READ COMMITTED snapshot after a
	// concurrent winner's unique-key insert has committed.
	var existingID string
	var existingBody, existingHash []byte
	var existingCreatedAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT id::text, request_body, request_hash, created_at
		FROM events
		WHERE tenant_id = $1 AND idempotency_key = $2`, tenantUUID, key).
		Scan(&existingID, &existingBody, &existingHash, &existingCreatedAt)
	if err != nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	if !bytes.Equal(existingHash, request.requestHash[:]) || !bytes.Equal(existingBody, request.rawBody) {
		return AcceptedEvent{}, ErrIdempotencyConflict
	}
	result, err := readAcceptedEvent(ctx, tx, tenantID, existingID, true)
	if err != nil {
		return AcceptedEvent{}, err
	}
	result.CreatedAt = existingCreatedAt.UTC()
	if err := tx.Commit(ctx); err != nil {
		return AcceptedEvent{}, ErrAmbiguousCommit
	}
	finished = true
	return result, nil
}

type destinationSnapshot struct {
	id     string
	url    string
	keyRef string
}

func loadDestinationSnapshots(ctx context.Context, tx pgx.Tx, tenantUUID pgtype.UUID, destinationIDs []string) ([]destinationSnapshot, error) {
	if len(destinationIDs) < 1 || len(destinationIDs) > 2 {
		return nil, ErrInvalidInput
	}
	placeholders := make([]string, 0, len(destinationIDs))
	args := make([]any, 0, len(destinationIDs)+1)
	args = append(args, tenantUUID)
	for i, id := range destinationIDs {
		destinationUUID, err := parseCanonicalUUID(id)
		if err != nil {
			return nil, ErrInvalidInput
		}
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+2))
		args = append(args, destinationUUID)
	}
	query := `SELECT id::text, url, key_ref FROM destinations WHERE tenant_id = $1 AND id IN (` + strings.Join(placeholders, ",") + `) ORDER BY id`
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, ErrStoreUnavailable
	}
	defer rows.Close()

	destinations := make([]destinationSnapshot, 0, len(destinationIDs))
	for rows.Next() {
		var destination destinationSnapshot
		if err := rows.Scan(&destination.id, &destination.url, &destination.keyRef); err != nil {
			return nil, ErrStoreUnavailable
		}
		destinations = append(destinations, destination)
	}
	if err := rows.Err(); err != nil {
		return nil, ErrStoreUnavailable
	}
	if len(destinations) != len(destinationIDs) {
		return nil, ErrUnknownDestination
	}
	return destinations, nil
}

func readAcceptedEvent(ctx context.Context, tx pgx.Tx, tenantID, eventID string, deduplicated bool) (AcceptedEvent, error) {
	tenantUUID, err := parseCanonicalUUID(tenantID)
	if err != nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	eventUUID, err := parseCanonicalUUID(eventID)
	if err != nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	rows, err := tx.Query(ctx, `
		SELECT id::text
		FROM deliveries
		WHERE tenant_id = $1 AND event_id = $2
		ORDER BY destination_id`, tenantUUID, eventUUID)
	if err != nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	defer rows.Close()
	deliveryIDs := make([]string, 0, 2)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return AcceptedEvent{}, ErrStoreUnavailable
		}
		deliveryIDs = append(deliveryIDs, id)
	}
	if err := rows.Err(); err != nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	var createdAt time.Time
	if err := tx.QueryRow(ctx, `SELECT created_at FROM events WHERE tenant_id = $1 AND id = $2`, tenantUUID, eventUUID).Scan(&createdAt); err != nil {
		return AcceptedEvent{}, ErrStoreUnavailable
	}
	return AcceptedEvent{EventID: eventID, Deduplicated: deduplicated, CreatedAt: createdAt.UTC(), DeliveryIDs: deliveryIDs}, nil
}

type outgoingEvent struct {
	EventID    string          `json:"event_id"`
	Type       string          `json:"type"`
	OccurredAt string          `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

func marshalOutgoingEvent(eventID, eventType string, occurredAt time.Time, payload json.RawMessage) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(outgoingEvent{
		EventID: eventID, Type: eventType, OccurredAt: occurredAt.UTC().Format(time.RFC3339Nano), Data: payload,
	}); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := consumeJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, depth int) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	if depth+1 > maxEventJSONDepth {
		return ErrInvalidInput
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrInvalidInput
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrInvalidInput
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrInvalidInput
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalidInput
		}
	default:
		return ErrInvalidInput
	}
	return nil
}

func validEventType(value string) bool {
	if len(value) < 1 || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for i := 1; i < len(value); i++ {
		c := value[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-') {
			return false
		}
	}
	return true
}

func isCanonicalUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for i := 0; i < len(value); i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f')) {
			return false
		}
	}
	return true
}

func newEventUUID() (string, pgtype.UUID, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", pgtype.UUID{}, err
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	hexID := hex.EncodeToString(id[:])
	value := hexID[:8] + "-" + hexID[8:12] + "-" + hexID[12:16] + "-" + hexID[16:20] + "-" + hexID[20:]
	uuid, err := parseCanonicalUUID(value)
	if err != nil {
		return "", pgtype.UUID{}, err
	}
	return value, uuid, nil
}

func parseCanonicalUUID(value string) (pgtype.UUID, error) {
	if !isCanonicalUUID(value) {
		return pgtype.UUID{}, ErrInvalidInput
	}
	var uuid pgtype.UUID
	if err := uuid.Scan(value); err != nil {
		return pgtype.UUID{}, ErrInvalidInput
	}
	return uuid, nil
}
