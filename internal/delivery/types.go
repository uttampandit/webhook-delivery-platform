// Package delivery defines the values passed between scheduling and sending.
package delivery

import (
	"errors"
	"time"
)

// ClaimedDelivery is a confirmed, committed claim. DeliveryID is the stable
// outbound operation ID; Generation and ClaimToken fence database writes only.
// WireBody owns a copy of the authoritative persisted bytes. No transaction or
// pooled connection is held by this value.
type ClaimedDelivery struct {
	DeliveryID     string
	EventID        string
	AttemptID      string
	CycleNo        int
	CycleAttemptNo int
	AttemptNo      int // Lifetime attempt number; never resets on replay.
	Generation     int64
	ClaimToken     string
	LeaseUntil     time.Time
	StartedAt      time.Time
	ScheduledAt    time.Time
	EventCreatedAt time.Time
	URLSnapshot    string
	KeyRefSnapshot string
	WireBody       []byte
}

type Outcome string

const (
	Acknowledged Outcome = "acknowledged"
	HTTPFailure  Outcome = "http_failure"
	Unconfirmed  Outcome = "unconfirmed"
	LocalFailure Outcome = "local_failure"
)

// SendResult carries bounded protocol observations, never response body text.
// A nil HTTPStatus means no valid status was received. Duration is monotonic
// elapsed time measured by the sender; RetryAfter preserves separate values.
type SendResult struct {
	Outcome    Outcome
	HTTPStatus *int
	ErrorCode  string
	Duration   time.Duration
	RetryAfter []string
}

type FinalizationResult struct {
	Outcome                 Outcome
	Status                  string
	TerminalReason          string
	RetryDelay              time.Duration
	NextAttemptAt           *time.Time
	DeliveryLatency         time.Duration
	DeliveryLatencyObserved bool
}

var ErrStaleFinalization = errors.New("stale delivery finalization rejected")
