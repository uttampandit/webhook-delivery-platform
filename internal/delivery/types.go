// Package delivery defines the values passed between scheduling and sending.
package delivery

import "time"

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
	URLSnapshot    string
	KeyRefSnapshot string
	WireBody       []byte
}
