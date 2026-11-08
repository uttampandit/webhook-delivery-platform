package delivery

import (
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const MaxRetryDelay = 30 * time.Second
const MaxRecordedDuration = 24 * time.Hour

var ErrInvalidResult = errors.New("invalid delivery result or retry policy input")

// Policy is pure when Jitter is supplied. Nil uses concurrency-safe uniform
// nanosecond jitter. Injected functions must be safe for concurrent use.
type Policy struct {
	Jitter func(lower, upper time.Duration) time.Duration
}

// Normalize validates outcome/status combinations and rejects arbitrary error
// text. Long elapsed times are capped defensively before persistence.
func Normalize(r SendResult) (SendResult, error) {
	if r.Duration < 0 {
		return SendResult{}, ErrInvalidResult
	}
	if r.Duration > MaxRecordedDuration {
		r.Duration = MaxRecordedDuration
	}
	status := 0
	if r.HTTPStatus != nil {
		status = *r.HTTPStatus
		if status < 100 || status > 599 {
			return SendResult{}, ErrInvalidResult
		}
	}
	switch r.Outcome {
	case Acknowledged:
		if status < 200 || status > 299 || r.ErrorCode != "" {
			return SendResult{}, ErrInvalidResult
		}
	case HTTPFailure:
		if status == 0 || status >= 200 && status <= 299 || r.ErrorCode != "" && r.ErrorCode != "http_non_2xx" {
			return SendResult{}, ErrInvalidResult
		}
		r.ErrorCode = "http_non_2xx"
	case Unconfirmed:
		if status != 0 {
			return SendResult{}, ErrInvalidResult
		}
		switch r.ErrorCode {
		case "timeout", "network_error", "canceled", "lease_expired":
		default:
			return SendResult{}, ErrInvalidResult
		}
	case LocalFailure:
		if status != 0 {
			return SendResult{}, ErrInvalidResult
		}
		switch r.ErrorCode {
		case "missing_signing_key", "invalid_destination_configuration":
		default:
			return SendResult{}, ErrInvalidResult
		}
	default:
		return SendResult{}, ErrInvalidResult
	}
	return r, nil
}

func (p Policy) Decide(r SendResult, cycleAttempt int, now time.Time) (FinalizationResult, error) {
	r, err := Normalize(r)
	if err != nil || cycleAttempt < 1 || cycleAttempt > 5 {
		return FinalizationResult{}, ErrInvalidResult
	}
	if r.Outcome == Acknowledged {
		return FinalizationResult{Status: "succeeded"}, nil
	}
	if r.Outcome == LocalFailure {
		return FinalizationResult{Status: "dead", TerminalReason: "local_configuration"}, nil
	}
	retryable := r.Outcome == Unconfirmed
	status := 0
	if r.HTTPStatus != nil {
		status = *r.HTTPStatus
		retryable = status == 429 || status >= 500
	}
	if !retryable {
		return FinalizationResult{Status: "dead", TerminalReason: "permanent_http"}, nil
	}
	if cycleAttempt == 5 {
		return FinalizationResult{Status: "dead", TerminalReason: "attempts_exhausted"}, nil
	}
	// Count is checked before shifting; nanosecond arithmetic cannot overflow.
	base := time.Second << uint(cycleAttempt-1)
	if base > MaxRetryDelay {
		base = MaxRetryDelay
	}
	lower := base / 2
	delay := time.Duration(0)
	if p.Jitter != nil {
		delay = p.Jitter(lower, base)
	} else {
		delay = lower + time.Duration(rand.Int64N(int64(base-lower)+1))
	}
	if delay < lower || delay > base {
		return FinalizationResult{}, ErrInvalidResult
	}
	if status == 429 || status == 503 {
		if after := RetryAfterDelay(r.RetryAfter, now); after > delay {
			delay = after
		}
	}
	return FinalizationResult{Status: "pending", RetryDelay: delay}, nil
}

// RetryAfterDelay accepts one bounded delta or HTTP-date. Parse integers before
// duration multiplication, cap before conversion, and ignore ambiguous input.
func RetryAfterDelay(values []string, now time.Time) time.Duration {
	if len(values) != 1 || len(values[0]) > 128 {
		return 0
	}
	value := strings.TrimSpace(values[0])
	if value == "" {
		return 0
	}
	digits := true
	for _, c := range value {
		if c < '0' || c > '9' {
			digits = false
			break
		}
	}
	if digits {
		seconds, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0
		}
		if seconds >= 30 {
			return MaxRetryDelay
		}
		return time.Duration(seconds) * time.Second
	}
	date, err := http.ParseTime(value)
	if err != nil || !date.After(now) {
		return 0
	}
	// time.Sub saturates on duration overflow; cap without multiplying epochs.
	delay := date.Sub(now)
	if delay > MaxRetryDelay {
		return MaxRetryDelay
	}
	return delay
}
