package delivery

import (
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

func statusResult(status int) SendResult {
	outcome := HTTPFailure
	if status >= 200 && status <= 299 {
		outcome = Acknowledged
	}
	return SendResult{Outcome: outcome, HTTPStatus: &status}
}
func TestClassificationEveryHTTPStatus(t *testing.T) {
	p := Policy{Jitter: func(lo, hi time.Duration) time.Duration { return lo }}
	for status := 100; status <= 599; status++ {
		r := statusResult(status)
		d, err := p.Decide(r, 1, time.Now())
		if err != nil {
			t.Fatal(status, err)
		}
		want := "dead"
		reason := "permanent_http"
		if status >= 200 && status <= 299 {
			want = "succeeded"
			reason = ""
		} else if status == 429 || status >= 500 {
			want = "pending"
			reason = ""
		}
		if d.Status != want || d.TerminalReason != reason {
			t.Fatalf("status %d: %+v", status, d)
		}
	}
	for _, code := range []string{"timeout", "network_error", "canceled", "lease_expired"} {
		d, err := p.Decide(SendResult{Outcome: Unconfirmed, ErrorCode: code}, 1, time.Now())
		if err != nil || d.Status != "pending" {
			t.Fatal(code, d, err)
		}
	}
	for _, code := range []string{"missing_signing_key", "invalid_destination_configuration"} {
		d, err := p.Decide(SendResult{Outcome: LocalFailure, ErrorCode: code}, 1, time.Now())
		if err != nil || d.TerminalReason != "local_configuration" {
			t.Fatal(code, d, err)
		}
	}
}
func TestJitterAttemptBudgetAndBounds(t *testing.T) {
	for n := 1; n <= 5; n++ {
		for _, upper := range []bool{false, true} {
			p := Policy{Jitter: func(lo, hi time.Duration) time.Duration {
				if upper {
					return hi
				}
				return lo
			}}
			d, err := p.Decide(statusResult(503), n, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if n == 5 {
				if d.Status != "dead" || d.TerminalReason != "attempts_exhausted" || d.RetryDelay != 0 {
					t.Fatal(d)
				}
				continue
			}
			want := time.Second << uint(n-1)
			if !upper {
				want /= 2
			}
			if d.Status != "pending" || d.RetryDelay != want {
				t.Fatal(n, d, want)
			}
		}
	}
	for _, n := range []int{0, -1, 6, math.MaxInt} {
		if _, err := (Policy{}).Decide(statusResult(503), n, time.Now()); err != ErrInvalidResult {
			t.Fatal("invalid attempt accepted")
		}
	}
	for _, bad := range []time.Duration{-1, 0, MaxRetryDelay, time.Duration(math.MaxInt64)} {
		p := Policy{Jitter: func(lo, hi time.Duration) time.Duration { return bad }}
		if _, err := p.Decide(statusResult(503), 1, time.Now()); err != ErrInvalidResult {
			t.Fatal("out of bound jitter accepted")
		}
	}
	for n := 1; n <= 4; n++ {
		d, err := (Policy{}).Decide(statusResult(500), n, time.Now())
		base := time.Second << uint(n-1)
		if err != nil || d.RetryDelay < base/2 || d.RetryDelay > base {
			t.Fatal("default jitter out of bounds")
		}
	}
}
func TestRetryAfterParsingAndCaps(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		values []string
		want   time.Duration
	}{
		{nil, 0}, {[]string{""}, 0}, {[]string{"0"}, 0}, {[]string{"2"}, 2 * time.Second}, {[]string{" 2 "}, 2 * time.Second}, {[]string{"29"}, 29 * time.Second}, {[]string{"30"}, 30 * time.Second}, {[]string{"31"}, 30 * time.Second}, {[]string{"9223372036854775807"}, 30 * time.Second}, {[]string{"9223372036854775808"}, 0}, {[]string{strings.Repeat("9", 129)}, 0}, {[]string{"-1"}, 0}, {[]string{"+1"}, 0}, {[]string{"1.5"}, 0}, {[]string{"1,2"}, 0}, {[]string{"2", "2"}, 0}, {[]string{"bad"}, 0},
		{[]string{now.Add(4 * time.Second).Format(http.TimeFormat)}, 4 * time.Second}, {[]string{now.Add(-time.Second).Format(http.TimeFormat)}, 0}, {[]string{now.Format(http.TimeFormat)}, 0}, {[]string{now.Add(time.Hour).Format(http.TimeFormat)}, 30 * time.Second},
	}
	for _, tc := range cases {
		if got := RetryAfterDelay(tc.values, now); got != tc.want {
			t.Fatalf("%q: %s want %s", tc.values, got, tc.want)
		}
	}
	p := Policy{Jitter: func(lo, hi time.Duration) time.Duration { return lo }}
	for _, status := range []int{200, 429, 500, 503, 504, 408} {
		r := statusResult(status)
		r.RetryAfter = []string{"30"}
		d, err := p.Decide(r, 1, now)
		if err != nil {
			t.Fatal(err)
		}
		want := time.Duration(0)
		if status == 429 || status == 503 {
			want = 30 * time.Second
		} else if status >= 500 {
			want = time.Second / 2
		}
		if d.RetryDelay != want {
			t.Fatal(status, d)
		}
	}
	r := statusResult(429)
	r.RetryAfter = []string{"0"}
	d, _ := p.Decide(r, 4, now)
	if d.RetryDelay != 4*time.Second {
		t.Fatal("Retry-After reduced jitter")
	}
	// Duration subtraction is safe even when dates exceed duration range.
	if got := RetryAfterDelay([]string{"Fri, 31 Dec 9999 23:59:59 GMT"}, now); got != MaxRetryDelay {
		t.Fatal("date overflow cap", got)
	}
}
func TestResultValidationAndDurationBounds(t *testing.T) {
	status := 200
	invalid := []SendResult{{}, {Outcome: Acknowledged}, {Outcome: HTTPFailure, HTTPStatus: &status}, {Outcome: Unconfirmed, HTTPStatus: &status, ErrorCode: "timeout"}, {Outcome: Unconfirmed, ErrorCode: "secret-body"}, {Outcome: LocalFailure, ErrorCode: "secret"}, {Outcome: Acknowledged, HTTPStatus: &status, Duration: -1}}
	for _, r := range invalid {
		if _, err := Normalize(r); err != ErrInvalidResult || strings.Contains(err.Error(), "secret") {
			t.Fatal("invalid result accepted or disclosed")
		}
	}
	r := statusResult(204)
	r.Duration = time.Duration(math.MaxInt64)
	n, err := Normalize(r)
	if err != nil || n.Duration != MaxRecordedDuration {
		t.Fatal("unbounded duration")
	}
	for _, s := range []int{0, 99, 600} {
		r := statusResult(s)
		if _, err := Normalize(r); err != ErrInvalidResult {
			t.Fatal("invalid HTTP status accepted")
		}
	}
}
