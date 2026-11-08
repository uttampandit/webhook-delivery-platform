package delivery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
)

// HTTPSender uses a private transport: no environment proxy, redirect following,
// connection reuse/hidden retries or transparent decompression.
type HTTPSender struct {
	auth    signing.Auth
	client  *http.Client
	timeout time.Duration
}

func NewHTTPSender(auth signing.Auth, timeout time.Duration) (*HTTPSender, error) {
	if timeout <= 0 {
		return nil, ErrInvalidResult
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext, TLSHandshakeTimeout: 2 * time.Second, DisableKeepAlives: true, DisableCompression: true, ResponseHeaderTimeout: timeout, MaxResponseHeaderBytes: 16 * 1024}
	return &HTTPSender{auth, &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, timeout}, nil
}
func (s *HTTPSender) Close() { s.client.CloseIdleConnections() }
func (s *HTTPSender) Send(ctx context.Context, c ClaimedDelivery) SendResult {
	start := time.Now()
	local := func(code string) SendResult {
		slog.ErrorContext(ctx, "delivery local configuration failure", "code", code)
		return SendResult{Outcome: LocalFailure, ErrorCode: code, Duration: time.Since(start)}
	}
	target, err := url.Parse(c.URLSnapshot)
	if err != nil || target.Hostname() == "" || (target.Scheme != "http" && target.Scheme != "https") || target.User != nil || target.Fragment != "" {
		return local("invalid_destination_configuration")
	}
	if s.auth.Keys == nil {
		return local("missing_signing_key")
	}
	if _, ok := s.auth.Keys.SigningKey(c.KeyRefSnapshot); !ok {
		return local("missing_signing_key")
	}
	headers, err := s.auth.Sign(c.DeliveryID, c.KeyRefSnapshot, c.WireBody)
	if err != nil {
		return local("invalid_destination_configuration")
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// Wrapping the reader prevents NewRequest from generating GetBody, so even
	// an idempotency header cannot enable replay of this POST by the transport.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URLSnapshot, io.NopCloser(bytes.NewReader(c.WireBody)))
	if err != nil {
		return local("invalid_destination_configuration")
	}
	req.ContentLength = int64(len(c.WireBody))
	req.GetBody = nil
	req.Header = headers
	response, err := s.client.Do(req)
	elapsed := time.Since(start) // Header-roundtrip latency, not response-body latency.
	if err != nil {
		code := "network_error"
		if errors.Is(ctx.Err(), context.Canceled) {
			code = "canceled"
		} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code = "timeout"
		} else {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				code = "timeout"
			}
		}
		return SendResult{Outcome: Unconfirmed, ErrorCode: code, Duration: elapsed}
	}
	defer response.Body.Close()
	status := response.StatusCode
	if status < 100 || status > 599 {
		return SendResult{Outcome: Unconfirmed, ErrorCode: "network_error", Duration: elapsed}
	}
	// Copy only bounded advice. Repeated values remain ambiguous to the policy.
	after := response.Header.Values("Retry-After")
	if len(after) > 1 {
		after = []string{"", ""}
	} else if len(after) == 1 {
		if len(after[0]) > 128 {
			after = nil
		} else {
			after = append([]string(nil), after...)
		}
	}
	// A body read failure does not erase a valid status, especially a 2xx.
	_, _ = io.CopyN(io.Discard, response.Body, 4096)
	if status >= 200 && status <= 299 {
		return SendResult{Outcome: Acknowledged, HTTPStatus: &status, Duration: elapsed}
	}
	return SendResult{Outcome: HTTPFailure, HTTPStatus: &status, ErrorCode: "http_non_2xx", Duration: elapsed, RetryAfter: after}
}
