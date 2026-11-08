package delivery

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
)

type publicKeys struct{}

func (publicKeys) SigningKey(ref string) ([]byte, bool) {
	return []byte(strings.Repeat("p", 32)), ref == "public-test-v1"
}
func httpClaim(url string) ClaimedDelivery {
	return ClaimedDelivery{DeliveryID: "00000000-0000-4000-8000-000000000101", URLSnapshot: url, KeyRefSnapshot: "public-test-v1", WireBody: []byte(" {\"message\":\"नमस्ते 🌍\"}\n")}
}
func testHTTPSender(t *testing.T, timeout time.Duration) *HTTPSender {
	t.Helper()
	s, err := NewHTTPSender(signing.Auth{Keys: publicKeys{}}, timeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}
func TestSignedHTTPExactBodyAndNoRedirect(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1); w.WriteHeader(204) }))
	defer target.Close()
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if _, err := (signing.Auth{Keys: publicKeys{}}).Verify(r.Header, raw); err != nil {
			t.Error("invalid signature", err)
		}
		if string(raw) != string(httpClaim("").WireBody) || r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("request bytes/headers changed")
		}
		w.Header().Set("Location", target.URL)
		w.WriteHeader(302)
	}))
	defer receiver.Close()
	s := testHTTPSender(t, time.Second)
	result := s.Send(context.Background(), httpClaim(receiver.URL))
	if result.Outcome != HTTPFailure || result.HTTPStatus == nil || *result.HTTPStatus != 302 || redirected.Load() != 0 {
		t.Fatal(result, "redirect followed")
	}
	tr := s.client.Transport.(*http.Transport)
	if tr.Proxy != nil || !tr.DisableKeepAlives || !tr.DisableCompression || tr.TLSHandshakeTimeout != 2*time.Second || tr.MaxResponseHeaderBytes != 16*1024 {
		t.Fatal("unsafe transport defaults")
	}
}
func TestHTTPTimeoutCancellationAndBoundedDisposal(t *testing.T) {
	t.Run("slow body keeps acknowledgment within deadline", func(t *testing.T) {
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-release
		}))
		defer server.Close()
		defer close(release)
		s := testHTTPSender(t, 80*time.Millisecond)
		start := time.Now()
		result := s.Send(context.Background(), httpClaim(server.URL))
		if result.Outcome != Acknowledged || time.Since(start) > time.Second || result.Duration > time.Second {
			t.Fatal(result, "unbounded body disposal")
		}
	})
	t.Run("huge body bounded", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(200)
			chunk := strings.Repeat("x", 64*1024)
			for i := 0; i < 1024; i++ {
				if _, err := io.WriteString(w, chunk); err != nil {
					return
				}
			}
		}))
		defer server.Close()
		s := testHTTPSender(t, time.Second)
		start := time.Now()
		result := s.Send(context.Background(), httpClaim(server.URL))
		if result.Outcome != Acknowledged || time.Since(start) > 2*time.Second {
			t.Fatal(result)
		}
	})
	t.Run("no status means timeout or cancellation", func(t *testing.T) {
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-release }))
		defer server.Close()
		defer close(release)
		s := testHTTPSender(t, 50*time.Millisecond)
		result := s.Send(context.Background(), httpClaim(server.URL))
		if result.Outcome != Unconfirmed || result.ErrorCode != "timeout" {
			t.Fatal(result)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result = s.Send(ctx, httpClaim(server.URL))
		if result.Outcome != Unconfirmed || result.ErrorCode != "canceled" {
			t.Fatal(result)
		}
	})
	t.Run("read error after valid 2xx remains acknowledged", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			fmt.Fprint(conn, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\ninvalid-chunk\r\n")
		}))
		defer server.Close()
		result := testHTTPSender(t, time.Second).Send(context.Background(), httpClaim(server.URL))
		if result.Outcome != Acknowledged {
			t.Fatal(result)
		}
	})
}
func TestHTTPNoHiddenRetryAndNoCallForLocalFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer server.Close()
	s := testHTTPSender(t, time.Second)
	result := s.Send(context.Background(), httpClaim(server.URL))
	if result.Outcome != Unconfirmed || calls.Load() != 1 {
		t.Fatal("hidden retry", result, calls.Load())
	}
	c := httpClaim(server.URL)
	c.KeyRefSnapshot = "missing"
	result = s.Send(context.Background(), c)
	if result.Outcome != LocalFailure || result.ErrorCode != "missing_signing_key" || calls.Load() != 1 {
		t.Fatal(result)
	}
	c = httpClaim("file:///secret")
	result = s.Send(context.Background(), c)
	if result.Outcome != LocalFailure || calls.Load() != 1 {
		t.Fatal(result)
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type countedBody struct {
	read   int
	closed bool
}

func (b *countedBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	b.read += len(p)
	return len(p), nil
}
func (b *countedBody) Close() error { b.closed = true; return nil }
func TestHTTPTransportBoundaryDisposalCapAndNoReplayBody(t *testing.T) {
	s := testHTTPSender(t, time.Second)
	body := &countedBody{}
	s.client.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.GetBody != nil {
			t.Error("transport can replay request body")
		}
		if r.ContentLength != int64(len(httpClaim("").WireBody)) {
			t.Error("wire length changed")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
	})
	result := s.Send(context.Background(), httpClaim("http://fixture.invalid/webhook"))
	if result.Outcome != Acknowledged || body.read != 4096 || !body.closed {
		t.Fatal("unbounded/undisposed response", result, body)
	}
}
