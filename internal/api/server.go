package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
)

type Repository interface {
	Replay(context.Context, string, string, string, string) (store.ReplayResult, error)
	Ping(context.Context) error
	CheckReadiness(context.Context, config.Registry) error
	ListDestinations(context.Context, string) ([]store.DestinationSummary, error)
	AcceptEvent(context.Context, string, string, store.ValidatedEventRequest) (store.AcceptedEvent, error)
	ListEvents(context.Context, string, int, *store.EventCursor) ([]store.EventListItem, *store.EventCursor, error)
	GetEvent(context.Context, string, string) (store.EventDetail, error)
}

type Server struct {
	settings   config.Settings
	repository Repository
	registry   config.Registry
	metrics    *telemetry.Metrics
}

func NewHandler(settings config.Settings, repository Repository) http.Handler {
	return NewHandlerWithMetrics(settings, repository, nil)
}

func NewHandlerWithMetrics(settings config.Settings, repository Repository, metrics *telemetry.Metrics) http.Handler {
	server := &Server{settings: settings, repository: repository, registry: settings.Registry(), metrics: metrics}
	return requestIDMiddleware(server)
}

func (server *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if server.metrics != nil && r.URL.Path == "/v1/events" && r.Method == http.MethodPost {
		started := time.Now()
		tracked := &ingestResponseWriter{ResponseWriter: w}
		server.serveHTTP(tracked, r)
		server.metrics.ObserveIngest(ingestResultFromStatus(tracked.statusCode()), time.Since(started))
		return
	}
	server.serveHTTP(w, r)
}

func (server *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health/live":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		live(w)
		return
	case "/health/ready":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		ready(w, r, server.repository, server.registry)
		return
	}

	if r.URL.Path == "/v1" || strings.HasPrefix(r.URL.Path, "/v1/") {
		if !authorizedRequest(r, server.settings.APIToken()) {
			writeError(w, r, codeUnauthorized)
			return
		}
		r = principalForRequest(r, server.registry.Tenant().ID)
		if r.URL.Path == "/v1/events" {
			switch r.Method {
			case http.MethodPost:
				server.createEvent(w, r)
			case http.MethodGet:
				server.listEvents(w, r)
			default:
				methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
			}
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/events/") {
			eventID := strings.TrimPrefix(r.URL.Path, "/v1/events/")
			// Reject any further path segments (e.g. /v1/events/id/something).
			if strings.ContainsRune(eventID, '/') {
				writeError(w, r, codeNotFound)
				return
			}
			if r.Method != http.MethodGet {
				methodNotAllowed(w, http.MethodGet)
				return
			}
			server.getEvent(w, r)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/v1/deliveries/") && strings.HasSuffix(r.URL.Path, "/replay") {
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/deliveries/"), "/replay")
			if strings.ContainsRune(id, '/') {
				writeError(w, r, codeNotFound)
				return
			}
			if r.Method != http.MethodPost {
				methodNotAllowed(w, http.MethodPost)
				return
			}
			server.replayDelivery(w, r, id)
			return
		}
		if r.URL.Path != "/v1/destinations" {
			writeError(w, r, codeNotFound)
			return
		}
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		server.listDestinations(w, r)
		return
	}

	writeError(w, r, codeNotFound)
}

type ingestResponseWriter struct {
	http.ResponseWriter
	status int
}

func (writer *ingestResponseWriter) WriteHeader(status int) {
	if writer.status != 0 {
		return
	}
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *ingestResponseWriter) Write(body []byte) (int, error) {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(body)
}

func (writer *ingestResponseWriter) statusCode() int {
	if writer.status == 0 {
		return http.StatusOK
	}
	return writer.status
}

func ingestResultFromStatus(status int) string {
	switch status {
	case http.StatusAccepted:
		return "accepted"
	case http.StatusOK:
		return "deduplicated"
	case http.StatusConflict:
		return "conflict"
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusRequestEntityTooLarge:
		return "invalid"
	default:
		return "error"
	}
}

type destinationsResponse struct {
	Items []destinationResponse `json:"items"`
}

type destinationResponse struct {
	ID   string `json:"id"`
	Code string `json:"code"`
}

func (server *Server) listDestinations(w http.ResponseWriter, r *http.Request) {
	if server.repository == nil {
		writeError(w, r, codeDependencyUnavailable)
		return
	}
	timeout := server.settings.DBStatementTimeout()
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeError(w, r, codeInternalError)
		return
	}
	items, err := server.repository.ListDestinations(ctx, principal.TenantID)
	if err != nil {
		writeError(w, r, codeDependencyUnavailable)
		return
	}
	response := destinationsResponse{Items: make([]destinationResponse, 0, len(items))}
	for _, item := range items {
		response.Items = append(response.Items, destinationResponse{ID: item.ID, Code: item.Code})
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

type requestIDContextKey struct{}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID, err := newRequestID()
		if err != nil {
			writeError(w, r, codeInternalError)
			return
		}
		w.Header().Set("X-Request-Id", requestID)
		r = r.WithContext(context.WithValue(r.Context(), requestIDContextKey{}, requestID))
		slog.InfoContext(r.Context(), "http request", "request_id", requestID)
		next.ServeHTTP(w, r)
	})
}

func requestIDFrom(r *http.Request) string {
	requestID, _ := r.Context().Value(requestIDContextKey{}).(string)
	return requestID
}

func newRequestID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(bytes[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}
