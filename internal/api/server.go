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
)

type Repository interface {
	Ping(context.Context) error
	CheckReadiness(context.Context, config.Registry) error
	ListDestinations(context.Context, string) ([]store.DestinationSummary, error)
}

type Server struct {
	settings   config.Settings
	repository Repository
	registry   config.Registry
}

func NewHandler(settings config.Settings, repository Repository) http.Handler {
	server := &Server{settings: settings, repository: repository, registry: settings.Registry()}
	return requestIDMiddleware(server)
}

func (server *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		slog.InfoContext(r.Context(), "http request", "request_id", requestID, "method", r.Method, "path", r.URL.Path)
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
