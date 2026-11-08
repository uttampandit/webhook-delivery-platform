package receiver

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

type Repository interface {
	Ping(context.Context) error
	CreateScenario(context.Context, string, string, bool) (bool, error)
	Inspect(context.Context, string) (store.DemoScenario, error)
	Apply(context.Context, string, store.DemoEnvelope) error
}

type Handler struct {
	repo    Repository
	auth    signing.Auth
	token   string
	timeout time.Duration
}

func New(repo Repository, auth signing.Auth, token string, timeout time.Duration) *Handler {
	return &Handler{repo, auth, token, timeout}
}
func (h *Handler) Webhook() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		raw, err := readBody(w, r, store.MaxDemoBodyBytes)
		if err != nil {
			writeError(w, err)
			return
		}
		operation, err := h.auth.Verify(r.Header, raw)
		if err != nil {
			http.Error(w, "unauthorized", 401)
			return
		}
		envelope, err := store.ValidateDemoEnvelope(raw)
		if err != nil {
			writeError(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()
		if err = h.repo.Apply(ctx, operation, envelope); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(204)
	})
	return mux
}
func (h *Handler) Control() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		if h.repo.Ping(r.Context()) != nil {
			http.Error(w, "not_ready", 503)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("POST /control/scenarios", func(w http.ResponseWriter, r *http.Request) {
		raw, err := readBody(w, r, 4096)
		if err != nil {
			writeError(w, err)
			return
		}
		// Pointers distinguish omitted/null values from explicit false.
		var in struct {
			ScenarioID *string `json:"scenario_id"`
			Mode       *string `json:"mode"`
			Dedupe     *bool   `json:"dedupe_enabled"`
		}
		if strict(raw, &in) != nil || in.ScenarioID == nil || in.Mode == nil || in.Dedupe == nil {
			writeError(w, store.ErrInvalidInput)
			return
		}
		created, err := h.repo.CreateScenario(r.Context(), *in.ScenarioID, *in.Mode, *in.Dedupe)
		if err != nil {
			writeError(w, err)
			return
		}
		if created {
			w.WriteHeader(201)
		} else {
			w.WriteHeader(200)
		}
	})
	mux.HandleFunc("GET /control/scenarios/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !store.ValidScenarioID(id) {
			writeError(w, store.ErrInvalidInput)
			return
		}
		out, err := h.repo.Inspect(r.Context(), id)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})
	// Recovery from faulty modes is deferred until those modes exist (Task 16).
	mux.HandleFunc("PATCH /control/scenarios/{id}", func(w http.ResponseWriter, r *http.Request) { writeError(w, store.ErrInvalidInput) })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		if len(values) != 1 || h.token == "" || !bearer(values[0], h.token) {
			http.Error(w, "unauthorized", 401)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func bearer(value, token string) bool {
	scheme, got, ok := strings.Cut(value, " ")
	return ok && strings.EqualFold(scheme, "Bearer") && len(got) == len(token) && subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}
func strict(raw []byte, out any) error {
	if store.ValidateDemoJSON(raw) != nil {
		return store.ErrInvalidInput
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	return d.Decode(out)
}

var errTooLarge = errors.New("body too large")

func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	body := http.MaxBytesReader(w, r.Body, limit)
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		var max *http.MaxBytesError
		if errors.As(err, &max) {
			return nil, errTooLarge
		}
		return nil, store.ErrInvalidInput
	}
	return raw, nil
}
func writeError(w http.ResponseWriter, err error) {
	status := 503
	message := "receiver unavailable"
	switch {
	case errors.Is(err, errTooLarge):
		status = 413
		message = "body too large"
	case errors.Is(err, store.ErrInvalidInput), errors.Is(err, store.ErrInvalidJSON):
		status = 400
		message = "invalid request"
	case errors.Is(err, store.ErrDemoUnsupported):
		status = 422
		message = "unsupported mode or event type"
	case errors.Is(err, store.ErrDemoNotFound):
		status = 404
		message = "scenario not found"
	case errors.Is(err, store.ErrDemoConflict):
		status = 409
		message = "conflict"
	}
	http.Error(w, message, status)
}
