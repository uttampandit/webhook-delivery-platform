package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

const readinessTimeout = time.Second

type readinessRepository interface {
	Ping(context.Context) error
	CheckReadiness(context.Context, config.Registry) error
}

type healthResponse struct {
	Status string `json:"status"`
}

func live(w http.ResponseWriter) {
	writeHealth(w, http.StatusOK, healthResponse{Status: "ok"})
}

func ready(w http.ResponseWriter, r *http.Request, repository readinessRepository, registry config.Registry) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()
	if repository == nil || repository.Ping(ctx) != nil || repository.CheckReadiness(ctx, registry) != nil {
		writeHealth(w, http.StatusServiceUnavailable, healthResponse{Status: "not_ready"})
		return
	}
	writeHealth(w, http.StatusOK, healthResponse{Status: "ready"})
}

func writeHealth(w http.ResponseWriter, status int, response healthResponse) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}
