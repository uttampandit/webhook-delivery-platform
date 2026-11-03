//go:build integration

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestAPIReadinessAndDestinationsUsePostgreSQLState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	database, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	settings := testAPISettings(t)
	repository := store.NewAPIReadRepository(database.Pool)
	handler := NewHandler(settings, repository)

	if recorder := serve(handler, http.MethodGet, "/health/ready", ""); recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness before migration = %d %q, want 503", recorder.Code, recorder.Body.String())
	}
	if err := store.Apply(ctx, database.Pool, testMigrationDir(t)); err != nil {
		t.Fatal("apply application schema:", err)
	}
	if err := store.Seed(ctx, database.Pool, settings.Registry()); err != nil {
		t.Fatal("seed application registry:", err)
	}
	readyRecorder := serve(handler, http.MethodGet, "/health/ready", "")
	if readyRecorder.Code != http.StatusOK || strings.TrimSpace(readyRecorder.Body.String()) != `{"status":"ready"}` {
		t.Fatalf("readiness after migration and seed = %d %q", readyRecorder.Code, readyRecorder.Body.String())
	}

	const otherTenantID = "00000000-0000-4000-8000-000000000002"
	const otherDestinationID = "00000000-0000-4000-8000-000000000202"
	const privateURL = "http://private-url-marker.invalid/webhook"
	const privateKeyRef = "private-key-ref-marker"
	if _, err := database.Pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $2)`, otherTenantID, "other tenant"); err != nil {
		t.Fatal("insert nonconfigured tenant:", err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO destinations (id, tenant_id, code, url, key_ref)
		VALUES ($1, $2, 'private', $3, $4)`, otherDestinationID, otherTenantID, privateURL, privateKeyRef); err != nil {
		t.Fatal("insert destination with fields forbidden from API output:", err)
	}

	destinationsRequest := httptest.NewRequest(http.MethodGet, "/v1/destinations", nil)
	destinationsRequest.Header.Set("Authorization", "Bearer "+testAPIToken)
	destinationsRequest.Header.Set("X-Tenant-ID", otherTenantID)
	destinationsRequest.Header.Set("X-Actor", "attacker-selected-actor")
	destinationsRecorder := httptest.NewRecorder()
	handler.ServeHTTP(destinationsRecorder, destinationsRequest)
	if destinationsRecorder.Code != http.StatusOK {
		t.Fatalf("destinations response = %d %q", destinationsRecorder.Code, destinationsRecorder.Body.String())
	}
	if strings.Contains(destinationsRecorder.Body.String(), privateURL) || strings.Contains(destinationsRecorder.Body.String(), privateKeyRef) {
		t.Fatal("destination response disclosed URL or key reference")
	}
	var destinations struct {
		Items []map[string]json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(destinationsRecorder.Body.Bytes(), &destinations); err != nil {
		t.Fatal("decode destination response:", err)
	}
	if len(destinations.Items) != 2 {
		t.Fatalf("destination count = %d, want fixed tenant's two destinations", len(destinations.Items))
	}
	want := map[string]string{
		config.FulfillmentDestination:  "fulfillment",
		config.NotificationDestination: "notification",
	}
	for _, item := range destinations.Items {
		var id, code string
		if len(item) != 2 || item["id"] == nil || item["code"] == nil {
			t.Fatalf("destination response included nonpublic fields: %+v", item)
		}
		if err := json.Unmarshal(item["id"], &id); err != nil {
			t.Fatal("decode destination id:", err)
		}
		if err := json.Unmarshal(item["code"], &code); err != nil {
			t.Fatal("decode destination code:", err)
		}
		if expectedCode, ok := want[id]; !ok || expectedCode != code {
			t.Fatalf("destination (%s, %s) did not come from the configured tenant", id, code)
		}
	}

	if _, err := database.Pool.Exec(ctx, `UPDATE destinations SET url = $1 WHERE id = $2`, privateURL, config.FulfillmentDestination); err != nil {
		t.Fatal("introduce seed drift for readiness check:", err)
	}
	driftRecorder := serve(handler, http.MethodGet, "/health/ready", "")
	if driftRecorder.Code != http.StatusServiceUnavailable || strings.Contains(driftRecorder.Body.String(), privateURL) {
		t.Fatalf("seed drift readiness = %d %q", driftRecorder.Code, driftRecorder.Body.String())
	}
	registry := settings.Registry().Destinations()
	if _, err := database.Pool.Exec(ctx, `UPDATE destinations SET url = $1 WHERE id = $2`, registry[0].URL, config.FulfillmentDestination); err != nil {
		t.Fatal("restore fixed destination URL:", err)
	}

	database.Pool.Close()
	database.Pool = nil
	dbFailureRecorder := serve(handler, http.MethodGet, "/health/ready", "")
	if dbFailureRecorder.Code != http.StatusServiceUnavailable || strings.TrimSpace(dbFailureRecorder.Body.String()) != `{"status":"not_ready"}` {
		t.Fatalf("readiness after DB failure = %d %q", dbFailureRecorder.Code, dbFailureRecorder.Body.String())
	}
	if strings.Contains(dbFailureRecorder.Body.String(), "postgres://") || strings.Contains(dbFailureRecorder.Body.String(), "SQL") {
		t.Fatal("DB failure readiness response disclosed connection or query details")
	}
}

func testAPISettings(t *testing.T) config.Settings {
	t.Helper()
	values := map[string]string{
		"DATABASE_URL": "postgres://test-user:test-password@127.0.0.1:5432/test?sslmode=disable",
		"RUN_MODE":     "compose",
		"API_TOKEN":    testAPIToken,
	}
	settings, err := config.LoadFrom(config.ServiceAPI, func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatal("load API integration settings:", err)
	}
	return settings
}

func testMigrationDir(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate API integration test")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../.."))
	return filepath.Join(root, "migrations")
}
