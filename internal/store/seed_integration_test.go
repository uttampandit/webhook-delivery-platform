//go:build integration

package store

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestSeedIsIdempotentAndKeepsFixedRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	database, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	applySeedSchema(t, ctx, database.Pool)
	registry, err := config.NewIntegrationTestRegistry(config.RunModeCompose,
		"http://127.0.0.1:49123/webhook", "http://127.0.0.1:49124/webhook")
	if err != nil {
		t.Fatal("create fixed registry:", err)
	}

	if err := Seed(ctx, database.Pool, registry); err != nil {
		t.Fatal("seed fresh database:", err)
	}
	if err := Seed(ctx, database.Pool, registry); err != nil {
		t.Fatal("repeat seed:", err)
	}

	var tenants, destinations int
	if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM tenants`).Scan(&tenants); err != nil {
		t.Fatal("count seeded tenants:", err)
	}
	if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM destinations`).Scan(&destinations); err != nil {
		t.Fatal("count seeded destinations:", err)
	}
	if tenants != 1 || destinations != 2 {
		t.Fatalf("seed counts = (%d tenant, %d destinations), want (1, 2)", tenants, destinations)
	}
	for _, expected := range registry.Destinations() {
		var tenantID, code, destinationURL, keyRef string
		if err := database.Pool.QueryRow(ctx, `
			SELECT tenant_id::text, code, url, key_ref FROM destinations WHERE id = $1`, expected.ID).
			Scan(&tenantID, &code, &destinationURL, &keyRef); err != nil {
			t.Fatalf("read seeded destination %s: %v", expected.Code, err)
		}
		if tenantID != expected.TenantID || code != expected.Code || destinationURL != expected.URL || keyRef != expected.KeyRef {
			t.Fatalf("seeded destination %s = (%s, %s, %s, %s), want (%s, %s, %s, %s)",
				expected.Code, tenantID, code, destinationURL, keyRef, expected.TenantID, expected.Code, expected.URL, expected.KeyRef)
		}
	}
}

func TestSeedDriftFailsAndRollsBackEarlierInserts(t *testing.T) {
	const plantedDrift = "planted-destination-secret-marker"
	cases := []struct {
		name  string
		field string
	}{
		{name: "URL drift", field: "url"},
		{name: "key reference drift", field: "key_ref"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			database, err := testutil.NewDatabase(ctx, t)
			if err != nil {
				t.Fatal(err)
			}
			applySeedSchema(t, ctx, database.Pool)
			registry, err := config.NewIntegrationTestRegistry(config.RunModeCompose,
				"http://127.0.0.1:49123/webhook", "http://127.0.0.1:49124/webhook")
			if err != nil {
				t.Fatal("create fixed registry:", err)
			}
			if err := Seed(ctx, database.Pool, registry); err != nil {
				t.Fatal("seed baseline rows:", err)
			}

			// The seed processes fulfillment before notification. Removing the
			// first row proves it is rolled back when drift in the second is found.
			if _, err := database.Pool.Exec(ctx, `DELETE FROM destinations WHERE code = 'fulfillment'`); err != nil {
				t.Fatal("remove first destination for rollback check:", err)
			}
			query := `UPDATE destinations SET url = $1 WHERE code = 'notification'`
			if test.field == "key_ref" {
				query = `UPDATE destinations SET key_ref = $1 WHERE code = 'notification'`
			}
			if _, err := database.Pool.Exec(ctx, query, plantedDrift); err != nil {
				t.Fatal("plant destination drift:", err)
			}

			err = Seed(ctx, database.Pool, registry)
			if err == nil {
				t.Fatal("seed accepted immutable destination drift")
			}
			if strings.Contains(err.Error(), plantedDrift) {
				t.Fatalf("seed error disclosed the changed destination value: %v", err)
			}

			var fulfillmentCount, destinationCount int
			if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM destinations WHERE code = 'fulfillment'`).Scan(&fulfillmentCount); err != nil {
				t.Fatal("check rollback of earlier insert:", err)
			}
			if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM destinations`).Scan(&destinationCount); err != nil {
				t.Fatal("count destinations after drift:", err)
			}
			if fulfillmentCount != 0 || destinationCount != 1 {
				t.Fatalf("after drift failure, fulfillment=%d destination_count=%d; want 0 and 1", fulfillmentCount, destinationCount)
			}

			var retainedDrift string
			column := "url"
			if test.field == "key_ref" {
				column = "key_ref"
			}
			if err := database.Pool.QueryRow(ctx, `SELECT `+column+` FROM destinations WHERE code = 'notification'`).Scan(&retainedDrift); err != nil {
				t.Fatal("read drifted destination after rollback:", err)
			}
			if retainedDrift != plantedDrift {
				t.Fatalf("existing drift = %q, want original planted value", retainedDrift)
			}
		})
	}
}

func applySeedSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate seed integration test")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../.."))
	if err := Apply(ctx, pool, filepath.Join(root, "migrations")); err != nil {
		t.Fatal("apply schema for seed test:", err)
	}
}
