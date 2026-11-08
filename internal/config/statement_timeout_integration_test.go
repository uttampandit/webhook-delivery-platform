//go:build integration

package config

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestStatementTimeoutAcceptedByPostgreSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := testutil.NewDatabase(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		input string
		want  int64
	}{
		{"3s", 3000}, {"1m", 60000}, {"1m2.345s", 62345}, {"1ns", 1}, {"1us", 1}, {"1.000001ms", 2}, {"596h31m23.647s", 2147483647}, {"600h", 2147483647},
	} {
		t.Run(tc.input, func(t *testing.T) {
			values := validEnvironment()
			values["DB_STATEMENT_TIMEOUT"] = tc.input
			settings, err := LoadFrom(ServiceAPI, mapLookup(values))
			if err != nil {
				t.Fatal(err)
			}
			cfg := db.Pool.Config()
			cfg.ConnConfig.RuntimeParams["statement_timeout"] = settings.PostgreSQLStatementTimeout()
			pool, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal("configure connection")
			}
			defer pool.Close()
			var got int64
			if err := pool.QueryRow(ctx, `SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'`).Scan(&got); err != nil {
				t.Fatal("validated duration prevented PostgreSQL connection")
			}
			if got != tc.want {
				t.Fatalf("server timeout=%dms, want %dms", got, tc.want)
			}
		})
	}
}
