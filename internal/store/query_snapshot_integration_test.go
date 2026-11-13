//go:build integration

package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

// Interleave a real committed write immediately before a history query. The
// reader and writer use independent connections; no SQL or locking is mocked.
type historyQueryTracer struct {
	table  string
	before func()
}

func (h *historyQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if h.before != nil && strings.Contains(data.SQL, "FROM "+h.table) && !strings.Contains(data.SQL, "FROM deliveries") {
		before := h.before
		h.before = nil
		before()
	}
	return ctx
}
func (*historyQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestEventDetailUsesOneSnapshot(t *testing.T) {
	for _, change := range []string{"finalize", "replay"} {
		t.Run(change, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			pool, _, finalizer, claim := finalFixture(t, ctx)
			table := "attempts"
			if change == "replay" {
				table = "replays"
				if _, err := finalizer.Finalize(ctx, claim, sendStatus(400)); err != nil {
					t.Fatal(err)
				}
			}
			wrote := false
			cfg := pool.Config()
			cfg.ConnConfig.Tracer = &historyQueryTracer{table: table, before: func() {
				wrote = true
				if change == "finalize" {
					if _, err := finalizer.Finalize(ctx, claim, sendStatus(204)); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := NewReplayRepository(pool).Replay(ctx, config.TenantID, claim.DeliveryID, "snapshot-replay", "local-operator"); err != nil {
						t.Fatal(err)
					}
				}
			}}
			reader, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			repo := NewQueryRepository(reader)
			detail, err := repo.GetEvent(ctx, config.TenantID, claim.EventID)
			if err != nil {
				t.Fatal(err)
			}
			if !wrote || len(detail.Deliveries) != 1 {
				t.Fatal("interleaving did not run")
			}
			d := detail.Deliveries[0]
			if change == "finalize" {
				if d.Status != "in_flight" || !d.HasUnconfirmedAttempts || len(d.Attempts) != 1 || d.Attempts[0].Outcome != "running" {
					t.Fatalf("mixed finalization snapshots: %+v", d)
				}
			} else if d.Status != "dead" || d.CycleNo != 1 || len(d.Replays) != 0 {
				t.Fatalf("mixed replay snapshots: %+v", d)
			}
			latest, err := repo.GetEvent(ctx, config.TenantID, claim.EventID)
			if err != nil {
				t.Fatal(err)
			}
			d = latest.Deliveries[0]
			if change == "finalize" {
				if d.Status != "succeeded" || d.HasUnconfirmedAttempts || d.Attempts[0].Outcome != "acknowledged" {
					t.Fatalf("fresh snapshot missed finalization: %+v", d)
				}
			} else if d.Status != "pending" || d.CycleNo != 2 || len(d.Replays) != 1 || d.Replays[0].ToCycle != 2 {
				t.Fatalf("fresh snapshot missed replay: %+v", d)
			}
		})
	}
}
