//go:build integration

package testutil_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/testutil"
)

func TestParallelDatabasesRemainIsolated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	ready := make(chan struct{}, 3)
	release := make(chan struct{})
	go func() {
		for i := 0; i < 3; i++ {
			select {
			case <-ready:
			case <-ctx.Done():
				return
			}
		}
		close(release)
	}()
	names := map[string]bool{}
	var mu sync.Mutex
	for i := 0; i < 3; i++ {
		i := i
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			db, e := testutil.NewDatabase(ctx, t)
			if e != nil {
				t.Fatal(e)
			}
			mu.Lock()
			duplicate := names[db.Name()]
			names[db.Name()] = true
			mu.Unlock()
			if duplicate {
				t.Fatal("database name reused")
			}
			// Identical table and primary key in all three live databases; values differ.
			if _, e = db.Pool.Exec(ctx, `CREATE TABLE isolation_probe(id integer PRIMARY KEY, marker integer NOT NULL)`); e != nil {
				t.Fatal(e)
			}
			if _, e = db.Pool.Exec(ctx, `INSERT INTO isolation_probe VALUES(1,$1)`, i); e != nil {
				t.Fatal(e)
			}
			ready <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				t.Fatal("parallel DB barrier deadline")
			}
			var name string
			var marker, count int
			e = db.Pool.QueryRow(ctx, `SELECT current_database(),min(marker),count(*) FROM isolation_probe`).Scan(&name, &marker, &count)
			if e != nil || name != db.Name() || marker != i || count != 1 {
				t.Fatalf("isolation final state marker=%d count=%d own_database=%t error=%v", marker, count, name == db.Name(), e)
			}
		})
	}
}
