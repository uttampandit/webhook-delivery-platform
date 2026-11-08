package delivery

import (
	"context"
	"log/slog"
	"time"
)

type Recoverer interface {
	RecoverExpired(context.Context) (int, error)
}

// RunReaper runs independently of send capacity. Each transaction gets a fresh
// bounded context; cancellation stops the tick and cancels active recovery.
func RunReaper(ctx context.Context, repository Recoverer, timeout time.Duration) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if ctx.Err() != nil {
				return
			}
			work, cancel := context.WithTimeout(ctx, timeout)
			_, err := repository.RecoverExpired(work)
			cancel()
			if err != nil {
				slog.ErrorContext(ctx, "lease recovery failed")
			}
		}
	}
}
