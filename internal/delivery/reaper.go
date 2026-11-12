package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
)

type Recoverer interface {
	RecoverExpired(context.Context) (int, error)
}

type DetailedRecoverer interface {
	RecoverExpiredDetailed(context.Context) (int, int, error)
}

// RunReaper runs independently of send capacity. Each transaction gets a fresh
// bounded context; cancellation stops the tick and cancels active recovery.
func RunReaper(ctx context.Context, repository Recoverer, timeout time.Duration, metrics ...*telemetry.Metrics) {
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
			var recovered, dead int
			var err error
			if detailed, ok := repository.(DetailedRecoverer); ok {
				recovered, dead, err = detailed.RecoverExpiredDetailed(work)
			} else {
				recovered, err = repository.RecoverExpired(work)
			}
			cancel()
			if err != nil {
				slog.ErrorContext(ctx, "lease recovery failed", "error_code", "recovery_failed")
				continue
			}
			if len(metrics) > 0 && metrics[0] != nil {
				metrics[0].AddRecoveredAttempts(recovered)
				metrics[0].AddDeliveries("dead", dead)
			}
		}
	}
}
