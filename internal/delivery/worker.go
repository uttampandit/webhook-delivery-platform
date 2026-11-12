package delivery

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/telemetry"
)

type Claimer interface {
	ClaimDue(context.Context, int) ([]ClaimedDelivery, error)
}
type Finalizer interface {
	Finalize(context.Context, ClaimedDelivery, SendResult) (FinalizationResult, error)
}
type Sender interface {
	Send(context.Context, ClaimedDelivery) SendResult
}
type WorkerOptions struct {
	Concurrency                                    int
	PollInterval, FinalizeTimeout, ShutdownTimeout time.Duration
	Metrics                                        *telemetry.Metrics
}
type Worker struct {
	claims    Claimer
	finalizer Finalizer
	sender    Sender
	options   WorkerOptions
}

var ErrWorkerConfiguration = errors.New("invalid worker configuration")
var ErrWorkerShutdown = errors.New("worker cleanup exceeded shutdown deadline")

func NewWorker(c Claimer, f Finalizer, s Sender, o WorkerOptions) (*Worker, error) {
	if c == nil || f == nil || s == nil || o.Concurrency < 1 || o.Concurrency > 32 || o.PollInterval <= 0 || o.FinalizeTimeout <= 0 || o.ShutdownTimeout <= 0 || o.ShutdownTimeout > 10*time.Second {
		return nil, ErrWorkerConfiguration
	}
	return &Worker{c, f, s, o}, nil
}

// Run never prefetches leased work: a committed claim occupies an available
// slot immediately. Any claim error, including ambiguous commit, discards all
// returned data. Shutdown cancels sends and finalizes with fresh bounded contexts.
func (w *Worker) Run(ctx context.Context) error {
	sends, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{}, w.options.Concurrency)
	active := 0
	poll := time.NewTicker(w.options.PollInterval)
	defer poll.Stop()
	claim := func() {
		if ctx.Err() != nil || active == w.options.Concurrency {
			return
		}
		slots := w.options.Concurrency - active
		claims, err := w.claims.ClaimDue(ctx, slots)
		if err != nil {
			slog.ErrorContext(ctx, "worker claim failed", "error_code", "claim_failed")
			return
		}
		if ctx.Err() != nil {
			return
		}
		if len(claims) > slots {
			slog.ErrorContext(ctx, "worker rejected claim batch exceeding free slots", "error_code", "claim_batch_overflow")
			return
		}
		for _, c := range claims {
			if w.options.Metrics != nil {
				w.options.Metrics.ObserveQueueDelay(c.StartedAt.Sub(c.ScheduledAt))
			}
			active++
			go func(c ClaimedDelivery) {
				testCheckpoint("after_claim_commit", c)
				result := w.sender.Send(sends, c)
				testCheckpoint("after_send_before_finalize", c)
				cleanup, cancel := context.WithTimeout(context.Background(), w.options.FinalizeTimeout)
				defer cancel()
				finalized, err := w.finalizer.Finalize(cleanup, c, result)
				if err != nil {
					if errors.Is(err, ErrStaleFinalization) {
						if w.options.Metrics != nil {
							w.options.Metrics.ObserveStaleFinalization()
						}
						slog.InfoContext(cleanup, "stale finalization rejected", "worker_id", "worker", "event_id", c.EventID, "delivery_id", c.DeliveryID, "attempt_id", c.AttemptID, "cycle_no", c.CycleNo, "attempt_no", c.AttemptNo, "generation", c.Generation, "error_code", "stale_finalization")
					} else {
						slog.ErrorContext(cleanup, "worker finalization failed", "worker_id", "worker", "event_id", c.EventID, "delivery_id", c.DeliveryID, "attempt_id", c.AttemptID, "cycle_no", c.CycleNo, "attempt_no", c.AttemptNo, "generation", c.Generation, "error_code", "finalization_failed")
					}
				} else if w.options.Metrics != nil {
					w.options.Metrics.ObserveAttempt(string(finalized.Outcome))
					if finalized.Status == "succeeded" {
						w.options.Metrics.ObserveDelivery("succeeded")
						if finalized.DeliveryLatencyObserved {
							w.options.Metrics.ObserveDeliveryLatency(finalized.DeliveryLatency)
						}
					} else if finalized.Status == "dead" {
						w.options.Metrics.ObserveDelivery("dead")
					}
				}
				done <- struct{}{}
			}(c)
		}
	}
	claim()
	for {
		select {
		case <-ctx.Done():
			cancel()
			deadline := time.NewTimer(w.options.ShutdownTimeout)
			defer deadline.Stop()
			for active > 0 {
				select {
				case <-done:
					active--
				case <-deadline.C:
					return ErrWorkerShutdown
				}
			}
			return nil
		case <-done:
			active--
			claim()
		case <-poll.C:
			claim()
		}
	}
}
