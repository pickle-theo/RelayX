package delivery

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"relayx/phase2/internal/session"
	"relayx/phase2/internal/store"
)

// Worker defaults applied when the matching Config field is not positive.
const (
	DefaultInterval    = time.Second
	DefaultMaxAttempts = 5
	DefaultRetryBatch  = 100
	DefaultExpireBatch = 100
)

// Config controls the retry/TTL worker.
type Config struct {
	// Interval is the scan interval; values <= 0 fall back to DefaultInterval.
	Interval time.Duration
	// MaxAttempts is the push limit for one delivery; values <= 0 fall back to
	// DefaultMaxAttempts.
	MaxAttempts int
	// RetryBatch is the maximum number of due retries per scan; values <= 0
	// fall back to DefaultRetryBatch.
	RetryBatch int
	// ExpireBatch is the maximum number of expired messages per scan; values
	// <= 0 fall back to DefaultExpireBatch.
	ExpireBatch int
}

// Stats is a snapshot of the worker counters accumulated since construction.
type Stats struct {
	Retried uint64
	Expired uint64
}

// Worker re-pushes due deliveries and expires outdated messages on a single
// goroutine driven by a time.Ticker.
type Worker struct {
	store     *store.Store
	sessions  *session.Manager
	deliverer *Deliverer
	cfg       Config
	logger    *slog.Logger

	retried atomic.Uint64
	expired atomic.Uint64

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWorker returns a Worker. The store and sessions are owned by the caller;
// d may be nil, in which case a Deliverer over sessions is created.
func NewWorker(st *store.Store, sessions *session.Manager, d *Deliverer, cfg Config, logger *slog.Logger) *Worker {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.RetryBatch <= 0 {
		cfg.RetryBatch = DefaultRetryBatch
	}
	if cfg.ExpireBatch <= 0 {
		cfg.ExpireBatch = DefaultExpireBatch
	}
	if logger == nil {
		logger = slog.Default()
	}
	if d == nil {
		d = NewDeliverer(sessions)
	}
	return &Worker{
		store:     st,
		sessions:  sessions,
		deliverer: d,
		cfg:       cfg,
		logger:    logger,
	}
}

// Start launches the scan goroutine. Calling Start on an already running
// worker is a no-op.
func (w *Worker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Add(1)
	go w.loop(ctx)
}

// Stop cancels the scan goroutine and waits for it to exit. It is safe to call
// Stop repeatedly and before Start.
func (w *Worker) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	w.cancel = nil
	w.mu.Unlock()

	if cancel == nil {
		return
	}
	cancel()
	w.wg.Wait()
}

// Stats returns the accumulated retry and expiry counters.
func (w *Worker) Stats() Stats {
	return Stats{Retried: w.retried.Load(), Expired: w.expired.Load()}
}

// RunOnce performs one retry scan followed by one expiry scan. Errors on a
// single row are logged and skipped; a failure of the whole scan is logged and
// never terminates the process.
func (w *Worker) RunOnce(ctx context.Context) {
	now := time.Now()
	w.retryDue(ctx, now)
	w.expireDue(ctx, now)
}

func (w *Worker) loop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.RunOnce(ctx)
		}
	}
}

func (w *Worker) retryDue(ctx context.Context, now time.Time) {
	due, err := w.store.DueRetries(ctx, now, w.cfg.RetryBatch)
	if err != nil {
		w.logger.Warn("delivery: scan due retries failed", "error", err)
		return
	}
	for i := range due {
		w.retryOne(ctx, &due[i], now)
	}
}

func (w *Worker) retryOne(ctx context.Context, d *store.Delivery, now time.Time) {
	log := w.logger.With(
		"delivery_id", d.ID,
		"message_id", d.MessageID,
		"user_id", d.UserID,
		"device_id", d.DeviceID)

	// Expired rows are handled by the expiry scan, never re-pushed.
	if !d.ExpireAt.After(now) {
		return
	}

	sess := w.sessions.FindByDevice(d.UserID, d.DeviceID)
	if sess == nil {
		if err := w.store.ClearRetrySchedule(ctx, d.ID); err != nil {
			log.Warn("delivery: clear retry schedule failed", "error", err)
		}
		return
	}

	if d.Attempts >= w.cfg.MaxAttempts {
		if err := w.store.MarkDeliveryFailed(ctx, d.ID); err != nil {
			log.Warn("delivery: mark failed failed", "error", err)
			return
		}
		log.Info("delivery: retry limit reached", "attempts", d.Attempts)
		return
	}

	if err := w.deliverer.Deliver(sess, d.MessageID, d.FromUser, d.FromDevice, d.Payload); err != nil {
		// The target is online but its queue is full or the connection is
		// closing; recovery is left to PULL.
		log.Warn("delivery: re-push failed, clearing retry schedule", "error", err)
		if err := w.store.ClearRetrySchedule(ctx, d.ID); err != nil {
			log.Warn("delivery: clear retry schedule failed", "error", err)
		}
		return
	}

	if err := w.store.MarkDeliveryRetried(ctx, d.ID); err != nil {
		log.Warn("delivery: mark retried failed", "error", err)
		return
	}
	w.retried.Add(1)
}

func (w *Worker) expireDue(ctx context.Context, now time.Time) {
	ids, err := w.store.ListExpiredMessages(ctx, now, w.cfg.ExpireBatch)
	if err != nil {
		w.logger.Warn("delivery: scan expired messages failed", "error", err)
		return
	}
	for _, id := range ids {
		if err := w.store.ExpireMessage(ctx, id); err != nil {
			w.logger.Warn("delivery: expire message failed", "message_id", id, "error", err)
			continue
		}
		w.expired.Add(1)
	}
}
