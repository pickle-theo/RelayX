// Package global enforces a tenant quota across the whole cluster.
//
// The Phase 5 limiter kept its token bucket inside the worker process, so a
// fleet of N workers delivered at N times the configured rate. This limiter
// keeps the token bucket in Redis and hands the tokens out in batches: a
// worker spends the tokens it already holds and only asks Redis for more when
// the local supply runs out. The request rate against Redis therefore drops
// by the batch size while the total delivery rate still honours the quota.
//
// Only the tenants that configure a global limit depend on Redis. A tenant
// without a rate and without an explicit in-flight limit is served by the
// local counters alone, so the common case keeps working when Redis is down.
package global

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"relayx/phase5.5/internal/store"
)

// QuotaSource loads the tenant quota rows. *store.Store satisfies it.
type QuotaSource interface {
	GetTenant(ctx context.Context, tenantID string) (store.Tenant, bool, error)
}

// Options configures the cluster-wide limiter.
type Options struct {
	// Batch is the number of tokens requested from Redis in one call.
	Batch int
	// CacheTTL is the lifetime of the cached tenant quota row.
	CacheTTL time.Duration
	// DefaultInflight is the per-process in-flight limit used by tenants
	// without an explicit global in-flight limit.
	DefaultInflight int
	// FailOpen keeps the deliveries running when Redis is unavailable. The
	// default is fail closed, so a Redis outage cannot amplify the traffic.
	FailOpen bool
	// Prefix is the Redis key namespace.
	Prefix string
	// Logger receives the diagnostics.
	Logger *slog.Logger
}

// Limiter enforces the cluster-wide tenant rate and in-flight limits.
type Limiter struct {
	redis  redis.UniversalClient
	source QuotaSource
	opts   Options
	logger *slog.Logger

	mu      sync.Mutex
	entries map[string]*entry
}

// entry is the cached quota and the local token supply of one tenant.
type entry struct {
	quota    store.Tenant
	loadedAt time.Time
	// tokens are the tokens granted by Redis and not spent yet.
	tokens int
	// localInflight counts the deliveries this process admitted.
	localInflight int
}

// New returns a cluster-wide limiter. A nil Redis client falls back to the
// local counters, which is the behaviour of a single-node deployment.
func New(client redis.UniversalClient, source QuotaSource, opts Options) *Limiter {
	if opts.Batch <= 0 {
		opts.Batch = 32
	}
	if opts.Prefix == "" {
		opts.Prefix = "relayx:quota:"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Limiter{
		redis:   client,
		source:  source,
		opts:    opts,
		logger:  opts.Logger,
		entries: make(map[string]*entry),
	}
}

// Acquire reserves one delivery slot for tenantID.
//
// When ok is false the caller must postpone the work for retryAfter; no
// attempt is consumed. On success the returned release function must be called
// exactly once when the push finishes.
func (l *Limiter) Acquire(ctx context.Context, tenantID string, _ time.Time) (release func(), retryAfter time.Duration, ok bool, err error) {
	e, err := l.entry(ctx, tenantID)
	if err != nil {
		return nil, 0, false, err
	}

	quota := e.quota
	rate := quota.RatePerSec
	// The global in-flight limit needs the shared counter. Without Redis the
	// limit falls back to the local counter of this process.
	globalInflight := quota.MaxInflight > 0 && l.redis != nil

	// The global in-flight limit is checked first: it protects the cluster
	// even when the rate limit still has tokens.
	if globalInflight && l.redis != nil {
		granted, err := l.acquireGlobalSlot(ctx, tenantID, quota.MaxInflight)
		if err != nil {
			if l.opts.FailOpen {
				l.logger.Warn("global in-flight slot failed, failing open", "tenant_id", tenantID, "error", err)
			} else {
				return nil, l.backoff(rate), false, fmt.Errorf("global: acquire in-flight slot: %w", err)
			}
		} else if !granted {
			return nil, 50 * time.Millisecond, false, nil
		}
	}

	if rate > 0 {
		granted, err := l.acquireTokens(ctx, tenantID, e, rate, quota.Burst)
		if err != nil {
			if l.opts.FailOpen {
				l.logger.Warn("global token bucket failed, failing open", "tenant_id", tenantID, "error", err)
			} else {
				if globalInflight {
					l.releaseGlobalSlot(ctx, tenantID)
				}
				return nil, l.backoff(rate), false, fmt.Errorf("global: acquire token: %w", err)
			}
		} else if !granted {
			if globalInflight {
				l.releaseGlobalSlot(ctx, tenantID)
			}
			return nil, l.backoff(rate), false, nil
		}
	}

	// The local in-flight limit applies to the tenants without an explicit
	// global limit and to the deployments without Redis.
	limit := quota.MaxInflight
	if limit <= 0 {
		limit = l.opts.DefaultInflight
	}
	if !globalInflight && limit > 0 {
		l.mu.Lock()
		if e.localInflight >= limit {
			l.mu.Unlock()
			if globalInflight {
				l.releaseGlobalSlot(ctx, tenantID)
			}
			return nil, 50 * time.Millisecond, false, nil
		}
		e.localInflight++
		l.mu.Unlock()
	} else {
		l.mu.Lock()
		e.localInflight++
		l.mu.Unlock()
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			if e.localInflight > 0 {
				e.localInflight--
			}
			l.mu.Unlock()
			if globalInflight {
				l.releaseGlobalSlot(ctx, tenantID)
			}
		})
	}, 0, true, nil
}

// Inflight returns the local in-flight count of tenantID.
func (l *Limiter) Inflight(tenantID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.entries[tenantID]; ok {
		return e.localInflight
	}
	return 0
}

// Snapshot returns the local in-flight count of every known tenant.
func (l *Limiter) Snapshot() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]int, len(l.entries))
	for id, e := range l.entries {
		if e.localInflight > 0 {
			out[id] = e.localInflight
		}
	}
	return out
}

// Forget drops the cached quota so the next Acquire reloads it.
func (l *Limiter) Forget(tenantID string) {
	l.mu.Lock()
	delete(l.entries, tenantID)
	l.mu.Unlock()
}

// acquireTokens spends one token, taking a fresh batch from Redis when the
// local supply is empty.
func (l *Limiter) acquireTokens(ctx context.Context, tenantID string, e *entry, rate int, burst int) (bool, error) {
	l.mu.Lock()
	if e.tokens > 0 {
		e.tokens--
		l.mu.Unlock()
		return true, nil
	}
	l.mu.Unlock()

	if l.redis == nil {
		// Without Redis the local counters are the only limit available.
		l.mu.Lock()
		e.tokens++
		l.mu.Unlock()
		return true, nil
	}

	granted, err := l.requestTokens(ctx, tenantID, rate, burst, l.opts.Batch)
	if err != nil {
		return false, err
	}
	if granted <= 0 {
		return false, nil
	}
	l.mu.Lock()
	e.tokens += granted - 1
	l.mu.Unlock()
	return true, nil
}

// entry returns the cached quota of one tenant, reloading it when the cache
// expired.
func (l *Limiter) entry(ctx context.Context, tenantID string) (*entry, error) {
	l.mu.Lock()
	e, ok := l.entries[tenantID]
	if ok && l.opts.CacheTTL > 0 && time.Since(e.loadedAt) < l.opts.CacheTTL {
		l.mu.Unlock()
		return e, nil
	}
	if !ok {
		e = &entry{}
		l.entries[tenantID] = e
	}
	l.mu.Unlock()

	if l.source == nil {
		l.mu.Lock()
		e.quota = store.Tenant{ID: tenantID}
		e.loadedAt = time.Now()
		l.mu.Unlock()
		return e, nil
	}
	quota, found, err := l.source.GetTenant(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("global: load tenant quota: %w", err)
	}
	if !found {
		quota = store.Tenant{ID: tenantID}
	}
	l.mu.Lock()
	e.quota = quota
	e.loadedAt = time.Now()
	l.mu.Unlock()
	return e, nil
}

// backoff returns the delay after a rejected delivery.
func (l *Limiter) backoff(rate int) time.Duration {
	if rate <= 0 {
		return 50 * time.Millisecond
	}
	wait := time.Duration(float64(time.Second) / float64(rate))
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	return wait
}
