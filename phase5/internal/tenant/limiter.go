// Package tenant implements the per-tenant delivery quota: a cached quota row,
// one token bucket per tenant and an in-flight gauge.
package tenant

import (
	"context"
	"sync"
	"time"

	"relayx/phase5/internal/store"
)

// QuotaSource loads tenant rows. *store.Store satisfies it.
type QuotaSource interface {
	GetTenant(ctx context.Context, tenantID string) (store.Tenant, bool, error)
}

type entry struct {
	quota    store.Tenant
	loadedAt time.Time
	tokens   float64
	last     time.Time
	inflight int
}

// Limiter enforces per-tenant rate and in-flight limits. Missing tenants use
// unlimited rate and the configured default in-flight limit.
type Limiter struct {
	source          QuotaSource
	cacheTTL        time.Duration
	defaultInflight int

	mu      sync.Mutex
	entries map[string]*entry
}

// NewLimiter returns a limiter. cacheTTL <= 0 disables caching.
func NewLimiter(source QuotaSource, cacheTTL time.Duration, defaultInflight int) *Limiter {
	if defaultInflight < 0 {
		defaultInflight = 0
	}
	return &Limiter{
		source:          source,
		cacheTTL:        cacheTTL,
		defaultInflight: defaultInflight,
		entries:         make(map[string]*entry),
	}
}

// Acquire reserves one in-flight slot and one token for tenantID. When ok is
// false the caller must postpone the work for retryAfter; no attempt is
// consumed. On success the returned release function must be called exactly
// once when the push finishes.
func (l *Limiter) Acquire(ctx context.Context, tenantID string, now time.Time) (release func(), retryAfter time.Duration, ok bool, err error) {
	l.mu.Lock()
	e, err := l.entry(ctx, tenantID, now)
	if err != nil {
		l.mu.Unlock()
		return nil, 0, false, err
	}

	limit := e.quota.MaxInflight
	if limit <= 0 {
		limit = l.defaultInflight
	}
	if limit > 0 && e.inflight >= limit {
		l.mu.Unlock()
		return nil, 50 * time.Millisecond, false, nil
	}

	if e.quota.RatePerSec > 0 {
		burst := float64(e.quota.Burst)
		if burst <= 0 {
			burst = float64(e.quota.RatePerSec)
		}
		elapsed := now.Sub(e.last).Seconds()
		if elapsed > 0 {
			e.tokens += elapsed * float64(e.quota.RatePerSec)
			if e.tokens > burst {
				e.tokens = burst
			}
			e.last = now
		}
		if e.tokens < 1 {
			wait := time.Duration((1-e.tokens)/float64(e.quota.RatePerSec)*float64(time.Second)) + time.Millisecond
			l.mu.Unlock()
			return nil, wait, false, nil
		}
		e.tokens--
	}

	e.inflight++
	l.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			if e.inflight > 0 {
				e.inflight--
			}
			l.mu.Unlock()
		})
	}, 0, true, nil
}

// Inflight returns the current in-flight count for tenantID.
func (l *Limiter) Inflight(tenantID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.entries[tenantID]; ok {
		return e.inflight
	}
	return 0
}

// Snapshot returns the current in-flight count of every tenant that has been
// admitted at least once.
func (l *Limiter) Snapshot() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]int, len(l.entries))
	for id, e := range l.entries {
		if e.inflight > 0 {
			out[id] = e.inflight
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

func (l *Limiter) entry(ctx context.Context, tenantID string, now time.Time) (*entry, error) {
	if e, ok := l.entries[tenantID]; ok {
		if l.cacheTTL <= 0 || now.Sub(e.loadedAt) < l.cacheTTL {
			return e, nil
		}
	}
	e, ok := l.entries[tenantID]
	if !ok {
		e = &entry{tokens: 1, last: now}
		l.entries[tenantID] = e
	}
	if l.source == nil {
		e.quota = store.Tenant{ID: tenantID}
		e.loadedAt = now
		return e, nil
	}
	quota, found, err := l.source.GetTenant(ctx, tenantID)
	if err != nil {
		// Keep serving with the previous quota instead of failing delivery
		// because the quota lookup failed.
		if e.loadedAt.IsZero() {
			e.quota = store.Tenant{ID: tenantID}
			e.loadedAt = now
		}
		return e, nil
	}
	if !found {
		quota = store.Tenant{ID: tenantID}
	}
	e.quota = quota
	e.loadedAt = now
	if e.last.IsZero() {
		e.last = now
	}
	return e, nil
}
