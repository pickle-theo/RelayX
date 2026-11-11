package tenant

import (
	"context"
	"errors"
	"testing"
	"time"

	"relayx/phase5/internal/store"
)

type fakeQuotaSource struct {
	quotas map[string]store.Tenant
	err    error
	calls  int
}

func (s *fakeQuotaSource) GetTenant(_ context.Context, tenantID string) (store.Tenant, bool, error) {
	s.calls++
	if s.err != nil {
		return store.Tenant{}, false, s.err
	}
	q, ok := s.quotas[tenantID]
	return q, ok, nil
}

func TestLimiterMissingTenantIsUnlimitedWithDefaultInflight(t *testing.T) {
	limiter := NewLimiter(&fakeQuotaSource{}, time.Minute, 2)
	now := time.Now()

	for i := 0; i < 2; i++ {
		release, _, ok, err := limiter.Acquire(context.Background(), "ghost", now)
		if err != nil || !ok {
			t.Fatalf("Acquire #%d = ok %v err %v, want admitted", i, ok, err)
		}
		defer release()
	}
	if got := limiter.Inflight("ghost"); got != 2 {
		t.Fatalf("Inflight = %d, want 2", got)
	}
	_, retryAfter, ok, err := limiter.Acquire(context.Background(), "ghost", now)
	if err != nil || ok {
		t.Fatalf("Acquire over default in-flight = ok %v err %v, want throttled", ok, err)
	}
	if retryAfter <= 0 {
		t.Fatalf("retryAfter = %v, want a positive hint", retryAfter)
	}
}

func TestLimiterRateDelaysWithoutConsumingInflight(t *testing.T) {
	src := &fakeQuotaSource{quotas: map[string]store.Tenant{
		"acme": {ID: "acme", RatePerSec: 1, Burst: 1, MaxInflight: 10},
	}}
	limiter := NewLimiter(src, time.Minute, 10)
	now := time.Now()

	release, _, ok, err := limiter.Acquire(context.Background(), "acme", now)
	if err != nil || !ok {
		t.Fatalf("first Acquire = ok %v err %v, want admitted", ok, err)
	}
	release()

	if got := limiter.Inflight("acme"); got != 0 {
		t.Fatalf("Inflight after release = %d, want 0", got)
	}

	_, retryAfter, ok, err := limiter.Acquire(context.Background(), "acme", now.Add(10*time.Millisecond))
	if err != nil || ok {
		t.Fatalf("second Acquire = ok %v err %v, want throttled", ok, err)
	}
	if retryAfter < 900*time.Millisecond {
		t.Fatalf("retryAfter = %v, want close to one second", retryAfter)
	}

	// One second later the bucket has refilled.
	if _, _, ok, err := limiter.Acquire(context.Background(), "acme", now.Add(time.Second)); err != nil || !ok {
		t.Fatalf("Acquire after refill = ok %v err %v, want admitted", ok, err)
	}
}

func TestLimiterTenantsAreIndependent(t *testing.T) {
	src := &fakeQuotaSource{quotas: map[string]store.Tenant{
		"acme":   {ID: "acme", RatePerSec: 1, Burst: 1, MaxInflight: 10},
		"normal": {ID: "normal", MaxInflight: 10},
	}}
	limiter := NewLimiter(src, time.Minute, 10)
	now := time.Now()

	release, _, ok, err := limiter.Acquire(context.Background(), "acme", now)
	if err != nil || !ok {
		t.Fatalf("acme first Acquire failed: ok %v err %v", ok, err)
	}
	release()

	if _, _, ok, err := limiter.Acquire(context.Background(), "acme", now); err != nil || ok {
		t.Fatalf("acme second Acquire = ok %v err %v, want throttled", ok, err)
	}
	for i := 0; i < 5; i++ {
		release, _, ok, err := limiter.Acquire(context.Background(), "normal", now)
		if err != nil || !ok {
			t.Fatalf("normal Acquire #%d = ok %v err %v, want admitted", i, ok, err)
		}
		defer release()
	}
}

func TestLimiterReleaseIsIdempotent(t *testing.T) {
	limiter := NewLimiter(nil, 0, 5)
	release, _, ok, err := limiter.Acquire(context.Background(), "default", time.Now())
	if err != nil || !ok {
		t.Fatalf("Acquire = ok %v err %v, want admitted", ok, err)
	}
	release()
	release()
	if got := limiter.Inflight("default"); got != 0 {
		t.Fatalf("Inflight after double release = %d, want 0", got)
	}
}

func TestLimiterLookupErrorKeepsPreviousQuota(t *testing.T) {
	src := &fakeQuotaSource{quotas: map[string]store.Tenant{
		"acme": {ID: "acme", MaxInflight: 1},
	}}
	limiter := NewLimiter(src, time.Millisecond, 10)
	now := time.Now()

	release, _, ok, err := limiter.Acquire(context.Background(), "acme", now)
	if err != nil || !ok {
		t.Fatalf("first Acquire = ok %v err %v", ok, err)
	}
	release()

	// The cached quota expires and the next lookup fails; delivery must keep
	// using the previous quota instead of failing.
	src.err = errors.New("redis down")
	limiter.mu.Lock()
	limiter.entries["acme"].loadedAt = now.Add(-time.Hour)
	limiter.mu.Unlock()

	release, _, ok, err = limiter.Acquire(context.Background(), "acme", now.Add(time.Hour))
	if err != nil || !ok {
		t.Fatalf("Acquire with failed lookup = ok %v err %v, want admitted with the old quota", ok, err)
	}
	release()
}

func TestLimiterForgetReloadsQuota(t *testing.T) {
	src := &fakeQuotaSource{quotas: map[string]store.Tenant{
		"acme": {ID: "acme", MaxInflight: 10},
	}}
	limiter := NewLimiter(src, time.Hour, 10)
	now := time.Now()
	release, _, ok, err := limiter.Acquire(context.Background(), "acme", now)
	if err != nil || !ok {
		t.Fatalf("first Acquire = ok %v err %v", ok, err)
	}
	release()

	src.quotas["acme"] = store.Tenant{ID: "acme", MaxInflight: 0}
	limiter.Forget("acme")

	if limiter.Inflight("acme") != 0 {
		t.Fatalf("Inflight after Forget = %d, want 0", limiter.Inflight("acme"))
	}
	if _, _, ok, err := limiter.Acquire(context.Background(), "acme", now); err != nil || !ok {
		t.Fatalf("Acquire after Forget = ok %v err %v, want admitted", ok, err)
	}
	if src.calls < 2 {
		t.Fatalf("quota lookup calls = %d, want at least 2", src.calls)
	}
}
