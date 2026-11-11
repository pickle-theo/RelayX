package global

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"relayx/phase5.5/internal/store"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fakeQuotaSource serves a fixed quota and counts the lookups.
type fakeQuotaSource struct {
	mu      sync.Mutex
	tenants map[string]store.Tenant
	lookups int
}

func (f *fakeQuotaSource) GetTenant(_ context.Context, tenantID string) (store.Tenant, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	q, ok := f.tenants[tenantID]
	return q, ok, nil
}

func (f *fakeQuotaSource) lookupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lookups
}

func TestAcquireHonoursTheLocalInflightLimit(t *testing.T) {
	src := &fakeQuotaSource{tenants: map[string]store.Tenant{
		"acme": {ID: "acme"},
	}}
	l := New(nil, src, Options{DefaultInflight: 2, CacheTTL: time.Minute, Logger: testLogger()})
	ctx := context.Background()

	release1, _, ok, err := l.Acquire(ctx, "acme", time.Now())
	if err != nil || !ok {
		t.Fatalf("first acquire ok=%v err=%v", ok, err)
	}
	release2, _, ok, err := l.Acquire(ctx, "acme", time.Now())
	if err != nil || !ok {
		t.Fatalf("second acquire ok=%v err=%v", ok, err)
	}
	if _, _, ok, err := l.Acquire(ctx, "acme", time.Now()); err != nil || ok {
		t.Fatalf("third acquire ok=%v err=%v, want rejected", ok, err)
	}
	if got := l.Inflight("acme"); got != 2 {
		t.Fatalf("inflight = %d, want 2", got)
	}

	release1()
	release2()
	if got := l.Inflight("acme"); got != 0 {
		t.Fatalf("inflight after release = %d, want 0", got)
	}
}

func TestAcquireHonoursTheExplicitInflightLimit(t *testing.T) {
	src := &fakeQuotaSource{tenants: map[string]store.Tenant{
		"acme": {ID: "acme", MaxInflight: 1},
	}}
	l := New(nil, src, Options{DefaultInflight: 100, CacheTTL: time.Minute, Logger: testLogger()})
	ctx := context.Background()

	release, _, ok, err := l.Acquire(ctx, "acme", time.Now())
	if err != nil || !ok {
		t.Fatalf("first acquire ok=%v err=%v", ok, err)
	}
	if _, _, ok, err := l.Acquire(ctx, "acme", time.Now()); err != nil || ok {
		t.Fatalf("second acquire ok=%v err=%v, want rejected", ok, err)
	}
	release()
	if _, _, ok, err := l.Acquire(ctx, "acme", time.Now()); err != nil || !ok {
		t.Fatalf("acquire after release ok=%v err=%v", ok, err)
	}
}

func TestAcquireCachesTheQuota(t *testing.T) {
	src := &fakeQuotaSource{tenants: map[string]store.Tenant{
		"acme": {ID: "acme"},
	}}
	l := New(nil, src, Options{DefaultInflight: 10, CacheTTL: time.Minute, Logger: testLogger()})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		release, _, ok, err := l.Acquire(ctx, "acme", time.Now())
		if err != nil || !ok {
			t.Fatalf("acquire %d ok=%v err=%v", i, ok, err)
		}
		release()
	}
	if got := src.lookupCount(); got != 1 {
		t.Fatalf("quota lookups = %d, want 1", got)
	}
}

func TestAcquireForgetsAnUnknownTenant(t *testing.T) {
	src := &fakeQuotaSource{tenants: map[string]store.Tenant{}}
	l := New(nil, src, Options{DefaultInflight: 1, CacheTTL: time.Minute, Logger: testLogger()})
	ctx := context.Background()

	release, _, ok, err := l.Acquire(ctx, "ghost", time.Now())
	if err != nil || !ok {
		t.Fatalf("acquire ok=%v err=%v", ok, err)
	}
	defer release()
	if _, _, ok, err := l.Acquire(ctx, "ghost", time.Now()); err != nil || ok {
		t.Fatalf("second acquire ok=%v err=%v, want rejected", ok, err)
	}
}

func TestSnapshotOnlyReportsBusyTenants(t *testing.T) {
	src := &fakeQuotaSource{tenants: map[string]store.Tenant{
		"acme": {ID: "acme"},
	}}
	l := New(nil, src, Options{DefaultInflight: 10, CacheTTL: time.Minute, Logger: testLogger()})
	ctx := context.Background()

	release, _, _, _ := l.Acquire(ctx, "acme", time.Now())
	snap := l.Snapshot()
	if snap["acme"] != 1 {
		t.Fatalf("snapshot = %v, want acme=1", snap)
	}
	release()
	snap = l.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("snapshot = %v, want empty", snap)
	}
}

func TestBackoffFollowsTheRate(t *testing.T) {
	l := New(nil, nil, Options{Logger: testLogger()})
	if got := l.backoff(0); got != 50*time.Millisecond {
		t.Fatalf("backoff(0) = %v, want 50ms", got)
	}
	if got := l.backoff(10); got != 100*time.Millisecond {
		t.Fatalf("backoff(10) = %v, want 100ms", got)
	}
}
