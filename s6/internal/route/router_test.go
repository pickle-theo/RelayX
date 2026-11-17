package route

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/clock"
	"relayx/s6/internal/model"
)

func testRouter(b backend.Backend, clk clock.Clock, nodeID string) *Router {
	return NewRouter(Options{
		Backend: b, Clock: clk, NodeID: nodeID,
		CacheTTL: time.Minute, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func TestBindIndexesBeforeBinding(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := testRouter(b, clk.Now, "gw1")
	ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}

	b.SetFault("AddEndpointIndex", errors.New("SADD failed"))
	if _, err := r.Bind(context.Background(), ep); err == nil {
		t.Fatal("bind must fail when index write fails")
	}
	if _, ok, _ := b.Lookup(context.Background(), ep); ok {
		t.Fatal("binding must not exist when index write failed")
	}
	b.SetFault("AddEndpointIndex", nil)

	binding, err := r.Bind(context.Background(), ep)
	if err != nil || binding.NodeID != "gw1" {
		t.Fatalf("bind: %+v err=%v", binding, err)
	}
	eps, _ := b.EndpointIndex(context.Background(), "gw1")
	if len(eps) != 1 {
		t.Fatalf("index must contain endpoint: %v", eps)
	}
}

func TestUnbindStaleIgnoredAndCacheCleared(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := testRouter(b, clk.Now, "gw1")
	ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}

	v1, _ := r.Bind(context.Background(), ep)
	v2, _ := r.Bind(context.Background(), ep)
	deleted, err := r.Unbind(context.Background(), ep, v1.Version)
	if err != nil || deleted {
		t.Fatalf("stale unbind: deleted=%v err=%v", deleted, err)
	}
	if _, ok := r.GetLocalCache(ep); ok {
		t.Fatal("stale unbind must clear local cache")
	}
	binding, ok, _ := r.Lookup(context.Background(), ep)
	if !ok || binding.Version != v2.Version {
		t.Fatalf("new binding must survive: %+v ok=%v", binding, ok)
	}
}

func TestDegradationRejectsLoginFallsBackToCache(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := testRouter(b, clk.Now, "gw1")
	ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}
	binding, err := r.Bind(context.Background(), ep)
	if err != nil {
		t.Fatal(err)
	}

	b.SetDown(true)
	if _, err := r.Bind(context.Background(), ep); !errors.Is(err, ErrBackendDown) {
		t.Fatalf("login during degradation must be rejected: %v", err)
	}
	got, ok, err := r.Lookup(context.Background(), ep)
	if err != nil || !ok || got.Version != binding.Version {
		t.Fatalf("cache fallback: %+v ok=%v err=%v", got, ok, err)
	}
	b.SetDown(false)
	if !b.Available(context.Background()) {
		t.Fatal("backend should recover")
	}
}

func TestLocalCacheTTLExpiry(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := NewRouter(Options{Backend: b, Clock: clk.Now, NodeID: "gw1", CacheTTL: 5 * time.Second})
	ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}
	_, _ = r.Bind(context.Background(), ep)
	b.SetDown(true)
	if _, ok := r.GetLocalCache(ep); !ok {
		t.Fatal("cache should be warm")
	}
	clk.Advance(6 * time.Second)
	if _, ok := r.GetLocalCache(ep); ok {
		t.Fatal("cache must expire after TTL")
	}
}
