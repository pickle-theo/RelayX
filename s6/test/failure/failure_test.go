package failure

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/dispatch"
	"relayx/s6/internal/model"
	"relayx/s6/internal/route"
	"relayx/s6/internal/telemetry"
	"relayx/s6/protocol"
)

// TestVersionGuardConcurrent 验证 100 轮"旧 DEL 与新 INCR+HSET 并发"下新路由始终存在
// （LLD §13.2 Version 守卫（并发竞态））。
func TestVersionGuardConcurrent(t *testing.T) {
	for round := 0; round < 100; round++ {
		b := backend.NewMemoryBackend()
		r := route.NewRouter(route.Options{
			Backend: b, NodeID: "gw1", Metrics: telemetry.New(),
		})
		ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}
		old, err := r.Bind(context.Background(), ep)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		var newBinding model.NodeBinding
		go func() {
			defer wg.Done()
			nb, _ := r.Bind(context.Background(), ep)
			newBinding = nb
		}()
		go func() {
			defer wg.Done()
			_, _ = r.Unbind(context.Background(), ep, old.Version)
		}()
		wg.Wait()

		binding, ok, err := r.Lookup(context.Background(), ep)
		if err != nil || !ok {
			t.Fatalf("round %d: binding lost: ok=%v err=%v", round, ok, err)
		}
		if binding.Version != newBinding.Version {
			t.Fatalf("round %d: version=%d want %d (stale unbind deleted new route)", round, binding.Version, newBinding.Version)
		}
	}
}

// TestForwardFailureNeverTerminal 验证转发失败不会被误记 FAILED（LLD §13.2）。
func TestForwardFailureNeverTerminal(t *testing.T) {
	// 连续 1-2 次不可达 → B（退避，状态不变、attempts 不增）。
	for i := 1; i < 3; i++ {
		o := dispatch.ClassifyFailure(dispatch.ErrNodeUnreachable, i, 3, 5)
		if o.Kind != dispatch.OutcomeRetryLater || o.AdvanceAttempts || o.NewState != nil {
			t.Fatalf("attempt %d must not fail delivery: %+v", i, o)
		}
	}
	// 第 3 次不可达 → D（回落离线，仍不改变状态）。
	o := dispatch.ClassifyFailure(dispatch.ErrNodeUnreachable, 3, 3, 5)
	if o.Kind != dispatch.OutcomeOfflineHold || !o.ClearNextAttemptAt || o.AdvanceAttempts {
		t.Fatalf("third unreachable must hold offline: %+v", o)
	}
}

// TestForwardNackNoAmplification 验证 NACK 只刷新缓存、不改变状态、不触发额外转发
// （LLD §13.2 FORWARD_NACK 不放大）。
func TestForwardNackNoAmplification(t *testing.T) {
	for _, reason := range []byte{protocol.NackNoSession, protocol.NackVersionMismatch, protocol.NackEnqueueFull} {
		o := dispatch.ClassifyNack(reason, 5)
		if o.Kind != dispatch.OutcomeNackRetry || o.AdvanceAttempts || o.NewState != nil || o.ClearNextAttemptAt {
			t.Fatalf("nack %s must not change delivery state: %+v", protocol.NackReasonName(reason), o)
		}
		if o.BackoffSeconds != 5 {
			t.Fatalf("nack %s must carry backoff (§4.4 C): %+v", protocol.NackReasonName(reason), o)
		}
	}
}

// TestRedisDownRejectsLoginButKeepsSession 验证 Redis 全停降级（LLD §7.4）。
func TestRedisDownRejectsLoginButKeepsSession(t *testing.T) {
	b := backend.NewMemoryBackend()
	r := route.NewRouter(route.Options{Backend: b, NodeID: "gw1", Metrics: telemetry.New()})
	ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}
	_, err := r.Bind(context.Background(), ep)
	if err != nil {
		t.Fatal(err)
	}
	b.SetDown(true)
	if _, err := r.Bind(context.Background(), ep); !errors.Is(err, route.ErrBackendDown) {
		t.Fatalf("login must be rejected: %v", err)
	}
	if _, ok, err := r.Lookup(context.Background(), ep); err != nil || !ok {
		t.Fatalf("existing route must be readable from cache: ok=%v err=%v", ok, err)
	}
}

// TestReclaimSkipsRebound 验证节点死亡回收不误删已重绑路由（LLD §13.2）。
func TestReclaimSkipsRebound(t *testing.T) {
	b := backend.NewMemoryBackend()
	ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}
	_ = b.AddEndpointIndex(context.Background(), "gw1", ep)
	_, _, _ = b.Bind(context.Background(), ep, "gw1", time.Now())
	_, _, _ = b.Bind(context.Background(), ep, "gw2", time.Now())

	deleted, err := b.DeleteIfOwner(context.Background(), ep, "gw1")
	if err != nil || deleted {
		t.Fatalf("reclaim must skip rebound: deleted=%v err=%v", deleted, err)
	}
	binding, ok, _ := b.Lookup(context.Background(), ep)
	if !ok || binding.NodeID != "gw2" {
		t.Fatalf("gw2 route must survive: %+v ok=%v", binding, ok)
	}
}
