package dispatch

import (
	"testing"

	"relayx/s6/protocol"
)

func TestForwardFailureNeverFailsDelivery(t *testing.T) {
	// §4.4 B：节点不可达 → 状态不变、不增 attempts、退避。
	o := ClassifyFailure(ErrNodeUnreachable, 1, 3, 5)
	if o.Kind != OutcomeRetryLater || o.AdvanceAttempts || o.NewState != nil || o.BackoffSeconds != 5 {
		t.Fatalf("unexpected outcome: %+v", o)
	}
	if o.LastError != ErrNodeUnreachable {
		t.Fatalf("last_error = %q", o.LastError)
	}
}

func TestThreeConsecutiveUnreachableGoesOffline(t *testing.T) {
	// §4.4 D：连续 3 次不可达 → 清缓存、回落离线路径（next_attempt_at=NULL）。
	o := ClassifyFailure(ErrNodeUnreachable, 3, 3, 5)
	if o.Kind != OutcomeOfflineHold || !o.ClearNextAttemptAt || o.BackoffSeconds != 0 {
		t.Fatalf("unexpected offline outcome: %+v", o)
	}
	if o.AdvanceAttempts || o.NewState != nil {
		t.Fatal("offline hold must not change state or attempts")
	}
}

func TestNackNeverChangesState(t *testing.T) {
	// §4.4 C：FORWARD_NACK 按 B 处理。
	for _, reason := range []byte{protocol.NackNoSession, protocol.NackVersionMismatch, protocol.NackEnqueueFull} {
		o := ClassifyNack(reason, 5)
		if o.Kind != OutcomeNackRetry || o.AdvanceAttempts || o.NewState != nil {
			t.Fatalf("nack %d changed state: %+v", reason, o)
		}
		// §4.4 C：NACK 按 B 处理，必须同时记录错误分类与退避，
		// 否则上层会把 next_attempt_at 设成 now，导致路由抖动时紧循环重试。
		if o.BackoffSeconds != 5 || o.LastError == "" {
			t.Fatalf("nack %d must record error and backoff: %+v", reason, o)
		}
	}
}

func TestAcceptedAdvancesAttempts(t *testing.T) {
	// §4.4 A：成功入队才算一次真正投递尝试。
	o := AcceptedOutcome()
	if o.Kind != OutcomeAccepted || !o.AdvanceAttempts || o.NewState != nil {
		t.Fatalf("unexpected accepted outcome: %+v", o)
	}
}

func TestFailedOnlyOnRealAttemptExhaustion(t *testing.T) {
	// §4.4 E：只有成功入队的尝试累计到上限才 FAILED。
	if got := FailedIfExhausted(4, 5); got != nil {
		t.Fatalf("must not fail before max: %+v", got)
	}
	got := FailedIfExhausted(5, 5)
	if got == nil || got.Kind != OutcomeFailed || got.NewState == nil || *got.NewState != StateFailed {
		t.Fatalf("must fail at max: %+v", got)
	}
	if !got.AdvanceAttempts {
		t.Fatal("failure at max still consumed the attempt")
	}
}

func TestMTLSFailureIsRetryNotOffline(t *testing.T) {
	// mTLS/拨号失败属于 B 类，不累积到"节点不可达"计数（§4.4 B vs D 区分）。
	o := ClassifyFailure(ErrMTLSFailure, 10, 3, 5)
	if o.Kind != OutcomeRetryLater || o.ClearNextAttemptAt {
		t.Fatalf("mtls failure must be retry, not offline: %+v", o)
	}
}
