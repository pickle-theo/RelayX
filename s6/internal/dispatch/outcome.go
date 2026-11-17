// Package dispatch 把跨节点转发结果映射到投递状态（LLD §4.4）。
//
// 这是 S6 与 S2 之间**唯一的语义接缝**。映射规则由 LLD 固定，
// 实现者不得自由决定：转发失败绝不等于投递失败。
//
// 三条不可协商的结论：
//  1. 转发失败不改变 delivery_state（节点暂时不可达不能写成终态 FAILED，
//     否则 At-Least-Once 破功，聚合守卫也永远无法满足）；
//  2. 转发失败不增 attempts（否则路由抖动耗尽重试预算）；
//  3. 只有"对端已接收 FORWARD"才算一次真正的投递尝试（attempts 的语义
//     必须与"是否可能已到达客户端"一致）。
package dispatch

import "relayx/s6/protocol"

// ErrKind 是转发失败的错误分类（写入 delivery.last_error）。
type ErrKind string

const (
	ErrNodeUnreachable ErrKind = "NODE_UNREACHABLE"
	ErrMTLSFailure     ErrKind = "MTLS_FAILURE"
	ErrDialTimeout     ErrKind = "DIAL_TIMEOUT"
	ErrPeerRejected    ErrKind = "PEER_REJECTED"
	ErrNackNoSession   ErrKind = "NO_SESSION"
	ErrNackVersion     ErrKind = "VERSION_MISMATCH"
	ErrNackEnqueueFull ErrKind = "ENQUEUE_FULL"
)

// Outcome 是 dispatcher 对上层（S2 语义层）的唯一输出。
// 上层按 Outcome 更新 store；本包不碰任何 S2 存储或事实。
type Outcome struct {
	// Kind 是 A–E 中哪一种结果。
	Kind OutcomeKind
	// AdvanceAttempts 为 true 时，上层才把 attempts+1 并写 delivery.sent 事实。
	// 仅 A（FORWARD 成功入队）为 true。
	AdvanceAttempts bool
	// NewState 非 nil 表示需要显式状态迁移（仅 E：→ FAILED）。
	NewState *string
	// ClearNextAttemptAt 表示清 next_attempt_at（D：回落离线路径等重连）。
	ClearNextAttemptAt bool
	// BackoffSeconds > 0 表示设置 next_attempt_at = now + backoff（B/C）。
	BackoffSeconds int32
	// LastError 是错误分类（B/C/D 填充）。
	LastError ErrKind
}

// OutcomeKind 对应 LLD §4.4 的 A–E 五类结果。
type OutcomeKind uint8

const (
	OutcomeAccepted    OutcomeKind = iota // A：成功入队 → SENT，attempts+1
	OutcomeRetryLater                     // B：转发失败 → 状态不变、不增 attempts、退避
	OutcomeNackRetry                      // C：FORWARD_NACK → 同 B
	OutcomeOfflineHold                    // D：连续 3 次不可达 → 清缓存、回落离线
	OutcomeFailed                         // E：attempts 已达 max_attempts → FAILED
)

// DeliveryState 是 S2 冻结的投递级状态（语义契约 §3.2）。
// 这里只定义名称，不重新定义状态机——状态机仍由 S2 实现。
const (
	StatePending      = "PENDING"
	StateSent         = "SENT"
	StateReceiptAcked = "RECEIPT_ACKED"
	StateFailed       = "FAILED"
	StateExpired      = "EXPIRED"
)

// ClassifyNack 把 FORWARD_NACK 的 reason 映射到 Outcome（§4.4 C）。
// NACK 不改变状态、不增 attempts，只记录错误与退避（next_attempt_at = now + backoff）。
// backoffSeconds 由调用方按配置传入，与 ClassifyFailure 的 B 类保持一致。
func ClassifyNack(reason byte, backoffSeconds int32) Outcome {
	var kind ErrKind
	switch reason {
	case protocol.NackNoSession:
		kind = ErrNackNoSession
	case protocol.NackVersionMismatch:
		kind = ErrNackVersion
	case protocol.NackEnqueueFull:
		kind = ErrNackEnqueueFull
	default:
		kind = ErrPeerRejected
	}
	return Outcome{Kind: OutcomeNackRetry, LastError: kind, BackoffSeconds: backoffSeconds}
}

// ClassifyFailure 把转发失败映射到 Outcome（§4.4 B / D）。
//
//   - consecutiveUnreachable 是本端点连续不可达次数（含本次）；
//     达到 limit（默认 3）→ D：回落离线路径。
//   - 其他失败 → B：状态不变、不增 attempts、退避。
//
// attempts 已达上限由 Caller 在成功后判定（见 AcceptedOutcome / FailedIfExhausted）。
func ClassifyFailure(kind ErrKind, consecutiveUnreachable, limit int, backoffSeconds int32) Outcome {
	if kind == ErrNodeUnreachable && limit > 0 && consecutiveUnreachable >= limit {
		return Outcome{
			Kind: OutcomeOfflineHold, LastError: kind,
			ClearNextAttemptAt: true,
		}
	}
	return Outcome{
		Kind: OutcomeRetryLater, LastError: kind,
		BackoffSeconds: backoffSeconds,
	}
}

// AcceptedOutcome 返回 A 类结果：成功入队 → 推进 attempts 并写 delivery.sent。
func AcceptedOutcome() Outcome {
	return Outcome{Kind: OutcomeAccepted, AdvanceAttempts: true}
}

// FailedIfExhausted 在 A 类成功后判断 attempts 是否耗尽（§4.4 E）。
// attempts 是本次成功入队后的值（即旧 attempts+1）。返回 nil 表示未耗尽。
func FailedIfExhausted(attempts, maxAttempts int32) *Outcome {
	if attempts < maxAttempts {
		return nil
	}
	state := StateFailed
	return &Outcome{Kind: OutcomeFailed, NewState: &state, AdvanceAttempts: true}
}
