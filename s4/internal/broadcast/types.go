// Package broadcast 实现 S3 订阅与广播：订阅索引、验签、共享编码、扇出与 HTTP 受理。
// 广播是连接级 Best-Effort 语义：不进 S2 可靠状态机、不落库、不补发（LLD §1）。
package broadcast

import (
	"context"
	"errors"
	"time"

	"relayx/s4/internal/sendpath"
)

// 广播域的容量与错误（LLD §5.1 / §10）。
const (
	DefaultMaxPayload = 8 << 10 // 8 KiB
	DefaultMaxTopic   = 256
)

var (
	ErrBadToken        = errors.New("broadcast: invalid subscribe token")
	ErrTokenExpired    = errors.New("broadcast: subscribe token expired")
	ErrUnknownKid      = errors.New("broadcast: unknown key id")
	ErrWrongTenant     = errors.New("broadcast: token tenant mismatch")
	ErrWrongTopic      = errors.New("broadcast: token topic mismatch")
	ErrPayloadTooBig   = errors.New("broadcast: payload exceeds limit")
	ErrBadTopic        = errors.New("broadcast: invalid topic")
	ErrIndexFull       = errors.New("broadcast: subscription index full")
	ErrSessionFull     = errors.New("broadcast: session topic limit reached")
	ErrTopicFull       = errors.New("broadcast: topic subscriber limit reached")
	ErrNotSubscribed   = errors.New("broadcast: not subscribed")
	ErrStaleSubscriber = errors.New("broadcast: stale subscriber (unsubscribed or closed)")
	ErrDegraded        = errors.New("broadcast: subscriber degraded (slow consumer)")
)

// SubscriptionClaims 是订阅 token 验签后得到的声明。
type SubscriptionClaims struct {
	TenantID  string
	UserID    string
	Topic     string
	ExpiresAt time.Time
	Kid       string
}

// SubscribeTokenVerifier 只验签：tenant 级密钥 + kid，绝不查业务成员库（LLD §4）。
type SubscribeTokenVerifier interface {
	Verify(ctx context.Context, token string) (SubscriptionClaims, error)
}

// P2Sender 是订阅者连接的 P2 通道发送函数（LLD §2.3）。
// 只接受指针：值传递会复制 atomic.Int32 引用计数器。
type P2Sender interface {
	TrySendBroadcast(frame *sendpath.ImmutableFrame) error
}

// SubscriberRef 是扇出快照中的一个订阅者引用。
// Version 用于投递前的退订竞态校验（LLD §9.1）。
type SubscriberRef struct {
	SessionID string
	Sender    P2Sender
	Version   uint64
}

// SenderFactory 由 gateway 注入：把一个已订阅会话解析为绑定 topic/version 的 P2Sender。
// 投递前由 P2Sender 自行校验连接存活与订阅版本未变。
type SenderFactory func(sessionID, topic string, version uint64) P2Sender

// IndexStats 是索引容量指标（供 /metrics）。
type IndexStats struct {
	Subscriptions int
	Topics        int
	Sessions      int
	FullEvents    int64
}

// SubscriptionIndex 是纯内存双向索引（LLD §5）。
// 快照按 (tenant, topic) 定位：广播只扇出给同租户订阅者。
type SubscriptionIndex interface {
	Subscribe(sessID string, claims SubscriptionClaims, senderFactory SenderFactory) (bool, error)
	Unsubscribe(sessID, topic string) (bool, error)
	Snapshot(tenantID, topic string) []SubscriberRef
	VersionMatches(sessID, topic string, version uint64) bool
	RemoveSession(sessID string) []string
	Stats() IndexStats
}

// BroadcastInput 是一次发布输入。IdemKey 仅用于 HTTP 受理层内存去重窗口。
type BroadcastInput struct {
	TenantID string
	Topic    string
	Payload  []byte
	IdemKey  string
}

// Accepted 是受理结果：202 语义「已进入扇出流程，不代表任何订阅者已收到」。
type Accepted struct {
	Accepted   bool
	Duplicate  bool
	AcceptedAt time.Time
}

// FanoutResult 是一次扇出的计数（目标数/成功入队数/丢弃数/跳过数/编码字节数）。
type FanoutResult struct {
	Targets      int
	Enqueued     int
	Dropped      int
	Skipped      int
	EncodedBytes int
	Seq          uint64
}

// BroadcastIngress 是广播受理入口（LLD §2.3）。
type BroadcastIngress interface {
	Publish(ctx context.Context, in BroadcastInput) (Accepted, error)
}

// BroadcastFanout 是扇出器（LLD §2.3 / §7）。
type BroadcastFanout interface {
	Fanout(ctx context.Context, in BroadcastInput) FanoutResult
}

// SharedEncoder 每次 publish 只调用一次，返回共享只读帧（LLD §2.3）。
type SharedEncoder interface {
	Encode(topic string, payload []byte, serverReceivedAt time.Time, seq uint64) (*sendpath.ImmutableFrame, error)
}
