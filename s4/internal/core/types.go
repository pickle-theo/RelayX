// Package core 提供 S4 单机验证所需的「S2 兼容事实生产者」。
//
// S4 的职责是消费 S2 冻结的 delivery_facts 并投影为客户端事件，不重新定义
// 事实语义。完整部署中 S4 事件投影运行在 S2 进程内；本仓库各阶段是独立 Go
// module，无法跨 module 引用 s2/internal，因此这里以与 S2 完全一致的字段与
// event_id 派生规则实现一个进程内可靠内核，作为 S4 的事实来源：
//
//   - 消息级 ACCEPTED → SETTLED / EXPIRED，投递级 PENDING → SENT → RECEIPT_ACKED；
//   - 投递级没有 SETTLED，SETTLED 只属于消息级；
//   - 事实只追加，event_id 确定性派生，计数来自结算事务内的同一份统计。
//
// 它只服务于 S4 的事件链路验证，不是第二套语义定义：字段、状态枚举与
// event_id 规则均取自 S2 冻结契约，投影层只读事实、不写状态。
package core

import (
	"time"

	"relayx/s4/protocol"
)

// 消息级状态。刻意不设置 COMPLETED。
const (
	MessageAccepted = "ACCEPTED"
	MessageSettled  = "SETTLED"
	MessageExpired  = "EXPIRED"
)

// 投递级状态。SETTLED 只属于消息级。
const (
	DeliveryPending      = "PENDING"
	DeliverySent         = "SENT"
	DeliveryReceiptAcked = "RECEIPT_ACKED"
	DeliveryFailed       = "FAILED"
	DeliveryExpired      = "EXPIRED"
)

// 事实类型（S2 冻结）。
const (
	FactMessageAccepted      = "message.accepted"
	FactMessageSettled       = "message.settled"
	FactMessageExpired       = "message.expired"
	FactDeliveryCreated      = "delivery.created"
	FactDeliverySent         = "delivery.sent"
	FactDeliveryReceiptAcked = "delivery.receipt_acked"
	FactDeliveryFailed       = "delivery.failed"
	FactDeliveryExpired      = "delivery.expired"
)

const (
	ProducerUser    = "user"
	ProducerService = "service"

	ModeAllEndpoints = "all_endpoints"
	ModeLastActive   = "last_active"
)

// Message 是一条逻辑消息。SourceDeviceID 由服务端从连接上下文派生，
// 空串表示服务/HTTP 提交；它绝不来自客户端字段。
type Message struct {
	ID             string
	TenantID       string
	ProducerID     string
	ProducerKind   string
	Sender         string
	SourceDeviceID string
	ClientMsgID    string
	Payload        []byte
	DeliveryMode   string
	Fanout         int32
	TTLSeconds     int32
	MaxAttempts    int32
	PlatformState  string
	StateVersion   int
	FanoutDoneAt   *time.Time
	CreatedAt      time.Time
	ExpiresAt      time.Time
	SettledAt      *time.Time
}

// Delivery 是单端点投递，永远不会携带 SETTLED 状态。
type Delivery struct {
	ID                 string
	TenantID           string
	MessageID          string
	UserID             string
	DeviceID           string
	EndpointID         string
	DeliveryState      string
	StateVersion       int
	Attempts           int32
	ApplicationStatus  *string
	ApplicationAckedAt *time.Time
	TerminalAt         *time.Time
}

// Fact 是只追加的传输事实。EndpointID == nil 表示消息级事实。
type Fact struct {
	EventID        string
	TenantID       string
	MessageID      string
	EndpointID     *string
	FactType       string
	StateVersion   int
	OccurredAt     time.Time
	PayloadSummary map[string]any
}

// Endpoint 是持久化端点目录条目（S2 endpoints 表的内存等价物）。
// Online 只是投影提示，判定在线仍以会话注册表为准。
type Endpoint struct {
	TenantID   string
	UserID     string
	DeviceID   string
	SessionID  string
	Online     bool
	LastSeenAt time.Time
}

// Input 是一次受理请求。身份由调用方从已验证令牌/会话提供。
type Input struct {
	TenantID       string
	ProducerID     string
	ProducerKind   string
	Sender         string
	SourceDeviceID string
	ClientMsgID    string
	TargetUserID   string
	TargetDevices  []string
	Payload        []byte
	Policy         Policy
}

// Policy 是消息级投递策略。
type Policy struct {
	TTLSeconds   int32
	MaxAttempts  int32
	DeliveryMode string
	Fanout       int32
}

// Result 是一次受理的结果。
type Result struct {
	MessageID  string
	State      string
	Duplicate  bool
	Counts     protocol.DeliveryCounts
	AcceptedAt time.Time
}

// Options 是内核的容量与默认策略。
type Options struct {
	MaxPayload         int32
	MaxTTL             int32
	MaxAttempts        int32
	DefaultTTL         int32
	DefaultMaxAttempts int32
}
