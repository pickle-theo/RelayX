// Package store 定义 S2 可靠内核的持久化契约。
//
// 所有过期/重试判定都由 store 完成（postgres 中使用 SQL now()，
// 语义内存实现中使用注入的 store 时钟）。调用方
// 从不通过这些接口传递时间值。
package store

import (
	"context"
	"errors"
	"time"
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

const (
	ProducerUser    = "user"
	ProducerService = "service"

	ModeAllEndpoints = "all_endpoints"
	ModeLastActive   = "last_active"

	EndpointOnline  = "ONLINE"
	EndpointOffline = "OFFLINE"

	FactMessageAccepted      = "message.accepted"
	FactMessageSettled       = "message.settled"
	FactMessageExpired       = "message.expired"
	FactDeliveryCreated      = "delivery.created"
	FactDeliverySent         = "delivery.sent"
	FactDeliveryReceiptAcked = "delivery.receipt_acked"
	FactDeliveryFailed       = "delivery.failed"
	FactDeliveryExpired      = "delivery.expired"
)

var (
	ErrNotFound           = errors.New("store: not found")
	ErrDuplicateMismatch  = errors.New("store: idempotency key reused with different payload")
	ErrDBUnavailable      = errors.New("store: database unavailable")
	ErrOffline            = errors.New("store: endpoint offline")
	ErrPolicyLimit        = errors.New("store: policy limit exceeded")
	ErrUnknownMessage     = errors.New("store: unknown message id")
	ErrConcurrentConflict = errors.New("store: concurrent state conflict")
)

// Message 表示一条逻辑消息。source_device_id 始终由服务端
// 填充：空串表示服务/HTTP 提交，绝不为 NULL。
type Message struct {
	ID             string
	TenantID       string
	ProducerID     string
	ProducerKind   string
	Sender         string
	SourceDeviceID string
	ClientMsgID    string
	Payload        []byte
	PayloadSize    int32
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

// Delivery 表示单端点投递，永远不会携带 SETTLED 状态。
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
	NextAttemptAt      *time.Time
	ApplicationStatus  *string
	ApplicationAckedAt *time.Time
	LastError          *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
	TerminalAt         *time.Time
	// Online 是扇出时的瞬时提示：为 true 时由 store 基于自身
	// 时钟派生 next_attempt_at。它从不作为持久化列。
	Online bool
}

// DeliveryPatch 承载一次转换要修改的字段。StateVersion 与
// UpdatedAt 始终由 store 推进，调用方不得指定。
// ClearNextAttemptAt 用于区分“保持不变”与“置为 NULL”。
type DeliveryPatch struct {
	State                  string
	NextAttemptAt          *time.Time
	ClearNextAttemptAt     bool
	Attempts               *int32
	ApplicationStatus      *string
	ClearApplicationStatus bool
	ApplicationAckedAt     *time.Time
	LastError              *string
	ClearLastError         bool
	TerminalAt             *time.Time
}

// Fact 是只追加的传输事实。endpoint_id == nil 表示
// 消息级事实。
type Fact struct {
	EventID        string
	TenantID       string
	MessageID      string
	EndpointID     *string
	FactType       string
	StateVersion   int
	OccurredAt     time.Time
	PayloadSummary map[string]any
	PublishedAt    *time.Time
}

// Endpoint 是内存会话注册表的持久化投影。
type Endpoint struct {
	TenantID   string
	UserID     string
	DeviceID   string
	Status     string
	SessionID  string
	LastSeenAt time.Time
}

// FinalCounts 由结算/过期 CAS 语句原子返回。
type FinalCounts struct {
	Total        int `json:"total"`
	ReceiptAcked int `json:"receipt_acked"`
	Failed       int `json:"failed"`
	Expired      int `json:"expired"`
}

// CASResult 是消息终态 CAS 的单行结果。
type CASResult struct {
	StateVersion int
	TerminalAt   time.Time
	Counts       FinalCounts
}

// Tx 是事务句柄。共享该句柄的事实与状态写入
// 在同一事务中提交。
type Tx interface {
	Messages() MessageStore
	Deliveries() DeliveryStore
	Facts() FactStore
	Commit() error
	Rollback() error
}

// Store 是根持久化句柄。
type Store interface {
	WithinTx(ctx context.Context, fn func(Tx) error) error
	Messages() MessageStore
	Deliveries() DeliveryStore
	Endpoints() EndpointRegistry
	Facts() FactStore
	Close() error
}

// MessageStore 持久化逻辑消息。
type MessageStore interface {
	// Insert 原子地按幂等键插入消息。inserted=false 表示同键消息已存在，
	// 此时 existing 返回库中那条权威消息（payload 可能不同，由调用方判定）。
	// 调用方必须在同一事务内据此决定受理还是返回幂等重放，杜绝先查后写的竞态。
	Insert(ctx context.Context, tx Tx, m *Message) (inserted bool, existing *Message, err error)
	GetByID(ctx context.Context, id string) (*Message, error)
	GetByIdempotencyKey(ctx context.Context, tenant, producerID, clientMsgID string) (*Message, error)
	ListExpired(ctx context.Context, limit int) ([]*Message, error)
	MarkState(ctx context.Context, tx Tx, id, state string, version int) error
	MarkFanoutDone(ctx context.Context, tx Tx, id string) (bool, error)
	SettleIfEligible(ctx context.Context, tx Tx, id string) (CASResult, bool, error)
	ExpireIfEligible(ctx context.Context, tx Tx, id string) (CASResult, bool, error)
}

// DeliveryStore 持久化单端点投递状态。
type DeliveryStore interface {
	InsertBatch(ctx context.Context, tx Tx, ds []*Delivery) error
	GetByKey(ctx context.Context, tenant, msgID, user, device string) (*Delivery, error)
	ListByMessage(ctx context.Context, msgID string) ([]*Delivery, error)
	ListDue(ctx context.Context, limit int) ([]*Delivery, error)
	ListPendingByEndpoint(ctx context.Context, tenant, user, device string) ([]*Delivery, error)
	ActivatePending(ctx context.Context, tx Tx, tenant, user, device string) error
	// MarkSent 推进 PENDING/SENT → SENT，并由 store 时钟加退避
	// 派生 next_attempt_at；不涉及调用方提供的时间戳。
	MarkSent(ctx context.Context, tx Tx, id string, attempts int32, backoffSeconds int32) (int, bool, error)
	Transition(ctx context.Context, tx Tx, id string, from []string, to string, patch DeliveryPatch, version int) (bool, error)
	ExpireIfEligible(ctx context.Context, tx Tx, id string) (int, bool, error)
	SetApplicationAck(ctx context.Context, tx Tx, id string, status string, at time.Time) error
}

// FactStore 持久化传输事实，并以确定性 event_id 保证幂等。
type FactStore interface {
	InsertBatch(ctx context.Context, tx Tx, facts []*Fact) error
	ListByMessage(ctx context.Context, msgID string) ([]*Fact, error)
}

// EndpointRegistry 解析投递目标并维护持久化投影。
type EndpointRegistry interface {
	Resolve(ctx context.Context, tenant, user string, devices []string, mode string, limit int32) ([]Endpoint, error)
	MarkOnline(ctx context.Context, tenant, user, device, sessionID string) error
	MarkOffline(ctx context.Context, tenant, user, device, sessionID string) error
	ResetAllOffline(ctx context.Context) error
	ListInconsistentOnline(ctx context.Context, isOnline func(tenant, user, device string) bool) ([]Endpoint, error)
}

// Clock 是 store 自身的时间源。它不是任何业务接口的
// 方法参数；在此注入用于模拟数据库时钟，并使
// “进程时钟偏移不改变判定”这一不变量可测试。
type Clock interface {
	Now() time.Time
}
