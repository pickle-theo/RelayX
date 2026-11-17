// Package sdk 是 S4 交付的 Go 客户端 SDK（可独立 import relayx/s4/sdk）。
//
// 契约要点（S4 LLD §4 / §8 / §9）：
//   - 连接生命周期由本地状态机维护，服务端只在断开前补 CLOSE 原因码；
//   - hooks 全部异步分发，有界队列，低优先级可丢，终态 hook 不丢；
//   - 客户端事件是 Best-Effort 通知，权威结果通过查询 API 兜底；
//   - MESSAGE_SETTLED 只表达「平台侧已无更多投递动作」，不代表业务成功。
package sdk

import (
	"time"

	"relayx/s4/protocol"
)

// ConnState 是 SDK 连接状态机（LLD §4.1）。
type ConnState string

const (
	StateIdle           ConnState = "IDLE"
	StateConnecting     ConnState = "CONNECTING"
	StateConnected      ConnState = "CONNECTED"
	StateAuthenticating ConnState = "AUTHENTICATING"
	StateReady          ConnState = "READY"
	StateReconnecting   ConnState = "RECONNECTING"
	StateDisconnected   ConnState = "DISCONNECTED"
	StateClosed         ConnState = "CLOSED"
)

// DeliveryCounts 是聚合终态计数（与线上帧同一类型）。
type DeliveryCounts = protocol.DeliveryCounts

// Event 是 SDK 侧的客户端事件视图。
type Event struct {
	EventID      string
	Seq          uint64
	Type         protocol.ClientEventType
	TenantID     string
	MessageID    string
	StateVersion int
	Counts       *DeliveryCounts
	OccurredAt   time.Time
}

// ---------- 连接生命周期 hook 参数 ----------

type ConnectingInfo struct {
	Addr    string
	Attempt int
}

type ConnectedInfo struct {
	Remote string
}

type AuthenticatedInfo struct {
	TenantID     string
	ProducerID   string
	DeviceID     string
	SessionID    string
	Capabilities uint32
}

type ReconnectInfo struct {
	Attempt int
	Delay   time.Duration
	Reason  string
}

type DisconnectInfo struct {
	Reason string
	Err    error
}

// CloseInfo 是终态关闭信息，reason code 来自 CLOSE 帧（LLD §3.4）。
type CloseInfo struct {
	ReasonCode uint16
	Reason     string
	Text       string
	// Recoverable 表示 SDK 原本会重连但被显式关闭/放弃。
	Recoverable bool
}

type AuthFailedInfo struct {
	Err error
}

// ---------- 消息生命周期 hook 参数 ----------

type MessageAcceptedInfo struct {
	ClientMsgID   string
	MessageID     string
	PlatformState string
	Duplicate     bool
}

type MessageSettledInfo struct {
	MessageID    string
	EventID      string
	StateVersion int
	Counts       DeliveryCounts
	OccurredAt   time.Time
	// FromQuery 表示该终态由查询 API 补齐，而非事件推送。
	FromQuery bool
}

type MessageExpiredInfo = MessageSettledInfo

type DeliveredInfo struct {
	MessageID  string
	ProducerID string
	Sender     string
	Payload    []byte
	ExpiresAt  time.Time
}

type ReceiptAckInfo struct {
	MessageID string
}

type DeliveryEventInfo struct {
	EventID      string
	MessageID    string
	StateVersion int
	OccurredAt   time.Time
}

type BroadcastInfo struct {
	Topic            string
	Payload          []byte
	Seq              uint64
	ServerReceivedAt time.Time
}

// DropInfo 是低优先级 hook 被丢弃的通知。
type DropInfo struct {
	Hook   string
	Reason string
	Count  int
}

// ClientMessageView 是端侧查询 API 的返回（不含 payload）。
type ClientMessageView struct {
	MessageID     string         `json:"message_id"`
	PlatformState string         `json:"platform_state"`
	StateVersion  int            `json:"state_version"`
	Counts        DeliveryCounts `json:"counts"`
	SettledAt     *time.Time     `json:"settled_at,omitempty"`
	ExpiresAt     time.Time      `json:"expires_at"`
}
