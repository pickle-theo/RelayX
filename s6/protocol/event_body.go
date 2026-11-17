package protocol

import (
	"encoding/binary"
	"fmt"
)

// 本文件定义 S4 客户端事件域（0x20–0x22）的 Body 布局与事件类型位掩码
// （LLD §3.2 / §3.2.1，冻结，只追加）。
//
//	SUBSCRIBE_EVENTS      [4B event_types]
//	SUBSCRIBE_EVENTS_ACK  [1B result][4B granted_mask]
//	CLIENT_EVENT          [event_id][8B seq][type][tenant_id][message_id]
//	                      [4B state_version][1B counts_present][4×4B counts][8B occurred_at ms]
//
// 长度前缀字符串统一 2 字节；事件不含 payload、接收方设备 ID 与业务状态。
//
// 位空间隔离：EventMask 是"客户端想收哪些事件"，Cap*（capabilities.go）
// 是"服务端支持哪些帧"。两者是不同类型，编译器会阻止互相赋值。

// ClientEventType 是客户端事件类型枚举（冻结，只增不改）。
type ClientEventType string

const (
	EvMessageSettled ClientEventType = "MESSAGE_SETTLED"
	EvMessageExpired ClientEventType = "MESSAGE_EXPIRED"
	// 可选高体积事件（默认关闭）：逐 endpoint 推送会产生 2N+1 放大（LLD §6）。
	EvDeliveryReceiptAcked ClientEventType = "DELIVERY_RECEIPT_ACKED"
	EvDeliveryFailed       ClientEventType = "DELIVERY_FAILED"
	EvDeliveryExpired      ClientEventType = "DELIVERY_EXPIRED"
	EvMessagePartial       ClientEventType = "MESSAGE_PARTIAL"
)

// EventMask 是 SUBSCRIBE_EVENTS.event_types / SUBSCRIBE_EVENTS_ACK.granted_mask
// 的位掩码类型（uint32 语义，独立于能力位）。
type EventMask uint32

const (
	EventBitMessageSettled       EventMask = 1 << 0 // 0x00000001 默认推送，服务端不得拒绝
	EventBitMessageExpired       EventMask = 1 << 1 // 0x00000002 可选
	EventBitDeliveryReceiptAcked EventMask = 1 << 2 // 0x00000004 高体积
	EventBitDeliveryFailed       EventMask = 1 << 3 // 0x00000008 高体积
	EventBitDeliveryExpired      EventMask = 1 << 4 // 0x00000010 高体积
	EventBitMessagePartial       EventMask = 1 << 5 // 0x00000020 可选

	// EventMaskAll 是已分配位全集；bit6–bit31 预留未分配，必须被忽略而非报错。
	EventMaskAll = EventBitMessageSettled | EventBitMessageExpired |
		EventBitDeliveryReceiptAcked | EventBitDeliveryFailed |
		EventBitDeliveryExpired | EventBitMessagePartial

	// EventMaskDefault 是客户端未显式声明时的默认集合：只推聚合终态。
	EventMaskDefault = EventBitMessageSettled

	// EventMaskRequired 是不可拒绝的位：聚合终态是 SDK「每条消息最多一次
	// 终态 hook」保证的基础。
	EventMaskRequired = EventBitMessageSettled
)

// EventTypeMask 返回事件类型对应的位；未定义类型返回 (0, false)。
func EventTypeMask(t ClientEventType) (EventMask, bool) {
	switch t {
	case EvMessageSettled:
		return EventBitMessageSettled, true
	case EvMessageExpired:
		return EventBitMessageExpired, true
	case EvDeliveryReceiptAcked:
		return EventBitDeliveryReceiptAcked, true
	case EvDeliveryFailed:
		return EventBitDeliveryFailed, true
	case EvDeliveryExpired:
		return EventBitDeliveryExpired, true
	case EvMessagePartial:
		return EventBitMessagePartial, true
	default:
		return 0, false
	}
}

// EventTypesForMask 把位掩码展开为事件类型列表（顺序固定，便于日志与测试）。
// 未分配位被忽略。
func EventTypesForMask(m EventMask) []ClientEventType {
	all := []struct {
		bit EventMask
		typ ClientEventType
	}{
		{EventBitMessageSettled, EvMessageSettled},
		{EventBitMessageExpired, EvMessageExpired},
		{EventBitDeliveryReceiptAcked, EvDeliveryReceiptAcked},
		{EventBitDeliveryFailed, EvDeliveryFailed},
		{EventBitDeliveryExpired, EvDeliveryExpired},
		{EventBitMessagePartial, EvMessagePartial},
	}
	out := make([]ClientEventType, 0, len(all))
	for _, e := range all {
		if m&e.bit != 0 {
			out = append(out, e.typ)
		}
	}
	return out
}

// MaskForEventTypes 把事件类型集合折叠为位掩码；未知类型被忽略。
func MaskForEventTypes(types []ClientEventType) EventMask {
	var m EventMask
	for _, t := range types {
		if bit, ok := EventTypeMask(t); ok {
			m |= bit
		}
	}
	return m
}

// SanitizeRequestedMask 实现 LLD §3.2.1 的授予规则：
// 请求位 ∩ 已分配位，且 MESSAGE_SETTLED 必须被授予（不可拒绝）。
// 未分配位静默忽略，保证新客户端遇旧服务端不失败。
func SanitizeRequestedMask(requested EventMask) EventMask {
	return requested&EventMaskAll | EventMaskRequired
}

// SubscribeEventsResult 是 SUBSCRIBE_EVENTS_ACK.result 的结果码。
type SubscribeEventsResult uint8

const (
	SubscribeEventsOK          SubscribeEventsResult = 0
	SubscribeEventsNotLoggedIn SubscribeEventsResult = 1
)

// SubscribeEventsBody 是 SUBSCRIBE_EVENTS 帧体。
type SubscribeEventsBody struct {
	Mask EventMask
}

func EncodeSubscribeEvents(mask EventMask) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(mask))
	return b
}

func DecodeSubscribeEvents(body []byte) (SubscribeEventsBody, error) {
	if len(body) != 4 {
		return SubscribeEventsBody{}, fmt.Errorf("%w: subscribe_events len=%d", ErrBadBody, len(body))
	}
	return SubscribeEventsBody{Mask: EventMask(binary.BigEndian.Uint32(body))}, nil
}

// SubscribeEventsAckBody 是 SUBSCRIBE_EVENTS_ACK 帧体。
// 服务端必须回 granted_mask，客户端不得假定"请求即授予"。
type SubscribeEventsAckBody struct {
	Result      SubscribeEventsResult
	GrantedMask EventMask
}

func EncodeSubscribeEventsAck(result SubscribeEventsResult, granted EventMask) []byte {
	b := make([]byte, 5)
	b[0] = byte(result)
	binary.BigEndian.PutUint32(b[1:], uint32(granted))
	return b
}

func DecodeSubscribeEventsAck(body []byte) (SubscribeEventsAckBody, error) {
	if len(body) != 5 {
		return SubscribeEventsAckBody{}, fmt.Errorf("%w: subscribe_events_ack len=%d", ErrBadBody, len(body))
	}
	return SubscribeEventsAckBody{
		Result:      SubscribeEventsResult(body[0]),
		GrantedMask: EventMask(binary.BigEndian.Uint32(body[1:])),
	}, nil
}

// DeliveryCounts 是聚合终态的投递计数。唯一权威来源是
// delivery_facts.payload_summary.counts（S2 FactWriter 在结算事务内算好）。
type DeliveryCounts struct {
	Total        int `json:"total"`
	ReceiptAcked int `json:"receipt_acked"`
	Failed       int `json:"failed"`
	Expired      int `json:"expired"`
}

// ClientEventBody 是 CLIENT_EVENT 帧体（S→C，self-scoped）。
// Seq 为 per-session 单调连续序号，由服务端在路由到会话时分配；
// Counts 仅在聚合终态事件（MESSAGE_SETTLED / MESSAGE_EXPIRED）中携带。
type ClientEventBody struct {
	EventID      string
	Seq          uint64
	Type         ClientEventType
	TenantID     string
	MessageID    string
	StateVersion int32
	Counts       *DeliveryCounts
	OccurredAt   int64 // unix milliseconds
}

// EncodeClientEvent 编码事件帧体。布局固定，后续只在尾部追加。
func EncodeClientEvent(e ClientEventBody) []byte {
	b := appendLP([]byte(nil), e.EventID)
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], e.Seq)
	b = append(b, num[:]...)
	b = appendLP(b, string(e.Type), e.TenantID, e.MessageID)
	var ver [4]byte
	binary.BigEndian.PutUint32(ver[:], uint32(e.StateVersion))
	b = append(b, ver[:]...)
	if e.Counts == nil {
		b = append(b, 0)
	} else {
		b = append(b, 1)
		var c [16]byte
		binary.BigEndian.PutUint32(c[0:4], uint32(e.Counts.Total))
		binary.BigEndian.PutUint32(c[4:8], uint32(e.Counts.ReceiptAcked))
		binary.BigEndian.PutUint32(c[8:12], uint32(e.Counts.Failed))
		binary.BigEndian.PutUint32(c[12:16], uint32(e.Counts.Expired))
		b = append(b, c[:]...)
	}
	var at [8]byte
	binary.BigEndian.PutUint64(at[:], uint64(e.OccurredAt))
	return append(b, at[:]...)
}

// DecodeClientEvent 解码事件帧体。任何长度错位都返回 ErrBadBody。
func DecodeClientEvent(body []byte) (ClientEventBody, error) {
	fields, rest, err := splitLPRest(body, 1)
	if err != nil {
		return ClientEventBody{}, err
	}
	if len(rest) < 8 {
		return ClientEventBody{}, fmt.Errorf("%w: client_event seq", ErrBadBody)
	}
	out := ClientEventBody{
		EventID: string(fields[0]),
		Seq:     binary.BigEndian.Uint64(rest[:8]),
	}
	rest = rest[8:]
	fields, rest, err = splitLPRest(rest, 3)
	if err != nil {
		return ClientEventBody{}, err
	}
	out.Type = ClientEventType(fields[0])
	out.TenantID = string(fields[1])
	out.MessageID = string(fields[2])
	if len(rest) < 5 {
		return ClientEventBody{}, fmt.Errorf("%w: client_event version", ErrBadBody)
	}
	out.StateVersion = int32(binary.BigEndian.Uint32(rest[:4]))
	present := rest[4]
	rest = rest[5:]
	if present == 1 {
		if len(rest) < 16 {
			return ClientEventBody{}, fmt.Errorf("%w: client_event counts", ErrBadBody)
		}
		out.Counts = &DeliveryCounts{
			Total:        int(binary.BigEndian.Uint32(rest[0:4])),
			ReceiptAcked: int(binary.BigEndian.Uint32(rest[4:8])),
			Failed:       int(binary.BigEndian.Uint32(rest[8:12])),
			Expired:      int(binary.BigEndian.Uint32(rest[12:16])),
		}
		rest = rest[16:]
	} else if present != 0 {
		return ClientEventBody{}, fmt.Errorf("%w: client_event counts flag", ErrBadBody)
	}
	if len(rest) != 8 {
		return ClientEventBody{}, fmt.Errorf("%w: client_event occurred_at len=%d", ErrBadBody, len(rest))
	}
	out.OccurredAt = int64(binary.BigEndian.Uint64(rest[:8]))
	return out, nil
}
