package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
)

// S6 内部帧号（架构方案 §8.2：0x30–0x3F，S6 起启用）。
// 该号段只在节点间 mTLS 通道出现，绝不对外暴露；对外帧号/能力位冻结表不变。
const (
	TypeForward     byte = 0x30 // 源节点 → 目标节点：尽力转发
	TypeForwardNack byte = 0x31 // 目标节点 → 源节点：异步可选回执（不参与重试决策）
)

// FORWARD_NACK 原因码（LLD §4.2）。数值空间与 ERROR 码、CLOSE 原因码均独立。
const (
	NackNoSession       byte = 1 // 目标节点上无该端点会话
	NackVersionMismatch byte = 2 // version_hint 与当前绑定不符（路由已过期）
	NackEnqueueFull     byte = 3 // 目标节点 P1 通道预算耗尽
)

// NackReasonName 返回原因码的可读名，未知码返回 "UNKNOWN"。
func NackReasonName(reason byte) string {
	switch reason {
	case NackNoSession:
		return "NO_SESSION"
	case NackVersionMismatch:
		return "VERSION_MISMATCH"
	case NackEnqueueFull:
		return "ENQUEUE_FULL"
	default:
		return "UNKNOWN"
	}
}

// ParseNackReason 是 NackReasonName 的逆操作；未知名字返回 ok=false。
func ParseNackReason(name string) (byte, bool) {
	switch name {
	case "NO_SESSION":
		return NackNoSession, true
	case "VERSION_MISMATCH":
		return NackVersionMismatch, true
	case "ENQUEUE_FULL":
		return NackEnqueueFull, true
	default:
		return 0, false
	}
}

// IsInternalFrame 判断帧号是否属于节点间内部段（0x30–0x3F）。
func IsInternalFrame(t byte) bool { return t >= 0x30 && t <= 0x3F }

// ForwardBody 是 FORWARD(0x30) 帧体。
//
//	[2B tenantLen][tenant][2B userLen][user][2B devLen][dev]
//	[8B version_hint][4B innerLen][1B innerType][innerBody]
//
// inner_frame 是原始业务帧（DELIVER 等）的 Type + Body，不含外层序号/CRC：
// 目标节点按本节点连接重新分配 Sequence（S1 冻结语义）。
type ForwardBody struct {
	TenantID    string
	UserID      string
	DeviceID    string
	VersionHint uint64
	InnerType   byte
	InnerBody   []byte
}

// EncodeForward 编码 FORWARD 帧体。内部帧不能嵌套，inner body 受 S0 MaxBodySize 约束。
func EncodeForward(f ForwardBody) ([]byte, error) {
	if err := checkLPFields(f.TenantID, f.UserID, f.DeviceID); err != nil {
		return nil, err
	}
	if IsInternalFrame(f.InnerType) {
		return nil, fmt.Errorf("%w: nested internal frame 0x%02x", ErrBadBody, f.InnerType)
	}
	innerLen := 1 + len(f.InnerBody)
	if innerLen > MaxBodySize || innerLen > math.MaxUint32 {
		return nil, fmt.Errorf("%w: inner frame too large", ErrBadBody)
	}
	b := appendLP([]byte(nil), f.TenantID, f.UserID, f.DeviceID)
	var hint [8]byte
	binary.BigEndian.PutUint64(hint[:], f.VersionHint)
	b = append(b, hint[:]...)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(innerLen))
	b = append(b, l[:]...)
	b = append(b, f.InnerType)
	b = append(b, f.InnerBody...)
	return b, nil
}

// DecodeForward 解码 FORWARD 帧体。InnerBody 被拷贝，避免别名读缓冲被下一次 Read 覆盖
// （同类缺陷曾见于 S5 的广播帧，故 S6 从一开始就复制）。
func DecodeForward(body []byte) (ForwardBody, error) {
	fields, rest, err := splitLPRest(body, 3)
	if err != nil {
		return ForwardBody{}, err
	}
	if len(rest) < 12 {
		return ForwardBody{}, fmt.Errorf("%w: forward prefix len=%d", ErrBadBody, len(rest))
	}
	f := ForwardBody{
		TenantID:    string(fields[0]),
		UserID:      string(fields[1]),
		DeviceID:    string(fields[2]),
		VersionHint: binary.BigEndian.Uint64(rest[:8]),
	}
	innerLen := int(binary.BigEndian.Uint32(rest[8:12]))
	rest = rest[12:]
	if innerLen < 1 || len(rest) < innerLen {
		return ForwardBody{}, fmt.Errorf("%w: inner frame overflow", ErrBadBody)
	}
	f.InnerType = rest[0]
	if IsInternalFrame(f.InnerType) {
		return ForwardBody{}, fmt.Errorf("%w: nested internal frame", ErrBadBody)
	}
	f.InnerBody = append([]byte(nil), rest[1:innerLen]...)
	if len(rest) != innerLen {
		return ForwardBody{}, fmt.Errorf("%w: forward trailing %d bytes", ErrBadBody, len(rest)-innerLen)
	}
	return f, nil
}

// NackBody 是 FORWARD_NACK(0x31) 帧体。
//
//	[2B tenantLen][tenant][2B userLen][user][2B devLen][dev][1B reason][8B version_hint]
type NackBody struct {
	TenantID    string
	UserID      string
	DeviceID    string
	Reason      byte
	VersionHint uint64
}

// EncodeNack 编码 FORWARD_NACK 帧体。
func EncodeNack(n NackBody) ([]byte, error) {
	if err := checkLPFields(n.TenantID, n.UserID, n.DeviceID); err != nil {
		return nil, err
	}
	switch n.Reason {
	case NackNoSession, NackVersionMismatch, NackEnqueueFull:
	default:
		return nil, fmt.Errorf("%w: nack reason %d", ErrBadBody, n.Reason)
	}
	b := appendLP([]byte(nil), n.TenantID, n.UserID, n.DeviceID)
	b = append(b, n.Reason)
	var hint [8]byte
	binary.BigEndian.PutUint64(hint[:], n.VersionHint)
	return append(b, hint[:]...), nil
}

// DecodeNack 解码 FORWARD_NACK 帧体。
func DecodeNack(body []byte) (NackBody, error) {
	fields, rest, err := splitLPRest(body, 3)
	if err != nil {
		return NackBody{}, err
	}
	if len(rest) != 9 {
		return NackBody{}, fmt.Errorf("%w: nack trailing/prefix len=%d", ErrBadBody, len(rest))
	}
	n := NackBody{
		TenantID:    string(fields[0]),
		UserID:      string(fields[1]),
		DeviceID:    string(fields[2]),
		Reason:      rest[0],
		VersionHint: binary.BigEndian.Uint64(rest[1:9]),
	}
	switch n.Reason {
	case NackNoSession, NackVersionMismatch, NackEnqueueFull:
	default:
		return NackBody{}, fmt.Errorf("%w: nack reason %d", ErrBadBody, n.Reason)
	}
	return n, nil
}

// checkLPFields 检查 appendLP 使用的 uint16 长度前缀不会截断字段。
func checkLPFields(fields ...string) error {
	for i, f := range fields {
		if len(f) > math.MaxUint16 {
			return fmt.Errorf("%w: field %d too long", ErrBadBody, i)
		}
	}
	return nil
}
