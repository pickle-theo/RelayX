package protocol

import (
	"encoding/binary"
	"fmt"
)

// CLOSE (0x0A) body 扩展与原因码表（S4 LLD §3.4，冻结，只追加不改语义）。
//
//	CLOSE body: [2B reason_code][2B reason_text_len][reason_text]
//
// 旧客户端忽略 body，行为与 S1 一致；空 body 等价于 NORMAL。
//
// 编号空间声明：本表是 CLOSE 帧的原因码空间（1000–1008），与 S2 ERROR 帧
// 的错误码空间（1001–1005）相互独立。数值重叠纯属巧合，SDK 必须按帧类型
// 分派解析，不得共用同一张码表。
type CloseReason uint16

const (
	CloseNormal           CloseReason = 1000 // 正常关闭
	CloseReplaced         CloseReason = 1001 // 新登录替换旧会话
	CloseKicked           CloseReason = 1002 // 被踢
	CloseTokenExpired     CloseReason = 1003 // 令牌过期
	CloseAuthFailed       CloseReason = 1004 // 鉴权失败
	CloseServerDraining   CloseReason = 1005 // 服务端排空/优雅停机
	CloseHeartbeatTimeout CloseReason = 1006 // 心跳超时
	ClosePolicyViolation  CloseReason = 1007 // 违反策略（超频/超限）
	CloseInternalError    CloseReason = 1008 // 内部错误
)

var closeReasonNames = map[CloseReason]string{
	CloseNormal:           "NORMAL",
	CloseReplaced:         "REPLACED",
	CloseKicked:           "KICKED",
	CloseTokenExpired:     "TOKEN_EXPIRED",
	CloseAuthFailed:       "AUTH_FAILED",
	CloseServerDraining:   "SERVER_DRAINING",
	CloseHeartbeatTimeout: "HEARTBEAT_TIMEOUT",
	ClosePolicyViolation:  "POLICY_VIOLATION",
	CloseInternalError:    "INTERNAL_ERROR",
}

// String 返回原因码的稳定名称；未知码返回 "UNKNOWN"。
func (r CloseReason) String() string {
	if s, ok := closeReasonNames[r]; ok {
		return s
	}
	return "UNKNOWN"
}

// ParseCloseReason 判定码是否在冻结表内。未知名不报错，
// 由调用方按 NORMAL 处理并记录（LLD §12 E7）。
func ParseCloseReason(code uint16) (CloseReason, bool) {
	r := CloseReason(code)
	_, ok := closeReasonNames[r]
	return r, ok
}

// Retryable 表示该原因码在 SDK 侧是否应自动重连（LLD §4.1.1）。
// 只有 SERVER_DRAINING 与 HEARTBEAT_TIMEOUT 必须重连；
// REPLACED / KICKED / TOKEN_EXPIRED / AUTH_FAILED / POLICY_VIOLATION
// 以及服务端主动 NORMAL 关闭都进入终态 CLOSED。
func (r CloseReason) Retryable() bool {
	switch r {
	case CloseServerDraining, CloseHeartbeatTimeout:
		return true
	default:
		return false
	}
}

// CloseBody 是 CLOSE 帧 body。
type CloseBody struct {
	ReasonCode uint16
	ReasonText string
}

// EncodeCloseBody 编码原因码 body。
func EncodeCloseBody(code CloseReason, text string) []byte {
	b := make([]byte, 4, 4+len(text))
	binary.BigEndian.PutUint16(b[0:2], uint16(code))
	binary.BigEndian.PutUint16(b[2:4], uint16(len(text)))
	return append(b, text...)
}

// DecodeCloseBody 解码原因码 body。空 body 返回 (0, "")，用于兼容
// S1–S3 服务端的无 body CLOSE；非空但格式非法则返回 ErrBadBody。
func DecodeCloseBody(body []byte) (CloseBody, error) {
	if len(body) == 0 {
		return CloseBody{}, nil
	}
	if len(body) < 4 {
		return CloseBody{}, fmt.Errorf("%w: close body len=%d", ErrBadBody, len(body))
	}
	code := binary.BigEndian.Uint16(body[0:2])
	textLen := int(binary.BigEndian.Uint16(body[2:4]))
	if len(body) != 4+textLen {
		return CloseBody{}, fmt.Errorf("%w: close text len=%d", ErrBadBody, textLen)
	}
	return CloseBody{ReasonCode: code, ReasonText: string(body[4:])}, nil
}
