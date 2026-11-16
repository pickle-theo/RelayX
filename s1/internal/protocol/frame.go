// Package protocol 实现全项目复用的二进制帧格式（S0 冻结产物）。
//
// 帧布局（Header 固定 14 字节，Body 变长）：
//
//	+--------+--------+--------+--------+--------+--------+--------+--------+
//	| Magic(2B)       | Ver(1B)| Type(1B)| Length(4B, BE)                  |
//	+--------+--------+--------+--------+--------+--------+--------+--------+
//	| Sequence(4B, BE)        | CRC16(2B)| Body (Length B)                 |
//	+--------+--------+--------+--------+--------+--------+--------+--------+
//
// 本文件定义的常量与结构为冻结契约，S1–S8 只扩展 Type 与 Body 语义，不改头部。
package protocol

import "math"

const (
	// Magic0/Magic1 为 "RX"，用于帧对齐与非法连接快速识别。
	Magic0 byte = 0x52
	Magic1 byte = 0x58

	// Version 为当前协议版本。
	Version byte = 0x01

	// HeaderSize 为固定头部长度：2+1+1+4+4+2 = 14 字节。
	HeaderSize = 14

	// CRC16Size 为头部 CRC 字段长度。
	CRC16Size = 2

	// MaxBodySize 为单帧 Body 上限（64 KiB）。Length 校验先于内存分配。
	MaxBodySize = 65536

	// MaxFrameSize 为单帧总长度上限。
	MaxFrameSize = HeaderSize + MaxBodySize
)

// 基础帧号（0x01–0x0E，冻结）。号段按用途域划分，禁止跨域复用。
const (
	TypeLogin       byte = 0x01 // C→S LOGIN，S1 实现鉴权
	TypeLoginAck    byte = 0x02 // S→C LOGIN_ACK
	TypeSend        byte = 0x03 // C→S SEND
	TypeSendAck     byte = 0x04 // S→C SEND_ACK
	TypeDeliver     byte = 0x05 // S→C DELIVER
	TypeReceiptAck  byte = 0x06 // C→S RECEIPT_ACK
	TypeBusinessAck byte = 0x07 // C→S BUSINESS_ACK
	TypePing        byte = 0x08 // 双向 心跳
	TypePong        byte = 0x09 // 双向 心跳响应
	TypeClose       byte = 0x0A // S→C CLOSE
	TypeError       byte = 0x0B // S→C ERROR
	TypeEcho        byte = 0x0C // 双向 测试帧，仅 --enable-echo 注册
	// 0x0D–0x0E 预留冻结；0x10–0x1F 订阅广播(S3)；0x20–0x2F 事件(S4)；0x30–0x3F 内部帧(S6)。
)

// Frame 是协议的基本传输单元。
// Sequence 由发送端连接内从 1 单调分配，不回绕；仅用于传输层可观测性，不作去重键。
type Frame struct {
	Type     byte
	Sequence uint32
	Body     []byte
}

// SequenceTracker 维护连接内单调递增序号。零值可用，须在单个连接内使用。
// 序号到达 math.MaxUint32 后不再可用（必须断开连接），不回绕。
type SequenceTracker struct {
	next uint32 // 下一个要分配的序号，初始 0 → 首次 Next() 返回 1
}

// Next 返回下一个序号。首次调用返回 1；达到上限后返回 ErrSequenceExhausted。
func (s *SequenceTracker) Next() (uint32, error) {
	if s.next >= math.MaxUint32 {
		return 0, ErrSequenceExhausted
	}
	s.next++
	return s.next, nil
}

// Seen 返回已分配的最大序号（0 表示尚未分配）。
func (s *SequenceTracker) Seen() uint32 { return s.next }
