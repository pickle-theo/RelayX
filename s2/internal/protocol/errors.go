package protocol

import "errors"

var (
	// ErrIncomplete 表示当前缓冲区不足一帧，读循环应继续累积。
	ErrIncomplete = errors.New("protocol: incomplete frame, need more bytes")

	// ErrBadMagic 表示 Magic 不匹配（非法协议/未对齐）。
	ErrBadMagic = errors.New("protocol: bad magic")

	// ErrBadVersion 表示协议版本不受支持。
	ErrBadVersion = errors.New("protocol: unsupported version")

	// ErrBadCRC 表示头部 CRC 校验失败（Length 等字段可能已损坏）。
	ErrBadCRC = errors.New("protocol: header crc mismatch")

	// ErrFrameTooLarge 表示 Length 超过 maxBodySize；必须先于内存分配返回。
	ErrFrameTooLarge = errors.New("protocol: frame body exceeds max size")

	// ErrSequenceExhausted 表示连接内序号达到 uint32 上限，必须断开连接。
	ErrSequenceExhausted = errors.New("protocol: sequence exhausted")

	// ErrUnknownFrameType 表示收到未注册的帧类型。
	ErrUnknownFrameType = errors.New("protocol: unknown frame type")
)
