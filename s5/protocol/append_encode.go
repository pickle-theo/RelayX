package protocol

import "encoding/binary"

// 本文件是 S5 的实现层增量：把「先分配 Body、再拷贝进帧缓冲」的两段式编码
// 改为「一次写入 dst」的追加式编码。帧布局与冻结语义完全不变（S0 头部 14B、
// S2–S4 Body 布局均不改），产出的字节与 Encode(EncodeXxx(...)) 逐字节相同，
// 由 append_encode_test.go 的等价性用例守护。
//
// 为什么这是实现层优化而不是语义变更：
//   - 输入输出契约不变（同一 Frame 语义 → 同一字节串）；
//   - 只减少中间 Body 切片的分配与一次 payload 拷贝；
//   - 调用方仍需自行保证 dst 容量，容量不足时 append 会重新分配。

// FrameSize 返回一帧的完整长度（头部 + Body）。
func FrameSize(bodyLen int) int { return HeaderSize + bodyLen }

// AppendFrame 把一帧直接追加到 dst 尾部并返回扩展后的切片。
// 这是 Encode 的追加式变体：语义相同（含 MaxBodySize 校验与 CRC 覆盖），
// 但不需要调用方预先准备恰好长度的缓冲，也不会丢弃 dst 的前缀内容——
// 批量写路径因此可以把 N 帧连续写进同一块缓冲，一次 Write 落盘（S5 §4.2）。
func AppendFrame(dst []byte, typ byte, seq uint32, body []byte) ([]byte, error) {
	if len(body) > MaxBodySize {
		return dst, ErrFrameTooLarge
	}
	return appendFrameHeader(dst, typ, seq, len(body), body), nil
}

// appendFrameHeader 写入 14 字节头部后追加 body。
// 头部 CRC 覆盖前 12 字节，因此必须先写完前 12 字节再计算（与 Encode 一致）。
func appendFrameHeader(dst []byte, typ byte, seq uint32, bodyLen int, body []byte) []byte {
	start := len(dst)
	dst = append(dst,
		Magic0, Magic1, Version, typ,
		0, 0, 0, 0, // Length 占位
		0, 0, 0, 0, // Sequence 占位
		0, 0, // CRC 占位
	)
	binary.BigEndian.PutUint32(dst[start+4:start+8], uint32(bodyLen))
	binary.BigEndian.PutUint32(dst[start+8:start+12], seq)
	binary.BigEndian.PutUint16(dst[start+12:start+14], CRC16(dst[start:start+12]))
	return append(dst, body...)
}

// BroadcastFrameSize 返回 BROADCAST 帧完整长度（不分配）。
// 调用方据此一次性准备缓冲，避免 append 扩容拷贝。
func BroadcastFrameSize(topic string, payloadLen int) int {
	return HeaderSize + 2 + len(topic) + 16 + payloadLen
}

// AppendBroadcast 直接构造 BROADCAST 帧（头部 + Body 一体化写入 dst）。
//
// 与 Encode(TypeBroadcast, EncodeBroadcast(...)) 的产物逐字节相同，但省掉
// EncodeBroadcast 的 Body 分配与随后的一次整帧拷贝：payload 只被拷一次。
// 扇出前的一次编码是 S3 §7.1 的冻结语义，此处只改变编码实现。
func AppendBroadcast(dst []byte, topic string, serverReceivedAt int64, seq uint64, payload []byte) ([]byte, error) {
	bodyLen := 2 + len(topic) + 16 + len(payload)
	if bodyLen > MaxBodySize {
		return dst, ErrFrameTooLarge
	}
	start := len(dst)
	dst = append(dst,
		Magic0, Magic1, Version, TypeBroadcast,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0,
	)
	binary.BigEndian.PutUint32(dst[start+4:start+8], uint32(bodyLen))
	// 共享帧无法按连接分配序号，body.Seq 才是单 topic 有序判据（S3 §7.1）。
	binary.BigEndian.PutUint32(dst[start+8:start+12], uint32(seq))
	binary.BigEndian.PutUint16(dst[start+12:start+14], CRC16(dst[start:start+12]))

	dst = binary.BigEndian.AppendUint16(dst, uint16(len(topic)))
	dst = append(dst, topic...)
	dst = binary.BigEndian.AppendUint64(dst, uint64(serverReceivedAt))
	dst = binary.BigEndian.AppendUint64(dst, seq)
	return append(dst, payload...), nil
}

// DeliverFrameSize 返回 DELIVER 帧完整长度（不分配）。
func DeliverFrameSize(d DeliverBody) int {
	return HeaderSize + 2 + len(d.MessageID) + 2 + len(d.ProducerID) + 2 + len(d.Sender) + 8 + 4 + len(d.Payload)
}

// AppendDeliver 直接构造 DELIVER 帧，省掉 EncodeDeliver 的 Body 分配与整帧拷贝。
func AppendDeliver(dst []byte, seq uint32, d DeliverBody) ([]byte, error) {
	bodyLen := DeliverFrameSize(d) - HeaderSize
	if bodyLen > MaxBodySize {
		return dst, ErrFrameTooLarge
	}
	start := len(dst)
	dst = append(dst,
		Magic0, Magic1, Version, TypeDeliver,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0,
	)
	binary.BigEndian.PutUint32(dst[start+4:start+8], uint32(bodyLen))
	binary.BigEndian.PutUint32(dst[start+8:start+12], seq)
	binary.BigEndian.PutUint16(dst[start+12:start+14], CRC16(dst[start:start+12]))

	dst = appendLenPrefixed(dst, d.MessageID)
	dst = appendLenPrefixed(dst, d.ProducerID)
	dst = appendLenPrefixed(dst, d.Sender)
	dst = binary.BigEndian.AppendUint64(dst, uint64(d.ExpiresAt))
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(d.Payload)))
	return append(dst, d.Payload...), nil
}

// SendAckFrameSize 返回 SEND_ACK 帧完整长度（不分配）。
func SendAckFrameSize(a SendAckBody) int {
	return HeaderSize + 2 + len(a.ClientMsgID) + 2 + len(a.MessageID) + 1 + 8 + 1
}

// AppendSendAck 直接构造 SEND_ACK 帧。
func AppendSendAck(dst []byte, seq uint32, a SendAckBody) ([]byte, error) {
	bodyLen := SendAckFrameSize(a) - HeaderSize
	if bodyLen > MaxBodySize {
		return dst, ErrFrameTooLarge
	}
	start := len(dst)
	dst = append(dst,
		Magic0, Magic1, Version, TypeSendAck,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0,
	)
	binary.BigEndian.PutUint32(dst[start+4:start+8], uint32(bodyLen))
	binary.BigEndian.PutUint32(dst[start+8:start+12], seq)
	binary.BigEndian.PutUint16(dst[start+12:start+14], CRC16(dst[start:start+12]))

	dst = appendLenPrefixed(dst, a.ClientMsgID)
	dst = appendLenPrefixed(dst, a.MessageID)
	if a.PlatformState == "SETTLED" {
		dst = append(dst, stateSettled)
	} else {
		dst = append(dst, stateAccepted)
	}
	dst = binary.BigEndian.AppendUint64(dst, uint64(a.AcceptedAt))
	if a.Duplicate {
		return append(dst, 1), nil
	}
	return append(dst, 0), nil
}

// appendLenPrefixed 追加 2 字节长度前缀 + 字符串（与 body.go 的 appendLP 同构）。
func appendLenPrefixed(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}
