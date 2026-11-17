package protocol

import "encoding/binary"

// Encode 把帧编码为二进制。dst 容量足够时复用，否则重新分配。
//
// 设计要点（为 S3 广播预埋）：编码是纯函数——同一 Frame 编码结果只依赖输入，
// 返回的是一段只读字节，可被多个订阅者共享。S0 不启用共享，但保持该接口形态。
func Encode(f Frame, dst []byte) ([]byte, error) {
	if len(f.Body) > MaxBodySize {
		return nil, ErrFrameTooLarge
	}
	total := HeaderSize + len(f.Body)
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}

	dst[0] = Magic0
	dst[1] = Magic1
	dst[2] = Version
	dst[3] = f.Type
	binary.BigEndian.PutUint32(dst[4:8], uint32(len(f.Body)))
	binary.BigEndian.PutUint32(dst[8:12], f.Sequence)
	binary.BigEndian.PutUint16(dst[12:14], CRC16(dst[:12]))
	copy(dst[HeaderSize:], f.Body)
	return dst, nil
}

// Decode 从 src 解析一帧，返回帧与消耗的字节数。
//
// 校验顺序（安全关键）：
//  1. 不足 HeaderSize → ErrIncomplete（不读 Length，避免误判）
//  2. Magic / Version → 非法协议快速失败
//  3. Length > MaxBodySize → ErrFrameTooLarge（发生在任何 Body 分配之前）
//  4. Header CRC → 防 Length/Sequence 字段被传输损坏
//  5. 不足 HeaderSize+Length → ErrIncomplete（读循环继续累积）
//
// 返回的 Frame.Body 引用 src 的一段只读切片（零拷贝），调用方若在下次 Decode
// 之前不会覆写 src，可直接使用；需要跨阶段持有时应自行拷贝。
func Decode(src []byte) (Frame, int, error) {
	if len(src) < HeaderSize {
		return Frame{}, 0, ErrIncomplete
	}
	if src[0] != Magic0 || src[1] != Magic1 {
		return Frame{}, 0, ErrBadMagic
	}
	if src[2] != Version {
		return Frame{}, 0, ErrBadVersion
	}

	length := binary.BigEndian.Uint32(src[4:8])
	if length > MaxBodySize {
		return Frame{}, 0, ErrFrameTooLarge
	}

	want := binary.BigEndian.Uint16(src[12:14])
	if got := CRC16(src[:12]); got != want {
		return Frame{}, 0, ErrBadCRC
	}

	total := HeaderSize + int(length)
	if len(src) < total {
		return Frame{}, 0, ErrIncomplete
	}

	return Frame{
		Type:     src[3],
		Sequence: binary.BigEndian.Uint32(src[8:12]),
		Body:     src[HeaderSize:total],
	}, total, nil
}
