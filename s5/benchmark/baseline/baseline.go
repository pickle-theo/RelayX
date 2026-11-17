// Package baseline 冻结 S1–S4 的「优化前」实现，只用于 S5 的前后对比（A/B）。
//
// 为什么需要它：S5 的纪律是「先测量、后优化」，而优化后的实现放在
// internal/* 里。要把「收益」量化成 ns/op 与 allocs/op 的差值，必须有一份
// 语义等价、可编译运行的旧实现作为对照组。本包即该对照组：
//
//   - EncodeSharedFrame / EncodePerSubscriber：S3 广播编码（两段式，含中间 Body 分配）
//   - Writer：S1–S4 写循环（每帧 SetWriteDeadline + 每帧 Write）
//   - Reader：需要空闲检测时的直观读循环（每帧 time.Now + 每帧 SetReadDeadline）
//   - Heartbeat：S1 心跳（每连接一个 ticker + 一个 goroutine）
//
// 本包不得被生产代码引用；它只出现在 benchmark 与 test 的 import 中。
// 语义与 S1–S4 一致，任何优化都不得以「改 baseline」的方式伪造对比。
package baseline

import (
	"sync"
	"time"

	"relayx/s5/internal/sendpath"
	"relayx/s5/protocol"
)

// EncodeSharedFrame 是 S3/S4 的共享帧编码：先 EncodeBroadcast 生成 Body，
// 再 Encode 把 Body 拷进池化缓冲（一次 Body 分配 + 一次整帧拷贝）。
func EncodeSharedFrame(p *sync.Pool, topic string, payload []byte, serverReceivedAt time.Time, seq uint64) (*sendpath.ImmutableFrame, error) {
	body, err := protocol.EncodeBroadcast(topic, serverReceivedAt.UnixMilli(), seq, payload)
	if err != nil {
		return nil, err
	}
	buf := p.Get().([]byte)
	encoded, err := protocol.Encode(protocol.Frame{
		Type:     protocol.TypeBroadcast,
		Sequence: uint32(seq),
		Body:     body,
	}, buf[:0])
	if err != nil {
		p.Put(buf)
		return nil, err
	}
	return sendpath.NewImmutableFrame(p, encoded), nil
}

// EncodePerSubscriber 是「共享编码」出现之前的做法：每个订阅者各编码一次。
// 扇出基准用它证明共享编码的收益随订阅者数线性放大。
func EncodePerSubscriber(topic string, payload []byte, serverReceivedAt time.Time, seq uint64) ([]byte, error) {
	body, err := protocol.EncodeBroadcast(topic, serverReceivedAt.UnixMilli(), seq, payload)
	if err != nil {
		return nil, err
	}
	return protocol.Encode(protocol.Frame{Type: protocol.TypeBroadcast, Sequence: uint32(seq), Body: body}, nil)
}
