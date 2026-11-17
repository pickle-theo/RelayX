package broadcast

import (
	"sync"
	"time"

	"relayx/s3/internal/protocol"
	"relayx/s3/internal/sendpath"
)

// Encoder 是 SharedEncoder 的唯一实现：每次 publish 只编码一次，
// 产出的只读字节由 ImmutableFrame 持有引用计数，N 个订阅者共享（LLD §7.1）。
type Encoder struct {
	pool       *sync.Pool
	maxPayload int
}

// NewSharedEncoder 创建共享编码器。
// pool 预分配 maxPayload + 头部余量，覆盖默认最大 payload；更大的帧由 Encode 自行分配，
// 归还时同样进入 pool，不破坏引用计数闭环。
func NewSharedEncoder(maxPayload int) *Encoder {
	if maxPayload <= 0 {
		maxPayload = DefaultMaxPayload
	}
	bufCap := maxPayload + protocol.HeaderSize + 64
	return &Encoder{
		maxPayload: maxPayload,
		pool: &sync.Pool{New: func() any {
			return make([]byte, 0, bufCap)
		}},
	}
}

// Encode 生成一帧完整的 BROADCAST 字节（帧头 + Body），返回引用计数为 1 的共享帧。
// 调用方（扇出方）负责在不再需要时 Release。
func (e *Encoder) Encode(topic string, payload []byte, serverReceivedAt time.Time, seq uint64) (*sendpath.ImmutableFrame, error) {
	if len(payload) > e.maxPayload {
		return nil, ErrPayloadTooBig
	}
	if topic == "" || len(topic) > DefaultMaxTopic {
		return nil, ErrBadTopic
	}
	body, err := protocol.EncodeBroadcast(topic, serverReceivedAt.UnixMilli(), seq, payload)
	if err != nil {
		return nil, err
	}
	buf := e.pool.Get().([]byte)
	encoded, err := protocol.Encode(protocol.Frame{
		Type:     protocol.TypeBroadcast,
		Sequence: uint32(seq), // 共享帧无法按连接分配序号；body.Seq 才是单 topic 有序判据
		Body:     body,
	}, buf[:0])
	if err != nil {
		e.pool.Put(buf)
		return nil, err
	}
	return sendpath.NewImmutableFrame(e.pool, encoded), nil
}

// Pool 返回编码器持有的 pool（测试断言引用计数归零后 buffer 归还用）。
func (e *Encoder) Pool() *sync.Pool { return e.pool }

// 编译期接口断言。
var _ SharedEncoder = (*Encoder)(nil)
