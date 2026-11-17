// Package sendpath 的 P2 广播扩展：共享只读帧与引用计数（S3 LLD §2.3 / §7.1）。
//
// 全链路只传 *ImmutableFrame：atomic.Int32 含 noCopy，按值传递会复制引用计数器，
// 导致 buffer 被提前归还、跨帧串数据、buffer 泄漏，且 go vet 会报 passes lock by value。
package sendpath

import (
	"io"
	"sync"
	"sync/atomic"
)

// ImmutableFrame 是一次编码、N 个订阅者共享的只读帧。
//
// 生命周期：NewImmutableFrame 创建 refs=1（扇出方持有）；
// TrySendBroadcast 入队成功 +1（队列持有）；Writer 写完 -1；扇出方 Release -1。
// refs 归零 → buf 归还 pool 并置 nil，此后 WriteTo 写 0 字节、Len 返回 0，
// 残留引用绝不会读到被复用的字节。
type ImmutableFrame struct {
	buf  []byte
	refs atomic.Int32
	pool *sync.Pool
}

// NewImmutableFrame 是唯一构造入口：pool 与 buf 必须同源，refs 初始为 1。
// pool 为 nil 或 buf 为 nil 均视为编程错误，立即 panic。
func NewImmutableFrame(pool *sync.Pool, buf []byte) *ImmutableFrame {
	if pool == nil {
		panic("broadcast: ImmutableFrame requires non-nil pool")
	}
	if buf == nil {
		panic("broadcast: ImmutableFrame requires non-nil buf")
	}
	f := &ImmutableFrame{buf: buf, pool: pool}
	f.refs.Store(1)
	return f
}

// WriteTo 是唯一写出入口：读字节与持引用绑在一次调用里，不暴露裸切片。
// 调用方必须在写完后调用 Release（Writer 保证如此）。
func (f *ImmutableFrame) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(f.buf)
	return int64(n), err
}

// Len 返回帧字节长度，供预算与指标使用；不暴露内容。
func (f *ImmutableFrame) Len() int { return len(f.buf) }

// Retain 由入队方在成功入队时调用，持有 1 个引用。
func (f *ImmutableFrame) Retain() {
	if f.refs.Load() <= 0 {
		panic("broadcast: ImmutableFrame.Retain on released frame")
	}
	f.refs.Add(1)
}

// Release 释放 1 个引用；最后一位释放者把 buffer 归还 pool。
// 负计数与 nil pool 立即 panic，把 double-release / 漏设 pool 变成快速失败。
func (f *ImmutableFrame) Release() {
	n := f.refs.Add(-1)
	if n < 0 {
		panic("broadcast: ImmutableFrame released more times than retained")
	}
	if n == 0 {
		if f.pool == nil {
			panic("broadcast: ImmutableFrame with nil pool")
		}
		f.pool.Put(f.buf)
		f.buf = nil
	}
}

// Refs 返回当前引用计数（测试与诊断用）。
func (f *ImmutableFrame) Refs() int32 { return f.refs.Load() }
