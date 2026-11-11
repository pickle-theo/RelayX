package transport

import (
	"sync"

	"relayx/phase5.5/internal/protocol"
)

// Buffer pools for the connection hot path. Two independent sets of pools are
// kept:
//
//   - encode buffers, handed out by getEncodeBuf and recycled by putEncodeBuf
//     after the writer goroutine has flushed a frame;
//   - frames, handed out by getFrame and recycled by putFrame once the reader
//     goroutine (and the handler it dispatched to) is done with them.
//
// Ownership rules: an encode buffer or frame returned by a get function is
// owned by the caller until it is passed to the matching put function. From
// that moment on it may be reused by any goroutine, so neither the slice nor
// f.Body may be read, retained or shared afterwards. In particular a
// HandlerFunc must copy f.Body if it needs the payload past its own return.
const (
	// maxPooledBuffer is the largest buffer the pools retain. Anything bigger
	// is allocated per call and left to the garbage collector afterwards.
	maxPooledBuffer = 64 << 10

	encodeBufClass512 = 512
	encodeBufClass4K  = 4 << 10
	encodeBufClass64K = 64 << 10
)

var (
	encodePool512 sync.Pool // []byte with cap encodeBufClass512
	encodePool4K  sync.Pool // []byte with cap encodeBufClass4K
	encodePool64K sync.Pool // []byte with cap encodeBufClass64K

	framePool sync.Pool // *protocol.Frame
)

// encodeBufClass returns the smallest pooled class that fits n and the pool
// that stores it. n must be <= maxPooledBuffer.
func encodeBufClass(n int) (int, *sync.Pool) {
	switch {
	case n <= encodeBufClass512:
		return encodeBufClass512, &encodePool512
	case n <= encodeBufClass4K:
		return encodeBufClass4K, &encodePool4K
	default:
		return encodeBufClass64K, &encodePool64K
	}
}

// getEncodeBuf returns a zero-length slice with the capacity of the smallest
// pool class >= n. Requests above maxPooledBuffer get an exact-size allocation
// that putEncodeBuf will not recycle.
func getEncodeBuf(n int) []byte {
	if n > maxPooledBuffer {
		return make([]byte, 0, n)
	}
	size, pool := encodeBufClass(n)
	if v := pool.Get(); v != nil {
		return v.([]byte)[:0]
	}
	return make([]byte, 0, size)
}

// putEncodeBuf returns b to its size-class pool. Only buffers whose capacity
// equals a class size are recycled; all others are dropped. The caller must
// not use b afterwards.
func putEncodeBuf(b []byte) {
	if b == nil {
		return
	}
	switch cap(b) {
	case encodeBufClass512:
		encodePool512.Put(b[:0])
	case encodeBufClass4K:
		encodePool4K.Put(b[:0])
	case encodeBufClass64K:
		encodePool64K.Put(b[:0])
	}
}

// getFrame returns a reusable frame owned by the caller until putFrame.
func getFrame() *protocol.Frame {
	if v := framePool.Get(); v != nil {
		return v.(*protocol.Frame)
	}
	return new(protocol.Frame)
}

// putFrame resets f and returns it to the frame pool. Bodies larger than
// maxPooledBuffer have their backing array dropped so one oversized frame does
// not pin memory in the pool.
func putFrame(f *protocol.Frame) {
	if f == nil {
		return
	}
	f.Type = 0
	f.Flags = 0
	f.MsgID = 0
	if cap(f.Body) > maxPooledBuffer {
		f.Body = nil
	} else {
		f.Body = f.Body[:0]
	}
	framePool.Put(f)
}
