// Package forward 实现跨节点尽力转发（LLD §4）。
//
// 尽力边界（不可协商）：
//   - 成功"入队"（写入对端连接）即返回，不同步等待 ACK、不持久化、不重试；
//   - 对端若无法投递，异步回 FORWARD_NACK；源节点据它刷新缓存/记录错误，
//     但不改变投递状态、不增 attempts（§4.4 由 dispatch 包落实）。
package forward

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"relayx/s6/internal/model"
	"relayx/s6/protocol"
)

var (
	// ErrNodeUnavailable 与目标节点不可用（不在 ALIVE 集合 / 解析失败）。
	ErrNodeUnavailable = errors.New("forward: node unavailable")
	// ErrFull 表示目标节点内部连接发送队列满（对应 NACK ENQUEUE_FULL）。
	ErrFull = errors.New("forward: target enqueue full")
)

// Resolver 解析目标节点 mTLS 地址并判定可用性（由 node.Registry 实现）。
type Resolver interface {
	Lookup(ctx context.Context, nodeID string) (model.NodeInfo, bool, error)
}

// NackHandler 处理异步回执（由 dispatch/gateway 注入）。
type NackHandler interface {
	OnNack(ctx context.Context, from string, nack protocol.NackBody)
}

// Metrics 是 Forwarder 需要的指标钩子。
type Metrics interface {
	CrossNodeForwardsInc()
	CrossNodeForwardFailuresInc()
	ForwardNackInc(reason string)
}

// Forwarder 通过 mTLS 长连接把 FORWARD 帧送到目标节点。
type Forwarder struct {
	resolver Resolver
	log      *slog.Logger
	metrics  Metrics
	onNack   NackHandler
	seq      uint32
	seqMu    sync.Mutex

	// writeTimeout 是单次 FORWARD 写（含 Flush）的上限；0 表示不设超时。
	writeTimeout time.Duration

	mu    sync.Mutex
	conns map[string]*peerConn
}

// peerConn 是到某个目标节点的 mTLS 长连接。
type peerConn struct {
	conn net.Conn
	w    *bufio.Writer
	r    *bufio.Reader
	mu   sync.Mutex
}

// Option 调整 Forwarder 的可选参数。
type Option func(*Forwarder)

// WithWriteTimeout 设置单次转发写入的超时（cfg.ForwardTimeout）。
func WithWriteTimeout(d time.Duration) Option {
	return func(f *Forwarder) { f.writeTimeout = d }
}

// NewForwarder 创建转发器。tls 配置由调用方在拨号时通过 DialTLS 注入，
// 这里用工厂函数避免本包暴露 *tls.Config 细节给上层。
func NewForwarder(resolver Resolver, log *slog.Logger, metrics Metrics, onNack NackHandler, opts ...Option) *Forwarder {
	if log == nil {
		log = slog.Default()
	}
	f := &Forwarder{resolver: resolver, log: log, metrics: metrics, onNack: onNack, conns: make(map[string]*peerConn)}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// DialFunc 由上层注入：给定 mTLS 地址返回已握手的安全连接。
type DialFunc func(ctx context.Context, addr string) (net.Conn, error)

// Forward 尽力把原始业务帧转发到目标节点（LLD §4.1）。
// 成功入队返回 nil；任何失败都返回错误，由 dispatch 按 §4.4 B 处理。
func (f *Forwarder) Forward(ctx context.Context, nodeID string, ep model.EndpointKey, innerType byte, innerBody []byte, versionHint uint64, dial DialFunc) error {
	info, ok, err := f.resolver.Lookup(ctx, nodeID)
	if err != nil || !ok {
		return fmt.Errorf("%w: %s", ErrNodeUnavailable, nodeID)
	}
	if info.Status == model.NodeDead {
		return fmt.Errorf("%w: %s is dead", ErrNodeUnavailable, nodeID)
	}

	body, err := protocol.EncodeForward(protocol.ForwardBody{
		TenantID: ep.TenantID, UserID: ep.UserID, DeviceID: ep.DeviceID,
		VersionHint: versionHint, InnerType: innerType, InnerBody: innerBody,
	})
	if err != nil {
		return err
	}
	frame := protocol.Frame{Type: protocol.TypeForward, Body: body}
	seq, ok := f.nextSeq()
	if !ok {
		return protocol.ErrSequenceExhausted
	}
	frame.Sequence = seq

	raw, err := protocol.Encode(frame, nil)
	if err != nil {
		return err
	}

	pc, err := f.getPeer(ctx, info.Addr, dial)
	if err != nil {
		return err
	}
	if err := pc.write(raw, f.writeTimeout); err != nil {
		f.dropPeer(info.Addr, pc)
		return err
	}
	if f.metrics != nil {
		f.metrics.CrossNodeForwardsInc()
	}
	// 不同步等待 ACK —— NACK 由 readLoop 异步处理。
	return nil
}

// nextSeq 分配连接内序号（节点间连接独立编号，不复用业务连接序号）。
// 序号耗尽时返回 false：协议要求不 wrap，宁可报错也不重复序号。
func (f *Forwarder) nextSeq() (uint32, bool) {
	f.seqMu.Lock()
	defer f.seqMu.Unlock()
	if f.seq == ^uint32(0) {
		return 0, false
	}
	f.seq++
	return f.seq, true
}

func (f *Forwarder) getPeer(ctx context.Context, addr string, dial DialFunc) (*peerConn, error) {
	f.mu.Lock()
	pc := f.conns[addr]
	f.mu.Unlock()
	if pc != nil {
		return pc, nil
	}
	c, err := dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	pc = &peerConn{conn: c, w: bufio.NewWriter(c), r: bufio.NewReader(c)}
	f.mu.Lock()
	if existing := f.conns[addr]; existing != nil {
		f.mu.Unlock()
		_ = c.Close()
		return existing, nil
	}
	f.conns[addr] = pc
	f.mu.Unlock()
	// 启动异步读循环处理 NACK。
	go f.readLoop(addr, pc)
	return pc, nil
}

func (f *Forwarder) dropPeer(addr string, pc *peerConn) {
	f.mu.Lock()
	if f.conns[addr] == pc {
		delete(f.conns, addr)
	}
	f.mu.Unlock()
	_ = pc.conn.Close()
}

func (f *Forwarder) readLoop(addr string, pc *peerConn) {
	defer f.dropPeer(addr, pc)
	for {
		frame, _, err := readFrame(pc.r)
		if err != nil {
			return
		}
		if frame.Type != protocol.TypeForwardNack {
			continue // 内部帧段其它类型由 S7 扩展；S6 只处理 NACK
		}
		nack, err := protocol.DecodeNack(frame.Body)
		if err != nil {
			f.log.Warn("bad FORWARD_NACK", "from", addr, "err", err)
			continue
		}
		if f.metrics != nil {
			f.metrics.ForwardNackInc(protocol.NackReasonName(nack.Reason))
		}
		if f.onNack != nil {
			f.onNack.OnNack(context.Background(), addr, nack)
		}
	}
}

func readFrame(r *bufio.Reader) (protocol.Frame, int, error) {
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return protocol.Frame{}, 0, err
	}
	// 先按 uint32 判定上限再转 int（32 位平台负 int 会绕过守卫并令 make panic）。
	length := uint32(header[4])<<24 | uint32(header[5])<<16 | uint32(header[6])<<8 | uint32(header[7])
	if length > protocol.MaxBodySize {
		return protocol.Frame{}, 0, protocol.ErrFrameTooLarge
	}
	buf := make([]byte, protocol.HeaderSize+int(length))
	copy(buf, header)
	if _, err := io.ReadFull(r, buf[protocol.HeaderSize:]); err != nil {
		return protocol.Frame{}, 0, err
	}
	frame, consumed, err := protocol.Decode(buf)
	return frame, consumed, err
}

func (pc *peerConn) write(raw []byte, timeout time.Duration) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if timeout > 0 {
		if err := pc.conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
	}
	if _, err := pc.w.Write(raw); err != nil {
		return err
	}
	return pc.w.Flush()
}

// Close 关闭全部连接池连接。
func (f *Forwarder) Close() {
	f.mu.Lock()
	conns := f.conns
	f.conns = make(map[string]*peerConn)
	f.mu.Unlock()
	for _, pc := range conns {
		_ = pc.conn.Close()
	}
}
