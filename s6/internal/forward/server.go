package forward

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"relayx/s6/internal/model"
	"relayx/s6/protocol"
)

// LocalSink 是目标节点上的本地投递入口（由 S1 的会话路由实现）。
// Enqueue 返回 nil 表示已写入本节点连接（不管客户端是否真的读到）。
type LocalSink interface {
	Enqueue(ctx context.Context, tenant, user, device string, innerType byte, innerBody []byte) error
}

// Router 提供对端 version_hint 校验所需的当前绑定查询（由 route.Router 实现）。
type Router interface {
	Lookup(ctx context.Context, ep model.EndpointKey) (model.NodeBinding, bool, error)
}

// Server 在节点间 mTLS 地址上接受 FORWARD 帧，按本地路由投递，
// 失败时异步回 FORWARD_NACK（LLD §4.1 步骤 5–7）。
type Server struct {
	listener net.Listener
	tlsCfg   *tls.Config
	sink     LocalSink
	router   Router
	log      *slog.Logger
	metrics  Metrics

	// handshakeTimeout 限制 mTLS 握手时长，避免半开连接长期占用 goroutine（slowloris）。
	handshakeTimeout time.Duration

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	stopped bool
	wg      sync.WaitGroup
}

// ServerOption 调整内部帧服务端的可选参数。
type ServerOption func(*Server)

// WithHandshakeTimeout 设置 mTLS 握手超时（0 表示不设）。
func WithHandshakeTimeout(d time.Duration) ServerOption {
	return func(s *Server) { s.handshakeTimeout = d }
}

// NewServer 创建内部帧服务端（尚未监听）。
func NewServer(tlsCfg *tls.Config, sink LocalSink, router Router, log *slog.Logger, metrics Metrics, opts ...ServerOption) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{tlsCfg: tlsCfg, sink: sink, router: router, log: log, metrics: metrics, conns: make(map[net.Conn]struct{})}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ListenAndServe 在 addr 上监听并阻塞，直到 Close。
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve 在注入的 listener 上服务（测试用）。
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		_ = ln.Close()
		return net.ErrClosed
	}
	s.listener = ln
	s.mu.Unlock()

	for {
		c, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			stopped := s.stopped
			s.mu.Unlock()
			if stopped {
				return net.ErrClosed
			}
			return err
		}
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			_ = c.Close()
			return net.ErrClosed
		}
		// conns 注册与 wg.Add 必须在同一把锁内完成：Close 会在置 stopped 后
		// 复制连接集合再 Wait，Add 与 Wait 绝不能并发（WaitGroup 误用）。
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(c)
	}
}

func (s *Server) handle(raw net.Conn) {
	defer s.wg.Done()
	defer raw.Close()
	defer func() {
		s.mu.Lock()
		delete(s.conns, raw)
		s.mu.Unlock()
	}()

	conn := tls.Server(raw, s.tlsCfg)
	if s.handshakeTimeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(s.handshakeTimeout))
	}
	if err := conn.Handshake(); err != nil {
		// LLD §8.2：证书校验失败 → 拒绝连接，无明文回退，审计日志。
		s.log.Warn("internal mTLS handshake rejected", "remote", raw.RemoteAddr(), "err", err)
		return
	}
	_ = conn.SetDeadline(time.Time{}) // 握手完成：长连接不设读写超时
	r := bufio.NewReader(conn)
	w := &syncWriter{buf: bufio.NewWriter(conn)}
	for {
		frame, _, err := readFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.log.Debug("internal conn read ended", "remote", raw.RemoteAddr(), "err", err)
			}
			return
		}
		switch frame.Type {
		case protocol.TypeForward:
			s.handleForward(w, frame)
		case protocol.TypeForwardNack:
			// S6 源节点不处理入站 NACK；NACK 由源节点的 Forwarder.readLoop 处理。
		default:
			// 未知内部帧类型静默忽略，为 S7 扩展留出空间。
		}
	}
}

// syncWriter 保证同一连接上的帧不会并发交错写出。
type syncWriter struct {
	mu  sync.Mutex
	buf *bufio.Writer
}

func (w *syncWriter) write(raw []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.buf.Write(raw); err != nil {
		return err
	}
	return w.buf.Flush()
}

func (s *Server) handleForward(w *syncWriter, frame protocol.Frame) {
	fwd, err := protocol.DecodeForward(frame.Body)
	if err != nil {
		s.log.Warn("bad FORWARD frame", "err", err)
		return
	}
	ep := model.EndpointKey{TenantID: fwd.TenantID, UserID: fwd.UserID, DeviceID: fwd.DeviceID}

	// version_hint 校验：不匹配说明源节点读到了过期路由（LLD §4.4 C）。
	if binding, ok, err := s.router.Lookup(context.Background(), ep); err != nil || !ok {
		s.sendNack(w, ep, protocol.NackNoSession, fwd.VersionHint)
		return
	} else if fwd.VersionHint != 0 && binding.Version != fwd.VersionHint {
		s.sendNack(w, ep, protocol.NackVersionMismatch, fwd.VersionHint)
		return
	}

	if err := s.sink.Enqueue(context.Background(), ep.TenantID, ep.UserID, ep.DeviceID, fwd.InnerType, fwd.InnerBody); err != nil {
		reason := protocol.NackNoSession
		if errors.Is(err, ErrFull) {
			reason = protocol.NackEnqueueFull
		}
		s.sendNack(w, ep, reason, fwd.VersionHint)
		return
	}
	// 成功入队即结束，不发 ACK（尽力语义）。
}

func (s *Server) sendNack(w *syncWriter, ep model.EndpointKey, reason byte, version uint64) {
	body, err := protocol.EncodeNack(protocol.NackBody{
		TenantID: ep.TenantID, UserID: ep.UserID, DeviceID: ep.DeviceID,
		Reason: reason, VersionHint: version,
	})
	if err != nil {
		return
	}
	raw, err := protocol.Encode(protocol.Frame{Type: protocol.TypeForwardNack, Body: body}, nil)
	if err != nil {
		return
	}
	_ = w.write(raw)
}

// Close 关闭监听与全部连接，等待处理协程退出。
func (s *Server) Close() error {
	s.mu.Lock()
	s.stopped = true
	ln := s.listener
	// 在锁内复制连接集合：handle() 的 defer 会并发从 s.conns 删除。
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	if ln != nil {
		_ = ln.Close()
	}
	s.wg.Wait()
	return nil
}
