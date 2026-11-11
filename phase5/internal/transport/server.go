// Package transport implements the RelayX Phase 0 TCP transport: a listener
// with per-connection reader and writer goroutines (the heartbeat is merged
// into the writer).
package transport

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/phase5/internal/config"
	"relayx/phase5/internal/protocol"
)

// HandlerFunc handles a decoded frame. Returning an error closes the
// connection. The frame is only valid for the duration of the call: f.Body
// points into a pooled buffer that may be reused as soon as the handler
// returns, so copy the payload if it must be retained.
type HandlerFunc func(c *Conn, f *protocol.Frame) error

// Server is a Phase 0 TCP gateway. It accepts connections, tracks them in a
// registry and shuts them down gracefully.
type Server struct {
	cfg     config.ServerConfig
	handler HandlerFunc
	logger  *slog.Logger

	listener  net.Listener
	listenErr error
	addr      string

	mu     sync.RWMutex
	conns  map[uint64]*Conn
	nextID atomic.Uint64
	wg     sync.WaitGroup

	onClose atomic.Pointer[func(*Conn)]

	shutdownCh   chan struct{}
	shuttingDown atomic.Bool
	shutdownOnce sync.Once
	shutdownErr  error
}

// New creates a server and immediately listens on addr. Any zero-valued
// config fields are replaced with Phase 0 defaults. A listen error is
// reported by Serve.
func New(addr string, cfg config.ServerConfig, handler HandlerFunc, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	cfg = withDefaults(cfg)
	if addr == "" {
		addr = cfg.Addr
	}

	s := &Server{
		cfg:        cfg,
		handler:    handler,
		logger:     logger,
		addr:       addr,
		conns:      make(map[uint64]*Conn),
		shutdownCh: make(chan struct{}),
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		s.listenErr = err
		return s
	}
	s.listener = ln
	s.addr = ln.Addr().String()
	return s
}

// Addr returns the actual listen address, which is useful when binding to
// 127.0.0.1:0.
func (s *Server) Addr() string { return s.addr }

// SetOnClose registers fn to be called after a connection has been removed
// from the server registry. It must be called before Serve.
//
// The callback runs synchronously inside Conn.Close and must therefore not
// block; the gateway's session cleanup only deletes map entries and updates
// counters.
func (s *Server) SetOnClose(fn func(*Conn)) {
	if fn == nil {
		s.onClose.Store(nil)
		return
	}
	s.onClose.Store(&fn)
}

// ActiveConnections returns the number of currently registered connections.
func (s *Server) ActiveConnections() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.conns)
}

// Serve accepts connections until the listener is closed. It returns nil on
// graceful shutdown.
func (s *Server) Serve() error {
	if s.listenErr != nil {
		return s.listenErr
	}
	if s.listener == nil {
		return errors.New("transport: server has no listener")
	}
	var backoff time.Duration
	for {
		nc, err := s.listener.Accept()
		if err != nil {
			if s.shuttingDown.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Temporary() {
				backoff = nextBackoff(backoff)
				s.logger.Warn("temporary accept error, retrying", "error", err, "backoff", backoff)
				timer := time.NewTimer(backoff)
				select {
				case <-timer.C:
				case <-s.shutdownCh:
					timer.Stop()
					return nil
				}
				continue
			}
			return err
		}
		backoff = 0
		s.handleConn(nc)
	}
}

// Shutdown stops accepting connections, sends a CLOSE frame to every active
// connection and waits for their goroutines to exit. If ctx expires first,
// remaining connections are force closed and ctx.Err() is returned.
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.shutdownErr = s.doShutdown(ctx)
	})
	return s.shutdownErr
}

func (s *Server) doShutdown(ctx context.Context) error {
	s.shuttingDown.Store(true)
	close(s.shutdownCh)

	var err error
	if s.listener != nil {
		if closeErr := s.listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = closeErr
		}
	}

	// Taking s.mu here is the barrier that guarantees every handleConn which
	// already passed the shuttingDown check has completed its wg.Add under the
	// same lock before wg.Wait starts, and that any later handleConn is
	// rejected without touching the WaitGroup.
	s.mu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	closeFrame := &protocol.Frame{Type: protocol.Close}
	for _, c := range conns {
		if sendErr := c.Send(closeFrame); sendErr != nil {
			c.closeWithReason("shutdown: " + sendErr.Error())
		}
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("graceful shutdown complete")
		return err
	case <-ctx.Done():
		s.logger.Warn("shutdown timeout, force closing connections")
		s.closeAll("shutdown timeout")
		<-done
		return ctx.Err()
	}
}

func (s *Server) handleConn(nc net.Conn) {
	id := s.nextID.Add(1)
	onClose := func(c *Conn) {
		s.unregister(c)
		if p := s.onClose.Load(); p != nil {
			(*p)(c)
		}
	}
	c := newConn(id, nc, s.cfg, s.handler, s.logger, onClose)

	// Registration and WaitGroup accounting happen under the same lock, so
	// doShutdown (which takes s.mu before waiting) can never race wg.Add
	// against wg.Wait or observe a registered connection without its goroutine
	// accounted for.
	if !s.register(c) {
		_ = nc.Close()
		return
	}
	c.logger.Info("connection accepted")

	go func() {
		defer s.wg.Done()
		c.run()
	}()
}

func (s *Server) register(c *Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown.Load() {
		return false
	}
	s.wg.Add(1)
	s.conns[c.id] = c
	return true
}

func (s *Server) unregister(c *Conn) {
	s.mu.Lock()
	if s.conns[c.id] == c {
		delete(s.conns, c.id)
	}
	s.mu.Unlock()
}

func (s *Server) snapshot() []*Conn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	conns := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	return conns
}

func (s *Server) closeAll(reason string) {
	for _, c := range s.snapshot() {
		c.closeWithReason(reason)
	}
}

func nextBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return 10 * time.Millisecond
	}
	if current *= 2; current > 100*time.Millisecond {
		return 100 * time.Millisecond
	}
	return current
}

func withDefaults(cfg config.ServerConfig) config.ServerConfig {
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = 1 << 20
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 90 * time.Second
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 120 * time.Second
	}
	if cfg.SendQueueSize <= 0 {
		cfg.SendQueueSize = 256
	}
	if cfg.SendQueueBytes <= 0 {
		cfg.SendQueueBytes = defaultSendQueueBytes
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}
	if cfg.WriteBatchFrames <= 0 {
		cfg.WriteBatchFrames = defaultWriteBatchFrames
	}
	if cfg.WriteBatchBytes <= 0 {
		cfg.WriteBatchBytes = defaultWriteBatchBytes
	}
	if cfg.ReadBufferSize <= 0 {
		cfg.ReadBufferSize = defaultReadBufferSize
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	return cfg
}
