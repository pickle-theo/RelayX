package connection

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/s3/internal/config"
	"relayx/s3/internal/telemetry"
)

// Manager 管理所有 TCP 连接的 Accept 与生命周期。
type Manager struct {
	cfg     config.Config
	handler Handler
	metrics *telemetry.Metrics
	log     *log.Logger

	mu       sync.Mutex
	listener net.Listener
	conns    map[*Connection]struct{}
	wg       sync.WaitGroup
	done     chan struct{}
	once     sync.Once
	idSeq    atomic.Uint64
	version  atomic.Uint64
}

// NewManager 创建连接管理器。
func NewManager(cfg config.Config, handler Handler, metrics *telemetry.Metrics, logger *log.Logger) *Manager {
	if logger == nil {
		logger = log.Default()
	}
	return &Manager{
		cfg: cfg, handler: handler, metrics: metrics, log: logger,
		conns: make(map[*Connection]struct{}), done: make(chan struct{}),
	}
}

func (m *Manager) nextID() string {
	return "c" + u64str(m.idSeq.Add(1))
}

// nextVersion 分配 Connection Version（S1 进程内单调；S6 换成 per-endpoint Redis INCR）。
func (m *Manager) nextVersion() uint64 { return m.version.Add(1) }

func u64str(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// ListenAndServe 监听并阻塞，直到 Shutdown。
func (m *Manager) ListenAndServe() error {
	ln, err := net.Listen("tcp", m.cfg.Addr)
	if err != nil {
		return err
	}
	return m.Serve(ln)
}

// Serve 在注入的 listener 上接受连接（便于测试）。
func (m *Manager) Serve(ln net.Listener) error {
	m.mu.Lock()
	m.listener = ln
	select {
	case <-m.done:
		m.mu.Unlock()
		_ = ln.Close()
		return nil
	default:
	}
	m.mu.Unlock()

	m.log.Printf("S1 gateway listening on %s", ln.Addr())
	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-m.done:
				return nil
			default:
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Temporary() {
				m.log.Printf("accept temporary error: %v", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			return err
		}

		c := New(Options{
			ID: m.nextID(), NC: nc, Cfg: m.cfg,
			Handler: m.handler, Metrics: m.metrics, Log: m.log,
			Version: m.nextVersion(),
		})
		// 注册与 Shutdown 快照在同一把锁下；若已关闭则拒绝并关闭连接。
		m.mu.Lock()
		select {
		case <-m.done:
			m.mu.Unlock()
			c.Close()
			continue
		default:
		}
		m.conns[c] = struct{}{}
		m.wg.Add(1)
		m.mu.Unlock()

		m.handler.OnOpen(c)
		go func() {
			defer m.wg.Done()
			c.Serve()
			m.mu.Lock()
			delete(m.conns, c)
			m.mu.Unlock()
		}()
	}
}

// Count 返回当前连接数（供 /metrics）。
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.conns)
}

// Shutdown 优雅停机：停止 Accept → 关闭所有连接 → 等待 goroutine 回收。
func (m *Manager) Shutdown(ctx context.Context) error {
	var firstErr error
	m.once.Do(func() {
		m.mu.Lock()
		close(m.done)
		ln := m.listener
		conns := make([]*Connection, 0, len(m.conns))
		for c := range m.conns {
			conns = append(conns, c)
		}
		m.mu.Unlock()
		if ln != nil {
			if err := ln.Close(); err != nil {
				firstErr = err
			}
		}
		for _, c := range conns {
			c.Close()
		}
	})
	finished := make(chan struct{})
	go func() { m.wg.Wait(); close(finished) }()
	select {
	case <-finished:
		return firstErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
