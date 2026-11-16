package server

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
)

// Server 是 S0 TCP Server。Accept 循环为每个连接启动独立 conn。
type Server struct {
	cfg      Config
	log      *log.Logger
	listener net.Listener

	mu    sync.Mutex
	conns map[*conn]struct{}
	wg    sync.WaitGroup
	done  chan struct{}
	once  sync.Once
}

// New 创建 Server 实例（尚未监听）。
func New(cfg Config, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	return &Server{
		cfg:   cfg,
		log:   logger,
		conns: make(map[*conn]struct{}),
		done:  make(chan struct{}),
	}
}

// ListenAndServe 开始监听并阻塞，直到 Shutdown 被调用或发生致命错误。
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve 在给定 listener 上接受连接（便于测试注入）。
func (s *Server) Serve(ln net.Listener) error {
	// 安装 listener 与检查 done 必须在同一把锁下，与 Shutdown 配对：
	// 若 Shutdown 先于本函数执行，它会看到 listener 为 nil 而无法关闭；
	// 此时这里必须自行关闭，否则 Accept 会永久阻塞造成 goroutine 泄漏。
	s.mu.Lock()
	s.listener = ln
	select {
	case <-s.done:
		s.mu.Unlock()
		_ = ln.Close()
		return nil
	default:
	}
	s.mu.Unlock()

	s.log.Printf("S0 server listening on %s (echo=%v)", ln.Addr(), s.cfg.EnableEcho)
	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				return nil
			default:
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Temporary() {
				s.log.Printf("accept temporary error: %v", err)
				continue
			}
			return err
		}

		if s.cfg.MaxConnections > 0 && s.connCount() >= s.cfg.MaxConnections {
			s.log.Printf("max connections reached, rejecting %s", nc.RemoteAddr())
			_ = nc.Close()
			continue
		}

		c := newConn(nc, s.cfg, s.log)
		// 注册连接必须与 Shutdown 的快照在同一把锁下完成：
		// 若 Shutdown 已取锁置 done 并快照（不含本连接），则本连接不会被关闭，
		// 且 wg.Add 会晚于 wg.Wait，造成优雅停机遗漏。此时直接拒绝并关闭。
		s.mu.Lock()
		select {
		case <-s.done:
			s.mu.Unlock()
			_ = nc.Close()
			return nil
		default:
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		go func() {
			defer s.wg.Done()
			c.serve()
			s.mu.Lock()
			delete(s.conns, c)
			s.mu.Unlock()
		}()
	}
}

func (s *Server) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Shutdown 优雅停机：停止 Accept → 关闭所有连接（触发排空与 goroutine 回收）→ 等待退出。
func (s *Server) Shutdown(ctx context.Context) error {
	var firstErr error
	s.once.Do(func() {
		// 先置 done，再在锁内读取 listener：与 Serve 的安装+检查共享同一把锁，
		// 保证「Serve 已发布 listener」与「Shutdown 已关闭」两个事件不会互相错过。
		s.mu.Lock()
		close(s.done)
		ln := s.listener
		conns := make([]*conn, 0, len(s.conns))
		for c := range s.conns {
			conns = append(conns, c)
		}
		s.mu.Unlock()

		if ln != nil {
			if err := ln.Close(); err != nil {
				firstErr = err
			}
		}
		for _, c := range conns {
			c.close()
		}
	})

	finished := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return firstErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
