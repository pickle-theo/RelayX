package backend

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 最小 RESP2 客户端（stdlib only，遵循项目无第三方依赖约定）。
// 仅实现 S6 需要的命令 + EVALSHA/EVAL，不追求通用 Redis 客户端的完整度。

var (
	errRESPProtocol = errors.New("backend: resp protocol error")
	errNilReply     = errors.New("backend: nil reply")
	errRedisError   = errors.New("backend: redis error")
)

// redisError 携带 Redis 返回的错误字符串（如 NOSCRIPT / MOVED / CROSSSLOT）。
type redisError struct{ msg string }

func (e *redisError) Error() string { return e.msg }
func (e *redisError) Unwrap() error { return errRedisError }

// respConn 是一条到 Redis 的连接，非并发安全，由 respPool 串行化使用。
type respConn struct {
	nc      net.Conn
	r       *bufio.Reader
	w       *bufio.Writer
	addr    string
	timeout time.Duration
}

func dialRESP(addr, password string, db int, timeout time.Duration) (*respConn, error) {
	nc, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	// 连接池中的连接会存活任意久；deadline 必须按每次命令刷新，
	// 不能在拨号时设置一次绝对时间（否则复用超过 RouteTimeout 后必然失败）。
	c := &respConn{nc: nc, r: bufio.NewReader(nc), w: bufio.NewWriter(nc), addr: addr, timeout: timeout}
	if password != "" {
		if err := c.cmd("AUTH", password); err != nil {
			nc.Close()
			return nil, err
		}
	}
	if db > 0 {
		if err := c.cmd("SELECT", strconv.Itoa(db)); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return c, nil
}

// cmd 发送命令并读取一个 RESP 回复，忽略返回值（用于 AUTH/SELECT/SET 等）。
func (c *respConn) cmd(args ...string) error {
	_, err := c.do(args...)
	return err
}

// do 发送命令并返回 RESP 值。
func (c *respConn) do(args ...string) (any, error) {
	if len(args) == 0 {
		return nil, errRESPProtocol
	}
	// 连接会被池复用，每次命令都刷新相对 deadline，避免拨号时的一次性
	// 绝对时间在 RouteTimeout 之后让所有复用连接失效。
	if c.timeout > 0 {
		if err := c.nc.SetDeadline(time.Now().Add(c.timeout)); err != nil {
			return nil, err
		}
	}
	if _, err := fmt.Fprintf(c.w, "*%d\r\n", len(args)); err != nil {
		return nil, err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(c.w, "$%d\r\n%s\r\n", len(a), a); err != nil {
			return nil, err
		}
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	return c.readReply()
}

func (c *respConn) readReply() (any, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nil, errRESPProtocol
	}
	line = line[:len(line)-2]
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, &redisError{msg: line[1:]}
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, errRESPProtocol
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, errRESPProtocol
		}
		if n == -1 {
			return nil, errNilReply
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, errRESPProtocol
		}
		if n == -1 {
			return nil, errNilReply
		}
		out := make([]any, n)
		for i := range out {
			v, err := c.readReply()
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	default:
		return nil, errRESPProtocol
	}
}

func (c *respConn) Close() error { return c.nc.Close() }

// respPool 是简单的 FIFO 连接池。同一时刻只有一条连接被借用，
// S6 单节点路由量不高，串行化足够且避免 connect 风暴。
type respPool struct {
	addr     string
	password string
	db       int
	timeout  time.Duration
	mu       sync.Mutex
	idle     []*respConn
	closed   bool
}

func newRespPool(addr, password string, db int, timeout time.Duration) *respPool {
	return &respPool{addr: addr, password: password, db: db, timeout: timeout}
}

func (p *respPool) get() (*respConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, net.ErrClosed
	}
	if n := len(p.idle); n > 0 {
		c := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	return dialRESP(p.addr, p.password, p.db, p.timeout)
}

func (p *respPool) put(c *respConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = c.Close()
		return
	}
	p.idle = append(p.idle, c)
}

func (p *respPool) do(args ...string) (any, error) {
	c, err := p.get()
	if err != nil {
		return nil, err
	}
	v, err := c.do(args...)
	if err != nil {
		// Redis 返回的错误回复（如 NOSCRIPT / WRONGTYPE）是完整的 RESP 消息，
		// 连接状态依然可信：归还池中供 EVALSHA→EVAL 回退复用，避免每次都重拨
		// 并重新 AUTH/SELECT。只有协议/网络错误才需要丢弃连接。
		var re *redisError
		if errors.As(err, &re) {
			p.put(c)
		} else {
			_ = c.Close()
		}
	} else {
		p.put(c)
	}
	return v, err
}

func (p *respPool) close() {
	p.mu.Lock()
	p.closed = true
	for _, c := range p.idle {
		_ = c.Close()
	}
	p.idle = nil
	p.mu.Unlock()
}

// eval 先尝试 EVALSHA；NOSCRIPT 时回退 EVAL 并缓存脚本。
func (p *respPool) eval(script, sha string, keys, argv []string) (any, error) {
	args := make([]string, 0, 4+len(keys)+len(argv))
	args = append(args, "EVALSHA", sha, strconv.Itoa(len(keys)))
	args = append(args, keys...)
	args = append(args, argv...)
	v, err := p.do(args...)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		return v, err
	}
	args[0] = "EVAL"
	args[1] = script
	return p.do(args...)
}
