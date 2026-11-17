package backend

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// respOKServer 是一个最小的 RESP 服务端：读取批量字符串数组并回 +OK。
func respOKServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					header, err := r.ReadString('\n')
					if err != nil {
						return
					}
					n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "*")))
					if err != nil || n < 1 {
						return
					}
					for i := 0; i < n; i++ {
						sizeLine, err := r.ReadString('\n')
						if err != nil {
							return
						}
						size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(sizeLine, "$")))
						if err != nil || size < 0 {
							return
						}
						buf := make([]byte, size+2) // 负载 + CRLF
						if _, err := io.ReadFull(r, buf); err != nil {
							return
						}
					}
					if _, err := c.Write([]byte("+OK\r\n")); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// TestRespPoolRefreshesDeadlinePerCommand 守护连接池超时语义：
// 拨号时设置一次性绝对 deadline 会让复用到 RouteTimeout 之后的连接全部失败。
func TestRespPoolRefreshesDeadlinePerCommand(t *testing.T) {
	ln := respOKServer(t)
	timeout := 100 * time.Millisecond
	p := newRespPool(ln.Addr().String(), "", 0, timeout)
	defer p.close()

	if _, err := p.do("PING"); err != nil {
		t.Fatalf("first command: %v", err)
	}
	// 超过拨号时若设置的一次性 deadline；连接仍应可用。
	time.Sleep(2 * timeout)
	if _, err := p.do("PING"); err != nil {
		t.Fatalf("pooled connection must refresh deadline per command: %v", err)
	}
}

// TestRespPoolReusesConnectionOnRedisError 守护：Redis 错误回复（如 NOSCRIPT）
// 是完整 RESP 消息，连接仍可用，必须归还池中供 EVAL 回退复用，而不是重拨。
func TestRespPoolReusesConnectionOnRedisError(t *testing.T) {
	var conns atomic.Int32
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns.Add(1)
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				first := true
				for {
					header, err := r.ReadString('\n')
					if err != nil {
						return
					}
					n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "*")))
					if err != nil || n < 1 {
						return
					}
					for i := 0; i < n; i++ {
						sizeLine, err := r.ReadString('\n')
						if err != nil {
							return
						}
						size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(sizeLine, "$")))
						if err != nil || size < 0 {
							return
						}
						buf := make([]byte, size+2)
						if _, err := io.ReadFull(r, buf); err != nil {
							return
						}
					}
					if first {
						first = false
						_, _ = c.Write([]byte("-NOSCRIPT No matching script\r\n"))
					} else {
						_, _ = c.Write([]byte("+OK\r\n"))
					}
				}
			}(c)
		}
	}()

	p := newRespPool(ln.Addr().String(), "", 0, time.Second)
	defer p.close()

	if _, err := p.do("EVALSHA", "deadbeef", "0"); err == nil {
		t.Fatal("expected NOSCRIPT error")
	}
	if _, err := p.do("EVAL", "return 1", "0"); err != nil {
		t.Fatalf("second command on reused connection: %v", err)
	}
	if got := conns.Load(); got != 1 {
		t.Fatalf("Redis error reply must keep connection usable: got %d connections, want 1", got)
	}
}
