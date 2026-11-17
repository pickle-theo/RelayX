package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"

	"relayx/s6/protocol"
)

// testClient 是一个协议级客户端（L0），仅用于 cmd/scenario 的路由验收。
// S6 不新增 SDK 语义；客户端帧仍是 S0–S4 冻结帧。
type testClient struct {
	conn net.Conn
	r    *bufio.Reader
	out  *bufio.Writer
	seq  uint32
}

func dial(addr string) (*testClient, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &testClient{conn: c, r: bufio.NewReader(c), out: bufio.NewWriter(c)}, nil
}

func (c *testClient) close() { _ = c.conn.Close() }

func (c *testClient) send(t byte, body []byte) error {
	c.seq++
	raw, err := protocol.Encode(protocol.Frame{Type: t, Sequence: c.seq, Body: body}, nil)
	if err != nil {
		return err
	}
	if _, err := c.out.Write(raw); err != nil {
		return err
	}
	return c.out.Flush()
}

func (c *testClient) read() (protocol.Frame, error) {
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(c.r, header); err != nil {
		return protocol.Frame{}, err
	}
	length := uint32(header[4])<<24 | uint32(header[5])<<16 | uint32(header[6])<<8 | uint32(header[7])
	if length > protocol.MaxBodySize {
		return protocol.Frame{}, protocol.ErrFrameTooLarge
	}
	buf := make([]byte, protocol.HeaderSize+int(length))
	copy(buf, header)
	if _, err := io.ReadFull(c.r, buf[protocol.HeaderSize:]); err != nil {
		return protocol.Frame{}, err
	}
	frame, _, err := protocol.Decode(buf)
	return frame, err
}

func (c *testClient) readWithin(d time.Duration) (protocol.Frame, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(d))
	f, err := c.read()
	_ = c.conn.SetReadDeadline(time.Time{})
	return f, err
}

func (c *testClient) login(token, device string) error {
	body, err := protocol.EncodeLogin(token, device)
	if err != nil {
		return err
	}
	if err := c.send(protocol.TypeLogin, body); err != nil {
		return err
	}
	f, err := c.readWithin(3 * time.Second)
	if err != nil {
		return err
	}
	if f.Type != protocol.TypeLoginAck {
		return fmt.Errorf("expected LOGIN_ACK, got 0x%02x", f.Type)
	}
	return nil
}
