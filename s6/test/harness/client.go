package harness

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"

	"relayx/s6/protocol"
)

// Client 是协议级测试客户端（仅用于 cmd/scenario 与 test/ 的路由验收）。
type Client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	seq  uint32
}

// Dial 连接客户端 TCP 地址。
func Dial(addr string) (*Client, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{conn: c, r: bufio.NewReader(c), w: bufio.NewWriter(c)}, nil
}

func (c *Client) Close() { _ = c.conn.Close() }

// Send 发送一帧。
func (c *Client) Send(t byte, body []byte) error {
	c.seq++
	raw, err := protocol.Encode(protocol.Frame{Type: t, Sequence: c.seq, Body: body}, nil)
	if err != nil {
		return err
	}
	if _, err := c.w.Write(raw); err != nil {
		return err
	}
	return c.w.Flush()
}

// Read 读取一帧。
func (c *Client) Read() (protocol.Frame, error) {
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

// ReadWithin 在超时内读取一帧。
func (c *Client) ReadWithin(d time.Duration) (protocol.Frame, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(d))
	f, err := c.Read()
	_ = c.conn.SetReadDeadline(time.Time{})
	return f, err
}

// Login 登录并等待 LOGIN_ACK。
func (c *Client) Login(token, device string) error {
	body, err := protocol.EncodeLogin(token, device)
	if err != nil {
		return err
	}
	if err := c.Send(protocol.TypeLogin, body); err != nil {
		return err
	}
	f, err := c.ReadWithin(3 * time.Second)
	if err != nil {
		return err
	}
	if f.Type != protocol.TypeLoginAck {
		return fmt.Errorf("expected LOGIN_ACK (0x02), got 0x%02x", f.Type)
	}
	return nil
}
