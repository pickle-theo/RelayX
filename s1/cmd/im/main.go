// Command im 是 S1 的验证客户端（LLD §12.4），场景：
//
//	im -addr 127.0.0.1:9800 -scenario login-and-chat
//	im -addr 127.0.0.1:9800 -scenario multi-device
//	im -addr 127.0.0.1:9800 -scenario presence-simple
//
// 客户端用共享密钥自行签发令牌（S1 尚无独立发令牌服务）。
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"relayx/s1/internal/identity"
	"relayx/s1/internal/protocol"
)

var secret = []byte("relayx-s1-dev-secret")

type client struct {
	nc      net.Conn
	auth    *identity.Authenticator
	seq     protocol.SequenceTracker
	tenant  string
	user    string
	device  string
	session string
}

func newClient(addr, tenant, user, device string) *client {
	nc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	_ = nc.SetDeadline(time.Now().Add(10 * time.Second))
	return &client{nc: nc, auth: identity.New(secret), tenant: tenant, user: user, device: device}
}

func (c *client) token() string {
	p := identity.Producer{ID: "u:" + c.user, Kind: "user", TenantID: c.tenant}
	tok, err := c.auth.Issue(p, time.Hour)
	if err != nil {
		log.Fatalf("issue token: %v", err)
	}
	return tok
}

func (c *client) send(t byte, body []byte) {
	seq, _ := c.seq.Next()
	b, err := protocol.Encode(protocol.Frame{Type: t, Sequence: seq, Body: body}, nil)
	if err != nil {
		log.Fatalf("encode: %v", err)
	}
	if _, err := c.nc.Write(b); err != nil {
		log.Fatalf("write: %v", err)
	}
}

func (c *client) read() protocol.Frame {
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(c.nc, header); err != nil {
		log.Fatalf("read header: %v", err)
	}
	length := uint64(header[4])<<24 | uint64(header[5])<<16 | uint64(header[6])<<8 | uint64(header[7])
	if length > protocol.MaxBodySize {
		log.Fatalf("frame too large: %d", length)
	}
	buf := make([]byte, protocol.HeaderSize+int(length))
	copy(buf, header)
	if _, err := io.ReadFull(c.nc, buf[protocol.HeaderSize:]); err != nil {
		log.Fatalf("read body: %v", err)
	}
	f, _, err := protocol.Decode(buf)
	if err != nil {
		log.Fatalf("decode: %v", err)
	}
	return f
}

func (c *client) login() {
	body, _ := protocol.EncodeLogin(c.token(), c.device)
	c.send(protocol.TypeLogin, body)
	ack := c.read()
	if ack.Type != protocol.TypeLoginAck {
		log.Fatalf("expect LOGIN_ACK, got 0x%02X", ack.Type)
	}
	caps, err := protocol.DecodeLoginAck(ack.Body)
	if err != nil {
		log.Fatalf("decode caps: %v", err)
	}
	fmt.Printf("[%s/%s/%s] LOGIN_ACK capabilities=%d\n", c.tenant, c.user, c.device, caps.Capabilities)
}

func (c *client) expectDeliver(want string) {
	for {
		f := c.read()
		switch f.Type {
		case protocol.TypeDeliver:
			d, err := protocol.DecodeDeliver(f.Body)
			if err != nil {
				log.Fatalf("decode deliver: %v", err)
			}
			fmt.Printf("[%s] DELIVER from=%s payload=%q\n", c.device, d.From, d.Payload)
			if string(d.Payload) == want {
				return
			}
		case protocol.TypePing:
			seq, _ := c.seq.Next()
			b, _ := protocol.Encode(protocol.Frame{Type: protocol.TypePong, Sequence: seq}, nil)
			_, _ = c.nc.Write(b)
		default:
			log.Printf("[%s] unexpected frame 0x%02X", c.device, f.Type)
		}
	}
}

func makeSend(tenant, user, device, payload string) []byte {
	b, err := protocol.EncodeSend(protocol.SendBody{
		TenantID: tenant, UserID: user, DeviceID: device, Payload: []byte(payload),
	})
	if err != nil {
		log.Fatalf("encode send: %v", err)
	}
	return b
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9800", "Gateway 地址")
	scenario := flag.String("scenario", "login-and-chat", "login-and-chat|multi-device|presence-simple")
	flag.Parse()

	switch *scenario {
	case "login-and-chat":
		runLoginAndChat(*addr)
	case "multi-device":
		runMultiDevice(*addr)
	case "presence-simple":
		runPresence(*addr)
	default:
		log.Fatalf("unknown scenario: %s", *scenario)
	}
	fmt.Println("scenario passed:", *scenario)
}

func runLoginAndChat(addr string) {
	a := newClient(addr, "t1", "alice", "phone")
	defer a.nc.Close()
	b := newClient(addr, "t1", "bob", "phone")
	defer b.nc.Close()
	a.login()
	b.login()
	a.send(protocol.TypeSend, makeSend("t1", "bob", "phone", "hello-bob"))
	b.expectDeliver("hello-bob")
}

func runMultiDevice(addr string) {
	sender := newClient(addr, "t1", "alice", "phone")
	defer sender.nc.Close()
	sender.login()

	d1 := newClient(addr, "t1", "bob", "phone")
	d2 := newClient(addr, "t1", "bob", "laptop")
	d3 := newClient(addr, "t1", "bob", "tablet")
	defer d1.nc.Close()
	defer d2.nc.Close()
	defer d3.nc.Close()
	d1.login()
	d2.login()
	d3.login()

	payload := "fanout-msg"
	sender.send(protocol.TypeSend, makeSend("t1", "bob", "", payload)) // 空 device → fan-out
	d1.expectDeliver(payload)
	d2.expectDeliver(payload)
	d3.expectDeliver(payload)
}

func runPresence(addr string) {
	a := newClient(addr, "t1", "alice", "phone")
	defer a.nc.Close()
	a.login()
	b := newClient(addr, "t1", "bob", "phone")
	b.login()

	// B 登录后 A 发给 B 应送达（证明上线路由已建立）。
	a.send(protocol.TypeSend, makeSend("t1", "bob", "phone", "presence-ok"))
	b.expectDeliver("presence-ok")

	// B 断线；A 再发应得到 ERROR（无路由，S1 无离线概念）。
	b.nc.Close()
	time.Sleep(100 * time.Millisecond)
	a.send(protocol.TypeSend, makeSend("t1", "bob", "phone", "after-offline"))
	f := a.read()
	if f.Type != protocol.TypeError {
		log.Fatalf("expect ERROR after offline, got 0x%02X", f.Type)
	}
	fmt.Println("[alice] got ERROR after bob offline as expected")
}
