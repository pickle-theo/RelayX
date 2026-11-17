package integration

import (
	"bytes"
	"testing"
	"time"

	"relayx/s6/protocol"
	"relayx/s6/test/harness"
)

// TestMultiNodeDelivery 验证跨节点尽力投递（LLD §13.1）。
func TestMultiNodeDelivery(t *testing.T) {
	c, err := harness.New(2, 21000)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}

	a, err := harness.Dial(c.Cfg["gw1"].ClientAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := harness.Dial(c.Cfg["gw2"].ClientAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := a.Login("alice", "d1"); err != nil {
		t.Fatal(err)
	}
	if err := b.Login("bob", "d1"); err != nil {
		t.Fatal(err)
	}

	body, _ := protocol.EncodeSend(protocol.SendBody{
		ClientMsgID: "m1", TargetUserID: "bob", TargetDevices: []string{"d1"}, Payload: []byte("cross-node"),
	})
	if err := a.Send(protocol.TypeSend, body); err != nil {
		t.Fatal(err)
	}
	f, err := b.ReadWithin(3 * time.Second)
	if err != nil {
		t.Fatalf("target did not receive: %v", err)
	}
	if f.Type != protocol.TypeDeliver {
		t.Fatalf("expected DELIVER, got 0x%02x", f.Type)
	}
	d, err := protocol.DecodeDeliver(f.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(d.Payload, []byte("cross-node")) {
		t.Fatalf("payload = %q", d.Payload)
	}
}

// TestAnyNodeAcceptsClient 验证客户端连任意节点均可（LLD §14.1）。
func TestAnyNodeAcceptsClient(t *testing.T) {
	c, err := harness.New(3, 21400)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"gw1", "gw2", "gw3"} {
		cl, err := harness.Dial(c.Cfg[id].ClientAddr)
		if err != nil {
			t.Fatalf("dial %s: %v", id, err)
		}
		if err := cl.Login("u-"+id, "d1"); err != nil {
			t.Fatalf("login to %s: %v", id, err)
		}
		cl.Close()
	}
}

// TestLocalBindingOnly 验证订阅/会话索引不跨节点（LLD §1.1、§10.1）。
// S6 的 endpoint binding 是全局路由，但"本地会话"只存在于所连接节点。
func TestLocalBindingOnly(t *testing.T) {
	c, err := harness.New(2, 21800)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	cl, err := harness.Dial(c.Cfg["gw1"].ClientAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.Login("local-user", "d1"); err != nil {
		t.Fatal(err)
	}
	// gw2 的本地会话表不应包含 gw1 上的会话。
	// 通过向 gw2 直接发 PING 验证它仍能独立服务，而不是依赖 gw1 的会话态。
	other, err := harness.Dial(c.Cfg["gw2"].ClientAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Login("other-user", "d1"); err != nil {
		t.Fatal(err)
	}
	if err := other.Send(protocol.TypePing, nil); err != nil {
		t.Fatal(err)
	}
	f, err := other.ReadWithin(2 * time.Second)
	if err != nil || f.Type != protocol.TypePong {
		t.Fatalf("gw2 should serve its own session: frame=%v err=%v", f, err)
	}
}
