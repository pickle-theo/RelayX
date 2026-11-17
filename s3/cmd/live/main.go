// Command live 是 S3 的端到端验证客户端（LLD §12.5），通过公开协议帧与 HTTP
// POST /v1/broadcasts 驱动网关，覆盖 topic-fanout / fanout-order / unsubscribe-race /
// compat-old-client / shared-buffer-integrity / slow-consumer-drop 六个场景。
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"relayx/s3/internal/broadcast"
	"relayx/s3/internal/identity"
	"relayx/s3/internal/protocol"
)

var (
	loginSecret = []byte("relayx-s3-dev-secret")
	subSecret   = []byte("relayx-s3-dev-subscribe-secret")
	subKid      = "dev"
)

type client struct {
	nc       net.Conn
	auth     *identity.Authenticator
	verifier *broadcast.TokenVerifier
	seq      protocol.SequenceTracker
	tenant   string
	user     string
	device   string
}

func newClient(addr, tenant, user, device string) *client {
	nc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	return &client{
		nc: nc, auth: identity.New(loginSecret), verifier: broadcast.NewTokenVerifier(nil),
		tenant: tenant, user: user, device: device,
	}
}

func (c *client) close() { _ = c.nc.Close() }

func (c *client) send(t byte, body []byte) {
	seq, _ := c.seq.Next()
	b, err := protocol.Encode(protocol.Frame{Type: t, Sequence: seq, Body: body}, nil)
	if err != nil {
		log.Fatalf("encode 0x%02x: %v", t, err)
	}
	if _, err := c.nc.Write(b); err != nil {
		log.Fatalf("write 0x%02x: %v", t, err)
	}
}

func (c *client) readTimeout(d time.Duration) (protocol.Frame, error) {
	_ = c.nc.SetReadDeadline(time.Now().Add(d))
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(c.nc, header); err != nil {
		return protocol.Frame{}, err
	}
	length := uint64(header[4])<<24 | uint64(header[5])<<16 | uint64(header[6])<<8 | uint64(header[7])
	if length > protocol.MaxBodySize {
		return protocol.Frame{}, fmt.Errorf("frame too large: %d", length)
	}
	buf := make([]byte, protocol.HeaderSize+int(length))
	copy(buf, header)
	if _, err := io.ReadFull(c.nc, buf[protocol.HeaderSize:]); err != nil {
		return protocol.Frame{}, err
	}
	f, _, err := protocol.Decode(buf)
	return f, err
}

func (c *client) login() {
	p := identity.Producer{ID: "u:" + c.user, Kind: "user", TenantID: c.tenant}
	tok, err := c.auth.Issue(p, time.Hour)
	if err != nil {
		log.Fatalf("issue login token: %v", err)
	}
	body, _ := protocol.EncodeLogin(tok, c.device)
	c.send(protocol.TypeLogin, body)
	f, err := c.readTimeout(5 * time.Second)
	if err != nil || f.Type != protocol.TypeLoginAck {
		log.Fatalf("expect LOGIN_ACK, got 0x%02X err=%v", f.Type, err)
	}
	caps, err := protocol.DecodeLoginAck(f.Body)
	if err != nil {
		log.Fatalf("decode caps: %v", err)
	}
	if caps.Capabilities&protocol.CapTopicSubscribe == 0 {
		log.Fatalf("CAP_TOPIC_SUBSCRIBE not advertised: 0x%08x", caps.Capabilities)
	}
}

func (c *client) subscribe(topic string) protocol.SubscribeResult {
	tok, err := c.verifier.Issue(subKid, subSecret, broadcast.SubscriptionClaims{
		TenantID: c.tenant, UserID: c.user, Topic: topic,
	}, time.Hour)
	if err != nil {
		log.Fatalf("issue subscribe token: %v", err)
	}
	body, _ := protocol.EncodeSubscribe(topic, tok)
	c.send(protocol.TypeSubscribe, body)
	f, err := c.readTimeout(5 * time.Second)
	if err != nil || f.Type != protocol.TypeSubscribeAck {
		log.Fatalf("expect SUBSCRIBE_ACK, got 0x%02X err=%v", f.Type, err)
	}
	ack, err := protocol.DecodeSubscribeAck(f.Body)
	if err != nil {
		log.Fatalf("decode subscribe_ack: %v", err)
	}
	return ack.Result
}

func (c *client) unsubscribe(topic string) protocol.UnsubscribeResult {
	body, _ := protocol.EncodeUnsubscribe(topic)
	c.send(protocol.TypeUnsubscribe, body)
	f, err := c.readTimeout(5 * time.Second)
	if err != nil || f.Type != protocol.TypeUnsubscribeAck {
		log.Fatalf("expect UNSUBSCRIBE_ACK, got 0x%02X err=%v", f.Type, err)
	}
	ack, err := protocol.DecodeUnsubscribeAck(f.Body)
	if err != nil {
		log.Fatalf("decode unsubscribe_ack: %v", err)
	}
	return ack.Result
}

func (c *client) expectBroadcast(topic string, wantPayload string) (protocol.BroadcastBody, error) {
	for i := 0; i < 20; i++ {
		f, err := c.readTimeout(3 * time.Second)
		if err != nil {
			return protocol.BroadcastBody{}, err
		}
		switch f.Type {
		case protocol.TypeBroadcast:
			b, err := protocol.DecodeBroadcast(f.Body)
			if err != nil {
				return protocol.BroadcastBody{}, err
			}
			if b.Topic == topic && (wantPayload == "" || string(b.Payload) == wantPayload) {
				return b, nil
			}
		case protocol.TypePing:
			seq, _ := c.seq.Next()
			b, _ := protocol.Encode(protocol.Frame{Type: protocol.TypePong, Sequence: seq}, nil)
			_, _ = c.nc.Write(b)
		}
	}
	return protocol.BroadcastBody{}, fmt.Errorf("no matching broadcast")
}

func (c *client) waitDeliver(want string) {
	for i := 0; i < 20; i++ {
		f, err := c.readTimeout(3 * time.Second)
		if err != nil {
			log.Fatalf("wait deliver: %v", err)
		}
		if f.Type == protocol.TypeDeliver {
			d, err := protocol.DecodeDeliver(f.Body)
			if err == nil && string(d.Payload) == want {
				return
			}
		}
	}
	log.Fatalf("no DELIVER with payload %q", want)
}

func (c *client) expectNoBroadcast(d time.Duration) error {
	_ = c.nc.SetReadDeadline(time.Now().Add(d))
	for {
		f, err := c.readTimeout(d)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil
		}
		if err != nil {
			return err
		}
		if f.Type == protocol.TypeBroadcast {
			return fmt.Errorf("unexpected broadcast on non-subscriber")
		}
	}
}

func serviceToken(tenant string) string {
	a := identity.New(loginSecret)
	tok, err := a.Issue(identity.Producer{ID: "svc:publisher", Kind: "service", TenantID: tenant}, time.Hour)
	if err != nil {
		log.Fatalf("issue service token: %v", err)
	}
	return tok
}

func publish(accessAddr, tenant, topic, payload, idem string) int {
	body, _ := json.Marshal(map[string]string{
		"topic": topic, "payload": base64.StdEncoding.EncodeToString([]byte(payload)),
		"idempotency_key": idem,
	})
	req, err := http.NewRequest(http.MethodPost, "http://"+accessAddr+"/v1/broadcasts", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+serviceToken(tenant))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("publish: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func runTopicFanout(addr, access string) {
	a := newClient(addr, "t1", "alice", "phone")
	b := newClient(addr, "t1", "bob", "phone")
	z := newClient(addr, "t1", "carol", "phone")
	defer a.close()
	defer b.close()
	defer z.close()
	a.login()
	b.login()
	z.login()
	if r := a.subscribe("room-1"); r != protocol.SubscribeOK {
		log.Fatalf("subscribe alice: result=%d", r)
	}
	if r := b.subscribe("room-1"); r != protocol.SubscribeOK {
		log.Fatalf("subscribe bob: result=%d", r)
	}
	if r := a.subscribe("room-1"); r != protocol.SubscribeDuplicate {
		log.Fatalf("duplicate subscribe: result=%d", r)
	}
	if code := publish(access, "t1", "room-1", "hello-room", ""); code != http.StatusAccepted {
		log.Fatalf("publish status=%d", code)
	}
	for _, c := range []*client{a, b} {
		bb, err := c.expectBroadcast("room-1", "hello-room")
		if err != nil {
			log.Fatalf("fanout receive: %v", err)
		}
		fmt.Printf("[fanout] %s/%s got seq=%d payload=%q\n", c.user, c.device, bb.Seq, bb.Payload)
	}
	if err := z.expectNoBroadcast(300 * time.Millisecond); err != nil {
		log.Fatalf("non-subscriber: %v", err)
	}
	fmt.Println("[fanout] non-subscriber received nothing")
}

func runFanoutOrder(addr, access string) {
	c := newClient(addr, "t2", "alice", "phone")
	defer c.close()
	c.login()
	if r := c.subscribe("ordered"); r != protocol.SubscribeOK {
		log.Fatalf("subscribe: %d", r)
	}
	const n = 40
	for i := 0; i < n; i++ {
		payload := fmt.Sprintf("m-%03d", i)
		if code := publish(access, "t2", "ordered", payload, ""); code != http.StatusAccepted {
			log.Fatalf("publish %d status=%d", i, code)
		}
	}
	var prev uint64
	for i := 0; i < n; i++ {
		bb, err := c.expectBroadcast("ordered", "")
		if err != nil {
			log.Fatalf("order receive %d: %v", i, err)
		}
		if i > 0 && bb.Seq != prev+1 {
			log.Fatalf("seq gap/order: got %d want %d", bb.Seq, prev+1)
		}
		want := fmt.Sprintf("m-%03d", i)
		if string(bb.Payload) != want {
			log.Fatalf("payload order: got %q want %q", bb.Payload, want)
		}
		prev = bb.Seq
	}
	fmt.Printf("[order] %d broadcasts in order, seq=%d..%d\n", n, 1, prev)
}

func runUnsubscribeRace(addr, access string) {
	c := newClient(addr, "t3", "alice", "phone")
	defer c.close()
	c.login()
	if r := c.subscribe("race"); r != protocol.SubscribeOK {
		log.Fatalf("subscribe: %d", r)
	}
	publish(access, "t3", "race", "before", "")
	if _, err := c.expectBroadcast("race", "before"); err != nil {
		log.Fatalf("before unsubscribe: %v", err)
	}
	if r := c.unsubscribe("race"); r != protocol.UnsubscribeOK {
		log.Fatalf("unsubscribe: %d", r)
	}
	if r := c.unsubscribe("race"); r != protocol.UnsubscribeNotExists {
		log.Fatalf("double unsubscribe: %d", r)
	}
	publish(access, "t3", "race", "after", "")
	if err := c.expectNoBroadcast(500 * time.Millisecond); err != nil {
		log.Fatalf("after unsubscribe: %v", err)
	}
	fmt.Println("[race] no broadcast after unsubscribe ack")
}

func runCompatOldClient(addr, access string) {
	sub := newClient(addr, "t4", "bob", "phone")
	old := newClient(addr, "t4", "alice", "phone")
	defer sub.close()
	defer old.close()
	sub.login()
	old.login() // 旧客户端：登录但不订阅
	if r := sub.subscribe("legacy"); r != protocol.SubscribeOK {
		log.Fatalf("subscribe: %d", r)
	}
	// 老式单播 SEND 仍应工作（未订阅不影响既有行为）。
	sendBody, _ := protocol.EncodeSend(protocol.SendBody{
		TenantID: "t4", UserID: "bob", DeviceID: "phone", Payload: []byte("legacy-im"),
	})
	old.send(protocol.TypeSend, sendBody)
	sub.waitDeliver("legacy-im") // DELIVER 到收件人，不是发送方
	publish(access, "t4", "legacy", "bcast-only", "")
	if _, err := sub.expectBroadcast("legacy", "bcast-only"); err != nil {
		log.Fatalf("subscriber broadcast: %v", err)
	}
	if err := old.expectNoBroadcast(300 * time.Millisecond); err != nil {
		log.Fatalf("old client got broadcast: %v", err)
	}
	fmt.Println("[compat] old client unaffected, subscriber got broadcast")
}

func runSharedBufferIntegrity(addr, access string) {
	s1 := newClient(addr, "t5", "alice", "phone")
	s2 := newClient(addr, "t5", "alice", "laptop")
	slow := newClient(addr, "t5", "bob", "phone")
	defer s1.close()
	defer s2.close()
	defer slow.close()
	s1.login()
	s2.login()
	slow.login()
	for _, c := range []*client{s1, s2, slow} {
		if r := c.subscribe("integrity"); r != protocol.SubscribeOK {
			log.Fatalf("subscribe: %d", r)
		}
	}

	const n = 24
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			publish(access, "t5", "integrity", fmt.Sprintf("payload-%02d", i), "")
		}(i)
	}
	wg.Wait()

	// slow 客户端故意晚读，制造 P2 队列积压；s1/s2 立即读。
	time.Sleep(200 * time.Millisecond)
	collect := func(c *client) map[uint64]string {
		m := make(map[uint64]string)
		for i := 0; i < n; i++ {
			f, err := c.readTimeout(5 * time.Second)
			if err != nil {
				log.Fatalf("integrity read: %v", err)
			}
			if f.Type != protocol.TypeBroadcast {
				continue
			}
			bb, err := protocol.DecodeBroadcast(f.Body)
			if err != nil {
				log.Fatalf("decode broadcast: %v", err)
			}
			if bb.Topic != "integrity" {
				continue
			}
			if _, dup := m[bb.Seq]; !dup {
				m[bb.Seq] = string(bb.Payload)
			}
			if i == n-1 {
				break
			}
		}
		return m
	}
	m1 := collect(s1)
	m2 := collect(s2)
	keys := make([]uint64, 0, len(m1))
	for k, v := range m1 {
		if m2[k] != v {
			log.Fatalf("cross-subscriber data mismatch at seq=%d: %q vs %q", k, v, m2[k])
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	fmt.Printf("[integrity] %d seq/payload pairs identical across subscribers\n", len(keys))
}

func runSlowConsumerDrop(addr, access string) {
	fast := newClient(addr, "t6", "alice", "phone")
	slow := newClient(addr, "t6", "bob", "phone")
	defer fast.close()
	defer slow.close()
	fast.login()
	slow.login()
	if r := fast.subscribe("slow"); r != protocol.SubscribeOK {
		log.Fatalf("subscribe fast: %d", r)
	}
	if r := slow.subscribe("slow"); r != protocol.SubscribeOK {
		log.Fatalf("subscribe slow: %d", r)
	}
	payload := string(bytes.Repeat([]byte("X"), 8<<10))
	for i := 0; i < 220; i++ {
		if code := publish(access, "t6", "slow", payload, ""); code != http.StatusAccepted {
			log.Fatalf("publish %d status=%d", i, code)
		}
	}
	// fast 应能读到全部（或绝大部分）；slow 因不读而丢弃。此处只断言 fast 未被拖死。
	got := 0
	for i := 0; i < 220; i++ {
		f, err := fast.readTimeout(5 * time.Second)
		if err != nil {
			break
		}
		if f.Type == protocol.TypeBroadcast {
			got++
		}
	}
	if got < 100 {
		log.Fatalf("fast consumer starved by slow consumer: got=%d", got)
	}
	fmt.Printf("[slow] fast consumer received %d/220 broadcasts while slow consumer stalled\n", got)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9800", "Gateway TCP 地址")
	access := flag.String("access", "127.0.0.1:9802", "HTTP 受理地址")
	scenario := flag.String("scenario", "topic-fanout", "topic-fanout|fanout-order|unsubscribe-race|compat-old-client|shared-buffer-integrity|slow-consumer-drop")
	flag.Parse()

	switch *scenario {
	case "topic-fanout":
		runTopicFanout(*addr, *access)
	case "fanout-order":
		runFanoutOrder(*addr, *access)
	case "unsubscribe-race":
		runUnsubscribeRace(*addr, *access)
	case "compat-old-client":
		runCompatOldClient(*addr, *access)
	case "shared-buffer-integrity":
		runSharedBufferIntegrity(*addr, *access)
	case "slow-consumer-drop":
		runSlowConsumerDrop(*addr, *access)
	default:
		log.Fatalf("unknown scenario: %s", *scenario)
	}
	fmt.Println("scenario passed:", *scenario)
}
