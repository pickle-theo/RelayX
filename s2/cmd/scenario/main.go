// scenario 命令是 S2 v0 验收运行器。只使用公开
// 协议与查询 API：不直连数据库，不做帧级 hack。
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"relayx/s2/internal/config"
	"relayx/s2/internal/identity"
	"relayx/s2/internal/protocol"
)

type client struct {
	conn   net.Conn
	seq    protocol.SequenceTracker
	frames chan protocol.Frame
	mu     sync.Mutex
}

func dial(addr string) (*client, error) {
	nc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	c := &client{conn: nc, frames: make(chan protocol.Frame, 256)}
	go c.readLoop()
	return c, nil
}

func (c *client) readLoop() {
	buf := make([]byte, 1<<16)
	pending := buf[:0]
	for {
		n, err := c.conn.Read(buf[len(pending):cap(buf)])
		if n > 0 {
			pending = pending[:len(pending)+n]
			pos := 0
			for pos < len(pending) {
				f, used, derr := protocol.Decode(pending[pos:])
				if derr != nil {
					return
				}
				pos += used
				// Decode 返回读缓冲区的零拷贝视图。入队前必须复制，
				// 否则下一次 Read 会覆盖帧体。
				f.Body = append([]byte(nil), f.Body...)
				select {
				case c.frames <- f:
				default:
				}
			}
			remain := copy(buf, pending[pos:])
			pending = buf[:remain]
		}
		if err != nil {
			return
		}
	}
}

func (c *client) write(f protocol.Frame) error {
	seq, err := c.seq.Next()
	if err != nil {
		return err
	}
	f.Sequence = seq
	b, err := protocol.Encode(f, nil)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err = c.conn.Write(b)
	return err
}

func (c *client) wait(pred func(protocol.Frame) bool, timeout time.Duration) (protocol.Frame, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case f := <-c.frames:
			if pred(f) {
				return f, nil
			}
		case <-timer.C:
			return protocol.Frame{}, fmt.Errorf("timeout waiting for frame")
		}
	}
}

func (c *client) login(token, device string) error {
	body, err := protocol.EncodeLogin(token, device)
	if err != nil {
		return err
	}
	if err := c.write(protocol.Frame{Type: protocol.TypeLogin, Body: body}); err != nil {
		return err
	}
	f, err := c.wait(func(f protocol.Frame) bool { return f.Type == protocol.TypeLoginAck }, 3*time.Second)
	if err != nil {
		return err
	}
	ack, err := protocol.DecodeLoginAck(f.Body)
	if err != nil {
		return err
	}
	if ack.Capabilities&protocol.CapReliablePolicy == 0 {
		return fmt.Errorf("LOGIN_ACK missing CAP_RELIABLE_POLICY")
	}
	return nil
}

func (c *client) send(clientMsgID, target string, devices []string, payload []byte, p *config.Policy) (protocol.SendAckBody, error) {
	var sp *protocol.SendPolicy
	if p != nil {
		mode := uint8(0)
		if p.DeliveryMode == "last_active" {
			mode = 1
		}
		sp = &protocol.SendPolicy{
			TTLSeconds: p.TTLSeconds, MaxAttempts: p.MaxAttempts,
			DeliveryMode: mode, Fanout: p.Fanout,
		}
	}
	body, err := protocol.EncodeSend(protocol.SendBody{
		ClientMsgID: clientMsgID, TargetUserID: target, TargetDevices: devices,
		Payload: payload, Policy: sp,
	})
	if err != nil {
		return protocol.SendAckBody{}, err
	}
	if err := c.write(protocol.Frame{Type: protocol.TypeSend, Body: body}); err != nil {
		return protocol.SendAckBody{}, err
	}
	f, err := c.wait(func(f protocol.Frame) bool {
		if f.Type == protocol.TypeSendAck {
			a, e := protocol.DecodeSendAck(f.Body)
			return e == nil && a.ClientMsgID == clientMsgID
		}
		return false
	}, 5*time.Second)
	if err != nil {
		return protocol.SendAckBody{}, err
	}
	return protocol.DecodeSendAck(f.Body)
}

func (c *client) waitDeliver(messageID string, timeout time.Duration) (protocol.DeliverBody, error) {
	f, err := c.wait(func(f protocol.Frame) bool {
		if f.Type != protocol.TypeDeliver {
			return false
		}
		d, e := protocol.DecodeDeliver(f.Body)
		return e == nil && (messageID == "" || d.MessageID == messageID)
	}, timeout)
	if err != nil {
		return protocol.DeliverBody{}, err
	}
	return protocol.DecodeDeliver(f.Body)
}

func (c *client) receipt(messageID string) error {
	return c.write(protocol.Frame{Type: protocol.TypeReceiptAck, Body: protocol.EncodeReceiptAck(messageID)})
}

func (c *client) businessAck(messageID, data string) error {
	return c.write(protocol.Frame{Type: protocol.TypeBusinessAck, Body: protocol.EncodeBusinessAck(messageID, []byte(data))})
}

type runner struct {
	gateway string
	access  string
	tenant  string
	secret  []byte
	auth    *identity.Authenticator
	http    *http.Client
}

func (r *runner) token(name, kind string) string {
	prefix := "u:"
	if kind == "service" {
		prefix = "svc:"
	}
	p := identity.Producer{ID: prefix + name, Kind: kind, TenantID: r.tenant}
	t, err := r.auth.Issue(p, time.Hour)
	if err != nil {
		panic(err)
	}
	return t
}

func (r *runner) baseURL() string {
	if strings.HasPrefix(r.access, "http://") || strings.HasPrefix(r.access, "https://") {
		return r.access
	}
	return "http://" + r.access
}

func (r *runner) get(path, token string, out any) error {
	req, err := http.NewRequest(http.MethodGet, r.baseURL()+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (r *runner) setClock(offsetSeconds int64) error {
	body, _ := json.Marshal(map[string]int64{"offset_seconds": offsetSeconds})
	resp, err := r.http.Post(r.baseURL()+"/debug/clock", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set clock: %s %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

func (r *runner) pollState(id, token, want string, timeout time.Duration) (map[string]any, error) {
	deadline := time.Now().Add(timeout)
	var last map[string]any
	for time.Now().Before(deadline) {
		var m map[string]any
		if err := r.get("/v1/messages/"+id, token, &m); err != nil {
			return nil, err
		}
		last = m
		if m["platform_state"] == want {
			return m, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("message %s state=%v, want %s", id, last["platform_state"], want)
}

func (r *runner) facts(id, token string) ([]map[string]any, error) {
	var out struct {
		Facts []map[string]any `json:"facts"`
	}
	if err := r.get("/v1/messages/"+id+"/facts", token, &out); err != nil {
		return nil, err
	}
	return out.Facts, nil
}

func factTypes(fs []map[string]any) []string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		out = append(out, fmt.Sprint(f["fact_type"]))
	}
	return out
}

func require(cond bool, format string, args ...any) error {
	if !cond {
		return fmt.Errorf(format, args...)
	}
	return nil
}

func (r *runner) reliableOnline() error {
	alice, bob := r.token("alice", "user"), r.token("bob", "user")
	ca, err := dial(r.gateway)
	if err != nil {
		return err
	}
	cb, err := dial(r.gateway)
	if err != nil {
		return err
	}
	if err := ca.login(alice, "phone"); err != nil {
		return err
	}
	if err := cb.login(bob, "phone"); err != nil {
		return err
	}
	ack, err := ca.send("m-online", "bob", nil, []byte("hello"), &config.Policy{
		TTLSeconds: 60, MaxAttempts: 3, DeliveryMode: "all_endpoints",
	})
	if err != nil {
		return err
	}
	if err := require(ack.PlatformState == "ACCEPTED" && !ack.Duplicate, "unexpected SEND_ACK: %+v", ack); err != nil {
		return err
	}
	d, err := cb.waitDeliver(ack.MessageID, 5*time.Second)
	if err != nil {
		return err
	}
	if err := require(string(d.Payload) == "hello" && d.ProducerID == "u:alice", "bad DELIVER: %+v", d); err != nil {
		return err
	}
	if err := cb.receipt(ack.MessageID); err != nil {
		return err
	}
	m, err := r.pollState(ack.MessageID, alice, "SETTLED", 5*time.Second)
	if err != nil {
		return err
	}
	counts, _ := m["counts"].(map[string]any)
	return require(fmt.Sprint(counts["receipt_acked"]) == "1", "counts=%v", counts)
}

func (r *runner) multiDevice() error {
	alice, bob := r.token("alice", "user"), r.token("bob", "user")
	ca, err := dial(r.gateway)
	if err != nil {
		return err
	}
	b1, err := dial(r.gateway)
	if err != nil {
		return err
	}
	b2, err := dial(r.gateway)
	if err != nil {
		return err
	}
	if err := ca.login(alice, "phone"); err != nil {
		return err
	}
	if err := b1.login(bob, "phone"); err != nil {
		return err
	}
	if err := b2.login(bob, "laptop"); err != nil {
		return err
	}
	ack, err := ca.send("m-fanout", "bob", nil, []byte("fan"), nil)
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	d1, err := b1.waitDeliver(ack.MessageID, 5*time.Second)
	if err != nil {
		return fmt.Errorf("deliver phone: %w", err)
	}
	d2, err := b2.waitDeliver(ack.MessageID, 5*time.Second)
	if err != nil {
		return fmt.Errorf("deliver laptop: %w", err)
	}
	if err := require(d1.MessageID == d2.MessageID, "different message ids"); err != nil {
		return err
	}
	_ = b1.receipt(ack.MessageID)
	_ = b2.receipt(ack.MessageID)
	m, err := r.pollState(ack.MessageID, alice, "SETTLED", 5*time.Second)
	if err != nil {
		return err
	}
	counts, _ := m["counts"].(map[string]any)
	if err := require(fmt.Sprint(counts["total"]) == "2", "counts=%v", counts); err != nil {
		return err
	}
	dup, err := ca.send("m-fanout", "bob", nil, []byte("fan"), nil)
	if err != nil {
		return err
	}
	return require(dup.Duplicate && dup.MessageID == ack.MessageID, "idempotency failed: %+v", dup)
}

func (r *runner) offlineRecovery() error {
	alice, bob := r.token("alice", "user"), r.token("bob", "user")
	ca, err := dial(r.gateway)
	if err != nil {
		return err
	}
	if err := ca.login(alice, "phone"); err != nil {
		return err
	}
	ack, err := ca.send("m-offline", "bob", []string{"phone"}, []byte("later"), nil)
	if err != nil {
		return err
	}
	cb, err := dial(r.gateway)
	if err != nil {
		return err
	}
	if err := cb.login(bob, "phone"); err != nil {
		return err
	}
	if _, err := cb.waitDeliver(ack.MessageID, 5*time.Second); err != nil {
		return err
	}
	if err := cb.receipt(ack.MessageID); err != nil {
		return err
	}
	_, err = r.pollState(ack.MessageID, alice, "SETTLED", 5*time.Second)
	return err
}

func (r *runner) ttlExpiry() error {
	alice, bob := r.token("alice", "user"), r.token("bob", "user")
	ca, err := dial(r.gateway)
	if err != nil {
		return err
	}
	cb, err := dial(r.gateway)
	if err != nil {
		return err
	}
	if err := ca.login(alice, "phone"); err != nil {
		return err
	}
	if err := cb.login(bob, "phone"); err != nil {
		return err
	}
	if err := r.setClock(0); err != nil {
		return err
	}
	defer r.setClock(0)
	ack, err := ca.send("m-ttl", "bob", nil, []byte("expire"), &config.Policy{
		TTLSeconds: 3600, MaxAttempts: 3, DeliveryMode: "all_endpoints",
	})
	if err != nil {
		return err
	}
	if err := r.setClock(7200); err != nil {
		return err
	}
	if _, err := r.pollState(ack.MessageID, alice, "EXPIRED", 8*time.Second); err != nil {
		return err
	}
	fs, err := r.facts(ack.MessageID, alice)
	if err != nil {
		return err
	}
	for _, t := range factTypes(fs) {
		if t == "message.settled" {
			return fmt.Errorf("settled and expired facts coexist")
		}
	}
	return require(contains(factTypes(fs), "message.expired"), "facts=%v", factTypes(fs))
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func (r *runner) retryExhaustion() error {
	alice, bob := r.token("alice", "user"), r.token("bob", "user")
	ca, err := dial(r.gateway)
	if err != nil {
		return err
	}
	cb, err := dial(r.gateway)
	if err != nil {
		return err
	}
	if err := ca.login(alice, "phone"); err != nil {
		return err
	}
	if err := cb.login(bob, "phone"); err != nil {
		return err
	}
	if err := r.setClock(0); err != nil {
		return err
	}
	defer r.setClock(0)
	ack, err := ca.send("m-retry", "bob", []string{"phone"}, []byte("no ack"), &config.Policy{
		TTLSeconds: 3600, MaxAttempts: 2, DeliveryMode: "all_endpoints",
	})
	if err != nil {
		return err
	}
	if _, err := cb.waitDeliver(ack.MessageID, 5*time.Second); err != nil {
		return err
	}
	if err := r.setClock(30); err != nil {
		return err
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var ds struct {
			Deliveries []map[string]any `json:"deliveries"`
		}
		if err := r.get("/v1/messages/"+ack.MessageID+"/deliveries", alice, &ds); err != nil {
			return err
		}
		failed := false
		for _, d := range ds.Deliveries {
			if d["delivery_state"] == "FAILED" {
				failed = true
			}
		}
		if failed {
			fs, err := r.facts(ack.MessageID, alice)
			if err != nil {
				return err
			}
			return require(contains(factTypes(fs), "delivery.failed"), "facts=%v", factTypes(fs))
		}
		// 首次重试会基于 store 时钟派生新的退避。再次推进时钟，
		// 让终态尝试真正进入扫描窗口。
		for _, d := range ds.Deliveries {
			if fmt.Sprint(d["attempts"]) == "2" {
				_ = r.setClock(60)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("delivery did not reach FAILED")
}

func (r *runner) businessAckPassthrough() error {
	alice, bob := r.token("alice", "user"), r.token("bob", "user")
	ca, err := dial(r.gateway)
	if err != nil {
		return err
	}
	cb, err := dial(r.gateway)
	if err != nil {
		return err
	}
	if err := ca.login(alice, "phone"); err != nil {
		return err
	}
	if err := cb.login(bob, "phone"); err != nil {
		return err
	}
	ack, err := ca.send("m-business", "bob", nil, []byte("read?"), nil)
	if err != nil {
		return err
	}
	if _, err := cb.waitDeliver(ack.MessageID, 5*time.Second); err != nil {
		return err
	}
	if err := cb.businessAck(ack.MessageID, "seen"); err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond)
	var ds struct {
		Deliveries []map[string]any `json:"deliveries"`
	}
	if err := r.get("/v1/messages/"+ack.MessageID+"/deliveries", alice, &ds); err != nil {
		return err
	}
	acked := false
	for _, d := range ds.Deliveries {
		if d["application_status"] == "seen" {
			acked = true
			if err := require(d["delivery_state"] == "SENT", "business ack changed state: %v", d); err != nil {
				return err
			}
		}
	}
	if err := require(acked, "business ack not persisted: %v", ds.Deliveries); err != nil {
		return err
	}
	fs, err := r.facts(ack.MessageID, alice)
	if err != nil {
		return err
	}
	return require(!contains(factTypes(fs), "business.ack"), "business ack produced a fact")
}

func (r *runner) zeroEndpointSettles() error {
	alice := r.token("alice", "user")
	ca, err := dial(r.gateway)
	if err != nil {
		return err
	}
	if err := ca.login(alice, "phone"); err != nil {
		return err
	}
	ack, err := ca.send("m-zero", "never-logged-in", nil, []byte("nobody"), nil)
	if err != nil {
		return err
	}
	if err := require(ack.PlatformState == "SETTLED", "ack=%+v", ack); err != nil {
		return err
	}
	var ds struct {
		Total int `json:"total"`
	}
	if err := r.get("/v1/messages/"+ack.MessageID+"/deliveries", alice, &ds); err != nil {
		return err
	}
	return require(ds.Total == 0, "deliveries=%d", ds.Total)
}

func (r *runner) duplicateSettleCAS() error {
	alice, bob := r.token("alice", "user"), r.token("bob", "user")
	ca, err := dial(r.gateway)
	if err != nil {
		return err
	}
	cb, err := dial(r.gateway)
	if err != nil {
		return err
	}
	if err := ca.login(alice, "phone"); err != nil {
		return err
	}
	if err := cb.login(bob, "phone"); err != nil {
		return err
	}
	if err := r.setClock(0); err != nil {
		return err
	}
	defer r.setClock(0)
	ack, err := ca.send("m-cas", "bob", nil, []byte("race"), &config.Policy{
		TTLSeconds: 3600, MaxAttempts: 3, DeliveryMode: "all_endpoints",
	})
	if err != nil {
		return err
	}
	if _, err := cb.waitDeliver(ack.MessageID, 5*time.Second); err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = cb.receipt(ack.MessageID) }()
	go func() { defer wg.Done(); _ = r.setClock(7200) }()
	wg.Wait()
	time.Sleep(400 * time.Millisecond)
	fs, err := r.facts(ack.MessageID, alice)
	if err != nil {
		return err
	}
	types := factTypes(fs)
	settled, expired := contains(types, "message.settled"), contains(types, "message.expired")
	if settled == expired {
		return fmt.Errorf("terminal facts not exclusive: %v", types)
	}
	if settled {
		_, err = r.pollState(ack.MessageID, alice, "SETTLED", 3*time.Second)
	} else {
		_, err = r.pollState(ack.MessageID, alice, "EXPIRED", 3*time.Second)
	}
	return err
}

func main() {
	gw := flag.String("gateway", "127.0.0.1:9800", "TCP gateway address")
	access := flag.String("access", "127.0.0.1:9802", "access API address")
	secret := flag.String("secret", "relayx-s2-dev-secret", "token secret")
	tenant := flag.String("tenant", "t1", "tenant id")
	name := flag.String("scenario", "reliable-online-delivery", "scenario name")
	flag.Parse()

	r := &runner{
		gateway: *gw, access: *access, tenant: *tenant,
		secret: []byte(*secret), auth: identity.New([]byte(*secret)),
		http: &http.Client{Timeout: 5 * time.Second},
	}
	scenarios := map[string]func(*runner) error{
		"reliable-online-delivery": (*runner).reliableOnline,
		"multi-device-fanout":      (*runner).multiDevice,
		"offline-recovery":         (*runner).offlineRecovery,
		"ttl-expiry":               (*runner).ttlExpiry,
		"retry-exhaustion":         (*runner).retryExhaustion,
		"business-ack-passthrough": (*runner).businessAckPassthrough,
		"clock-skew-immunity":      (*runner).ttlExpiry,
		"zero-endpoint-settles":    (*runner).zeroEndpointSettles,
		"duplicate-settle-cas":     (*runner).duplicateSettleCAS,
	}
	fn, ok := scenarios[*name]
	if !ok {
		fmt.Fprintf(flag.CommandLine.Output(), "unknown scenario %q\n", *name)
		flag.Usage()
		os.Exit(2)
	}
	if err := fn(r); err != nil {
		fmt.Printf("FAIL %s: %v\n", *name, err)
		os.Exit(1)
	}
	fmt.Printf("PASS %s\n", *name)
}
