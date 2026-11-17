package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"time"

	"relayx/s5/internal/loadclient"
)

// runConn 是一条压测连接的生命周期：登录 → 订阅 → 按调度执行混合操作。
// 每条连接一个 goroutine，读/写/心跳由 loadclient 内部负责。
func (r *runner) runConn(c *connState) {
	defer r.wg.Done()
	rnd := rand.New(rand.NewSource(r.cfg.Seed + int64(c.id)))
	slow := rnd.Float64() < r.cfg.InjectSlow
	big := rnd.Float64() < r.cfg.InjectBig

	token, err := loadclient.IssueLoginToken([]byte(r.cfg.TokenSecret), r.cfg.Tenant, c.user, "user", 24*time.Hour)
	if err != nil {
		r.ctr.connsFailed.Add(1)
		return
	}
	cli, err := loadclient.Dial(loadclient.Config{
		Addr: r.cfg.Gateway, Token: token, DeviceID: c.dev,
		Tenant: r.cfg.Tenant, User: c.user,
		AckTimeout:        r.cfg.AckTimeout,
		Wheel:             r.wheel,
		HeartbeatInterval: 30 * time.Second,
		PongTimeout:       10 * time.Second,
	})
	if err != nil {
		r.ctr.connsFailed.Add(1)
		return
	}
	defer func() {
		// 入站丢弃是连接级计数，连接退出时汇总到全局报告。
		st := cli.Stats()
		r.ctr.inboundDrops.Add(st.DeliverDropped + st.BroadcastDrops)
		cli.Close()
	}()

	if _, err := cli.Login(); err != nil {
		r.ctr.connsFailed.Add(1)
		return
	}
	r.ctr.connsOpened.Add(1)
	active := r.ctr.activeConns.Add(1)
	defer r.ctr.activeConns.Add(-1)
	for {
		peak := r.ctr.peakConns.Load()
		if active <= peak || r.ctr.peakConns.CompareAndSwap(peak, active) {
			break
		}
	}

	if r.cfg.Topics > 0 {
		topic := r.topics[c.id%len(r.topics)]
		subToken, err := loadclient.IssueSubscribeToken(
			r.cfg.SubscribeKid, []byte(r.cfg.SubscribeSecret), r.cfg.Tenant, c.user, topic, 24*time.Hour)
		if err == nil {
			if _, err := cli.Subscribe(topic, subToken); err != nil {
				r.ctr.errors.Add(1)
			}
		}
	}

	body := make([]byte, r.cfg.Payload)
	var seq uint32
	var lastDeliver string
	inflight := make(map[string]inflightEntry, 128)

	for {
		select {
		case <-r.done:
			return
		case <-c.stop:
			r.ctr.connsClosed.Add(1)
			return
		case <-cli.Closed():
			r.ctr.connsClosed.Add(1)
			return
		case <-c.opCh:
			if slow && rnd.Intn(8) != 0 {
				continue // 慢消费者：只保留 1/8 的操作
			}
			switch r.cfg.Mix.PickRand(rnd) {
			case KindSend:
				n := len(body)
				if big {
					n *= 5
				}
				sentAt := time.Now().UnixNano()
				seq++
				payload := loadclient.EncodeLatencyPayload(sentAt, seq, body[:n])
				res, err := cli.Send(c.user, []string{c.dev}, payload)
				if err != nil {
					if errors.Is(err, loadclient.ErrTimeout) {
						r.ctr.timeouts.Add(1)
					} else {
						r.ctr.errors.Add(1)
					}
					continue
				}
				r.ctr.sends.Add(1)
				r.ctr.acks.Add(1)
				r.rec.Add("send_ack", Sample{Total: res.WaitNs, Encode: res.EncodeNs, Server: -1, Egress: -1})
				entry := inflightEntry{
					sentAt: sentAt, encodeNs: res.EncodeNs,
					// 用客户端本地 ACK 时刻切分，而不是服务端 accepted_at（毫秒精度）。
					acceptedAt: res.AckAt,
				}
				inflight[res.MessageID] = entry
				if len(inflight) > 128 {
					for k := range inflight {
						delete(inflight, k)
						break
					}
				}
			case KindReceipt:
				if lastDeliver != "" {
					if err := cli.ReceiptAck(lastDeliver); err != nil {
						r.ctr.errors.Add(1)
						continue
					}
					r.ctr.receipts.Add(1)
					lastDeliver = ""
				}
			case KindPing:
				d, err := cli.PingWait(r.cfg.AckTimeout)
				if err != nil {
					r.ctr.timeouts.Add(1)
					continue
				}
				r.ctr.pings.Add(1)
				r.rec.Add("ping", Sample{Total: d.Nanoseconds(), Encode: -1, Server: -1, Egress: -1})
			}

		case d := <-cli.Deliver():
			now := time.Now().UnixNano()
			r.ctr.delivers.Add(1)
			lastDeliver = d.MessageID
			sentAt, _, ok := loadclient.DecodeLatencyPayload(d.Payload)
			if !ok {
				continue
			}
			r.rec.Add("deliver_e2e", Sample{Total: now - sentAt, Encode: -1, Server: -1, Egress: -1})
			if e, found := inflight[d.MessageID]; found {
				delete(inflight, d.MessageID)
				server := e.acceptedAt - e.sentAt
				egress := now - e.acceptedAt
				if server < 0 {
					server = 0
				}
				if egress < 0 {
					egress = 0
				}
				r.rec.Add("send_e2e", Sample{
					Total: now - e.sentAt, Encode: e.encodeNs, Server: server, Egress: egress,
				})
			}

		case b := <-cli.Broadcast():
			r.ctr.broadcasts.Add(1)
			now := time.Now().UnixNano()
			sentAt, _, ok := loadclient.DecodeLatencyPayload(b.Payload)
			if !ok {
				continue
			}
			// server_received_at 只有毫秒精度（本地环回延迟本身就在毫秒级），
			// 用它切分只会得到 0/±1ms 的噪声，因此广播只报端到端；入站/出站
			// 由 publish_http（客户端可观测）与 send_e2e 的分段间接体现。
			r.rec.Add("broadcast_e2e", Sample{Total: now - sentAt, Encode: -1, Server: -1, Egress: -1})
		}
	}
}

// publishLoop 是 HTTP 广播发布器：按混合比例中的 broadcast 权重发布，
// 走 access 端口的 POST /v1/broadcasts（S3 受理语义，202 Accepted）。
func (r *runner) publishLoop(start time.Time, total time.Duration) {
	const tick = 100 * time.Millisecond
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	var credit float64
	last := start
	idx := 0
	for {
		select {
		case <-r.done:
			return
		case now := <-ticker.C:
			elapsed := now.Sub(start)
			if elapsed >= total {
				return
			}
			factor := r.cfg.Curve.Factor(elapsed)
			dt := now.Sub(last).Seconds()
			last = now
			rate := r.cfg.Mix.RateFor(KindBroadcast, float64(r.cfg.Conns)*r.cfg.Rate)
			credit += rate * factor * dt
			n := int(credit)
			credit -= float64(n)
			for i := 0; i < n; i++ {
				topic := "lg-topic-0"
				if len(r.topics) > 0 {
					topic = r.topics[idx%len(r.topics)]
				}
				idx++
				payload := loadclient.EncodeLatencyPayload(time.Now().UnixNano(), uint32(idx), []byte("broadcast-payload"))
				start := time.Now()
				if err := r.publish(topic, payload); err != nil {
					r.ctr.publishFail.Add(1)
					continue
				}
				r.ctr.published.Add(1)
				r.rec.Add("publish_http", Sample{
					Total: time.Since(start).Nanoseconds(), Encode: -1, Server: -1, Egress: -1,
				})
			}
		}
	}
}

func (r *runner) publish(topic string, payload []byte) error {
	body, err := json.Marshal(map[string]any{"topic": topic, "payload": payload})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.Access+"/v1/broadcasts", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.serviceToken)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return errors.New("publish: status " + resp.Status)
	}
	return nil
}
