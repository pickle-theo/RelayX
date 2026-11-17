// Command scenario 运行 S6 多节点验收场景（LLD §13.4）。
//
// 场景：multi-node-delivery / node-failover / version-guard / redis-degradation。
// 每个场景用共享 memory backend 的进程内节点 + 真实 mTLS TCP 通道完成，
// 断言以客户端实际收到的帧为准，不看服务端日志。
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/model"
	"relayx/s6/internal/route"
	"relayx/s6/internal/telemetry"
	"relayx/s6/protocol"
)

func main() {
	scenario := flag.String("scenario", "all", "场景名或 all")
	flag.Parse()

	scenarios := map[string]func() error{
		"multi-node-delivery": multiNodeDelivery,
		"node-failover":       nodeFailover,
		"version-guard":       versionGuard,
		"redis-degradation":   redisDegradation,
	}
	if *scenario == "all" {
		for _, name := range []string{"multi-node-delivery", "node-failover", "version-guard", "redis-degradation"} {
			if err := run(name, scenarios[name]); err != nil {
				os.Exit(1)
			}
		}
		return
	}
	fn, ok := scenarios[*scenario]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenario)
		os.Exit(2)
	}
	if err := run(*scenario, fn); err != nil {
		os.Exit(1)
	}
}

func run(name string, fn func() error) error {
	fmt.Printf("=== scenario %s ===\n", name)
	if err := fn(); err != nil {
		fmt.Printf("FAIL %s: %v\n", name, err)
		return err
	}
	fmt.Printf("PASS %s\n", name)
	return nil
}

func require(ok bool, format string, args ...any) error {
	if !ok {
		return fmt.Errorf(format, args...)
	}
	return nil
}

// multiNodeDelivery: 客户端 A 连 gw1，B 连 gw2；A 发消息经 FORWARD 到 B。
func multiNodeDelivery() error {
	c, err := newCluster(2)
	if err != nil {
		return err
	}
	defer c.shutdown()
	if err := c.start(); err != nil {
		return err
	}

	a, err := dial(c.cfg["gw1"].ClientAddr)
	if err != nil {
		return err
	}
	defer a.close()
	b, err := dial(c.cfg["gw2"].ClientAddr)
	if err != nil {
		return err
	}
	defer b.close()
	if err := a.login("alice", "phone"); err != nil {
		return fmt.Errorf("alice login: %w", err)
	}
	if err := b.login("bob", "phone"); err != nil {
		return fmt.Errorf("bob login: %w", err)
	}

	// alice 发 SEND 给 bob；gw1 查路由发现 bob 在 gw2 → FORWARD。
	sendBody, err := protocol.EncodeSend(protocol.SendBody{
		ClientMsgID: "m1", TargetUserID: "bob", TargetDevices: []string{"phone"}, Payload: []byte("hello"),
	})
	if err != nil {
		return err
	}
	if err := a.send(protocol.TypeSend, sendBody); err != nil {
		return err
	}
	f, err := b.readWithin(3 * time.Second)
	if err != nil {
		return fmt.Errorf("bob did not receive deliver: %w", err)
	}
	if err := require(f.Type == protocol.TypeDeliver, "expected DELIVER, got 0x%02x", f.Type); err != nil {
		return err
	}
	d, err := protocol.DecodeDeliver(f.Body)
	if err != nil {
		return err
	}
	return require(string(d.Payload) == "hello", "payload = %q", d.Payload)
}

// nodeFailover: gw2 宕机后，bob 重连到 gw1 并重新绑定，消息继续投递。
func nodeFailover() error {
	c, err := newCluster(2)
	if err != nil {
		return err
	}
	defer c.shutdown()
	if err := c.start(); err != nil {
		return err
	}
	a, err := dial(c.cfg["gw1"].ClientAddr)
	if err != nil {
		return err
	}
	defer a.close()
	if err := a.login("alice", "phone"); err != nil {
		return err
	}
	b, err := dial(c.cfg["gw2"].ClientAddr)
	if err != nil {
		return err
	}
	if err := b.login("bob", "phone"); err != nil {
		return err
	}

	// gw2 宕机：注销触发其绑定回收。
	if err := c.nodes["gw2"].Shutdown(context.Background()); err != nil {
		return err
	}
	b.close()

	// bob 重连到 gw1 重新登录绑定。
	b2, err := dial(c.cfg["gw1"].ClientAddr)
	if err != nil {
		return err
	}
	defer b2.close()
	if err := b2.login("bob", "phone"); err != nil {
		return fmt.Errorf("bob relogin: %w", err)
	}

	// 查路由确认 bob 现在在 gw1。
	router := c.nodes["gw1"].Router()
	binding, ok, err := router.Lookup(context.Background(), modelKey("default", "bob", "phone"))
	if err != nil || !ok || binding.NodeID != "gw1" {
		return fmt.Errorf("bob route after failover: %+v ok=%v err=%v", binding, ok, err)
	}

	// alice 发消息，本地投递到 bob。
	sendBody, _ := protocol.EncodeSend(protocol.SendBody{
		ClientMsgID: "m2", TargetUserID: "bob", TargetDevices: []string{"phone"}, Payload: []byte("after-failover"),
	})
	if err := a.send(protocol.TypeSend, sendBody); err != nil {
		return err
	}
	f, err := b2.readWithin(3 * time.Second)
	if err != nil {
		return fmt.Errorf("bob did not receive after failover: %w", err)
	}
	return require(f.Type == protocol.TypeDeliver, "expected DELIVER, got 0x%02x", f.Type)
}

// versionGuard: 旧连接清理事件不得删除新连接的路由（LLD §9.2 顺序 + §13.2 并发）。
func versionGuard() error {
	b := backend.NewMemoryBackend()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := route.NewRouter(route.Options{
		Backend: b, NodeID: "gw1", Log: discard, Metrics: telemetry.New(),
	})
	ctx := context.Background()
	ep := modelKey("t1", "u1", "d1")

	v1binding, err := r.Bind(ctx, ep)
	if err != nil {
		return err
	}
	// 新连接重绑，version 递增。
	v2binding, err := r.Bind(ctx, ep)
	if err != nil {
		return err
	}
	if v2binding.Version <= v1binding.Version {
		return fmt.Errorf("version not monotonic: v1=%d v2=%d", v1binding.Version, v2binding.Version)
	}
	// 旧连接的延迟清理事件到达（携带 v1）。
	deleted, err := r.Unbind(ctx, ep, v1binding.Version)
	if err != nil || deleted {
		return fmt.Errorf("stale unbind should be ignored: deleted=%v err=%v", deleted, err)
	}
	binding, ok, err := r.Lookup(ctx, ep)
	if err != nil || !ok || binding.Version != v2binding.Version {
		return fmt.Errorf("new binding was deleted: %+v ok=%v err=%v", binding, ok, err)
	}
	return nil
}

// redisDegradation: Redis 全停 → 已有会话继续收发、新登录被拒、恢复后自动正常。
func redisDegradation() error {
	c, err := newCluster(2)
	if err != nil {
		return err
	}
	defer c.shutdown()
	if err := c.start(); err != nil {
		return err
	}
	a, err := dial(c.cfg["gw1"].ClientAddr)
	if err != nil {
		return err
	}
	defer a.close()
	if err := a.login("alice", "phone"); err != nil {
		return err
	}

	c.backend.SetDown(true)

	// 已有会话仍可 PING/PONG（本地会话态）。
	if err := a.send(protocol.TypePing, nil); err != nil {
		return err
	}
	f, err := a.readWithin(2 * time.Second)
	if err != nil || f.Type != protocol.TypePong {
		return fmt.Errorf("existing session degraded ping: frame=%v err=%v", f, err)
	}

	// 新登录被拒。
	newClient, err := dial(c.cfg["gw1"].ClientAddr)
	if err != nil {
		return err
	}
	defer newClient.close()
	loginBody, _ := protocol.EncodeLogin("newuser", "phone")
	_ = newClient.send(protocol.TypeLogin, loginBody)
	resp, err := newClient.readWithin(2 * time.Second)
	if err != nil || resp.Type != protocol.TypeError {
		return fmt.Errorf("new login during degradation should be rejected: frame=%v err=%v", resp, err)
	}

	c.backend.SetDown(false)
	// 恢复后新登录成功。
	recovered, err := dial(c.cfg["gw1"].ClientAddr)
	if err != nil {
		return err
	}
	defer recovered.close()
	return recovered.login("newuser", "phone")
}

func modelKey(tenant, user, device string) model.EndpointKey {
	return model.EndpointKey{TenantID: tenant, UserID: user, DeviceID: device}
}
