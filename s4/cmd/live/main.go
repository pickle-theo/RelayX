// Command live 是改用 S4 Go SDK 的弹幕客户端：SDK 负责连接/订阅/收帧，
// 通过 access 端口的 POST /v1/broadcasts 触发广播，验证 SDK 的
// broadcast.received hook 与 S3 广播语义（订阅 token 只验签）。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"relayx/s4/internal/broadcast"
	"relayx/s4/internal/identity"
	"relayx/s4/sdk"
)

func main() {
	gw := flag.String("gateway", "127.0.0.1:9800", "Gateway TCP 地址")
	access := flag.String("access", "http://127.0.0.1:9802", "HTTP base URL")
	secret := flag.String("token-secret", "relayx-s4-dev-secret", "登录令牌密钥")
	subSecret := flag.String("subscribe-secret", "relayx-s4-dev-subscribe-secret", "订阅 token kid=dev 密钥")
	tenant := flag.String("tenant", "t1", "租户")
	user := flag.String("user", "viewer", "订阅用户")
	device := flag.String("device", "dev-live", "设备")
	topic := flag.String("topic", "room-1", "topic")
	count := flag.Int("count", 3, "广播条数")
	timeout := flag.Duration("timeout", 15*time.Second, "总超时")
	flag.Parse()

	auth := identity.New([]byte(*secret))
	loginToken, err := auth.Issue(identity.Producer{ID: "u:" + *user, Kind: "user", TenantID: *tenant}, time.Hour)
	if err != nil {
		fmt.Fprintf(os.Stderr, "issue login token: %v\n", err)
		os.Exit(1)
	}
	svcToken, err := auth.Issue(identity.Producer{ID: "svc:publisher", Kind: "service", TenantID: *tenant}, time.Hour)
	if err != nil {
		fmt.Fprintf(os.Stderr, "issue service token: %v\n", err)
		os.Exit(1)
	}
	subToken, err := broadcast.NewTokenVerifier(nil).Issue("dev", []byte(*subSecret), broadcast.SubscriptionClaims{
		TenantID: *tenant, UserID: *user, Topic: *topic,
	}, time.Hour)
	if err != nil {
		fmt.Fprintf(os.Stderr, "issue subscribe token: %v\n", err)
		os.Exit(1)
	}

	received := make(chan sdk.BroadcastInfo, *count+8)
	ready := make(chan struct{}, 1)
	c := sdk.New(sdk.Config{
		Addr: *gw, Token: loginToken, DeviceID: *device,
		TenantID: *tenant, ProducerID: "u:" + *user,
		Hooks: sdk.Hooks{
			Ready: func() {
				select {
				case ready <- struct{}{}:
				default:
				}
			},
			BroadcastReceived: func(i sdk.BroadcastInfo) {
				fmt.Printf("broadcast topic=%s seq=%d payload=%q\n", i.Topic, i.Seq, i.Payload)
				received <- i
			},
			Closed: func(i sdk.CloseInfo) { fmt.Printf("closed reason=%s\n", i.Reason) },
		},
		Logger: func(format string, args ...any) { fmt.Fprintf(os.Stderr, "sdk: "+format+"\n", args...) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()
	<-ready
	if err := c.Subscribe(ctx, *topic, subToken); err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < *count; i++ {
		payload, _ := json.Marshal(map[string]any{
			"topic": *topic, "payload": []byte(fmt.Sprintf("danmaku-%d", i)),
		})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, *access+"/v1/broadcasts", bytes.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+svcToken)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "publish: %v\n", err)
			os.Exit(1)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			fmt.Fprintf(os.Stderr, "publish status %d\n", resp.StatusCode)
			os.Exit(1)
		}
	}
	for i := 0; i < *count; i++ {
		select {
		case <-received:
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "timeout waiting for broadcast (%d/%d)\n", i, *count)
			os.Exit(1)
		}
	}
	fmt.Printf("received all %d broadcasts on %s\n", *count, *topic)
}
