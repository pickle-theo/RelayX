// Command im 是改用 S4 Go SDK 的终端 IM 验证客户端。
// 发送模式：发送 N 条消息并等待 message.settled hook；
// 接收模式：打印 delivered 并自动回 RECEIPT_ACK。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"relayx/s4/internal/identity"
	"relayx/s4/sdk"
)

func main() {
	gw := flag.String("gateway", "127.0.0.1:9800", "Gateway TCP 地址")
	access := flag.String("access", "http://127.0.0.1:9802", "查询 API base URL（空表示禁用查询补齐）")
	secret := flag.String("token-secret", "relayx-s4-dev-secret", "登录令牌 HMAC 密钥")
	tenant := flag.String("tenant", "t1", "租户")
	user := flag.String("user", "alice", "本端用户")
	device := flag.String("device", "dev-a", "本端设备")
	peer := flag.String("peer", "bob", "对端用户")
	count := flag.Int("count", 3, "发送条数")
	mode := flag.String("mode", "send", "send|receive")
	timeout := flag.Duration("timeout", 15*time.Second, "场景总超时")
	flag.Parse()

	auth := identity.New([]byte(*secret))
	token, err := auth.Issue(identity.Producer{ID: "u:" + *user, Kind: "user", TenantID: *tenant}, time.Hour)
	if err != nil {
		fmt.Fprintf(os.Stderr, "issue token: %v\n", err)
		os.Exit(1)
	}

	ready := make(chan struct{}, 1)
	settled := make(chan sdk.MessageSettledInfo, *count+8)
	var client *sdk.Client
	hooks := sdk.Hooks{
		Ready: func() {
			select {
			case ready <- struct{}{}:
			default:
			}
		},
		MessageAccepted: func(i sdk.MessageAcceptedInfo) {
			fmt.Printf("accepted client_msg=%s message=%s state=%s\n", i.ClientMsgID, i.MessageID, i.PlatformState)
		},
		MessageSettled: func(i sdk.MessageSettledInfo) {
			fmt.Printf("settled message=%s counts={total:%d receipt_acked:%d failed:%d expired:%d} from_query=%v\n",
				i.MessageID, i.Counts.Total, i.Counts.ReceiptAcked, i.Counts.Failed, i.Counts.Expired, i.FromQuery)
			select {
			case settled <- i:
			default:
			}
		},
		Delivered: func(i sdk.DeliveredInfo) {
			fmt.Printf("delivered message=%s from=%s payload=%q\n", i.MessageID, i.Sender, i.Payload)
			if err := client.SendReceipt(i.MessageID); err != nil {
				fmt.Fprintf(os.Stderr, "send receipt: %v\n", err)
			}
		},
		Closed: func(i sdk.CloseInfo) {
			fmt.Printf("closed code=%d reason=%s\n", i.ReasonCode, i.Reason)
		},
	}

	client = sdk.New(sdk.Config{
		Addr: *gw, Token: token, DeviceID: *device,
		TenantID: *tenant, ProducerID: "u:" + *user,
		QueryBaseURL: *access, Hooks: hooks,
		Logger: func(format string, args ...any) { fmt.Fprintf(os.Stderr, "sdk: "+format+"\n", args...) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()
	<-ready

	switch *mode {
	case "receive":
		fmt.Printf("listening as %s/%s (ctrl-c to stop)\n", *user, *device)
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-sig:
		case <-ctx.Done():
		}
	case "send":
		for i := 0; i < *count; i++ {
			id, err := client.Send(ctx, *peer, []byte(fmt.Sprintf("hello-%d", i)),
				sdk.WithClientMsgID(fmt.Sprintf("%s-%d-%d", *user, time.Now().UnixNano(), i)))
			if err != nil {
				fmt.Fprintf(os.Stderr, "send: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("sent %s\n", id)
		}
		for i := 0; i < *count; i++ {
			select {
			case <-settled:
			case <-ctx.Done():
				fmt.Fprintf(os.Stderr, "timeout waiting for settled (%d/%d)\n", i, *count)
				os.Exit(1)
			}
		}
		fmt.Printf("all %d messages settled\n", *count)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		os.Exit(2)
	}
}
