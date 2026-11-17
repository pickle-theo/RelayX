// Command gateway 启动 S4 单机 Gateway：TCP 客户端接入 + 端侧查询/受理 HTTP +
// /metrics，三者在同一进程内。事件投影在本进程内轮询 facts（LLD §5.4）。
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"relayx/s4/internal/config"
	"relayx/s4/internal/gateway"
	"relayx/s4/internal/telemetry"
)

func main() {
	addr := flag.String("addr", "", "客户端 TCP 监听地址（默认 127.0.0.1:9800）")
	metricsAddr := flag.String("metrics-addr", "", "/metrics 与 /healthz 监听地址（默认 127.0.0.1:9801）")
	accessAddr := flag.String("access-addr", "", "HTTP 查询/受理监听地址（默认 127.0.0.1:9802）")
	secret := flag.String("token-secret", "", "登录令牌 HMAC 密钥（默认开发密钥）")
	subSecret := flag.String("subscribe-secret", "", "订阅 token kid=dev 的 HMAC 密钥")
	eventPoll := flag.Duration("event-poll", 0, "投影游标轮询间隔（默认 100ms）")
	eventBatch := flag.Int("event-batch", 0, "单轮投影事实条数上限（默认 500）")
	cursor := flag.String("cursor", "", "投影游标存储：memory|file（默认 memory）")
	cursorPath := flag.String("cursor-path", "", "file 游标路径")
	testHooks := flag.Bool("test-hooks", false, "打开 /debug/* 验证钩子（默认关闭）")
	legacy := flag.Bool("legacy-server", false, "只宣告旧服务端能力（兼容矩阵验证）")
	flag.Parse()

	cfg := config.Default()
	if *addr != "" {
		cfg.Addr = *addr
	}
	if *metricsAddr != "" {
		cfg.MetricsAddr = *metricsAddr
	}
	if *accessAddr != "" {
		cfg.AccessAddr = *accessAddr
	}
	if *secret != "" {
		cfg.TokenSecret = []byte(*secret)
	}
	if *subSecret != "" {
		cfg.BroadcastTokenKeys["dev"] = []byte(*subSecret)
	}
	if *eventPoll > 0 {
		cfg.EventPollInterval = *eventPoll
	}
	if *eventBatch > 0 {
		cfg.EventBatch = *eventBatch
	}
	if *cursor != "" {
		cfg.CursorKind = *cursor
	}
	if *cursorPath != "" {
		cfg.CursorPath = *cursorPath
	}
	cfg.TestHooks = *testHooks
	cfg.LegacyServer = *legacy // 运行期也可由 /debug/legacy-mode 切换

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	gw := gateway.New(gateway.Options{Cfg: cfg, Log: logger})
	gw.Start()

	metricsSrv := telemetry.NewHTTPServer(gw.Metrics())
	accessSrv := &http.Server{Handler: gw.AccessHandler(), ReadHeaderTimeout: 5 * time.Second}

	accessLn, err := net.Listen("tcp", cfg.AccessAddr)
	if err != nil {
		logger.Error("listen access", "addr", cfg.AccessAddr, "err", err)
		os.Exit(1)
	}

	errCh := make(chan error, 3)
	go func() { errCh <- gw.ConnManager().ListenAndServe() }()
	go func() { errCh <- metricsSrv.ListenAndServe(cfg.MetricsAddr) }()
	go func() { errCh <- accessSrv.Serve(accessLn) }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", "signal", sig.String())
		// 优雅停机先补 SERVER_DRAINING(1005)，让端侧按契约进入重连而不是静默 EOF。
		if n := gw.Drain(); n > 0 {
			logger.Info("drained connections", "count", n)
		}
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server exited", "err", err)
			os.Exit(1)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := gw.Shutdown(ctx); err != nil {
		logger.Error("gateway shutdown", "err", err)
	}
	if err := accessSrv.Shutdown(ctx); err != nil {
		logger.Error("access shutdown", "err", err)
	}
	if err := metricsSrv.Shutdown(ctx); err != nil {
		logger.Error("metrics shutdown", "err", err)
	}
	logger.Info("gateway stopped")
}
