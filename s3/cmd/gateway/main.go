// Command gateway 启动 S3 单机 Gateway：TCP 客户端接入 + HTTP 广播受理 + /metrics，
// 三者在同一进程内（LLD §2.1「新增进程：无」）。
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

	"relayx/s3/internal/config"
	"relayx/s3/internal/gateway"
	"relayx/s3/internal/telemetry"
)

func main() {
	addr := flag.String("addr", "", "客户端 TCP 监听地址（默认 127.0.0.1:9800）")
	metricsAddr := flag.String("metrics-addr", "", "/metrics 与 /healthz 监听地址（默认 127.0.0.1:9801）")
	accessAddr := flag.String("access-addr", "", "HTTP POST /v1/broadcasts 监听地址（默认 127.0.0.1:9802）")
	secret := flag.String("token-secret", "", "登录令牌 HMAC 密钥（默认开发密钥）")
	subSecret := flag.String("subscribe-secret", "", "订阅 token kid=dev 的 HMAC 密钥")
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

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	gw := gateway.New(gateway.Options{Cfg: cfg, Log: logger})
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
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server exited", "err", err)
			os.Exit(1)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := gw.ConnManager().Shutdown(ctx); err != nil {
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
