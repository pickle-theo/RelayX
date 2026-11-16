// Command gateway 启动 S1 单机 Gateway。
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"relayx/s1/internal/config"
	"relayx/s1/internal/gateway"
	"relayx/s1/internal/telemetry"
)

func main() {
	addr := flag.String("addr", "", "客户端 TCP 监听地址（默认 127.0.0.1:9800）")
	metricsAddr := flag.String("metrics-addr", "", "/metrics 与 /healthz 监听地址（默认 127.0.0.1:9801）")
	secret := flag.String("token-secret", "", "HMAC 令牌密钥（默认使用开发密钥）")
	flag.Parse()

	cfg := config.Default()
	if *addr != "" {
		cfg.Addr = *addr
	}
	if *metricsAddr != "" {
		cfg.MetricsAddr = *metricsAddr
	}
	if *secret != "" {
		cfg.TokenSecret = []byte(*secret)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	gw := gateway.New(gateway.Options{Cfg: cfg, Log: logger})
	httpSrv := telemetry.NewHTTPServer(gw.Metrics())

	errCh := make(chan error, 2)
	go func() { errCh <- gw.ConnManager().ListenAndServe() }()
	go func() { errCh <- httpSrv.ListenAndServe(cfg.MetricsAddr) }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", "signal", sig.String())
	case err := <-errCh:
		if err != nil {
			logger.Error("server exited", "err", err)
			os.Exit(1)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := gw.ConnManager().Shutdown(ctx); err != nil {
		logger.Error("gateway shutdown", "err", err)
	}
	if err := httpSrv.Shutdown(ctx); err != nil {
		logger.Error("http shutdown", "err", err)
	}
	logger.Info("gateway stopped")
}
