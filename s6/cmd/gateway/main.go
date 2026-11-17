// Command gateway 启动 S6 分布式 Gateway 节点：
// 客户端 TCP 接入 + 节点间 mTLS 转发服务端 + Redis 路由 + /metrics。
//
// 语义层（S1–S4）在每个节点内原样运行；本进程只负责「连接在哪」（LLD §1.1）。
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"relayx/s6/internal/config"
	"relayx/s6/internal/forward"
	"relayx/s6/internal/gateway"
	"relayx/s6/internal/node"
	"relayx/s6/internal/telemetry"
)

func main() {
	cfg := parseFlags()
	if msg := cfg.Validate(); msg != "" {
		fmt.Fprintln(os.Stderr, "gateway: config invalid:", msg)
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if cfg.NodeID == "" {
		cfg.NodeID = node.NewID()
	}

	var tlsCfg *tls.Config
	if cfg.GenerateDevCerts {
		// CA 目录与节点目录分离：同一台机器上启动的多个节点必须复用同一 CA，
		// 否则叶证书互相不受信（文档中的两节点本地 mTLS 验收会直接失败）。
		baseDir := filepath.Join(os.TempDir(), "relayx-s6-dev-certs")
		dir := filepath.Join(baseDir, cfg.NodeID)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			logger.Error("create dev cert dir", "err", err)
			os.Exit(1)
		}
		certFile := filepath.Join(dir, "cert.pem")
		keyFile := filepath.Join(dir, "key.pem")
		caFile := filepath.Join(baseDir, "ca.pem")
		if err := forward.GenerateDevCertificates(certFile, keyFile, caFile, cfg.NodeID, "localhost"); err != nil {
			logger.Error("generate dev certificates", "err", err)
			os.Exit(1)
		}
		var err error
		tlsCfg, err = forward.LoadTLSConfig(certFile, keyFile, caFile, cfg.ServerName)
		if err != nil {
			logger.Error("load dev certificates", "err", err)
			os.Exit(1)
		}
		logger.Warn("using generated dev certificates (NOT for production)", "dir", dir, "ca", caFile)
	} else if cfg.CertFile != "" && cfg.KeyFile != "" && cfg.CAFile != "" {
		var err error
		tlsCfg, err = forward.LoadTLSConfig(cfg.CertFile, cfg.KeyFile, cfg.CAFile, cfg.ServerName)
		if err != nil {
			logger.Error("load mTLS certificates", "err", err)
			os.Exit(1)
		}
	}

	gw, err := gateway.New(gateway.Options{Cfg: cfg, Log: logger, TLSConfig: tlsCfg})
	if err != nil {
		logger.Error("create gateway", "err", err)
		os.Exit(1)
	}
	if err := gw.Start(); err != nil {
		logger.Error("start gateway", "err", err)
		os.Exit(1)
	}

	metricsSrv := telemetry.NewHTTPServer(gw.Metrics())
	metricsLn, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		logger.Error("listen metrics", "err", err)
		os.Exit(1)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- metricsSrv.Serve(metricsLn) }()

	logger.Info("gateway started",
		"node", cfg.NodeID,
		"client", cfg.ClientAddr,
		"internal", cfg.InternalAddr,
		"backend", cfg.Backend)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", "signal", sig.String())
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server exited", "err", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := gw.Shutdown(ctx); err != nil {
		logger.Error("gateway shutdown", "err", err)
	}
	if err := metricsSrv.Shutdown(ctx); err != nil {
		logger.Error("metrics shutdown", "err", err)
	}
	logger.Info("gateway stopped")
}

func parseFlags() config.Config {
	var (
		nodeID       = flag.String("node-id", "", "节点 ID（默认自动生成）")
		clientAddr   = flag.String("addr", "", "客户端 TCP 监听地址（默认 127.0.0.1:9800）")
		internalAddr = flag.String("internal-addr", "", "节点间 mTLS 监听地址（默认 127.0.0.1:9803）")
		metricsAddr  = flag.String("metrics-addr", "", "/metrics 与 /healthz 监听地址（默认 127.0.0.1:9801）")
		backend      = flag.String("backend", "", "路由后端：memory|redis（默认 memory）")
		redisAddr    = flag.String("redis-addr", "", "Redis 地址（默认 127.0.0.1:6379）")
		certFile     = flag.String("cert", "", "mTLS 客户端/服务端证书 PEM 路径")
		keyFile      = flag.String("key", "", "mTLS 私钥 PEM 路径")
		caFile       = flag.String("ca", "", "内部 CA PEM 路径")
		serverName   = flag.String("server-name", "", "证书校验的 ServerName（默认目标地址 host）")
		devCerts     = flag.Bool("dev-certs", false, "生成临时自签证书（仅本地开发/验收，禁止生产）")
	)
	flag.Parse()

	cfg := config.Default()
	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if *clientAddr != "" {
		cfg.ClientAddr = *clientAddr
	}
	if *internalAddr != "" {
		cfg.InternalAddr = *internalAddr
	}
	if *metricsAddr != "" {
		cfg.MetricsAddr = *metricsAddr
	}
	if *backend != "" {
		cfg.Backend = *backend
	}
	if *redisAddr != "" {
		cfg.RedisAddr = *redisAddr
	}
	cfg.CertFile, cfg.KeyFile, cfg.CAFile = *certFile, *keyFile, *caFile
	cfg.ServerName, cfg.GenerateDevCerts = *serverName, *devCerts
	return cfg
}
