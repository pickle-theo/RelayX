// gateway 命令是 S2 单机宿主进程：TCP 网关、统一访问
// API、投递引擎扫描器与观测端点。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"relayx/s2/internal/config"
	"relayx/s2/internal/gateway"
	"relayx/s2/internal/store"
	"relayx/s2/internal/store/memory"
	"relayx/s2/internal/store/postgres"
	"relayx/s2/internal/telemetry"
)

func openStore(ctx context.Context, cfg config.Config) (store.Store, error) {
	switch cfg.StoreKind {
	case "memory":
		if cfg.TestHooks {
			return memory.New(memory.NewSkewClock()), nil
		}
		return memory.New(nil), nil
	case "postgres":
		return postgres.Open(ctx, cfg.DSN, cfg.MigrationPath)
	default:
		return nil, fmt.Errorf("unknown store kind %q", cfg.StoreKind)
	}
}

func main() {
	cfg := config.Default()
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "TCP gateway listen address")
	flag.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "/metrics and /healthz address")
	flag.StringVar(&cfg.AccessAddr, "access-addr", cfg.AccessAddr, "access API address")
	flag.StringVar(&cfg.StoreKind, "store", cfg.StoreKind, "memory or postgres")
	flag.StringVar(&cfg.DSN, "dsn", "", "PostgreSQL DSN")
	flag.StringVar(&cfg.MigrationPath, "migration", "migrations/0001_reliable_core.sql", "migration file")
	secret := flag.String("token-secret", string(cfg.TokenSecret), "HMAC token secret")
	interval := flag.Duration("scan-interval", cfg.ScanInterval, "background scan interval")
	flag.BoolVar(&cfg.TestHooks, "test-hooks", false, "enable /debug/clock (local tests only)")
	flag.Parse()
	cfg.TokenSecret = []byte(*secret)
	cfg.ScanInterval = *interval

	logger := log.New(os.Stderr, "s2 ", log.LstdFlags|log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := openStore(ctx, cfg)
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}
	defer st.Close()

	// 进程重启后不可信任 ONLINE 行：内存中已无会话。
	if err := st.Endpoints().ResetAllOffline(ctx); err != nil {
		logger.Fatalf("reset endpoints: %v", err)
	}

	gw := gateway.New(gateway.Options{Cfg: cfg, Store: st, Log: logger})
	gw.Start()

	httpServer := &http.Server{
		Addr:              cfg.AccessAddr,
		Handler:           gw.AccessHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsServer := telemetry.NewHTTPServer(gw.Metrics())

	errCh := make(chan error, 3)
	go func() { errCh <- gw.ServeTCP() }()
	go func() { errCh <- httpServer.ListenAndServe() }()
	go func() { errCh <- metricsServer.ListenAndServe(cfg.MetricsAddr) }()
	logger.Printf("S2 gateway %s, access %s, metrics %s, store %s",
		cfg.Addr, cfg.AccessAddr, cfg.MetricsAddr, cfg.StoreKind)

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("server error: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = gw.Shutdown(shutdownCtx)
	_ = httpServer.Shutdown(shutdownCtx)
	_ = metricsServer.Shutdown(shutdownCtx)
}
