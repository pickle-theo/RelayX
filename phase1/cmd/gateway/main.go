// Command gateway is the Phase 1 RelayX TCP gateway. It accepts client
// connections, runs the login handshake, routes messages, echoes legacy DATA
// frames and shuts down gracefully on Ctrl+C.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"runtime"
	"time"

	"relayx/phase1/internal/config"
	"relayx/phase1/internal/gateway"
	"relayx/phase1/internal/session"
	"relayx/phase1/internal/transport"
)

func main() {
	cfg, err := config.ParseServer(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(2)
	}

	if err := run(cfg, slog.Default()); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

// run owns the server for the lifetime of the process. A listener error is
// reported by Serve, so a bind failure arrives on serveErr rather than from
// New.
func run(cfg config.ServerConfig, logger *slog.Logger) error {
	sessions := session.NewManager()
	handler := gateway.New(sessions, logger)

	srv := transport.New(cfg.Addr, cfg, handler.Handle, logger)
	srv.SetOnClose(handler.OnConnClosed)
	fmt.Printf("gateway listening on %s\n", srv.Addr())
	logger.Info("gateway listening", "addr", srv.Addr())

	stopStats := make(chan struct{})
	defer close(stopStats)
	if cfg.StatsInterval > 0 {
		go statsLoop(cfg.StatsInterval, srv, sessions, logger, stopStats)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("serve on %s: %w", srv.Addr(), err)
		}
		return nil
	case sig := <-sigCh:
		logger.Info("signal received, shutting down",
			"signal", sig.String(), "active_connections", srv.ActiveConnections())

		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Warn("graceful shutdown incomplete", "error", err)
		}

		select {
		case err := <-serveErr:
			if err != nil {
				logger.Warn("serve returned error after shutdown", "error", err)
			}
		case <-time.After(cfg.ShutdownTimeout):
			logger.Warn("serve did not return after shutdown")
		}
		return nil
	}
}

// statsLoop logs gateway resource usage on a fixed interval. It is a Phase 0
// observability aid and deliberately avoids a metrics endpoint.
func statsLoop(interval time.Duration, srv *transport.Server, sessions *session.Manager, logger *slog.Logger, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			logger.Info("gateway stats",
				"goroutines", runtime.NumGoroutine(),
				"heap_alloc_mb", round2(float64(m.HeapAlloc)/(1<<20)),
				"heap_sys_mb", round2(float64(m.HeapSys)/(1<<20)),
				"active_conns", srv.ActiveConnections(),
				"active_sessions", sessions.ActiveSessions())
		}
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
