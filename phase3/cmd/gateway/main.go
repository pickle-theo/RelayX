// Command gateway is the Phase 2 RelayX TCP gateway. It accepts client
// connections, runs the login handshake, routes messages and persists reliable
// deliveries in PostgreSQL. The store is opened and migrated before the
// transport starts, and the process shuts down gracefully on Ctrl+C.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"time"

	"relayx/phase3/internal/config"
	"relayx/phase3/internal/delivery"
	"relayx/phase3/internal/gateway"
	"relayx/phase3/internal/session"
	"relayx/phase3/internal/store"
	"relayx/phase3/internal/transport"
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
//
// Startup order: connect and ping PostgreSQL -> apply migrations -> start the
// transport -> start the retry/TTL worker. Exit order: stop the worker -> shut
// down the server -> close the connection pool.
func run(cfg config.ServerConfig, logger *slog.Logger) error {
	st, err := openStore(cfg, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			logger.Warn("close database", "error", err)
		}
	}()

	stopPprof, err := startPprof(cfg, logger)
	if err != nil {
		return err
	}
	if stopPprof != nil {
		defer stopPprof()
	}

	sessions := session.NewManager()
	handler := gateway.New(sessions, st, gateway.Config{
		MessageTTL:   cfg.MessageTTL,
		PullLimit:    cfg.PullLimit,
		PullMaxLimit: cfg.PullMaxLimit,
	}, logger)

	srv := transport.New(cfg.Addr, cfg, handler.Handle, logger)
	srv.SetOnClose(handler.OnConnClosed)

	worker := delivery.NewWorker(st, sessions, delivery.NewDeliverer(sessions), delivery.Config{
		Interval:    cfg.RetryInterval,
		MaxAttempts: cfg.RetryMaxAttempts,
		RetryBatch:  cfg.RetryBatch,
		ExpireBatch: cfg.ExpireBatch,
	}, logger)
	worker.Start()
	logger.Info("delivery worker started",
		"interval", cfg.RetryInterval,
		"max_attempts", cfg.RetryMaxAttempts)
	defer func() {
		worker.Stop()
		logger.Info("delivery worker stopped")
	}()

	fmt.Printf("gateway listening on %s\n", srv.Addr())
	logger.Info("gateway listening", "addr", srv.Addr())

	stopStats := make(chan struct{})
	defer close(stopStats)
	if cfg.StatsInterval > 0 {
		go statsLoop(cfg.StatsInterval, srv, sessions, handler, st, worker, logger, stopStats)
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

		// Stop the worker before the server so no scan touches a closing
		// connection pool.
		worker.Stop()
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

// openStore connects to PostgreSQL and applies the embedded schema migrations.
// Every failure happens before the transport starts listening, so a missing
// database never leaves a half-started gateway. The returned error names the
// database host and points at `make db-up`.
func openStore(cfg config.ServerConfig, logger *slog.Logger) (*store.Store, error) {
	openCtx, cancelOpen := context.WithTimeout(context.Background(), cfg.DBConnectTimeout)
	defer cancelOpen()

	st, err := store.Open(openCtx, store.Config{
		DSN:              cfg.DSN,
		MaxOpenConns:     cfg.DBMaxOpenConns,
		MaxIdleConns:     cfg.DBMaxIdleConns,
		ConnectTimeout:   cfg.DBConnectTimeout,
		RetryBackoffBase: cfg.RetryBackoffBase,
		RetryBackoffMax:  cfg.RetryBackoffMax,
	})
	if err != nil {
		logger.Error("database unavailable; run `make db-up` in phase2/ first", "error", err)
		return nil, err
	}

	migrateCtx, cancelMigrate := context.WithTimeout(context.Background(), cfg.DBConnectTimeout)
	defer cancelMigrate()
	if err := st.Migrate(migrateCtx); err != nil {
		_ = st.Close()
		logger.Error("database migration failed; run `make db-up` and check the container logs", "error", err)
		return nil, fmt.Errorf("migrate: %w", err)
	}

	logger.Info("database ready",
		"max_open_conns", cfg.DBMaxOpenConns,
		"max_idle_conns", cfg.DBMaxIdleConns)
	return st, nil
}

// startPprof enables runtime block/mutex profiling and serves the standard
// pprof handlers on cfg.PprofAddr. It returns nil when pprof is disabled, so
// the default configuration listens on no extra port.
func startPprof(cfg config.ServerConfig, logger *slog.Logger) (func(), error) {
	if cfg.PprofAddr == "" {
		return nil, nil
	}

	runtime.SetBlockProfileRate(cfg.BlockProfileRate)
	runtime.SetMutexProfileFraction(cfg.MutexProfileFraction)

	ln, err := net.Listen("tcp", cfg.PprofAddr)
	if err != nil {
		return nil, fmt.Errorf("pprof listen on %s: %w", cfg.PprofAddr, err)
	}

	srv := &http.Server{
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	logger.Info("pprof listening",
		"addr", ln.Addr().String(),
		"block_profile_rate", cfg.BlockProfileRate,
		"mutex_profile_fraction", cfg.MutexProfileFraction)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("pprof server stopped", "error", err)
		}
	}()

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Warn("pprof shutdown incomplete", "error", err)
		}
	}, nil
}

// statsLoop logs gateway resource usage and reliable-delivery counters on a
// fixed interval. It deliberately avoids a metrics endpoint.
func statsLoop(interval time.Duration, srv *transport.Server, sessions *session.Manager, handler *gateway.Handler, st *store.Store, worker *delivery.Worker, logger *slog.Logger, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			accepted, duplicate := handler.Stats()
			workerStats := worker.Stats()

			pending := int64(-1)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if n, err := st.CountPendingDeliveries(ctx); err != nil {
				logger.Warn("gateway stats: count pending deliveries failed", "error", err)
			} else {
				pending = n
			}
			cancel()

			gcPauseMs := 0.0
			if m.NumGC > 0 {
				gcPauseMs = round2(float64(m.PauseNs[(m.NumGC+255)%256]) / float64(time.Millisecond))
			}

			logger.Info("gateway stats",
				"goroutines", runtime.NumGoroutine(),
				"heap_alloc_mb", round2(float64(m.HeapAlloc)/(1<<20)),
				"heap_sys_mb", round2(float64(m.HeapSys)/(1<<20)),
				"gc_cycles", m.NumGC,
				"gc_pause_ms", gcPauseMs,
				"active_conns", srv.ActiveConnections(),
				"active_sessions", sessions.ActiveSessions(),
				"messages_accepted", accepted,
				"messages_duplicate", duplicate,
				"deliveries_pending", pending,
				"deliveries_retried", workerStats.Retried,
				"deliveries_expired", workerStats.Expired,
				"db_open_conns", st.DB().Stats().OpenConnections)
		}
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
