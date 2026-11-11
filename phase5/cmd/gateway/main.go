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
	"strings"
	"sync"
	"time"

	"relayx/phase5/internal/broker"
	"relayx/phase5/internal/cluster"
	"relayx/phase5/internal/config"
	"relayx/phase5/internal/gateway"
	"relayx/phase5/internal/outbox"
	"relayx/phase5/internal/protocol"
	"relayx/phase5/internal/registry"
	"relayx/phase5/internal/session"
	"relayx/phase5/internal/store"
	"relayx/phase5/internal/transport"
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
// Startup order: connect and ping PostgreSQL -> apply migrations -> pprof ->
// cluster (registry, hub, keeper) -> handler -> transport -> retry/TTL worker.
// Exit order: stop the worker -> shut down the server -> stop the keeper and
// close the hub/registry -> close the connection pool.
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

	// The cluster is created before the handler so its registry and hub can be
	// injected. The cleanup runs after an explicit server shutdown and stops the
	// keeper first, which deregisters this node.
	cl, cleanupCluster, err := openCluster(cfg, logger)
	if err != nil {
		return err
	}
	defer cleanupCluster()

	sessions := session.NewManager()
	handlerCfg := gateway.Config{
		MessageTTL:   cfg.MessageTTL,
		PullLimit:    cfg.PullLimit,
		PullMaxLimit: cfg.PullMaxLimit,
		TenantID:     cfg.TenantID,
	}
	if cl != nil {
		handlerCfg.NodeID = cl.nodeID
		handlerCfg.Registry = cl.reg
		handlerCfg.Forwarder = cl.hub
	}
	handler := gateway.New(sessions, st, handlerCfg, logger)
	if cl != nil {
		cl.hub.SetHandler(nodeAdapter{h: handler})
	}

	srv := transport.New(cfg.Addr, cfg, handler.Handle, logger)
	srv.SetOnClose(handler.OnConnClosed)

	// Phase 5 no longer pushes deliveries from the connection goroutine and no
	// longer retries them in-process: messages are accepted with an outbox row
	// and the delivery worker drives every push.
	stopRelay := func() {}
	if cfg.KafkaBrokers != "" {
		producer, err := broker.NewProducer(splitBrokers(cfg.KafkaBrokers))
		if err != nil {
			return err
		}
		relay := outbox.NewRelay(st, producer, cfg.OutboxBatch, cfg.OutboxInterval,
			cfg.RetryBackoffBase, cfg.RetryBackoffMax, logger)
		relayCtx, relayCancel := context.WithCancel(context.Background())
		var relayWG sync.WaitGroup
		relayWG.Add(1)
		go func() {
			defer relayWG.Done()
			relay.Run(relayCtx)
		}()
		stopRelay = func() {
			relayCancel()
			relayWG.Wait()
			if err := producer.Close(); err != nil {
				logger.Warn("close kafka producer", "error", err)
			}
		}
		logger.Info("outbox relay started", "brokers", cfg.KafkaBrokers)
	}
	defer stopRelay()

	fmt.Printf("gateway listening on %s\n", srv.Addr())
	logger.Info("gateway listening", "addr", srv.Addr())

	stopStats := make(chan struct{})
	defer close(stopStats)
	if cfg.StatsInterval > 0 {
		go statsLoop(cfg.StatsInterval, srv, sessions, handler, st, cl, logger, stopStats)
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

		// Stop the relay before the server so no publish touches a closing
		// connection pool.
		stopRelay()
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

// clusterRuntime groups the cluster resources owned by run. A nil value means
// the process runs in the Phase 3 single-node mode.
type clusterRuntime struct {
	reg    registry.Registry
	hub    *cluster.Hub
	keeper *registry.Keeper
	nodeID string
}

// nodeAdapter bridges cluster.Handler to gateway.Handler. The gateway does not
// implement cluster.Handler itself so the two packages stay decoupled.
type nodeAdapter struct {
	h *gateway.Handler
}

func (a nodeAdapter) HandleNodeDeliver(_ *cluster.Peer, msgID uint64, body *protocol.NodeDeliverBody) (uint8, string) {
	return a.h.HandleNodeDeliver(msgID, body)
}

func (a nodeAdapter) HandleNodeKick(_ *cluster.Peer, body *protocol.NodeKickBody) {
	a.h.HandleNodeKick(body)
}

// openCluster wires Redis, the node hub and the registry keeper when clustering
// is enabled. An empty redis-addr keeps the Phase 3 single-node path with a
// nil runtime. Any failure is returned before the transport starts, so a broken
// cluster configuration never leaves a half-started gateway.
func openCluster(cfg config.ServerConfig, logger *slog.Logger) (*clusterRuntime, func(), error) {
	if cfg.RedisAddr == "" {
		return nil, func() {}, nil
	}

	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = defaultNodeID()
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DBConnectTimeout)
	reg, err := registry.NewRedis(ctx, registry.RedisConfig{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		NodeTTL:  cfg.NodeTTL,
	})
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: Redis unavailable at %s: %w", cfg.RedisAddr, err)
	}

	advertise := cfg.NodeAdvertiseAddr
	if advertise == "" {
		advertise = cfg.NodeAddr
	}
	hub, err := cluster.NewHub(cluster.Config{
		NodeID:        nodeID,
		ListenAddr:    cfg.NodeAddr,
		AdvertiseAddr: advertise,
		MaxBody:       cfg.MaxBody,
		DialTimeout:   cfg.NodeDialTimeout,
		Logger:        logger,
	})
	if err != nil {
		_ = reg.Close()
		return nil, nil, err
	}
	hub.Start()

	keeper := registry.NewKeeper(reg, registry.KeeperConfig{
		NodeID:    nodeID,
		Addr:      advertise,
		Heartbeat: cfg.NodeHeartbeatInterval,
		Cleanup:   cfg.NodeCleanupInterval,
		Logger:    logger,
	})
	if err := keeper.Start(context.Background()); err != nil {
		_ = hub.Close()
		_ = reg.Close()
		return nil, nil, err
	}
	logger.Info("cluster enabled",
		"node_id", nodeID,
		"node_addr", hub.Addr(),
		"advertise_addr", advertise,
		"redis_addr", cfg.RedisAddr)

	cleanup := func() {
		// Deregistering before closing the hub lets peers notice this node is
		// gone instead of dialing a dead address.
		if err := keeper.Stop(); err != nil {
			logger.Warn("stop registry keeper", "error", err)
		}
		if err := hub.Close(); err != nil {
			logger.Warn("close cluster hub", "error", err)
		}
		if err := reg.Close(); err != nil {
			logger.Warn("close registry", "error", err)
		}
	}
	return &clusterRuntime{reg: reg, hub: hub, keeper: keeper, nodeID: nodeID}, cleanup, nil
}

// defaultNodeID derives a stable-per-process node ID from the hostname and PID.
func defaultNodeID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "node"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
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
func statsLoop(interval time.Duration, srv *transport.Server, sessions *session.Manager, handler *gateway.Handler, st *store.Store, cl *clusterRuntime, logger *slog.Logger, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	nodeID := ""
	if cl != nil {
		nodeID = cl.nodeID
	}

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			accepted, duplicate := handler.Stats()

			pending := int64(-1)
			outboxPending := int64(-1)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if n, err := st.CountPendingDeliveries(ctx); err != nil {
				logger.Warn("gateway stats: count pending deliveries failed", "error", err)
			} else {
				pending = n
			}
			if n, err := st.CountPendingOutbox(ctx); err != nil {
				logger.Warn("gateway stats: count pending outbox failed", "error", err)
			} else {
				outboxPending = n
			}
			cancel()

			gcPauseMs := 0.0
			if m.NumGC > 0 {
				gcPauseMs = round2(float64(m.PauseNs[(m.NumGC+255)%256]) / float64(time.Millisecond))
			}

			clusterPeers := 0
			if cl != nil {
				clusterPeers = cl.hub.PeerCount()
			}

			logger.Info("gateway stats",
				"node_id", nodeID,
				"cluster_peers", clusterPeers,
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
				"outbox_pending", outboxPending,
				"db_open_conns", st.DB().Stats().OpenConnections)
		}
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// splitBrokers turns the comma-separated -kafka-brokers flag into addresses.
func splitBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
