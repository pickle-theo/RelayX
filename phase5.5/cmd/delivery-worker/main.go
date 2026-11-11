// Command delivery-worker runs the Phase 5.5 distributed delivery engine:
// outbox relay, fan-out consumer, delivery-task consumer and the lease-based
// push loop with online virtual shard rebalancing.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"relayx/phase5.5/internal/broker"
	"relayx/phase5.5/internal/cluster"
	"relayx/phase5.5/internal/config"
	"relayx/phase5.5/internal/engine"
	"relayx/phase5.5/internal/outbox"
	"relayx/phase5.5/internal/registry"
	"relayx/phase5.5/internal/shard"
	"relayx/phase5.5/internal/store"
	"relayx/phase5.5/internal/tenant"
)

func main() {
	cfg, err := config.ParseWorker(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "delivery-worker: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg, slog.Default()); err != nil {
		fmt.Fprintf(os.Stderr, "delivery-worker: %v\n", err)
		os.Exit(1)
	}
}

func run(cfg config.WorkerConfig, logger *slog.Logger) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	openCtx, cancelOpen := context.WithTimeout(ctx, cfg.DBConnectTimeout)
	st, err := store.Open(openCtx, store.Config{
		DSN:              cfg.DSN,
		MaxOpenConns:     cfg.DBMaxOpenConns,
		MaxIdleConns:     cfg.DBMaxIdleConns,
		ConnectTimeout:   cfg.DBConnectTimeout,
		RetryBackoffBase: cfg.RetryBackoffBase,
		RetryBackoffMax:  cfg.RetryBackoffMax,
	})
	if err == nil {
		err = st.Migrate(openCtx)
	}
	cancelOpen()
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			logger.Warn("close database", "error", err)
		}
	}()

	workerID := cfg.NodeID
	if workerID == "" {
		workerID = defaultWorkerID()
	}

	if err := broker.EnsureTopics(ctx, cfg.KafkaBrokers, broker.DefaultPartitions,
		cfg.FanoutTopic, cfg.DeliverTopic, cfg.DLQTopic); err != nil {
		return err
	}

	producer, err := broker.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Warn("close kafka producer", "error", err)
		}
	}()

	// Cluster routing is optional: without Redis the engine can only reach
	// sessions running in this very process, which never happens in the worker.
	var routes registry.Registry
	var hub *cluster.Hub
	if cfg.RedisAddr != "" {
		reg, err := registry.NewRedis(ctx, registry.RedisConfig{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		})
		if err != nil {
			return fmt.Errorf("cluster: Redis unavailable at %s: %w", cfg.RedisAddr, err)
		}
		defer func() {
			if err := reg.Close(); err != nil {
				logger.Warn("close registry", "error", err)
			}
		}()
		routes = reg

		dialHub, err := cluster.NewHub(cluster.Config{
			NodeID:      workerID,
			DialOnly:    true,
			MaxBody:     cfg.MaxBody,
			DialTimeout: cfg.NodeDialTimeout,
			Logger:      logger,
		})
		if err != nil {
			return err
		}
		defer func() {
			if err := dialHub.Close(); err != nil {
				logger.Warn("close cluster hub", "error", err)
			}
		}()
		hub = dialHub
	}

	limiter := tenant.NewLimiter(st, cfg.TenantCacheTTL, cfg.DefaultInflight)

	// The virtual shard assignment is maintained online. Every worker
	// registers itself with a liveness lease and the rebalancer splits the
	// shard space across the live workers, so growing or shrinking the fleet
	// needs no restart and moves no rows.
	shardProvider := store.NewShardRangeProvider(st, workerID, cfg.ScanInterval)
	rebalancer := shard.NewRebalancer(st, cfg.ShardSpace, cfg.ShardRebalanceInterval,
		cfg.ShardLeaseTTL, logger)

	pool := engine.New(engine.Config{
		WorkerID:     workerID,
		Batch:        cfg.ClaimBatch,
		LeaseTTL:     cfg.LeaseTTL,
		ScanInterval: cfg.ScanInterval,
		MaxAttempts:  cfg.MaxAttempts,
		BackoffBase:  cfg.RetryBackoffBase,
		BackoffMax:   cfg.RetryBackoffMax,
		ExpireBatch:  cfg.ExpireBatch,
		Logger:       logger,
	}, st, nil, nil, routes, hub, limiter, shardProvider)

	fanout := engine.NewFanout(st, producer, cfg.FanoutBatch, logger)
	relay := outbox.NewRelay(st, producer, cfg.OutboxBatch, cfg.OutboxInterval,
		cfg.RetryBackoffBase, cfg.RetryBackoffMax, logger)

	consumerOpts := broker.ConsumerOptions{
		DeadLetterTopic:     cfg.DLQTopic,
		DeadLetterPublisher: producer,
		DeadLetterRecorder:  engine.NewDeadLetterRecorder(st),
		MaxHandlerAttempts:  cfg.ConsumerMaxAttempts,
		RetryBackoff:        cfg.ConsumerRetryBackoff,
		FetchBackoff:        cfg.ConsumerFetchBackoff,
		Logger:              logger,
	}

	fanoutConsumer, err := broker.NewConsumerWithOptions(cfg.KafkaBrokers, cfg.FanoutGroup, cfg.FanoutTopic, consumerOpts)
	if err != nil {
		return err
	}
	defer func() {
		if err := fanoutConsumer.Close(); err != nil {
			logger.Warn("close fan-out consumer", "error", err)
		}
	}()

	deliverConsumer, err := broker.NewConsumerWithOptions(cfg.KafkaBrokers, cfg.DeliverGroup, cfg.DeliverTopic, consumerOpts)
	if err != nil {
		return err
	}
	defer func() {
		if err := deliverConsumer.Close(); err != nil {
			logger.Warn("close deliver consumer", "error", err)
		}
	}()

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		runShardCoordinator(ctx, st, workerID, cfg, rebalancer, logger)
	}()
	go func() {
		defer wg.Done()
		if err := pool.Run(ctx); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		if err := fanoutConsumer.Run(ctx, func(ctx context.Context, msg broker.Message) error {
			return fanout.Handle(ctx, msg.Value)
		}); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		// The delivery event is only a wake-up hint: the engine always scans
		// PostgreSQL, so a duplicate or lost hint changes latency, not results.
		if err := deliverConsumer.Run(ctx, func(ctx context.Context, msg broker.Message) error {
			if _, err := broker.DecodeDeliver(msg.Value); err != nil {
				// A malformed hint can never be used: dead-letter it instead of
				// dropping it silently.
				logger.Error("malformed deliver event", "error", err)
				return broker.Permanent(err)
			}
			pool.Notify()
			return nil
		}); err != nil {
			errCh <- err
		}
	}()

	logger.Info("delivery worker started",
		"worker_id", workerID,
		"shard_space", cfg.ShardSpace,
		"lease_ttl", cfg.LeaseTTL,
		"dlq_topic", cfg.DLQTopic,
		"kafka_brokers", cfg.KafkaBrokers)
	fmt.Printf("delivery worker %s started\n", workerID)

	stopStats := make(chan struct{})
	if cfg.StatsInterval > 0 {
		go statsLoop(cfg.StatsInterval, st, pool, limiter, logger, stopStats)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	var runErr error
	select {
	case runErr = <-errCh:
	case sig := <-sigCh:
		logger.Info("signal received, draining", "signal", sig.String())
		// Stop claiming new work and release every lease this worker still
		// holds, so another worker can take the rows over immediately instead
		// of waiting for the lease to expire.
		drainCtx, cancelDrain := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		if err := pool.Shutdown(drainCtx); err != nil {
			logger.Warn("drain failed", "error", err)
		}
		cancelDrain()
	}
	close(stopStats)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(cfg.ShutdownTimeout):
		logger.Warn("shutdown timed out")
	}
	return runErr
}

func statsLoop(interval time.Duration, st *store.Store, pool *engine.Engine, limiter *tenant.Limiter, logger *slog.Logger, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			stats := pool.Snapshot()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			outboxPending, err := st.CountPendingOutbox(ctx)
			if err != nil {
				logger.Warn("stats: count pending outbox failed", "error", err)
			}
			leased, err := st.CountLeasedDeliveries(ctx, time.Now())
			if err != nil {
				logger.Warn("stats: count leased deliveries failed", "error", err)
			}
			cancel()

			logger.Info("delivery worker stats",
				"outbox_pending", outboxPending,
				"leases_held", leased,
				"tenant_inflight", limiter.Snapshot(),
				"claimed", stats.Claimed,
				"sent", stats.Sent,
				"retried", stats.Retried,
				"failed", stats.Failed,
				"throttled", stats.Throttled,
				"no_route", stats.NoRoute,
				"expired", stats.Expired)
		}
	}
}

// runShardCoordinator keeps this worker registered and the shard assignment in
// sync.
//
// The registration happens before the first rebalance, so a worker owns an
// interval from the start instead of competing with the other workers for the
// whole shard space. A worker whose liveness lease expires is dropped from the
// assignment and its interval is taken over by the remaining workers.
func runShardCoordinator(ctx context.Context, st *store.Store, workerID string, cfg config.WorkerConfig, rb *shard.Rebalancer, logger *slog.Logger) {
	interval := cfg.ShardHeartbeatInterval
	if cfg.ShardRebalanceInterval < interval {
		interval = cfg.ShardRebalanceInterval
	}
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := st.HeartbeatShardWorker(ctx, workerID, cfg.ShardLeaseTTL); err != nil {
			logger.Warn("shard heartbeat failed", "worker_id", workerID, "error", err)
		} else if err := rb.Rebalance(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("shard rebalance failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func defaultWorkerID() string {
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("worker-%d", os.Getpid())
	}
	return "worker-" + hex.EncodeToString(buf[:])
}
