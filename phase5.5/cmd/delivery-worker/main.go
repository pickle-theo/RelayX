// Command delivery-worker runs the Phase 5 distributed delivery engine: outbox
// relay, fan-out consumer, delivery-task consumer and the lease-based push loop.
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
	"time"

	"relayx/phase5.5/internal/broker"
	"relayx/phase5.5/internal/cluster"
	"relayx/phase5.5/internal/config"
	"relayx/phase5.5/internal/engine"
	"relayx/phase5.5/internal/outbox"
	"relayx/phase5.5/internal/registry"
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
	pool := engine.New(engine.Config{
		WorkerID:     workerID,
		Shards:       cfg.Shards,
		ShardIndex:   cfg.ShardIndex,
		Batch:        cfg.ClaimBatch,
		LeaseTTL:     cfg.LeaseTTL,
		ScanInterval: cfg.ScanInterval,
		MaxAttempts:  cfg.MaxAttempts,
		BackoffBase:  cfg.RetryBackoffBase,
		BackoffMax:   cfg.RetryBackoffMax,
		ExpireBatch:  cfg.ExpireBatch,
		Logger:       logger,
	}, st, nil, nil, routes, hub, limiter)

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
	wg.Add(4)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
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
		"shards", cfg.Shards,
		"shard_index", cfg.ShardIndex,
		"lease_ttl", cfg.LeaseTTL,
		"kafka_brokers", cfg.KafkaBrokers)
	fmt.Printf("delivery worker %s started\n", workerID)

	stopStats := make(chan struct{})
	if cfg.StatsInterval > 0 {
		go statsLoop(cfg.StatsInterval, st, pool, limiter, logger, stopStats)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	var runErr error
	select {
	case runErr = <-errCh:
	case sig := <-sigCh:
		logger.Info("signal received, shutting down", "signal", sig.String())
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

func defaultWorkerID() string {
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("worker-%d", os.Getpid())
	}
	return "worker-" + hex.EncodeToString(buf[:])
}
