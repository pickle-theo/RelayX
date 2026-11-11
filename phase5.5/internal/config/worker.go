package config

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

// WorkerConfig holds the delivery-worker settings: PostgreSQL, Kafka, the
// outbox relay, fan-out and the lease-based delivery engine.
type WorkerConfig struct {
	DSN              string
	DBMaxOpenConns   int
	DBMaxIdleConns   int
	DBConnectTimeout time.Duration

	KafkaBrokers []string
	FanoutTopic  string
	DeliverTopic string
	FanoutGroup  string
	DeliverGroup string
	FanoutBatch  int
	ClaimBatch   int

	LeaseTTL        time.Duration
	Shards          int
	ShardIndex      int
	DefaultInflight int
	TenantCacheTTL  time.Duration

	ScanInterval     time.Duration
	MaxAttempts      int
	RetryBackoffBase time.Duration
	RetryBackoffMax  time.Duration
	ExpireBatch      int

	OutboxBatch    int
	OutboxInterval time.Duration
	StatsInterval  time.Duration

	RedisAddr       string
	RedisPassword   string
	RedisDB         int
	NodeID          string
	NodeDialTimeout time.Duration
	MaxBody         int
	ShutdownTimeout time.Duration
}

// Validate checks the worker settings.
func (c WorkerConfig) Validate() error {
	if strings.TrimSpace(c.DSN) == "" {
		return fmt.Errorf("dsn must not be empty")
	}
	if err := validatePositiveInt("db-max-open-conns", c.DBMaxOpenConns); err != nil {
		return err
	}
	if err := validatePositiveInt("db-max-idle-conns", c.DBMaxIdleConns); err != nil {
		return err
	}
	if err := validatePositiveDuration("db-connect-timeout", c.DBConnectTimeout); err != nil {
		return err
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("kafka-brokers must contain at least one broker")
	}
	for _, broker := range c.KafkaBrokers {
		if err := validateHostPort("kafka-brokers", broker); err != nil {
			return err
		}
	}
	if strings.TrimSpace(c.FanoutTopic) == "" {
		return fmt.Errorf("fanout-topic must not be empty")
	}
	if strings.TrimSpace(c.DeliverTopic) == "" {
		return fmt.Errorf("deliver-topic must not be empty")
	}
	if strings.TrimSpace(c.FanoutGroup) == "" {
		return fmt.Errorf("fanout-group must not be empty")
	}
	if strings.TrimSpace(c.DeliverGroup) == "" {
		return fmt.Errorf("deliver-group must not be empty")
	}
	if err := validatePositiveInt("fanout-batch", c.FanoutBatch); err != nil {
		return err
	}
	if err := validatePositiveInt("claim-batch", c.ClaimBatch); err != nil {
		return err
	}
	if err := validatePositiveDuration("lease-ttl", c.LeaseTTL); err != nil {
		return err
	}
	if c.Shards < 1 {
		return fmt.Errorf("worker-shards must be at least 1, got %d", c.Shards)
	}
	if c.ShardIndex < 0 || c.ShardIndex >= c.Shards {
		return fmt.Errorf("worker-shard-index must be in [0,%d), got %d", c.Shards, c.ShardIndex)
	}
	if c.DefaultInflight < 0 {
		return fmt.Errorf("default-inflight must not be negative, got %d", c.DefaultInflight)
	}
	if err := validatePositiveDuration("tenant-cache-ttl", c.TenantCacheTTL); err != nil {
		return err
	}
	if err := validatePositiveDuration("scan-interval", c.ScanInterval); err != nil {
		return err
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("retry-max-attempts must be at least 1, got %d", c.MaxAttempts)
	}
	if err := validatePositiveDuration("retry-backoff-base", c.RetryBackoffBase); err != nil {
		return err
	}
	if err := validatePositiveDuration("retry-backoff-max", c.RetryBackoffMax); err != nil {
		return err
	}
	if c.RetryBackoffMax < c.RetryBackoffBase {
		return fmt.Errorf("retry-backoff-max %s must not be less than retry-backoff-base %s",
			c.RetryBackoffMax, c.RetryBackoffBase)
	}
	if err := validatePositiveInt("expire-batch", c.ExpireBatch); err != nil {
		return err
	}
	if err := validatePositiveInt("outbox-batch", c.OutboxBatch); err != nil {
		return err
	}
	if err := validatePositiveDuration("outbox-interval", c.OutboxInterval); err != nil {
		return err
	}
	if c.StatsInterval < 0 {
		return fmt.Errorf("stats-interval must not be negative, got %s", c.StatsInterval)
	}
	if c.RedisDB < 0 {
		return fmt.Errorf("redis-db must not be negative, got %d", c.RedisDB)
	}
	// Cluster routing for the engine is optional: without Redis the engine only
	// pushes to locally connected sessions.
	if c.RedisAddr != "" {
		if err := validateHostPort("redis-addr", c.RedisAddr); err != nil {
			return err
		}
	}
	if err := validatePositiveDuration("node-dial-timeout", c.NodeDialTimeout); err != nil {
		return err
	}
	if err := validateMaxBody(c.MaxBody); err != nil {
		return err
	}
	if err := validatePositiveDuration("shutdown-timeout", c.ShutdownTimeout); err != nil {
		return err
	}
	return nil
}

// ParseWorker parses delivery-worker flags.
func ParseWorker(args []string) (WorkerConfig, error) {
	fs := flag.NewFlagSet("delivery-worker", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var cfg WorkerConfig
	var brokers string
	fs.StringVar(&cfg.DSN, "dsn", "postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable", "PostgreSQL DSN")
	fs.IntVar(&cfg.DBMaxOpenConns, "db-max-open-conns", 32, "maximum open database connections")
	fs.IntVar(&cfg.DBMaxIdleConns, "db-max-idle-conns", 8, "maximum idle database connections")
	fs.DurationVar(&cfg.DBConnectTimeout, "db-connect-timeout", 5*time.Second, "timeout for the startup connect and migration")
	fs.StringVar(&brokers, "kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	fs.StringVar(&cfg.FanoutTopic, "fanout-topic", "relayx.fanout.v1", "fan-out topic")
	fs.StringVar(&cfg.DeliverTopic, "deliver-topic", "relayx.deliver.v1", "delivery task topic")
	fs.StringVar(&cfg.FanoutGroup, "fanout-group", "relayx-fanout", "fan-out consumer group")
	fs.StringVar(&cfg.DeliverGroup, "deliver-group", "relayx-delivery", "delivery consumer group")
	fs.IntVar(&cfg.FanoutBatch, "fanout-batch", 500, "maximum recipients inserted per fan-out batch")
	fs.IntVar(&cfg.ClaimBatch, "claim-batch", 100, "maximum deliveries leased per scan")
	fs.DurationVar(&cfg.LeaseTTL, "lease-ttl", 30*time.Second, "delivery lease lifetime")
	fs.IntVar(&cfg.Shards, "worker-shards", 1, "number of delivery scan shards")
	fs.IntVar(&cfg.ShardIndex, "worker-shard-index", 0, "shard owned by this worker")
	fs.IntVar(&cfg.DefaultInflight, "default-inflight", 256, "default in-flight limit for tenants without a quota")
	fs.DurationVar(&cfg.TenantCacheTTL, "tenant-cache-ttl", 30*time.Second, "tenant quota cache lifetime")
	fs.DurationVar(&cfg.ScanInterval, "scan-interval", time.Second, "delivery due scan interval")
	fs.IntVar(&cfg.MaxAttempts, "retry-max-attempts", 5, "maximum push attempts per delivery")
	fs.DurationVar(&cfg.RetryBackoffBase, "retry-backoff-base", 2*time.Second, "base retry backoff")
	fs.DurationVar(&cfg.RetryBackoffMax, "retry-backoff-max", time.Minute, "maximum retry backoff")
	fs.IntVar(&cfg.ExpireBatch, "expire-batch", 100, "maximum expired messages per scan")
	fs.IntVar(&cfg.OutboxBatch, "outbox-batch", 100, "maximum outbox rows published per batch")
	fs.DurationVar(&cfg.OutboxInterval, "outbox-interval", 500*time.Millisecond, "outbox relay interval")
	fs.DurationVar(&cfg.StatsInterval, "stats-interval", 10*time.Second, "stats log interval (<=0 disables)")
	fs.StringVar(&cfg.RedisAddr, "redis-addr", "", "Redis address for route lookup (empty disables routing)")
	fs.StringVar(&cfg.RedisPassword, "redis-password", "", "Redis password (empty means no auth)")
	fs.IntVar(&cfg.RedisDB, "redis-db", 0, "Redis logical database")
	fs.StringVar(&cfg.NodeID, "node-id", "", "worker node id used for leases (empty generates one)")
	fs.DurationVar(&cfg.NodeDialTimeout, "node-dial-timeout", time.Second, "timeout for dialing a gateway node")
	fs.IntVar(&cfg.MaxBody, "max-body", 1<<20, "maximum node frame body size")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 5*time.Second, "graceful shutdown timeout")

	if err := fs.Parse(args); err != nil {
		return WorkerConfig{}, err
	}
	for _, part := range strings.Split(brokers, ",") {
		if broker := strings.TrimSpace(part); broker != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
		}
	}
	if err := cfg.Validate(); err != nil {
		return WorkerConfig{}, err
	}
	return cfg, nil
}
