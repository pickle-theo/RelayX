// Package config provides flag-based configuration parsing for the RelayX
// gateway server and demo client.
package config

import (
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// maxBodyLimit is the largest frame body the protocol layer accepts.
const maxBodyLimit = 16 << 20 // 16 MiB

// ServerConfig holds gateway server settings.
type ServerConfig struct {
	Addr              string
	MaxBody           int
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	IdleTimeout       time.Duration
	SendQueueSize     int
	SendQueueBytes    int
	WriteTimeout      time.Duration
	WriteBatchFrames  int
	WriteBatchBytes   int
	ReadBufferSize    int
	ShutdownTimeout   time.Duration
	// StatsInterval enables periodic gateway stats logging. Values <= 0
	// disable the stats goroutine.
	StatsInterval time.Duration
	// DSN is the PostgreSQL connection string. Phase 2 always talks to the
	// local Docker PostgreSQL started by `make db-up`.
	DSN string
	// DBMaxOpenConns and DBMaxIdleConns size the database connection pool.
	DBMaxOpenConns int
	DBMaxIdleConns int
	// DBConnectTimeout bounds the startup connect and schema migration.
	DBConnectTimeout time.Duration
	// MessageTTL is the lifetime of an accepted message; expire_at is
	// created_at + MessageTTL.
	MessageTTL time.Duration
	// RetryInterval is the scan interval of the retry/TTL worker.
	RetryInterval time.Duration
	// RetryMaxAttempts is the push limit for one delivery before it becomes
	// FAILED.
	RetryMaxAttempts int
	// RetryBackoffBase and RetryBackoffMax bound the exponential retry delay.
	RetryBackoffBase time.Duration
	RetryBackoffMax  time.Duration
	// RetryBatch and ExpireBatch bound how many rows one scan processes.
	RetryBatch  int
	ExpireBatch int
	// PullLimit is the PULL page size used when a client requests 0.
	PullLimit int
	// PullMaxLimit caps a client-requested PULL page size and must not be less
	// than PullLimit.
	PullMaxLimit int
	// PprofAddr is the pprof HTTP listen address. An empty value disables the
	// pprof endpoint entirely.
	PprofAddr string
	// BlockProfileRate configures runtime.SetBlockProfileRate. Zero disables
	// block profiling.
	BlockProfileRate int
	// MutexProfileFraction configures runtime.SetMutexProfileFraction. Zero
	// disables mutex profiling.
	MutexProfileFraction int

	// NodeID identifies this gateway inside the cluster. An empty value makes
	// the gateway generate one at startup.
	NodeID string
	// NodeAddr is the internal listen address for node-to-node traffic.
	NodeAddr string
	// NodeAdvertiseAddr is the address published to peers so they can dial
	// back. An empty value falls back to NodeAddr.
	NodeAdvertiseAddr string
	// RedisAddr is the Redis address backing cluster membership. An empty
	// value disables clustering and keeps Phase 3 single-node behaviour.
	RedisAddr string
	// RedisPassword authenticates against Redis. An empty value means no auth.
	RedisPassword string
	// RedisDB selects the Redis logical database.
	RedisDB int
	// NodeHeartbeatInterval is how often the node refreshes its registry entry.
	NodeHeartbeatInterval time.Duration
	// NodeTTL is the registry entry lifetime. It must be at least twice
	// NodeHeartbeatInterval.
	NodeTTL time.Duration
	// NodeCleanupInterval is how often stale registry entries are swept. It
	// must be shorter than NodeTTL.
	NodeCleanupInterval time.Duration
	// NodeDialTimeout bounds dialing a peer node.
	NodeDialTimeout time.Duration
	// TenantID is the tenant recorded on messages accepted through SEND. The
	// TCP protocol has no tenant field, so one gateway process serves one
	// tenant; the HTTP Message Service exposes the tenant per request instead.
	TenantID string

	// KafkaBrokers enables the in-process outbox relay when non-empty. In a
	// real deployment the delivery worker owns the relay and the gateway only
	// writes outbox rows.
	KafkaBrokers string
	// OutboxBatch bounds how many outbox rows one relay pass publishes.
	OutboxBatch int
	// OutboxInterval is the relay poll interval.
	OutboxInterval time.Duration
}

// Validate checks that all numeric server settings are within range.
func (c ServerConfig) Validate() error {
	if err := validateMaxBody(c.MaxBody); err != nil {
		return err
	}
	if err := validatePositiveInt("send-queue-size", c.SendQueueSize); err != nil {
		return err
	}
	if err := validatePositiveInt("send-queue-bytes", c.SendQueueBytes); err != nil {
		return err
	}
	if err := validatePositiveDuration("write-timeout", c.WriteTimeout); err != nil {
		return err
	}
	if err := validatePositiveInt("write-batch-frames", c.WriteBatchFrames); err != nil {
		return err
	}
	if err := validatePositiveInt("write-batch-bytes", c.WriteBatchBytes); err != nil {
		return err
	}
	if err := validatePositiveInt("read-buffer-size", c.ReadBufferSize); err != nil {
		return err
	}
	if err := validatePositiveDuration("heartbeat-interval", c.HeartbeatInterval); err != nil {
		return err
	}
	if err := validatePositiveDuration("heartbeat-timeout", c.HeartbeatTimeout); err != nil {
		return err
	}
	if err := validatePositiveDuration("idle-timeout", c.IdleTimeout); err != nil {
		return err
	}
	if err := validatePositiveDuration("shutdown-timeout", c.ShutdownTimeout); err != nil {
		return err
	}
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
	if err := validatePositiveDuration("message-ttl", c.MessageTTL); err != nil {
		return err
	}
	if err := validatePositiveDuration("retry-interval", c.RetryInterval); err != nil {
		return err
	}
	if c.RetryMaxAttempts < 1 {
		return fmt.Errorf("retry-max-attempts must be at least 1, got %d", c.RetryMaxAttempts)
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
	if err := validatePositiveInt("retry-batch", c.RetryBatch); err != nil {
		return err
	}
	if err := validatePositiveInt("expire-batch", c.ExpireBatch); err != nil {
		return err
	}
	if err := validatePositiveInt("pull-limit", c.PullLimit); err != nil {
		return err
	}
	if err := validatePositiveInt("pull-max-limit", c.PullMaxLimit); err != nil {
		return err
	}
	if c.PullMaxLimit < c.PullLimit {
		return fmt.Errorf("pull-max-limit %d must not be less than pull-limit %d",
			c.PullMaxLimit, c.PullLimit)
	}
	if c.PullMaxLimit > 65535 {
		return fmt.Errorf("pull-max-limit %d must be at most 65535", c.PullMaxLimit)
	}
	if err := validateHostPort("pprof-addr", c.PprofAddr); err != nil {
		return err
	}
	if c.BlockProfileRate < 0 {
		return fmt.Errorf("block-profile-rate must not be negative, got %d", c.BlockProfileRate)
	}
	if c.MutexProfileFraction < 0 {
		return fmt.Errorf("mutex-profile-fraction must not be negative, got %d", c.MutexProfileFraction)
	}
	if err := validatePositiveDuration("node-heartbeat-interval", c.NodeHeartbeatInterval); err != nil {
		return err
	}
	if err := validatePositiveDuration("node-ttl", c.NodeTTL); err != nil {
		return err
	}
	if err := validatePositiveDuration("node-cleanup-interval", c.NodeCleanupInterval); err != nil {
		return err
	}
	if err := validatePositiveDuration("node-dial-timeout", c.NodeDialTimeout); err != nil {
		return err
	}
	if c.RedisDB < 0 {
		return fmt.Errorf("redis-db must not be negative, got %d", c.RedisDB)
	}
	if strings.TrimSpace(c.TenantID) == "" {
		return fmt.Errorf("tenant-id must not be empty")
	}
	if c.OutboxBatch < 0 {
		return fmt.Errorf("outbox-batch must not be negative, got %d", c.OutboxBatch)
	}
	if c.OutboxInterval < 0 {
		return fmt.Errorf("outbox-interval must not be negative, got %s", c.OutboxInterval)
	}
	// Cluster-only constraints. Single-node mode (empty redis-addr) leaves the
	// node settings unvalidated so Phase 3 deployments keep working.
	if c.RedisAddr != "" {
		if err := validateHostPort("node-addr", c.NodeAddr); err != nil {
			return err
		}
		if err := validateHostPort("node-advertise-addr", c.NodeAdvertiseAddr); err != nil {
			return err
		}
		if c.NodeTTL < 2*c.NodeHeartbeatInterval {
			return fmt.Errorf("node-ttl %s must be at least twice node-heartbeat-interval %s",
				c.NodeTTL, c.NodeHeartbeatInterval)
		}
		if c.NodeCleanupInterval >= c.NodeTTL {
			return fmt.Errorf("node-cleanup-interval %s must be less than node-ttl %s",
				c.NodeCleanupInterval, c.NodeTTL)
		}
	}
	return nil
}

// ParseServer parses server configuration flags from args.
func ParseServer(args []string) (ServerConfig, error) {
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var cfg ServerConfig
	fs.StringVar(&cfg.Addr, "addr", "127.0.0.1:9000", "listen address")
	fs.IntVar(&cfg.MaxBody, "max-body", 1<<20, "maximum frame body size in bytes")
	fs.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", 30*time.Second, "interval between heartbeats")
	fs.DurationVar(&cfg.HeartbeatTimeout, "heartbeat-timeout", 90*time.Second, "heartbeat timeout")
	fs.DurationVar(&cfg.IdleTimeout, "idle-timeout", 120*time.Second, "idle connection timeout")
	// 256 slots are required by the PULL contract: a page can enqueue up to 100
	// DELIVER frames plus the PULL_DONE frame before the writer drains, and a
	// smaller queue would close the connection as a slow consumer (regression
	// caught by TestPullPagination). The byte budget still caps memory.
	fs.IntVar(&cfg.SendQueueSize, "send-queue-size", 256, "per-connection send queue size")
	fs.IntVar(&cfg.SendQueueBytes, "send-queue-bytes", 4<<20, "per-connection send queue byte budget")
	fs.DurationVar(&cfg.WriteTimeout, "write-timeout", 10*time.Second, "deadline for a single socket write")
	fs.IntVar(&cfg.WriteBatchFrames, "write-batch-frames", 16, "maximum frames coalesced into one write")
	fs.IntVar(&cfg.WriteBatchBytes, "write-batch-bytes", 64<<10, "target bytes coalesced into one write")
	// Phase 3 round 2: bufio reads small control frames from this buffer and
	// bypasses it for bodies larger than the buffer, so 1KB is enough for the
	// common frame and saves 3KB per connection.
	fs.IntVar(&cfg.ReadBufferSize, "read-buffer-size", 1024, "per-connection read buffer size in bytes")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 5*time.Second, "graceful shutdown timeout")
	fs.DurationVar(&cfg.StatsInterval, "stats-interval", 10*time.Second, "interval between gateway stats logs (<=0 disables)")
	fs.StringVar(&cfg.DSN, "dsn", "postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable", "PostgreSQL DSN")
	fs.IntVar(&cfg.DBMaxOpenConns, "db-max-open-conns", 64, "maximum open database connections")
	fs.IntVar(&cfg.DBMaxIdleConns, "db-max-idle-conns", 8, "maximum idle database connections")
	fs.DurationVar(&cfg.DBConnectTimeout, "db-connect-timeout", 5*time.Second, "timeout for the startup connect and migration")
	fs.DurationVar(&cfg.MessageTTL, "message-ttl", 24*time.Hour, "message lifetime before it expires")
	fs.DurationVar(&cfg.RetryInterval, "retry-interval", time.Second, "interval between retry/TTL worker scans")
	fs.IntVar(&cfg.RetryMaxAttempts, "retry-max-attempts", 5, "maximum push attempts per delivery")
	fs.DurationVar(&cfg.RetryBackoffBase, "retry-backoff-base", 2*time.Second, "base retry backoff")
	fs.DurationVar(&cfg.RetryBackoffMax, "retry-backoff-max", time.Minute, "maximum retry backoff")
	fs.IntVar(&cfg.RetryBatch, "retry-batch", 100, "maximum due retries per scan")
	fs.IntVar(&cfg.ExpireBatch, "expire-batch", 100, "maximum expired messages per scan")
	fs.IntVar(&cfg.PullLimit, "pull-limit", 100, "default PULL page size")
	fs.IntVar(&cfg.PullMaxLimit, "pull-max-limit", 500, "maximum PULL page size")
	fs.StringVar(&cfg.PprofAddr, "pprof-addr", "", "pprof listen address (empty disables)")
	fs.IntVar(&cfg.BlockProfileRate, "block-profile-rate", 0, "runtime block profile sampling rate (0 disables)")
	fs.IntVar(&cfg.MutexProfileFraction, "mutex-profile-fraction", 0, "runtime mutex profile sampling fraction (0 disables)")
	fs.StringVar(&cfg.NodeID, "node-id", "", "cluster node id (empty generates one at startup)")
	fs.StringVar(&cfg.NodeAddr, "node-addr", "127.0.0.1:9100", "internal node-to-node listen address")
	fs.StringVar(&cfg.NodeAdvertiseAddr, "node-advertise-addr", "", "address advertised to peers (empty uses node-addr)")
	fs.StringVar(&cfg.RedisAddr, "redis-addr", "", "Redis address for cluster membership (empty disables clustering)")
	fs.StringVar(&cfg.RedisPassword, "redis-password", "", "Redis password (empty means no auth)")
	fs.IntVar(&cfg.RedisDB, "redis-db", 0, "Redis logical database")
	fs.DurationVar(&cfg.NodeHeartbeatInterval, "node-heartbeat-interval", 5*time.Second, "interval between node heartbeat writes")
	fs.DurationVar(&cfg.NodeTTL, "node-ttl", 15*time.Second, "lifetime of a node registry entry")
	fs.DurationVar(&cfg.NodeCleanupInterval, "node-cleanup-interval", 10*time.Second, "interval between stale node sweeps")
	fs.DurationVar(&cfg.NodeDialTimeout, "node-dial-timeout", time.Second, "timeout for dialing a peer node")
	fs.StringVar(&cfg.TenantID, "tenant-id", "default", "tenant recorded on messages accepted through SEND")
	fs.StringVar(&cfg.KafkaBrokers, "kafka-brokers", "", "Kafka brokers for the in-process outbox relay (empty disables it)")
	fs.IntVar(&cfg.OutboxBatch, "outbox-batch", 100, "maximum outbox rows published per relay pass")
	fs.DurationVar(&cfg.OutboxInterval, "outbox-interval", 500*time.Millisecond, "outbox relay poll interval")

	if err := fs.Parse(args); err != nil {
		return ServerConfig{}, err
	}
	if err := cfg.Validate(); err != nil {
		return ServerConfig{}, err
	}
	return cfg, nil
}

// ClientConfig holds demo client settings.
type ClientConfig struct {
	// Addr is the raw -addr flag. Addrs is its parsed form: one or more
	// gateway addresses a client falls back through when a node dies.
	Addr        string
	Addrs       []string
	Name        string
	User        string
	Device      string
	To          string
	Interval    time.Duration
	Count       int
	PayloadSize int
	MaxBody     int
	IdleTimeout time.Duration
}

// Validate checks that all numeric client settings are within range.
func (c ClientConfig) Validate() error {
	if err := validateMaxBody(c.MaxBody); err != nil {
		return err
	}
	if err := validatePositiveDuration("interval", c.Interval); err != nil {
		return err
	}
	if err := validatePositiveDuration("idle-timeout", c.IdleTimeout); err != nil {
		return err
	}
	if c.Count < 0 {
		return fmt.Errorf("count must not be negative, got %d", c.Count)
	}
	if c.PayloadSize < 0 {
		return fmt.Errorf("payload-size must not be negative, got %d", c.PayloadSize)
	}
	if c.PayloadSize > c.MaxBody {
		return fmt.Errorf("payload-size %d exceeds max-body %d", c.PayloadSize, c.MaxBody)
	}
	return nil
}

// ParseClient parses client configuration flags from args.
func ParseClient(args []string) (ClientConfig, error) {
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var cfg ClientConfig
	fs.StringVar(&cfg.Addr, "addr", "127.0.0.1:9000", "server address")
	fs.StringVar(&cfg.Name, "name", "demo", "client name")
	fs.StringVar(&cfg.User, "user", "", "login user id (empty disables LOGIN)")
	fs.StringVar(&cfg.Device, "device", "", "login device id (defaults to -name)")
	fs.StringVar(&cfg.To, "to", "", "send target: user id or endpoint:<session id>")
	fs.DurationVar(&cfg.Interval, "interval", time.Second, "interval between messages")
	fs.IntVar(&cfg.Count, "count", 0, "number of messages to send (0 means unlimited)")
	fs.IntVar(&cfg.PayloadSize, "payload-size", 64, "payload size in bytes")
	fs.IntVar(&cfg.MaxBody, "max-body", 1<<20, "maximum frame body size in bytes")
	fs.DurationVar(&cfg.IdleTimeout, "idle-timeout", 120*time.Second, "idle connection timeout")

	if err := fs.Parse(args); err != nil {
		return ClientConfig{}, err
	}
	addrs, err := ParseAddrList(cfg.Addr)
	if err != nil {
		return ClientConfig{}, err
	}
	cfg.Addrs = addrs
	if err := cfg.Validate(); err != nil {
		return ClientConfig{}, err
	}
	return cfg, nil
}

// ParseAddrList splits a comma-separated gateway address list. Surrounding
// whitespace is trimmed and empty items are ignored, so a trailing comma is
// harmless. Every remaining item must be a host:port pair. Clients need the
// list to fall back to another gateway node after a node crash or disconnect.
func ParseAddrList(s string) ([]string, error) {
	var addrs []string
	for _, part := range strings.Split(s, ",") {
		addr := strings.TrimSpace(part)
		if addr == "" {
			continue
		}
		if err := validateHostPort("addr", addr); err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("addr must contain at least one host:port address")
	}
	return addrs, nil
}

// NextAddrIndex returns the index of the address after cur, wrapping around so
// every node is retried before a failover gives up.
func NextAddrIndex(addrs []string, cur int) int {
	if len(addrs) == 0 {
		return 0
	}
	return (cur + 1) % len(addrs)
}

func validateMaxBody(v int) error {
	if v <= 0 {
		return fmt.Errorf("max-body must be positive, got %d", v)
	}
	if v > maxBodyLimit {
		return fmt.Errorf("max-body must be at most %d bytes, got %d", maxBodyLimit, v)
	}
	return nil
}

func validatePositiveInt(name string, v int) error {
	if v <= 0 {
		return fmt.Errorf("%s must be positive, got %d", name, v)
	}
	return nil
}

func validatePositiveDuration(name string, v time.Duration) error {
	if v <= 0 {
		return fmt.Errorf("%s must be positive, got %s", name, v)
	}
	return nil
}

// validateHostPort accepts an empty value (disabled) or a host:port pair with
// a numeric port in 1-65535.
func validateHostPort(name, v string) error {
	if v == "" {
		return nil
	}
	host, portStr, err := net.SplitHostPort(v)
	if err != nil {
		return fmt.Errorf("%s must be host:port, got %q: %w", name, v, err)
	}
	if host == "" {
		return fmt.Errorf("%s must include a host, got %q", name, v)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("%s port must be numeric, got %q", name, portStr)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("%s port must be in 1-65535, got %d", name, port)
	}
	return nil
}
