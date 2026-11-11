// Package config provides flag-based configuration parsing for the RelayX
// gateway server and demo client.
package config

import (
	"flag"
	"fmt"
	"io"
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
	fs.IntVar(&cfg.SendQueueSize, "send-queue-size", 256, "per-connection send queue size")
	fs.IntVar(&cfg.SendQueueBytes, "send-queue-bytes", 4<<20, "per-connection send queue byte budget")
	fs.DurationVar(&cfg.WriteTimeout, "write-timeout", 10*time.Second, "deadline for a single socket write")
	fs.IntVar(&cfg.WriteBatchFrames, "write-batch-frames", 16, "maximum frames coalesced into one write")
	fs.IntVar(&cfg.WriteBatchBytes, "write-batch-bytes", 64<<10, "target bytes coalesced into one write")
	fs.IntVar(&cfg.ReadBufferSize, "read-buffer-size", 4096, "per-connection read buffer size in bytes")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 5*time.Second, "graceful shutdown timeout")
	fs.DurationVar(&cfg.StatsInterval, "stats-interval", 10*time.Second, "interval between gateway stats logs (<=0 disables)")
	fs.StringVar(&cfg.DSN, "dsn", "postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable", "PostgreSQL DSN")
	fs.IntVar(&cfg.DBMaxOpenConns, "db-max-open-conns", 16, "maximum open database connections")
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
	Addr        string
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
	if err := cfg.Validate(); err != nil {
		return ClientConfig{}, err
	}
	return cfg, nil
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
