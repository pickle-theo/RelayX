// Package config provides flag-based configuration parsing for the RelayX
// gateway server and demo client.
package config

import (
	"flag"
	"fmt"
	"io"
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
	return validatePositiveDuration("shutdown-timeout", c.ShutdownTimeout)
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
