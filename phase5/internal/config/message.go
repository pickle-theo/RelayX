package config

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

// MessageConfig holds the HTTP Message Service settings.
type MessageConfig struct {
	HTTPAddr         string
	DSN              string
	MaxBody          int
	MessageTTL       time.Duration
	DefaultTenant    string
	DBMaxOpenConns   int
	DBMaxIdleConns   int
	DBConnectTimeout time.Duration
	ShutdownTimeout  time.Duration
}

// Validate checks the Message Service settings.
func (c MessageConfig) Validate() error {
	if err := validateHostPort("http-addr", c.HTTPAddr); err != nil {
		return err
	}
	if strings.TrimSpace(c.DSN) == "" {
		return fmt.Errorf("dsn must not be empty")
	}
	if err := validateMaxBody(c.MaxBody); err != nil {
		return err
	}
	if err := validatePositiveDuration("message-ttl", c.MessageTTL); err != nil {
		return err
	}
	if strings.TrimSpace(c.DefaultTenant) == "" {
		return fmt.Errorf("default-tenant must not be empty")
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
	if err := validatePositiveDuration("shutdown-timeout", c.ShutdownTimeout); err != nil {
		return err
	}
	return nil
}

// ParseMessageService parses Message Service flags.
func ParseMessageService(args []string) (MessageConfig, error) {
	fs := flag.NewFlagSet("message-service", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var cfg MessageConfig
	fs.StringVar(&cfg.HTTPAddr, "http-addr", "127.0.0.1:8081", "HTTP listen address")
	fs.StringVar(&cfg.DSN, "dsn", "postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable", "PostgreSQL DSN")
	fs.IntVar(&cfg.MaxBody, "max-body", 1<<20, "maximum payload size in bytes")
	fs.DurationVar(&cfg.MessageTTL, "message-ttl", 24*time.Hour, "default message lifetime")
	fs.StringVar(&cfg.DefaultTenant, "default-tenant", "default", "tenant used when neither header nor body sets one")
	fs.IntVar(&cfg.DBMaxOpenConns, "db-max-open-conns", 32, "maximum open database connections")
	fs.IntVar(&cfg.DBMaxIdleConns, "db-max-idle-conns", 8, "maximum idle database connections")
	fs.DurationVar(&cfg.DBConnectTimeout, "db-connect-timeout", 5*time.Second, "timeout for the startup connect and migration")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 5*time.Second, "graceful shutdown timeout")

	if err := fs.Parse(args); err != nil {
		return MessageConfig{}, err
	}
	if err := cfg.Validate(); err != nil {
		return MessageConfig{}, err
	}
	return cfg, nil
}
