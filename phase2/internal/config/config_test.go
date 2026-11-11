package config

import (
	"testing"
	"time"
)

func TestParseServerReadBufferSize(t *testing.T) {
	cfg, err := ParseServer([]string{"-read-buffer-size", "8192"})
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	if cfg.ReadBufferSize != 8192 {
		t.Fatalf("ReadBufferSize = %d, want 8192", cfg.ReadBufferSize)
	}
}

func TestParseServerReadBufferSizeDefault(t *testing.T) {
	cfg, err := ParseServer(nil)
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	if cfg.ReadBufferSize != 4096 {
		t.Fatalf("ReadBufferSize = %d, want 4096", cfg.ReadBufferSize)
	}
}

func TestServerConfigReadBufferSizeValidation(t *testing.T) {
	valid, err := ParseServer(nil)
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	for _, v := range []int{0, -1} {
		cfg := valid
		cfg.ReadBufferSize = v
		if err := cfg.Validate(); err == nil {
			t.Fatalf("Validate() with ReadBufferSize = %d succeeded, want error", v)
		}
	}
}

func TestParseServerDatabaseDefaults(t *testing.T) {
	cfg, err := ParseServer(nil)
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	if want := "postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable"; cfg.DSN != want {
		t.Fatalf("DSN = %q, want %q", cfg.DSN, want)
	}
	if cfg.DBMaxOpenConns != 16 {
		t.Fatalf("DBMaxOpenConns = %d, want 16", cfg.DBMaxOpenConns)
	}
	if cfg.DBMaxIdleConns != 8 {
		t.Fatalf("DBMaxIdleConns = %d, want 8", cfg.DBMaxIdleConns)
	}
	if cfg.DBConnectTimeout != 5*time.Second {
		t.Fatalf("DBConnectTimeout = %s, want 5s", cfg.DBConnectTimeout)
	}
}

func TestParseServerDatabaseOverrides(t *testing.T) {
	cfg, err := ParseServer([]string{
		"-dsn", "postgres://relayx:relayx@127.0.0.1:5433/relayx?sslmode=disable",
		"-db-max-open-conns", "32",
		"-db-max-idle-conns", "4",
		"-db-connect-timeout", "2s",
	})
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	if cfg.DSN != "postgres://relayx:relayx@127.0.0.1:5433/relayx?sslmode=disable" {
		t.Fatalf("DSN = %q, want the override", cfg.DSN)
	}
	if cfg.DBMaxOpenConns != 32 || cfg.DBMaxIdleConns != 4 {
		t.Fatalf("pool sizes = %d/%d, want 32/4", cfg.DBMaxOpenConns, cfg.DBMaxIdleConns)
	}
	if cfg.DBConnectTimeout != 2*time.Second {
		t.Fatalf("DBConnectTimeout = %s, want 2s", cfg.DBConnectTimeout)
	}
}

func TestServerConfigDatabaseValidation(t *testing.T) {
	valid, err := ParseServer(nil)
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*ServerConfig)
	}{
		{name: "empty dsn", mutate: func(c *ServerConfig) { c.DSN = "" }},
		{name: "blank dsn", mutate: func(c *ServerConfig) { c.DSN = "   " }},
		{name: "zero max open conns", mutate: func(c *ServerConfig) { c.DBMaxOpenConns = 0 }},
		{name: "negative max open conns", mutate: func(c *ServerConfig) { c.DBMaxOpenConns = -1 }},
		{name: "zero max idle conns", mutate: func(c *ServerConfig) { c.DBMaxIdleConns = 0 }},
		{name: "negative max idle conns", mutate: func(c *ServerConfig) { c.DBMaxIdleConns = -1 }},
		{name: "zero connect timeout", mutate: func(c *ServerConfig) { c.DBConnectTimeout = 0 }},
		{name: "negative connect timeout", mutate: func(c *ServerConfig) { c.DBConnectTimeout = -time.Second }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() succeeded, want error")
			}
		})
	}
}

func TestParseServerDeliveryDefaults(t *testing.T) {
	cfg, err := ParseServer(nil)
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	if cfg.MessageTTL != 24*time.Hour {
		t.Fatalf("MessageTTL = %s, want 24h", cfg.MessageTTL)
	}
	if cfg.RetryInterval != time.Second {
		t.Fatalf("RetryInterval = %s, want 1s", cfg.RetryInterval)
	}
	if cfg.RetryMaxAttempts != 5 {
		t.Fatalf("RetryMaxAttempts = %d, want 5", cfg.RetryMaxAttempts)
	}
	if cfg.RetryBackoffBase != 2*time.Second {
		t.Fatalf("RetryBackoffBase = %s, want 2s", cfg.RetryBackoffBase)
	}
	if cfg.RetryBackoffMax != time.Minute {
		t.Fatalf("RetryBackoffMax = %s, want 1m", cfg.RetryBackoffMax)
	}
	if cfg.RetryBatch != 100 || cfg.ExpireBatch != 100 {
		t.Fatalf("batches = %d/%d, want 100/100", cfg.RetryBatch, cfg.ExpireBatch)
	}
	if cfg.PullLimit != 100 {
		t.Fatalf("PullLimit = %d, want 100", cfg.PullLimit)
	}
	if cfg.PullMaxLimit != 500 {
		t.Fatalf("PullMaxLimit = %d, want 500", cfg.PullMaxLimit)
	}
}

func TestParseServerDeliveryOverrides(t *testing.T) {
	cfg, err := ParseServer([]string{
		"-message-ttl", "10m",
		"-retry-interval", "250ms",
		"-retry-max-attempts", "8",
		"-retry-backoff-base", "500ms",
		"-retry-backoff-max", "30s",
		"-retry-batch", "7",
		"-expire-batch", "9",
	})
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	if cfg.MessageTTL != 10*time.Minute {
		t.Fatalf("MessageTTL = %s, want 10m", cfg.MessageTTL)
	}
	if cfg.RetryInterval != 250*time.Millisecond {
		t.Fatalf("RetryInterval = %s, want 250ms", cfg.RetryInterval)
	}
	if cfg.RetryMaxAttempts != 8 {
		t.Fatalf("RetryMaxAttempts = %d, want 8", cfg.RetryMaxAttempts)
	}
	if cfg.RetryBackoffBase != 500*time.Millisecond || cfg.RetryBackoffMax != 30*time.Second {
		t.Fatalf("backoff = %s/%s, want 500ms/30s", cfg.RetryBackoffBase, cfg.RetryBackoffMax)
	}
	if cfg.RetryBatch != 7 || cfg.ExpireBatch != 9 {
		t.Fatalf("batches = %d/%d, want 7/9", cfg.RetryBatch, cfg.ExpireBatch)
	}
}

func TestParseServerPullOverrides(t *testing.T) {
	cfg, err := ParseServer([]string{"-pull-limit", "50", "-pull-max-limit", "200"})
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	if cfg.PullLimit != 50 {
		t.Fatalf("PullLimit = %d, want 50", cfg.PullLimit)
	}
	if cfg.PullMaxLimit != 200 {
		t.Fatalf("PullMaxLimit = %d, want 200", cfg.PullMaxLimit)
	}
}

func TestServerConfigDeliveryValidation(t *testing.T) {
	valid, err := ParseServer(nil)
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*ServerConfig)
	}{
		{name: "zero message ttl", mutate: func(c *ServerConfig) { c.MessageTTL = 0 }},
		{name: "negative message ttl", mutate: func(c *ServerConfig) { c.MessageTTL = -time.Second }},
		{name: "zero retry interval", mutate: func(c *ServerConfig) { c.RetryInterval = 0 }},
		{name: "negative retry interval", mutate: func(c *ServerConfig) { c.RetryInterval = -time.Second }},
		{name: "zero max attempts", mutate: func(c *ServerConfig) { c.RetryMaxAttempts = 0 }},
		{name: "negative max attempts", mutate: func(c *ServerConfig) { c.RetryMaxAttempts = -1 }},
		{name: "zero backoff base", mutate: func(c *ServerConfig) { c.RetryBackoffBase = 0 }},
		{name: "negative backoff base", mutate: func(c *ServerConfig) { c.RetryBackoffBase = -time.Second }},
		{name: "zero backoff max", mutate: func(c *ServerConfig) { c.RetryBackoffMax = 0 }},
		{
			name: "backoff max below base",
			mutate: func(c *ServerConfig) {
				c.RetryBackoffBase = time.Minute
				c.RetryBackoffMax = time.Second
			},
		},
		{name: "zero retry batch", mutate: func(c *ServerConfig) { c.RetryBatch = 0 }},
		{name: "negative retry batch", mutate: func(c *ServerConfig) { c.RetryBatch = -1 }},
		{name: "zero expire batch", mutate: func(c *ServerConfig) { c.ExpireBatch = 0 }},
		{name: "negative expire batch", mutate: func(c *ServerConfig) { c.ExpireBatch = -1 }},
		{name: "zero pull limit", mutate: func(c *ServerConfig) { c.PullLimit = 0 }},
		{name: "negative pull limit", mutate: func(c *ServerConfig) { c.PullLimit = -1 }},
		{name: "zero pull max limit", mutate: func(c *ServerConfig) { c.PullMaxLimit = 0 }},
		{name: "negative pull max limit", mutate: func(c *ServerConfig) { c.PullMaxLimit = -1 }},
		{
			name: "pull max limit below pull limit",
			mutate: func(c *ServerConfig) {
				c.PullLimit = 200
				c.PullMaxLimit = 100
			},
		},
		{name: "pull max limit above uint16", mutate: func(c *ServerConfig) { c.PullMaxLimit = 65536 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() succeeded, want error")
			}
		})
	}
}
