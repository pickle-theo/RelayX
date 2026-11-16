package config

import "time"

// Config 是完成解析的 S2 进程配置。
type Config struct {
	// 传输层（继承自 S1）。
	Addr              string
	MetricsAddr       string
	HeartbeatInterval time.Duration
	PongTimeout       time.Duration
	WriteDeadline     time.Duration
	TokenSecret       []byte

	ControlMaxFrames   int
	ReliableMaxFrames  int
	ReliableMaxBytes   int64
	BroadcastMaxFrames int
	BroadcastMaxBytes  int64
	EventMaxFrames     int
	EventMaxBytes      int64
	EnqueueTimeout     time.Duration

	// S2 可靠消息内核。
	GatewayAddr        string
	AccessAddr         string
	StoreKind          string // "memory" | "postgres"
	DSN                string
	MigrationPath      string
	ScanInterval       time.Duration
	RetryLimit         int
	ExpiryLimit        int
	RetryBackoff       time.Duration
	MaxTTL             int32
	MaxAttempts        int32
	MaxPayload         int32
	DriftCheckInterval time.Duration
	TestHooks          bool // enables /debug/clock only when explicitly requested
}

// Default 返回 S2 基线配置。平台硬上限冻结为：
// TTL 30 天、最多 10 次尝试、payload 60 KiB。
func Default() Config {
	return Config{
		Addr:               "127.0.0.1:9800",
		MetricsAddr:        "127.0.0.1:9801",
		HeartbeatInterval:  30 * time.Second,
		PongTimeout:        10 * time.Second,
		WriteDeadline:      10 * time.Second,
		TokenSecret:        []byte("relayx-s2-dev-secret"),
		ControlMaxFrames:   64,
		ReliableMaxFrames:  256,
		ReliableMaxBytes:   8 << 20,
		BroadcastMaxFrames: 128,
		BroadcastMaxBytes:  2 << 20,
		EventMaxFrames:     128,
		EventMaxBytes:      2 << 20,
		EnqueueTimeout:     2 * time.Second,

		GatewayAddr:        "127.0.0.1:9800",
		AccessAddr:         "127.0.0.1:9802",
		StoreKind:          "memory",
		ScanInterval:       200 * time.Millisecond,
		RetryLimit:         100,
		ExpiryLimit:        100,
		RetryBackoff:       time.Second,
		MaxTTL:             2592000,
		MaxAttempts:        10,
		MaxPayload:         61440,
		DriftCheckInterval: 60 * time.Second,
	}
}

// Policy 是消息级投递策略的平台侧视图。
type Policy struct {
	TTLSeconds   int32
	MaxAttempts  int32
	DeliveryMode string
	Fanout       int32
}

// 客户端未宣告 CAP_RELIABLE_POLICY 时使用 DefaultPolicy。
func DefaultPolicy() Policy {
	return Policy{
		TTLSeconds:   86400,
		MaxAttempts:  5,
		DeliveryMode: "all_endpoints",
	}
}

// ClampAndValidate 应用平台硬上限。请求策略非法时返回 false，
// 而不是静默改写非法值。
func ClampAndValidate(p Policy, cfg Config) (Policy, bool) {
	if p.TTLSeconds <= 0 || p.TTLSeconds > cfg.MaxTTL {
		return p, false
	}
	if p.MaxAttempts <= 0 || p.MaxAttempts > cfg.MaxAttempts {
		return p, false
	}
	if p.Fanout < 0 {
		return p, false
	}
	switch p.DeliveryMode {
	case "", "all_endpoints":
		p.DeliveryMode = "all_endpoints"
	case "last_active":
	default:
		return p, false
	}
	return p, true
}
