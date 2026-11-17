// Package config 定义 S4 单机 Gateway 的运行参数。
// 传输层与四通道预算继承 S1/S3；可靠内核参数沿用 S2 默认值；
// S4 新增投影轮询、事件预算与游标持久化配置。
package config

import "time"

// Config 是 Gateway 的运行配置。默认值见 Default。
type Config struct {
	// ---- 传输层（继承 S1/S3）----
	Addr              string
	MetricsAddr       string
	AccessAddr        string
	HeartbeatInterval time.Duration
	PongTimeout       time.Duration
	WriteDeadline     time.Duration
	TokenSecret       []byte

	// 四通道预算：P3 EVENT 使用独立预算，与 P1/P2 互不占用（LLD §7.1）。
	ControlMaxFrames   int
	ReliableMaxFrames  int
	ReliableMaxBytes   int64
	BroadcastMaxFrames int
	BroadcastMaxBytes  int64
	EventMaxFrames     int
	EventMaxBytes      int64

	// EnqueueTimeout 为 P1 RELIABLE 背压等待上限；超时视为慢消费者并断连。
	EnqueueTimeout time.Duration

	// ---- S3 订阅与广播（继承）----
	BroadcastPayloadMax        int
	BroadcastTokenKeys         map[string][]byte
	MaxTopicsPerSession        int
	MaxSubscribersPerTopic     int
	MaxTopicBytes              int
	MaxGlobalSubscriptions     int
	PublishIdemTTL             time.Duration
	BroadcastSlowDropThreshold int64

	// ---- S2 兼容可靠内核参数（事实生产者）----
	MaxPayload         int32
	MaxTTL             int32
	MaxAttempts        int32
	DefaultTTL         int32
	DefaultMaxAttempts int32
	RetryBackoff       time.Duration
	ScanInterval       time.Duration

	// ---- S4 事件投影（LLD §5.4）----
	// EventPollInterval 是投影游标轮询的兜底间隔（正确性由游标保证；
	// 事实写入后的唤醒信号只是低延迟优化）。
	EventPollInterval time.Duration
	// EventBatch 是单轮投影最多读取的事实条数（键集分页，不用 OFFSET）。
	EventBatch int
	// CursorKind 是投影游标存储：memory（默认，进程内）| file（跨重启保留）。
	CursorKind string
	// CursorPath 是 file 游标路径。
	CursorPath string

	// ---- 运行/验证开关 ----
	// TestHooks 打开 /debug/* 验证钩子（默认关闭）。
	TestHooks bool
	// LegacyServer 让 LOGIN_ACK 只宣告 LegacyCapabilities，
	// 用于验证「新客户端 + 旧服务端」的降级路径（LLD §11）。
	LegacyServer bool
	// DrainGrace 是 SERVER_DRAINING 时 CLOSE 帧写出后的等待窗口。
	DrainGrace time.Duration
}

// Default 返回 S4 默认配置。
func Default() Config {
	return Config{
		Addr:               "127.0.0.1:9800",
		MetricsAddr:        "127.0.0.1:9801",
		AccessAddr:         "127.0.0.1:9802",
		HeartbeatInterval:  30 * time.Second,
		PongTimeout:        10 * time.Second,
		WriteDeadline:      10 * time.Second,
		TokenSecret:        []byte("relayx-s4-dev-secret"),
		ControlMaxFrames:   64,
		ReliableMaxFrames:  256,
		ReliableMaxBytes:   8 << 20,
		BroadcastMaxFrames: 128,
		BroadcastMaxBytes:  2 << 20,
		EventMaxFrames:     128,
		EventMaxBytes:      2 << 20,
		EnqueueTimeout:     2 * time.Second,

		BroadcastPayloadMax:        8 << 10,
		BroadcastTokenKeys:         map[string][]byte{"dev": []byte("relayx-s4-dev-subscribe-secret")},
		MaxTopicsPerSession:        100,
		MaxSubscribersPerTopic:     100000,
		MaxTopicBytes:              256,
		MaxGlobalSubscriptions:     1000000,
		PublishIdemTTL:             30 * time.Second,
		BroadcastSlowDropThreshold: 32,

		MaxPayload:         61440,
		MaxTTL:             2592000,
		MaxAttempts:        10,
		DefaultTTL:         86400,
		DefaultMaxAttempts: 5,
		RetryBackoff:       time.Second,
		ScanInterval:       200 * time.Millisecond,

		EventPollInterval: 100 * time.Millisecond,
		EventBatch:        500,
		CursorKind:        "memory",
		CursorPath:        "relayx-s4-projection-cursor.json",

		DrainGrace: 100 * time.Millisecond,
	}
}
