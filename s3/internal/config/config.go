// Package config 定义 S1 Gateway 的运行参数。
package config

import "time"

// Config 是 Gateway 的运行配置。默认值见 Default。
type Config struct {
	// Addr 为客户端 TCP 监听地址。
	Addr string
	// MetricsAddr 为 /metrics 与 /healthz 的 HTTP 监听地址。
	MetricsAddr string
	// HeartbeatInterval 为空闲多久后发 PING。
	HeartbeatInterval time.Duration
	// PongTimeout 为发 PING 后等待活动的超时。
	PongTimeout time.Duration
	// WriteDeadline 为单次写 Socket 的超时。
	WriteDeadline time.Duration

	// TokenSecret 为 HMAC-SHA256 令牌签名密钥。S1 最小鉴权使用共享静态密钥；
	// 密钥轮转/吊销在 S8 完善（架构方案 §5 / S1 LLD §5.3）。
	TokenSecret []byte

	// Channel 预算：四通道各自的帧数与字节上限。零值由 SendPath 填默认。
	ControlMaxFrames   int
	ReliableMaxFrames  int
	ReliableMaxBytes   int64
	BroadcastMaxFrames int
	BroadcastMaxBytes  int64
	EventMaxFrames     int
	EventMaxBytes      int64

	// EnqueueTimeout 为 P1 RELIABLE 背压等待上限；超时视为慢消费者并断连。
	EnqueueTimeout time.Duration

	// ---- S3 订阅与广播 ----
	// AccessAddr 为 HTTP 受理 API（POST /v1/broadcasts）的监听地址。
	AccessAddr string
	// BroadcastPayloadMax 为单条广播 payload 上限（LLD §3.2：8 KiB）。
	BroadcastPayloadMax int
	// BroadcastTokenSecret 为订阅 token 的 kid→密钥映射（业务签发，平台只验签）。
	BroadcastTokenKeys map[string][]byte
	// 订阅索引容量上限（LLD §5.1）。
	MaxTopicsPerSession    int
	MaxSubscribersPerTopic int
	MaxTopicBytes          int
	MaxGlobalSubscriptions int
	// PublishIdemTTL 为 HTTP 受理幂等内存去重窗口（LLD §7.2）。
	PublishIdemTTL time.Duration
	// BroadcastSlowDropThreshold 为连续丢弃阈值，超过则降级该连接只保留可靠消息。
	BroadcastSlowDropThreshold int64
}

// Default 返回 S1 默认配置（LLD §6.1 / §4.3 预算约定）。
func Default() Config {
	return Config{
		Addr:               "127.0.0.1:9800",
		MetricsAddr:        "127.0.0.1:9801",
		HeartbeatInterval:  30 * time.Second,
		PongTimeout:        10 * time.Second,
		WriteDeadline:      10 * time.Second,
		TokenSecret:        []byte("relayx-s3-dev-secret"),
		ControlMaxFrames:   64,
		ReliableMaxFrames:  256,
		ReliableMaxBytes:   8 << 20, // 8 MiB
		BroadcastMaxFrames: 128,     // 约 P1 的 1/2，预留 25% 语义（S3 细化）
		BroadcastMaxBytes:  2 << 20,
		EventMaxFrames:     128,
		EventMaxBytes:      2 << 20,
		EnqueueTimeout:     2 * time.Second,

		AccessAddr:                 "127.0.0.1:9802",
		BroadcastPayloadMax:        8 << 10, // 8 KiB
		BroadcastTokenKeys:         map[string][]byte{"dev": []byte("relayx-s3-dev-subscribe-secret")},
		MaxTopicsPerSession:        100,
		MaxSubscribersPerTopic:     100000,
		MaxTopicBytes:              256,
		MaxGlobalSubscriptions:     1000000,
		PublishIdemTTL:             30 * time.Second,
		BroadcastSlowDropThreshold: 32,
	}
}
