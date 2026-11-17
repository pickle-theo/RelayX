// Package config 定义 S6 分布式 Gateway 的运行参数。
// S1–S5 的语义参数在本阶段不变；本包只新增「连接在哪」相关的协调参数
// （节点注册、Redis 路由、跨节点转发、mTLS、降级）。
package config

import "time"

// Config 是 S6 节点的运行配置。默认值见 Default。
type Config struct {
	// ---- 节点身份与监听 ----
	// NodeID 为空时由 identity.New 生成；显式指定用于固定部署标识。
	NodeID string
	// ClientAddr 是客户端 TCP 接入地址（S1 通道语义在此沿用）。
	ClientAddr string
	// InternalAddr 是节点间 FORWARD 帧的 mTLS 监听地址。
	InternalAddr string
	// MetricsAddr 暴露 /metrics 与 /healthz。
	MetricsAddr string
	// WriteDeadline 是本地投递写 socket 的超时。
	WriteDeadline time.Duration

	// ---- 路由后端（LLD §2.4、§7）----
	// Backend 取 "memory"（默认，进程内 faithful 模型）或 "redis"。
	// memory 后端完整模拟 Lua 的原子语义，供本地运行与测试；
	// redis 后端走 RESP + EVALSHA，供生产部署。
	Backend       string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	// RouteTimeout 是单次 Redis 路由操作超时。
	RouteTimeout time.Duration

	// ---- 节点注册与故障检测（LLD §6）----
	HeartbeatInterval time.Duration
	NodeTTL           time.Duration
	// SuspectAfter 为心跳缺失多久后标 SUSPECT；超过 NodeTTL 标 DEAD 并触发回收。
	SuspectAfter time.Duration
	// LocalCacheTTL 是路由本地缓存的 TTL（LLD §7.2「路由读取本地缓存兜底」）。
	LocalCacheTTL time.Duration

	// ---- 跨节点转发（LLD §4）----
	DialTimeout          time.Duration
	ForwardTimeout       time.Duration
	InternalPoolSize     int
	NodeUnreachableLimit int // 连续不可达达到该值回落离线路径（默认 3）
	RetryBackoff         time.Duration
	MaxAttempts          int32

	// ---- 节点间 mTLS（LLD §8）----
	// 三份 PEM 由文件读取；GenerateDevCerts 为 true 时生成临时自签 CA/叶证书，
	// 仅供本地开发/验收场景，生产必须关闭。
	CertFile         string
	KeyFile          string
	CAFile           string
	GenerateDevCerts bool
	// ServerName 用于校验证书 SAN；为空时用目标节点的 Addr host。
	ServerName string

	// TestHooks 打开 /debug/* 验证钩子（默认关闭）。
	TestHooks bool
}

// Default 返回 S6 默认配置。
func Default() Config {
	return Config{
		ClientAddr:    "127.0.0.1:9800",
		InternalAddr:  "127.0.0.1:9803",
		MetricsAddr:   "127.0.0.1:9801",
		WriteDeadline: 5 * time.Second,

		Backend:      "memory",
		RedisAddr:    "127.0.0.1:6379",
		RouteTimeout: 2 * time.Second,

		HeartbeatInterval: 5 * time.Second,
		NodeTTL:           15 * time.Second,
		SuspectAfter:      7 * time.Second,
		LocalCacheTTL:     30 * time.Second,

		DialTimeout:          2 * time.Second,
		ForwardTimeout:       2 * time.Second,
		InternalPoolSize:     8,
		NodeUnreachableLimit: 3,
		RetryBackoff:         time.Second,
		MaxAttempts:          10,

		GenerateDevCerts: false,
	}
}

// Validate 检查配置自洽；返回第一个错误描述，空串表示合法。
func (c Config) Validate() string {
	switch c.Backend {
	case "memory", "redis":
	default:
		return "backend 必须是 memory 或 redis"
	}
	if c.Backend == "redis" && c.RedisAddr == "" {
		return "redis 后端必须指定 RedisAddr"
	}
	if c.NodeTTL <= c.SuspectAfter || c.SuspectAfter <= 0 {
		return "需满足 0 < SuspectAfter < NodeTTL"
	}
	if c.NodeTTL < time.Second {
		// Redis EXPIRE 以秒为单位；<1s 会被夹到 1s，显式拒绝以免运行语义与配置不符。
		return "NodeTTL 必须 >= 1s"
	}
	if c.LocalCacheTTL <= 0 {
		return "LocalCacheTTL 必须为正"
	}
	if c.NodeUnreachableLimit < 1 {
		return "NodeUnreachableLimit 必须 >= 1"
	}
	if c.MaxAttempts < 1 {
		return "MaxAttempts 必须 >= 1"
	}
	return ""
}
