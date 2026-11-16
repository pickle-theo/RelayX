package server

import "time"

// Config 是 TCP Server 的运行参数。默认值见 DefaultConfig。
type Config struct {
	Addr              string
	HeartbeatInterval time.Duration // 空闲多久后发 PING
	PongTimeout       time.Duration // 发 PING 后等待 PONG 的超时
	WriteDeadline     time.Duration // 单次写超时，防止对端不读导致永久阻塞
	EnableEcho        bool          // 是否注册 ECHO(0x0C) 测试帧 handler
	MaxConnections    int           // 0 表示不限制
}

// DefaultConfig 返回 LLD §6.1 规定的默认参数。
func DefaultConfig() Config {
	return Config{
		Addr:              "127.0.0.1:9700",
		HeartbeatInterval: 30 * time.Second,
		PongTimeout:       10 * time.Second,
		WriteDeadline:     10 * time.Second,
		MaxConnections:    0,
	}
}
