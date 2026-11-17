// Package harness 为 S6 集成/故障/兼容测试提供进程内多节点集群。
//
// 设计：多个 Gateway 共享同一个 MemoryBackend（模拟 Redis 的全局视图），
// 节点间走真实 mTLS TCP 连接。这样测试同时覆盖「路由可见性」与「mTLS 通道」，
// 而不需要外部 Redis 实例。
package harness

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/config"
	"relayx/s6/internal/forward"
	"relayx/s6/internal/gateway"
)

// Cluster 是一组节点。
type Cluster struct {
	Backend *backend.MemoryBackend
	Nodes   map[string]*gateway.Gateway
	Cfg     map[string]config.Config
	dir     string
	portSeq atomic.Int32
}

// New 创建一个 n 节点集群（尚未启动）。端口从 basePort 起分配。
func New(n int, basePort int) (*Cluster, error) {
	dir, err := os.MkdirTemp("", "relayx-s6-test-")
	if err != nil {
		return nil, err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	b := backend.NewMemoryBackend()

	caCert, caKey, err := forward.GenerateDevCA(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.key"))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	c := &Cluster{Backend: b, Nodes: make(map[string]*gateway.Gateway), Cfg: make(map[string]config.Config), dir: dir}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("gw%d", i)
		certFile := filepath.Join(dir, id+".crt")
		keyFile := filepath.Join(dir, id+".key")
		if err := forward.GenerateDevLeaf(caCert, caKey, id, "localhost", certFile, keyFile); err != nil {
			c.Close()
			return nil, err
		}
		tlsCfg, err := forward.LoadTLSConfig(certFile, keyFile, filepath.Join(dir, "ca.pem"), "localhost")
		if err != nil {
			c.Close()
			return nil, err
		}
		cfg := config.Default()
		cfg.NodeID = id
		cfg.Backend = "memory"
		cfg.ClientAddr = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		cfg.InternalAddr = fmt.Sprintf("127.0.0.1:%d", basePort+10+i)
		cfg.MetricsAddr = fmt.Sprintf("127.0.0.1:%d", basePort+20+i)
		cfg.HeartbeatInterval = 0
		gw, err := gateway.New(gateway.Options{Cfg: cfg, Log: log, Backend: b, TLSConfig: tlsCfg})
		if err != nil {
			c.Close()
			return nil, err
		}
		c.Nodes[id] = gw
		c.Cfg[id] = cfg
	}
	return c, nil
}

// Start 启动所有节点。
func (c *Cluster) Start() error {
	for _, gw := range c.Nodes {
		if err := gw.Start(); err != nil {
			return err
		}
	}
	return nil
}

// Close 关闭所有节点并清理临时证书目录。
func (c *Cluster) Close() {
	for _, gw := range c.Nodes {
		_ = gw.Shutdown(context.Background())
	}
	_ = os.RemoveAll(c.dir)
}

// WaitFor 等待条件成立（最多 timeout）。
func WaitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}
