package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/config"
	"relayx/s6/internal/forward"
	"relayx/s6/internal/gateway"
)

// cluster 是一组共享同一 memory backend 的进程内 Gateway 节点。
// 共享 backend 让「端点→节点」路由在多节点间真实可见，而 mTLS 走真实 TCP。
type cluster struct {
	backend *backend.MemoryBackend
	nodes   map[string]*gateway.Gateway
	cfg     map[string]config.Config
	log     *slog.Logger
	dir     string
}

func newCluster(n int) (*cluster, error) {
	dir, err := os.MkdirTemp("", "relayx-s6-scenario-")
	if err != nil {
		return nil, err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	b := backend.NewMemoryBackend()

	// 一套 CA 签发给所有节点，模拟内部 CA。
	caCert, caKey, err := forward.GenerateDevCA(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.key"))
	if err != nil {
		return nil, err
	}
	c := &cluster{
		backend: b, nodes: make(map[string]*gateway.Gateway),
		cfg: make(map[string]config.Config), log: log, dir: dir,
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("gw%d", i)
		certFile := filepath.Join(dir, id+".crt")
		keyFile := filepath.Join(dir, id+".key")
		if err := forward.GenerateDevLeaf(caCert, caKey, id, "localhost", certFile, keyFile); err != nil {
			return nil, err
		}
		tlsCfg, err := forward.LoadTLSConfig(certFile, keyFile, filepath.Join(dir, "ca.pem"), "localhost")
		if err != nil {
			return nil, err
		}
		cfg := config.Default()
		cfg.NodeID = id
		cfg.Backend = "memory"
		cfg.ClientAddr = fmt.Sprintf("127.0.0.1:%d", 19800+i)
		cfg.InternalAddr = fmt.Sprintf("127.0.0.1:%d", 19803+i)
		cfg.MetricsAddr = fmt.Sprintf("127.0.0.1:%d", 19806+i)
		cfg.HeartbeatInterval = 0 // 场景里手动推进，避免后台心跳干扰 TTL 断言
		cfg.LocalCacheTTL = 30 * time.Second
		gw, err := gateway.New(gateway.Options{Cfg: cfg, Log: log, Backend: b, TLSConfig: tlsCfg})
		if err != nil {
			return nil, err
		}
		c.nodes[id] = gw
		c.cfg[id] = cfg
	}
	return c, nil
}

func (c *cluster) start() error {
	for _, gw := range c.nodes {
		if err := gw.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) shutdown() {
	for _, gw := range c.nodes {
		_ = gw.Shutdown(context.Background())
	}
	_ = os.RemoveAll(c.dir)
}
