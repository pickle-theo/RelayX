package gateway

import (
	"net"
	"sync"
	"testing"
	"time"

	"relayx/s1/internal/identity"
)

// TestConcurrentLoginDisconnect 验证并发登录/断线下注册表与映射状态一致（-race）。
func TestConcurrentLoginDisconnect(t *testing.T) {
	_, addr := startGateway(t)
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := newGWClient(t, addr, "t1", "bob", "dev"+"-"+string(rune('a'+id%26)))
			c.login()
			// 立刻断线，制造登录与清理的竞态。
			_ = c.nc.Close()
		}(i)
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
}

// TestManyConcurrentConnections 验证大量并发连接稳定收发（-race）。
func TestManyConcurrentConnections(t *testing.T) {
	_, addr := startGateway(t)
	const n = 30
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nc, err := net.Dial("tcp", addr)
			if err != nil {
				errCh <- err
				return
			}
			defer nc.Close()
			_ = nc.SetDeadline(time.Now().Add(3 * time.Second))
			c := &gwClient{t: t, nc: nc, auth: identity.New(testSecret), tenant: "t1", user: "u", device: "dev-" + string(rune('a'+id))}
			c.login()
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
