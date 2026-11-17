package gateway

import (
	"sync"
	"testing"
	"time"
)

// TestFanoutDuringUnsubscribeAndClose 覆盖 LLD §9.1/§9.2 的退订/断连竞态：
// 扇出进行中并发退订与断连，不得 panic、不得写已关闭连接（-race 下无数据竞争）。
func TestFanoutDuringUnsubscribeAndClose(t *testing.T) {
	_, addr, access := startGateway(t)
	const clients = 6
	cs := make([]*testClient, 0, clients)
	for i := 0; i < clients; i++ {
		c := newTestClient(t, addr, "t1", "alice", string(rune('a'+i)))
		c.login()
		if got := c.subscribe("race-topic", testSubSecret); got != 0 {
			t.Fatalf("subscribe = %d", got)
		}
		cs = append(cs, c)
	}

	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = publishHTTP(t, access.URL, "t1", "race-topic", "payload")
		}(i)
	}
	for i, c := range cs {
		wg.Add(1)
		go func(i int, c *testClient) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * 2 * time.Millisecond)
			if i%2 == 0 {
				_ = c.unsubscribe("race-topic")
			} else {
				_ = c.nc.Close()
			}
		}(i, c)
	}
	wg.Wait()
	for _, c := range cs {
		_ = c.nc.Close()
	}
}

// TestConcurrentSubscribeSameTopic 并发订阅同一 topic，索引计数必须一致（不重复计数）。
func TestConcurrentSubscribeSameTopic(t *testing.T) {
	gw, addr, _ := startGateway(t)
	const n = 20
	cs := make([]*testClient, 0, n)
	for i := 0; i < n; i++ {
		c := newTestClient(t, addr, "t1", "alice", string(rune('a'+i)))
		c.login()
		cs = append(cs, c)
	}
	defer func() {
		for _, c := range cs {
			_ = c.nc.Close()
		}
	}()

	var wg sync.WaitGroup
	for _, c := range cs {
		wg.Add(1)
		go func(c *testClient) {
			defer wg.Done()
			if got := c.subscribe("busy", testSubSecret); got != 0 {
				t.Errorf("subscribe = %d", got)
			}
		}(c)
	}
	wg.Wait()

	if got := len(gw.Broadcast().Index.Snapshot("t1", "busy")); got != n {
		t.Fatalf("snapshot size=%d want %d", got, n)
	}
}
