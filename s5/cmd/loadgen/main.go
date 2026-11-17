// Command loadgen 是 S5 的负载生成器 v1（LLD §9）：
// 支撑 1k–100k 连接阶梯、爬坡/稳态/下降曲线、混合流量、故障注入与
// 客户端侧延迟分解。它通过 internal/loadclient（L1 层）说话，
// 不直接拼帧（L2 不得绕过 L1 的硬约束）。
//
// 示例（对本机 S4 Gateway）：
//
//	go run ./cmd/loadgen -gateway 127.0.0.1:9800 -access http://127.0.0.1:9802 \
//	  -conns 200 -topics 2 -rate 5 -curve "ramp:5s:0->1,steady:10s,down:3s:1->0" \
//	  -mix "send:0.4,broadcast:0.2,receipt:0.2,ping:0.2" \
//	  -md docs/loadtest-report.md
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"relayx/s5/internal/heartbeat"
	"relayx/s5/internal/loadclient"
)

// config 是压测配置（对应 LLD §2.3 LoadConfig）。
type config struct {
	Gateway string
	Access  string
	Tenant  string
	Conns   int
	Topics  int
	Rate    float64 // 每连接每秒操作数（factor=1 时）
	Payload int
	Curve   Curve
	Mix     TrafficMix

	TokenSecret     string
	SubscribeSecret string
	SubscribeKid    string

	InjectSlow  float64 // 慢消费者比例（速率降为 1/8）
	InjectBig   float64 // 大 payload 比例（5×）
	InjectChurn float64 // 每秒断开重连的连接比例

	AckTimeout time.Duration
	Seed       int64
	Out        string
	MD         string
	Quiet      bool
}

// counters 是全局计数。
type counters struct {
	connsOpened  atomic.Int64
	connsFailed  atomic.Int64
	connsClosed  atomic.Int64
	activeConns  atomic.Int64
	peakConns    atomic.Int64
	sends        atomic.Int64
	acks         atomic.Int64
	delivers     atomic.Int64
	broadcasts   atomic.Int64
	published    atomic.Int64
	publishFail  atomic.Int64
	receipts     atomic.Int64
	pings        atomic.Int64
	timeouts     atomic.Int64
	errors       atomic.Int64
	inboundDrops atomic.Int64
}

type runner struct {
	cfg   config
	ctr   counters
	rec   *Recorder
	sp    *rand.Rand
	wheel *heartbeat.Wheel

	topics []string
	// 服务令牌用于 HTTP 广播受理（kind=service）。
	serviceToken string
	httpClient   *http.Client

	done chan struct{}
	wg   sync.WaitGroup

	// clockGranularity 是本机 time.Now 的可分辨粒度（报告用）。
	clockGranularity time.Duration

	mu     sync.Mutex
	conns  map[int]*connState
	nextID int

	inflightMu sync.Mutex
	inflight   map[string]inflightEntry
}

type inflightEntry struct {
	sentAt     int64
	acceptedAt int64
	encodeNs   int64
}

type connState struct {
	id   int
	user string
	dev  string
	opCh chan struct{}
	stop chan struct{}
}

func main() {
	cfg := parseFlags()

	curve := cfg.Curve
	total := curve.Total()
	if total <= 0 {
		fmt.Fprintln(os.Stderr, "loadgen: curve total duration must be > 0")
		os.Exit(2)
	}

	r := &runner{
		cfg: cfg, rec: NewRecorder(),
		sp:         rand.New(rand.NewSource(cfg.Seed)),
		conns:      make(map[int]*connState),
		inflight:   make(map[string]inflightEntry),
		done:       make(chan struct{}),
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
	if cfg.Topics > 0 {
		for i := 0; i < cfg.Topics; i++ {
			r.topics = append(r.topics, fmt.Sprintf("lg-topic-%d", i))
		}
	}
	// 服务令牌：广播受理要求 kind=service（S3 §4.2 / gateway HTTP 受理）。
	svc, err := loadclient.IssueLoginToken([]byte(cfg.TokenSecret), cfg.Tenant, "broadcaster", "service", 24*time.Hour)
	if err != nil {
		fail("issue service token: %v", err)
	}
	r.serviceToken = svc

	// 全局时间轮：所有连接共用一个 tick goroutine（S5 §4.4）。
	r.wheel = heartbeat.New(heartbeat.Config{Tick: time.Second, Buckets: 64})
	r.wheel.Start()
	defer r.wheel.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	r.clockGranularity = measureClockGranularity(50000)
	start := time.Now()
	fmt.Printf("loadgen: gateway=%s access=%s conns=%d rate=%.1f/conn curve=%s mix=%s clock=%.1fµs\n",
		cfg.Gateway, cfg.Access, cfg.Conns, cfg.Rate, cfg.Curve, cfg.Mix,
		float64(r.clockGranularity)/1000.0)

	// 一个全局 pacing goroutine 驱动所有连接的操作（避免每连接一个 ticker）。
	pacingDone := make(chan struct{})
	go func() { defer close(pacingDone); r.pace(start, total) }()

	// HTTP 广播发布器（混合流量中的 broadcast 部分）。
	bcastDone := make(chan struct{})
	if cfg.Mix.Broadcast > 0 {
		go func() { defer close(bcastDone); r.publishLoop(start, total) }()
	} else {
		close(bcastDone)
	}

	timer := time.NewTimer(total)
	select {
	case <-timer.C:
	case <-sigCh:
		fmt.Println("loadgen: interrupted, stopping early")
	}

	close(r.done)
	<-pacingDone
	<-bcastDone
	r.closeAll()
	r.wg.Wait()

	elapsed := time.Since(start)
	rep := r.buildReport(elapsed)
	if cfg.Out != "" {
		if err := writeJSON(cfg.Out, rep); err != nil {
			fail("write report: %v", err)
		}
	}
	if cfg.MD != "" {
		if err := writeFile(cfg.MD, r.markdown(rep)); err != nil {
			fail("write markdown: %v", err)
		}
	}
	if !cfg.Quiet {
		fmt.Print(r.markdown(rep))
	}
}

func parseFlags() config {
	var (
		gateway = flag.String("gateway", "127.0.0.1:9800", "Gateway TCP 地址")
		access  = flag.String("access", "http://127.0.0.1:9802", "HTTP 受理地址（广播 publish）")
		tenant  = flag.String("tenant", "lg", "租户")
		conns   = flag.Int("conns", 200, "连接数上限（曲线倍率作用于它）")
		topics  = flag.Int("topics", 2, "订阅 topic 数（0 表示不订阅）")
		rate    = flag.Float64("rate", 5, "每连接每秒操作数（factor=1 时）")
		payload = flag.Int("payload", 64, "payload 字节数")
		curveS  = flag.String("curve", "ramp:5s:0->1,steady:10s,down:3s:1->0", "负载曲线")
		mixS    = flag.String("mix", "send:0.4,broadcast:0.2,receipt:0.2,ping:0.2", "混合流量比例")
		secret  = flag.String("token-secret", "relayx-s4-dev-secret", "登录令牌 HMAC 密钥")
		subSec  = flag.String("subscribe-secret", "relayx-s4-dev-subscribe-secret", "订阅令牌 kid=dev 的 HMAC 密钥")
		subKid  = flag.String("subscribe-kid", "dev", "订阅令牌 kid")
		slow    = flag.Float64("inject-slow", 0, "慢消费者比例（0–1）")
		big     = flag.Float64("inject-big", 0, "大 payload 比例（0–1，5× 字节）")
		churn   = flag.Float64("inject-churn", 0, "每秒断开重连的连接比例（0–1）")
		ackTo   = flag.Duration("ack-timeout", 5*time.Second, "等待 ACK 超时")
		seed    = flag.Int64("seed", time.Now().UnixNano(), "随机种子")
		out     = flag.String("out", "", "JSON 报告输出路径")
		md      = flag.String("md", "", "Markdown 报告输出路径")
		quiet   = flag.Bool("quiet", false, "不打印 Markdown 摘要")
	)
	flag.Parse()

	curve, err := ParseCurve(*curveS)
	if err != nil {
		fail("%v", err)
	}
	mix, err := ParseMix(*mixS)
	if err != nil {
		fail("%v", err)
	}
	if *conns <= 0 {
		fail("conns must be > 0")
	}
	return config{
		Gateway: *gateway, Access: *access, Tenant: *tenant,
		Conns: *conns, Topics: *topics, Rate: *rate, Payload: *payload,
		Curve: curve, Mix: mix,
		TokenSecret: *secret, SubscribeSecret: *subSec, SubscribeKid: *subKid,
		InjectSlow: *slow, InjectBig: *big, InjectChurn: *churn,
		AckTimeout: *ackTo, Seed: *seed, Out: *out, MD: *md, Quiet: *quiet,
	}
}

// pace 是全局调度器：每 10ms 按曲线倍率决定「这一拍要发多少个操作」，
// 并按轮转方式分派给存活连接（非阻塞，忙连接直接跳过）。
func (r *runner) pace(start time.Time, total time.Duration) {
	const tick = 10 * time.Millisecond
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var credit float64
	lastScale := time.Now()
	churnAcc := 0.0
	for {
		select {
		case <-r.done:
			return
		case now := <-ticker.C:
			elapsed := now.Sub(start)
			factor := r.cfg.Curve.Factor(elapsed)
			target := int(float64(r.cfg.Conns) * factor)
			r.scale(target)

			dt := now.Sub(lastScale).Seconds()
			lastScale = now
			// 每秒总操作数 = 活跃连接 × 每连接速率 × 倍率。
			active := float64(r.ctr.activeConns.Load())
			credit += active * r.cfg.Rate * factor * dt
			n := int(credit)
			credit -= float64(n)
			r.dispatch(n)

			// 集中断线/重连注入（S5 §9.3 重连风暴）。
			if r.cfg.InjectChurn > 0 {
				churnAcc += active * r.cfg.InjectChurn * dt
				if churnAcc >= 1 {
					k := int(churnAcc)
					churnAcc -= float64(k)
					r.churn(k)
				}
			}
			if elapsed >= total {
				return
			}
		}
	}
}

// dispatch 把 n 个操作轮转分派给存活连接。
func (r *runner) dispatch(n int) {
	if n <= 0 {
		return
	}
	r.mu.Lock()
	states := make([]*connState, 0, len(r.conns))
	for _, c := range r.conns {
		states = append(states, c)
	}
	r.mu.Unlock()
	if len(states) == 0 {
		return
	}
	for i := 0; i < n; i++ {
		c := states[i%len(states)]
		select {
		case c.opCh <- struct{}{}:
		default:
			// 连接忙（上一个操作还没回 ACK）：跳过，避免压测端自我排队。
		}
	}
}

// scale 把连接数调整到 target（爬坡时建连，下降时断连）。
func (r *runner) scale(target int) {
	if target < 0 {
		target = 0
	}
	if target > r.cfg.Conns {
		target = r.cfg.Conns
	}
	r.mu.Lock()
	cur := len(r.conns)
	r.mu.Unlock()
	for cur < target {
		r.spawn()
		cur++
	}
	if cur > target {
		r.closeSome(cur - target)
	}
}

func (r *runner) spawn() {
	r.mu.Lock()
	id := r.nextID
	r.nextID++
	c := &connState{
		id:   id,
		user: fmt.Sprintf("lg-%d", id),
		dev:  "d1",
		opCh: make(chan struct{}, 1),
		stop: make(chan struct{}),
	}
	r.conns[id] = c
	r.mu.Unlock()

	r.wg.Add(1)
	go r.runConn(c)
}

func (r *runner) closeSome(n int) {
	r.mu.Lock()
	victims := make([]*connState, 0, n)
	for id, c := range r.conns {
		if len(victims) >= n {
			break
		}
		victims = append(victims, c)
		delete(r.conns, id)
	}
	r.mu.Unlock()
	for _, c := range victims {
		close(c.stop)
	}
}

func (r *runner) churn(n int) {
	r.mu.Lock()
	victims := make([]*connState, 0, n)
	for id, c := range r.conns {
		if len(victims) >= n {
			break
		}
		victims = append(victims, c)
		delete(r.conns, id)
	}
	r.mu.Unlock()
	for _, c := range victims {
		close(c.stop)
	}
	for i := 0; i < len(victims); i++ {
		r.spawn()
	}
}

func (r *runner) closeAll() {
	r.mu.Lock()
	for id, c := range r.conns {
		close(c.stop)
		delete(r.conns, id)
	}
	r.mu.Unlock()
}
