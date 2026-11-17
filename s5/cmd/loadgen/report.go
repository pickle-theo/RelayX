package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"relayx/s5/benchmark"
)

// report 是一次压测的结构化结果（与 S8 容量报告字段对齐，可直接横向对比）。
type report struct {
	Commit      string        `json:"commit"`
	Timestamp   time.Time     `json:"timestamp"`
	Env         benchmark.Env `json:"env"`
	Gateway     string        `json:"gateway"`
	Curve       string        `json:"curve"`
	Mix         string        `json:"mix"`
	ConnsTarget int           `json:"conns_target"`
	Topics      int           `json:"topics"`
	RatePerConn float64       `json:"rate_per_conn"`
	// LoadWindowS 是曲线总时长（负载窗口）；吞吐按它计算，避免把收尾等待
	// ACK 超时的时间算进分母而低估 QPS。
	LoadWindowS float64 `json:"load_window_seconds"`
	// DurationS 是进程墙钟时长（含收尾）。
	DurationS float64 `json:"duration_seconds"`

	PeakConns   int64 `json:"peak_conns"`
	ConnsOpened int64 `json:"conns_opened"`
	ConnsFailed int64 `json:"conns_failed"`
	ConnsClosed int64 `json:"conns_closed"`

	Sends       int64 `json:"sends"`
	Acks        int64 `json:"acks"`
	Delivers    int64 `json:"delivers"`
	Receipts    int64 `json:"receipts"`
	Pings       int64 `json:"pings"`
	Published   int64 `json:"broadcasts_published"`
	Broadcasts  int64 `json:"broadcasts_received"`
	PublishFail int64 `json:"broadcast_publish_failed"`
	Timeouts    int64 `json:"timeouts"`
	Errors      int64 `json:"errors"`
	InboundDrop int64 `json:"inbound_drops"`

	OpsPerSecond float64 `json:"ops_per_second"`
	SendsPerSec  float64 `json:"sends_per_second"`
	DeliversPerS float64 `json:"delivers_per_second"`

	// BadSamples 是因时钟跳变/损坏 payload 被丢弃的延迟样本数（应为 0 或极小）。
	BadSamples int64 `json:"bad_latency_samples"`
	// ClockGranularityNs 是本机 time.Now 的可分辨粒度，决定亚毫秒列的解读口径。
	ClockGranularityNs int64     `json:"clock_granularity_ns"`
	Latency            []Summary `json:"latency"`
}

func (r *runner) buildReport(elapsed time.Duration) report {
	sec := r.cfg.Curve.Total().Seconds()
	if sec <= 0 {
		sec = elapsed.Seconds()
	}
	if sec <= 0 {
		sec = 1e-9
	}
	c := &r.ctr
	rep := report{
		Commit:             benchmark.CurrentCommit(),
		Timestamp:          time.Now().UTC(),
		Env:                benchmark.CaptureEnv(),
		Gateway:            r.cfg.Gateway,
		Curve:              r.cfg.Curve.String(),
		Mix:                r.cfg.Mix.String(),
		ConnsTarget:        r.cfg.Conns,
		Topics:             r.cfg.Topics,
		RatePerConn:        r.cfg.Rate,
		LoadWindowS:        sec,
		DurationS:          elapsed.Seconds(),
		PeakConns:          c.peakConns.Load(),
		ConnsOpened:        c.connsOpened.Load(),
		ConnsFailed:        c.connsFailed.Load(),
		ConnsClosed:        c.connsClosed.Load(),
		Sends:              c.sends.Load(),
		Acks:               c.acks.Load(),
		Delivers:           c.delivers.Load(),
		Receipts:           c.receipts.Load(),
		Pings:              c.pings.Load(),
		Published:          c.published.Load(),
		Broadcasts:         c.broadcasts.Load(),
		PublishFail:        c.publishFail.Load(),
		Timeouts:           c.timeouts.Load(),
		Errors:             c.errors.Load(),
		InboundDrop:        c.inboundDrops.Load(),
		BadSamples:         r.rec.BadSamples(),
		ClockGranularityNs: r.clockGranularity.Nanoseconds(),
		Latency:            r.rec.Summaries(),
	}
	ops := rep.Sends + rep.Pings + rep.Receipts + rep.Published
	rep.OpsPerSecond = float64(ops) / sec
	rep.SendsPerSec = float64(rep.Sends) / sec
	rep.DeliversPerS = float64(rep.Delivers) / sec
	return rep
}

// markdown 生成可直接贴进 docs/loadtest-report.md 的结果段。
func (r *runner) markdown(rep report) string {
	out := "\n### 运行参数\n\n"
	out += fmt.Sprintf("| 项 | 值 |\n|---|---|\n")
	out += fmt.Sprintf("| commit | `%s` |\n", rep.Commit)
	out += fmt.Sprintf("| 环境 | %s/%s，%d vCPU，GOMAXPROCS=%d，GOGC=%s |\n",
		rep.Env.GOOS, rep.Env.GOARCH, rep.Env.NumCPU, rep.Env.GOMAXPROCS, rep.Env.GOGC)
	out += fmt.Sprintf("| Gateway | `%s` |\n", rep.Gateway)
	out += fmt.Sprintf("| 曲线 | `%s` |\n", rep.Curve)
	out += fmt.Sprintf("| 混合流量 | `%s` |\n", rep.Mix)
	out += fmt.Sprintf("| 连接数（目标/峰值） | %d / %d |\n", rep.ConnsTarget, rep.PeakConns)
	out += fmt.Sprintf("| 每连接速率 | %.1f ops/s |\n", rep.RatePerConn)
	out += fmt.Sprintf("| 负载窗口 / 墙钟 | %.1fs / %.1fs |\n\n", rep.LoadWindowS, rep.DurationS)

	out += "### 结果\n\n"
	out += "| 指标 | 值 |\n|---|---|\n"
	out += fmt.Sprintf("| 连接建立 / 失败 / 关闭 | %d / %d / %d |\n", rep.ConnsOpened, rep.ConnsFailed, rep.ConnsClosed)
	out += fmt.Sprintf("| SEND / SEND_ACK 超时 | %d / %d |\n", rep.Sends, rep.Timeouts)
	out += fmt.Sprintf("| DELIVER 接收 | %d |\n", rep.Delivers)
	out += fmt.Sprintf("| RECEIPT_ACK | %d |\n", rep.Receipts)
	out += fmt.Sprintf("| 广播发布 / 接收 / 发布失败 | %d / %d / %d |\n", rep.Published, rep.Broadcasts, rep.PublishFail)
	out += fmt.Sprintf("| PING 往返 | %d |\n", rep.Pings)
	out += fmt.Sprintf("| 错误 / 入站丢弃 | %d / %d |\n", rep.Errors, rep.InboundDrop)
	out += fmt.Sprintf("| 异常延迟样本（已丢弃） | %d |\n", rep.BadSamples)
	out += fmt.Sprintf("| 操作速率 | %.1f ops/s |\n", rep.OpsPerSecond)
	out += fmt.Sprintf("| SEND 速率 / DELIVER 速率 | %.1f / %.1f per s |\n\n", rep.SendsPerSec, rep.DeliversPerS)

	out += "### 客户端侧延迟分解（S5 §9.4）\n\n"
	out += r.rec.Markdown()
	out += fmt.Sprintf("\n> 本机时钟粒度：%.1fµs。%s\n", float64(rep.ClockGranularityNs)/1000.0,
		latencyResolutionHint(time.Duration(rep.ClockGranularityNs)))
	out += "> 口径：`send_e2e` = 客户端编码 + 服务端受理 + 投递（同一连接自发自收），\n"
	out += "> 分段为 encode（本地编码+入队）/ server（send→SEND_ACK，客户端本地时刻切分，纳秒精度）/ egress（ACK→DELIVER）；\n"
	out += "> `broadcast_e2e` = 端到端（publish→在线订阅端收到）；`publish_http` = POST /v1/broadcasts 往返。\n"
	return out
}

func writeJSON(path string, rep report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loadgen: "+format+"\n", args...)
	os.Exit(1)
}
