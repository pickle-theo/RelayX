package main

import "time"

// measureClockGranularity 估计本机时钟的可分辨粒度（纳秒）。
//
// 为什么必须测：S5 §9.4 的延迟分解要区分「编解码 / 排队 / 网络」，
// 而 Windows 某些虚拟机上 time.Now() 的粒度可达 0.5–15ms。此时任何小于
// 一个粒度的延迟都会被测成 0（实测本机 ping RTT ≈ 0.54ms 落在同一 tick 内，
// P50 显示 0µs）。把粒度写进报告，读数字的人才知道 0µs 是什么意思，
// 而不是误以为「延迟真的是 0」。
//
// 方法：连续采样 n 次 time.Now()，取「两次不同读数之间的最大间隔」作为粒度上界。
func measureClockGranularity(n int) time.Duration {
	if n <= 0 {
		n = 200000
	}
	var maxDelta time.Duration
	prev := time.Now()
	for i := 0; i < n; i++ {
		now := time.Now()
		if d := now.Sub(prev); d > maxDelta {
			maxDelta = d
		}
		prev = now
	}
	return maxDelta
}

// latencyResolutionHint 依据时钟粒度给出报告里的解读提示。
func latencyResolutionHint(granularity time.Duration) string {
	if granularity <= 0 {
		return ""
	}
	if granularity < 100*time.Microsecond {
		return "本机时钟粒度足够细，亚毫秒延迟可直接解读。"
	}
	return "本机时钟粒度较粗：小于该粒度的延迟会被量化为 0（如编码耗时、环回 P0 往返），" +
		"这些列请按「< 粒度」理解；亚毫秒结论请以 Linux 环境复跑为准。"
}
