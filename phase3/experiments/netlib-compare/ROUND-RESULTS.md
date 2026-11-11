# ROUND-RESULTS — 标准库 vs gnet

> 状态：**仅完成 Windows 本机功能冒烟，正式对比数据待容器压测后填入。**
> 本文档不包含任何推测或伪造的基准数字。

## 环境

| 项 | 值 |
| --- | --- |
| Go | go1.27.3 windows/amd64 |
| gnet | github.com/panjf2000/gnet/v2 v2.10.0 |
| 压测客户端 | `phase3/cmd/loadtest`（echo 模式，`-scenario` 留空） |
| CPU/内存 | 未记录（Windows 本机，非基准环境） |

## 冒烟（Windows 本机，仅验证功能）

用途：确认两个服务都能被现有 `loadtest` 的 echo 模式驱动，帧头字段与主线一致。
**这不是基准测试，数字不可用于性能结论。**

命令（服务分别以 `go run`/二进制启动后执行）：

```bash
loadtest -addr 127.0.0.1:19000 -conns 20 -duration 3s -interval 200ms -ramp 0s
loadtest -addr 127.0.0.1:19001 -conns 20 -duration 3s -interval 200ms -ramp 0s
```

| 服务 | 连接 | sent | echoed | missed | decode_errors | timeouts | 退出码 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| stdlib（`127.0.0.1:19000`） | 20/20 成功 | 280 | 280 | 0 | 0 | 0 | 0 |
| gnet（`127.0.0.1:19001`） | 20/20 成功 | 280 | 280 | 0 | 0 | 0 | 0 |

RTT（同一台机器、回环、20 连接、200ms 间隔）：

| 服务 | p50 (ms) | p90 (ms) | p99 (ms) | max (ms) | avg (ms) |
| --- | --- | --- | --- | --- | --- |
| stdlib | 0.000 | 0.508 | 0.682 | 0.682 | 0.137 |
| gnet | 0.000 | 0.597 | 2.079 | 2.079 | 0.222 |

结论（仅功能层面）：两个服务的 `sent == echoed` 且 `decode_errors == 0`，
说明 21 字节帧头、CRC32 与 MsgID 回显语义与主线一致。

## 容器压测结果

环境：Docker Desktop + `golang:1.27` Linux 容器（8 核 / 8GB，`ulimit -n=1048576`），
服务端与压测客户端在**同一容器**内通过 `127.0.0.1` 通信。服务端 RSS 与 CPU 由
`ps -o rss=,%cpu=,nlwp=` 在压测结束时采样；CPU% 是进程生命周期内的平均值
（可超过 100，表示多核并行）。每档只跑一轮，不是三次中位数。

命令：

```bash
docker run --rm --ulimit nofile=1048576 \
  -v "<repo>/phase3:/app" -v relayx-phase3-gomod:/go/pkg/mod \
  -e GOPROXY=https://goproxy.cn,direct \
  -e CONNS=10000 -e DURATION=40s -e INTERVAL=1s -w /app \
  golang:1.27 bash -c "./experiments/netlib-compare/bench/compare.sh"
```

### 档位 A：10,000 连接，1 msg/s/连接（约 1 万 msg/s）

| 服务 | conns | sent | echoed | decode_errors | p50 (ms) | p90 (ms) | p99 (ms) | max (ms) | 服务端 RSS (MB) | CPU (%) | 线程数 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| stdlib | 10000/10000 | 391212 | 391212 | 0 | 0.326 | 1.728 | 12.370 | 87.482 | 18.4 | 28.0 | 17 |
| gnet | 10000/10000 | 390182 | 390182 | 0 | 0.357 | 4.357 | 36.134 | 88.363 | 286.0 | 38.1 | 17 |

### 档位 B：1,000 连接，100 msg/s/连接（约 10 万 msg/s）

| 服务 | conns | sent | echoed | decode_errors | p50 (ms) | p90 (ms) | p99 (ms) | max (ms) | 服务端 RSS (MB) | CPU (%) | 线程数 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| stdlib | 1000/1000 | 1982142 | 1982142 | 0 | 1.054 | 7.063 | 23.429 | 79.299 | 13.1 | 187 | 19 |
| gnet | 1000/1000 | 1970558 | 1970558 | 0 | 0.513 | 4.260 | 19.495 | 204.362 | 93.6 | 236 | 19 |

原始报告：`phase3/docs/bench/raw/netlib/{stdlib,gnet}-conns{10000,1000}.txt`
与同名 `-server-resource.txt`。

### 观察

1. 吞吐差异在 1% 以内（391212 vs 390182；1982142 vs 1970558），两种模型都能驱动同等流量。
2. 内存差异显著：10k 连接时 gnet RSS 约 286MB，stdlib 仅 18.4MB（约 15.5 倍）；
   1000 连接高频时 93.6MB vs 13.1MB（约 7.2 倍）。gnet 为每条连接维护 ring buffer。
3. 尾延迟互有胜负：10k 低频 gnet p99 明显更差（36.1ms vs 12.4ms），
   高频档 gnet p50/p99 更好（0.513/19.5ms vs 1.054/23.4ms），但 max 更高（204ms vs 79ms）。
4. 线程数相同（GOMAXPROCS=8，实际线程 17/19），说明差异来自 goroutine 模型与每连接缓冲，
   而不是 OS 线程数量；CPU 上 gnet 反而更高（10k：38.1% vs 28.0%；高频：236% vs 187%）。

### 结论

在 RelayX 的目标区间（万级长连接、每连接每消息有会话/ACK 状态、要求可控内存），
标准库 per-connection 模型在本轮实验中不输吞吐、内存低一个数量级；
gnet 的优势（减少 goroutine 调度）没有转化为吞吐收益，反而带来更高的常驻内存。
因此主线继续使用标准库 `net`，详见 `phase3/docs/adr/ADR-012.md`。

> 注意：容器内客户端与服务端共享 CPU，数字只用于同环境相对比较，不可外推为绝对容量。
