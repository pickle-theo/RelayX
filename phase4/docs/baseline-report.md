# RelayX Phase 3 基线报告（Phase 2 代码 · Linux 容器）

本报告记录 **Phase 2 代码在 Linux 容器内的性能基线**，所有数据均早于 Phase 3 的任何优化改动，后续优化以此为对照。全部数字直接摘自 `docs/bench/raw/` 下的原始压测报告；原始文件未采集的字段标注为「未采集」，不做估算。

## 1. 环境与口径

以下信息取自 `baseline-10k-idle.txt` 的 `== bench host ==`、`## parameters`、`## environment` 三段。

| 项目 | 取值 | 来源字段 |
|------|------|----------|
| 宿主 OS | Microsoft Windows 11 Pro 10.0.26300 | `host_os` |
| 宿主 CPU | Intel(R) Core(TM) i3-10105 CPU @ 3.70GHz | `host_cpu` |
| 宿主核数 | 4 物理核 / 8 逻辑核 | `host_cpu_cores` |
| 宿主内存 | 15.8 GB | `host_memory_gb` |
| Docker 客户端 / 服务端 | 29.9.2 / 29.9.2 | `docker_client_version`、`docker_server_version` |
| 运行链 | `bench/run.ps1` → `golang:1.27` → `bash bench/run.sh` | `runner` |
| 容器内核 | Linux 6.18.58.1-microsoft-standard-WSL2（x86_64） | `uname` |
| Go 版本 | go1.27.3 linux/amd64 | `go:` |
| 容器可见 CPU | 8 | `nproc` |
| 容器内存 | MemTotal 8052384 kB（MemAvailable 7088616 kB） | `meminfo` |
| `ulimit -n` | 1048576（请求值 1048576） | `ulimit_nofile` |
| 临时端口范围 | 32768–60999 | `ip_local_port_range` |
| Gateway 监听 | 127.0.0.1:19330 | `ADDR` |
| pprof 监听 | 127.0.0.1:16060 | `PPROF_ADDR` |
| PostgreSQL | `postgres://relayx@host.docker.internal:5432/relayx` | `DSN` |

**同机关系与口径说明**

- Gateway 与 loadtest 运行在**同一个容器**内，压测客户端通过 `127.0.0.1:19330` 回环连接 Gateway，二者共享 8 个逻辑 CPU 与同一份容器内存。
- PostgreSQL 运行在**宿主机的 Docker** 中，Gateway 经 `host.docker.internal:5432` 跨容器边界访问，数据库往返包含该转发开销。
- 所有场景为**单次运行**，原始报告中每场景仅一份样本，未采集重复运行的方差。
- **结果不可外推**：以上数字来自 4 核桌面级 CPU、WSL2 上的单容器、且压测端与被测端同机共享资源的环境，不能直接外推为多机部署或生产硬件下的容量结论；跨机网络、独立压测机、生产级 PostgreSQL 的表现需另行测量。

## 2. 1k 场景矩阵

数据来源：`baseline-1k-idle.txt`、`baseline-1k-heartbeat.txt`、`baseline-1k-steady-low.txt`、`baseline-1k-steady-high.txt`、`baseline-1k-slow.txt`、`baseline-1k-large.txt`、`baseline-1k-reconnect.txt`。
统一参数：`CONNS=1000`、`RAMP=5s`、`DURATION=30s`（reconnect 为 40s）；steady-high 的 `INTERVAL=100ms`，slow 为 `500ms`，其余为 `1s`。idle / heartbeat 为 `MODE=auto`，其余为 `MODE=echo`。

| 场景 | payload | 连接 成功/失败 | sent / echoed / missed | p50 (ms) | p99 (ms) | max (ms) | timeouts | 错误分类（非 timeout） | 慢消费者 | 重连 尝试/成功/失败 | retries | in_window | rss_per_conn_kb |
|------|---------|---------------|------------------------|----------|----------|----------|----------|------------------------|----------|---------------------|---------|-----------|-----------------|
| idle | 64 B | 1000 / 0 | 0 / 0 / 0 | 0.000 | 0.000 | 0.000 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 29.13 |
| heartbeat | 64 B | 1000 / 0 | 0 / 0 / 0 | 0.000 | 0.000 | 0.000 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 29.44 |
| steady-low | 64 B | 1000 / 0 | 27004 / 27004 / 0 | 0.260 | 0.513 | 5.785 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 44.38 |
| steady-high | 64 B | 1000 / 0 | 274550 / 274550 / 0 | 0.233 | 0.646 | 50.712 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 50.97 |
| slow-consumer | 64 B | 1000 / 0 | 48606 / 48606 / 0 | 0.234 | 0.519 | 9.697 | 0 | 全 0 | 100 | 0 / 0 / 0 | 未采集 | n/a | 46.11 |
| large-payload | 65536 B | 1000 / 0 | 27005 / 27005 / 0 | 0.465 | 2.588 | 5.726 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 178.91 |
| reconnect-storm | 64 B | 1000 / 0 | 36010 / 36010 / 0 | 0.275 | 0.617 | 22.846 | 0 | 全 0 | 0 | 1000 / 1000 / 0 | 未采集 | yes | 41.94 |

注：idle / heartbeat 的 `[rtt ms] samples: 0`，故 p50/p99/max 在原始文件中即为 `0.000`；「错误分类（非 timeout）」列对应 `connect_failures / login_failures / route_errors / decode_errors / unexpected_frames / write_errors`，七个场景均为 0。1k 档 reconnect 报告未提供 `reconnect_retries` 字段，记「未采集」；非重连场景不发生重连，同为「未采集」。slow-consumer 的背压丢帧计数在原始报告中未采集（仅有 `slow_consumers=100`）。

**观察**

1. 1k steady-high 吞吐约 **9143 msg/s**（274550 / 30.028s），p99 0.646ms；1k steady-low 约 **900 msg/s**（27004 / 30.01s），p99 0.513ms；两者 timeouts 均为 0。
2. 1k large-payload（65536 B）的 p99 2.588ms 约为 steady-low（0.513ms）的 **5.0 倍**，客户端 `rss_per_conn_kb` 178.91KB 约为 steady-low（44.38KB）的 **4.0 倍**。
3. 1k slow-consumer 在 100 个慢消费者下 `sent=echoed=48606`、`missed=0`、timeouts 0、p99 0.519ms，与 steady-low 的 p99 0.513ms 处于同一量级，未观察到超时或丢帧。
4. 1k reconnect-storm 的 1000 次重连全部成功、`in_window=yes`，p99 0.617ms，max 22.846ms。

## 3. 10k 场景矩阵

数据来源：`baseline-10k-idle.txt`、`baseline-10k-heartbeat.txt`、`baseline-10k-steady-low.txt`、`baseline-10k-steady-high.txt`、`baseline-10k-slow.txt`、`baseline-10k-large.txt`、`baseline-10k-reconnect.txt`。
统一参数：`CONNS=10000`、`DURATION=40s`、`RAMP=15s`；steady-low / slow 的 `INTERVAL=5s`，large 为 `10s`，其余为 1s；reconnect 单独为 `DURATION=75s`、`RAMP=30s`、`INTERVAL=5s`、`TIMEOUT=20s`。

| 场景 | payload | 连接 成功/失败 | sent / echoed / missed | p50 (ms) | p99 (ms) | max (ms) | timeouts | 错误分类（非 timeout） | 慢消费者 | 重连 尝试/成功/失败 | retries | in_window | rss_per_conn_kb |
|------|---------|---------------|------------------------|----------|----------|----------|----------|------------------------|----------|---------------------|---------|-----------|-----------------|
| idle | 64 B | 10000 / 0 | 0 / 0 / 0 | 0.000 | 0.000 | 0.000 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 23.09 |
| heartbeat | 64 B | 10000 / 0 | 0 / 0 / 0 | 0.000 | 0.000 | 0.000 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 22.72 |
| steady-low | 64 B | 10000 / 0 | 60142 / 60142 / 0 | 0.219 | 0.492 | 127.704 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 28.14 |
| steady-high | 64 B | 10000 / 0 | 320725 / 320725 / 0 | 0.231 | 0.724 | 97.971 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 37.09 |
| slow-consumer | 64 B | 10000 / 0 | 53142 / 53142 / 0 | 0.227 | 1.014 | 149.652 | 0 | 全 0 | 1000 | 0 / 0 / 0 | 未采集 | n/a | 26.46 |
| large-payload | 65536 B | 10000 / 0 | 26766 / 26766 / 0 | 0.429 | 9.547 | 122.788 | 0 | 全 0 | 0 | 0 / 0 / 0 | 未采集 | n/a | 171.98 |
| reconnect-storm | 64 B | 10000 / 0 | 106119 / 106119 / 0 | 0.259 | 160.235 | 1149.339 | 0 | 全 0 | 0 | 10000 / 10000 / 0 | 0 | yes | 42.26 |

注：idle / heartbeat 的 `[rtt ms] samples: 0`，p50/p99/max 为原始文件中的 `0.000`；「错误分类（非 timeout）」七项在七个场景中均为 0。10k 档仅 reconnect 场景输出 `reconnect_retries=0`，其余场景未输出该字段，记「未采集」。slow-consumer 背压丢帧计数未采集，仅 `slow_consumers=1000`。

**观察**

1. 10k steady-high 吞吐约 **7975 msg/s**（320725 / 40.218s）、p99 0.724ms、服务端 20,007 协程；同场景的 profile 运行 `profile-10k-high.txt` 为 370807 / 45.233s ≈ **8198 msg/s**、p99 1.624ms、20,011 协程（峰值）。
2. 10k idle 稳定态 `heap_alloc_mb=147.01`、`goroutines=20007`、`active_conns=10000`，即约 **14.7KB / 连接**、约 **2 个协程 / 连接**；`gc_pause_ms=0.1`。
3. 10k large-payload 的 p99 9.547ms 约为 1k large（2.588ms）的 **3.7 倍**，max 122.788ms，客户端 `rss_per_conn_kb` 171.98KB。
4. 10k reconnect-storm 的 10000 次重连全部成功、retries 0、`in_window=yes`，但 p99 升至 160.235ms、max 1149.339ms；同一时段 gateway 出现 `gc_pause_ms=9.48`。

## 4. 连接规模边界（50k / 100k）

数据来源：`baseline-50k-idle.txt`（`RAMP=60s`、`DURATION=100s`、`TIMEOUT=10s`）与 `baseline-100k-idle.txt`（`RAMP=20s`、`DURATION=35s`、`TIMEOUT=10s`）。

| 目标连接数 | exit_code | 建连 成功/失败 | success_rate | 最大 active_conns | 失败阶段 / 首个错误 | 服务端 goroutines 峰值 | 服务端 heap_alloc 峰值 | 客户端 rss_per_conn_kb |
|-----------|-----------|---------------|--------------|------------------|---------------------|----------------------|----------------------|----------------------|
| 50000 | 1 | 28236 / 21764 | 56.47% | 28232 | dial；`conn 28254: dial: dial tcp 127.0.0.1:19330: connect: cannot assign requested address` | 56471 | 459.64 MB | 16.98 |
| 100000 | 1 | 39740 / 60260 | 39.74% | 28232 | dial；`conn 60348: dial: dial tcp 127.0.0.1:19330: connect: cannot assign requested address`，另有 `login_failures=14065` | 56471 | 623.72 MB | 10.57 |

补充：50k 档 `connect_failures=21764`、`login_failures=0`；100k 档 `connect_failures=60260`、`login_failures=14065`。两档 `ulimit_nofile` 均为 1048576，`ip_local_port_range` 均为 `32768 60999`。50k 档服务端在 28232 连接时 `heap_alloc_mb=455.45`（04:14:00），收尾前升至 459.64（04:15:05）。100k 档服务端堆在 04:16:43 达 623.72MB，收尾阶段（04:16:49、04:16:54）依次为 365.34MB、529.82MB。100k 档还出现 `WARN gateway stats: count pending deliveries failed error="store: count pending deliveries: context deadline exceeded"`，对应 `deliveries_pending=-1`。

**观察**

1. 两档的 `active_conns` 均稳定停在 **28232**，与临时端口范围可用端口数 **28232**（60999 − 32768 + 1）完全一致；首个错误均为 `cannot assign requested address`，指向客户端侧临时源端口耗尽，而不是 fd 上限（`ulimit_nofile=1048576`）。`netstat` / TIME_WAIT 统计未采集。
2. 50k 档失败集中在 dial 阶段（`login_failures=0`）；100k 档除 60260 次 dial 失败外，另有 14065 次 login 失败，失败阶段扩展到登录。
3. 服务端在 28232 连接时约 56k 协程、459.64MB 堆（50k 档）；100k 档峰值 623.72MB，且统计查询本身超时（`deliveries_pending=-1`），说明管理面查询已受到资源挤压。
4. 100k 档 `active_sessions` 峰值 11454，远低于 `active_conns` 的 28232，与 `login_failures=14065` 一致：大量连接只完成 TCP 建连、未完成会话建立。

## 5. 可靠链路小规模基线（200 / 500 / 1000 msg/s）

数据来源：`baseline-200-reliable.txt`、`baseline-500-reliable.txt`、`baseline-1k-reliable.txt`。三份均为 `MODE=reliable`、`SCENARIO=steady-low`、`INTERVAL=1s`、`RAMP=5s`、`TIMEOUT=10s`；200/500 档 `DURATION=25s`，1000 档 `DURATION=45s`。

| 目标速率 | 连接数 | exit_code | sent | send_ack | delivered | business_acked | ack_missed | accept p50 (ms) | accept p99 (ms) | timeouts | unexpected_frames | deliveries_pending 轨迹 | db_open_conns |
|---------|--------|-----------|------|----------|-----------|----------------|------------|-----------------|-----------------|----------|-------------------|-------------------------|---------------|
| 200 msg/s | 200 | 0 | 4400 | 4400 | 4400 | 4400 | 0 | 5.205 | 110.070 | 0 | 0 | 2 → 0 → 1 → 0 → 0 | 6 → 8 |
| 500 msg/s | 500 | 0 | 11002 | 11002 | 11002 | 11002 | 0 | 19.068 | 329.174 | 0 | 0 | 9 → 5 → 8 → 7 → 6 | 16 |
| 1000 msg/s | 1000 | 1 | 41937 | 28058 | 28125 | 28125 | 13812 | 4323.400 | 10723.803 | 20834 | 13977 | 99 → 1021 → 1970 → 3050 → 4390 → 5427 → 6379 → -1 → -1 → 9868 → 10031 | 16 |

注：三档 `duplicate_ack` 均为 0；`receipt_acked` 与 `delivered` 数值相同（4400 / 11002 / 28125）。`deliver_rtt` 的 p50/p99 与 `accept_rtt` 基本一致（1000 档为 4323.400 / 10723.804）。`-1` 表示原始报告中 `deliveries_pending=-1`，即 pending 统计查询失败（`count pending deliveries: context deadline exceeded`）。200 档 `db_open_conns` 由 6 变为 8；500/1000 档全程为 16。

**观察**

1. 200 → 500 msg/s：`ack_missed` 均为 0、timeouts 均为 0，但 accept p50 由 5.205ms 升至 19.068ms（约 **3.7 倍**），p99 由 110.070ms 升至 329.174ms（约 **3.0 倍**）。
2. 500 档 `deliveries_pending` 在 9 / 5 / 8 / 7 / 6 之间波动、未累积；1000 档从 99 起单调升至 6379，此后统计查询超时（-1），结束时达 **10031**，积压未被消化。
3. **1000 msg/s 出现性能悬崖**：`exit_code=1`；sent 41937 但 send_ack 仅 28058（差 13879），`ack_missed=13812`、`timeouts=20834`、`unexpected_frames=13977`；accept p50 **4323.400ms**、p99 **10723.803ms**、max 12841.370ms。
4. 1000 档实测吞吐约 **762 msg/s**（41937 / 55.011s），仅约为 500 档 439 msg/s（11002 / 25.033s）的 **1.7 倍**，而目标速率翻倍；`db_open_conns` 全程为 16。

## 6. 原始文件索引

相对 `phase3/` 的路径：

- `docs/bench/raw/baseline-1k-idle.txt`
- `docs/bench/raw/baseline-1k-heartbeat.txt`
- `docs/bench/raw/baseline-1k-steady-low.txt`
- `docs/bench/raw/baseline-1k-steady-high.txt`
- `docs/bench/raw/baseline-1k-slow.txt`
- `docs/bench/raw/baseline-1k-large.txt`
- `docs/bench/raw/baseline-1k-reconnect.txt`
- `docs/bench/raw/baseline-10k-idle.txt`
- `docs/bench/raw/baseline-10k-heartbeat.txt`
- `docs/bench/raw/baseline-10k-steady-low.txt`
- `docs/bench/raw/baseline-10k-steady-high.txt`
- `docs/bench/raw/baseline-10k-slow.txt`
- `docs/bench/raw/baseline-10k-large.txt`
- `docs/bench/raw/baseline-10k-reconnect.txt`
- `docs/bench/raw/baseline-50k-idle.txt`
- `docs/bench/raw/baseline-100k-idle.txt`
- `docs/bench/raw/baseline-200-reliable.txt`
- `docs/bench/raw/baseline-500-reliable.txt`
- `docs/bench/raw/baseline-1k-reliable.txt`
- `docs/bench/raw/profile-10k-steady.txt`
- `docs/bench/raw/profile-10k-high.txt`
- `docs/bench/raw/profiles/cpu-top.txt`
- `docs/bench/raw/profiles/cpu-top-high.txt`
- `docs/bench/raw/profiles/heap-top.txt`
- `docs/bench/raw/profiles/heap-top-high.txt`
- `docs/bench/raw/profiles/goroutine-top.txt`
- `docs/bench/raw/profiles/block-top.txt`
- `docs/bench/raw/profiles/mutex-top.txt`

（`profile-1k-reliable.txt`、`profiles/cpu-reliable.txt`、`profiles/goroutine-reliable-cum.txt`、`profiles/block-reliable-cum.txt` 的引用见 `bottleneck-analysis.md`。）
