# Tasks

- [x] Task 1: 初始化 Go module 与目录骨架
  - [x] SubTask 1.1: 在项目根创建 `phase0/` 子目录并执行 `go mod init relayx/phase0`，设置 `go 1.27`
  - [x] SubTask 1.2: 创建 `phase0/cmd/gateway`、`phase0/cmd/client`、`phase0/internal/protocol`、`phase0/internal/transport`、`phase0/internal/config`、`phase0/docs`、`phase0/docs/adr`
  - [x] SubTask 1.3: 在 `phase0/internal/config` 中用 `flag` 实现参数解析（`-addr`、`-max-body`、`-heartbeat-interval`、`-heartbeat-timeout`、`-idle-timeout`、`-send-queue-size`、`-shutdown-timeout`）并提供默认值校验

- [x] Task 2: 实现二进制帧编解码
  - [x] SubTask 2.1: 定义 `Frame`、帧常量、21 字节帧头布局与哨兵错误
  - [x] SubTask 2.2: 实现 `Encode`（含 BodyLen 与 CRC32 计算）与 `Decode`（基于 `bufio.Reader` + `io.ReadFull`）
  - [x] SubTask 2.3: 编写表驱动单测：正常往返、空 Body、最大 Body、Magic/Version/Flags 非法、超长、CRC 错误、粘包、半包、帧中途 EOF

- [x] Task 3: 实现单连接读写管线
  - [x] SubTask 3.1: 定义 `Conn` 抽象：持有 `net.Conn`、连接 ID、发送队列、关闭状态与 `sync.Once` 关闭逻辑
  - [x] SubTask 3.2: 实现单一 Writer 协程消费有界 `chan []byte`；队列满时记录告警并关闭连接
  - [x] SubTask 3.3: 实现读循环：按帧解码、分发到处理器、每次成功读取后刷新读 Deadline
  - [x] SubTask 3.4: 保证 `Close` 幂等：关闭底层 socket、停止读写协程、注销注册表，重复调用不 panic

- [x] Task 4: 实现 TCP Server 主干
  - [x] SubTask 4.1: 实现 `Listen`/`Serve`：Accept 循环、临时错误退避、连接注册表（`map[uint64]*Conn` + `RWMutex`）
  - [x] SubTask 4.2: 实现 DATA 帧回显处理器与 PING→PONG 响应，并输出结构化 slog 日志
  - [x] SubTask 4.3: 实现 `Shutdown(ctx)`：停止 Accept、广播 CLOSE 帧、等待所有连接协程退出或超时
  - [x] SubTask 4.4: 在 `phase0/cmd/gateway` 接入信号处理（`os.Interrupt`），打印监听地址与活动连接数

- [x] Task 5: 实现心跳与超时
  - [x] SubTask 5.1: 服务端为每连接启动心跳 ticker，按 `-heartbeat-interval` 发送 PING 帧
  - [x] SubTask 5.2: 记录最近入站帧时间，超过 `-heartbeat-timeout` 关闭连接
  - [x] SubTask 5.3: 读循环使用 `-idle-timeout` 设置读 Deadline，超时视为连接失效并清理

- [x] Task 6: 实现 CLI 客户端
  - [x] SubTask 6.1: 连接 gateway 并支持 `-name`、`-interval`、`-count`、`-payload-size` 参数
  - [x] SubTask 6.2: 自动响应 PING、打印收到的 DATA/ERROR/CLOSE 帧
  - [x] SubTask 6.3: 定时发送 DATA 帧，支持 Ctrl+C 优雅退出与 `-count` 发满后退出

- [x] Task 7: 补齐集成测试与质量门禁
  - [x] SubTask 7.1: 使用 `127.0.0.1:0` 真实监听，编写多客户端回显、心跳超时、断线清理、优雅关闭集成测试
  - [x] SubTask 7.2: 编写协程泄漏检查（测试前后 `runtime.NumGoroutine` 对比，带重试）
  - [x] SubTask 7.3: 全量执行 `go vet ./...` 与 `go test ./...` 并修复所有问题；本机 Windows 无 cgo 编译器，`-race` 改由本机 Docker（golang:1.27 镜像）执行 `go test -race -count=2 ./...`，结果全部通过

- [x] Task 8: 完成千连接压测
  - [x] SubTask 8.1: 实现压测入口（`phase0/cmd/loadtest` 或 `-tags stress` 测试），支持 `-conns 1000`、`-duration 60s`
  - [x] SubTask 8.2: 采集连接成功率、服务端内存、协程数、RTT P50/P99，输出到 `phase0/docs/压测报告.md`

- [x] Task 9: 补充 Phase 0 文档
  - [x] SubTask 9.1: `phase0/docs/README.md`：运行方式、配置项、帧格式表、验收对照
  - [x] SubTask 9.2: `phase0/docs/adr/ADR-001.md`（为什么使用标准库 net）、`phase0/docs/adr/ADR-002.md`（为什么单连接单 Writer），按背景/问题/方案/理由/优缺点/演进编写

# Task Dependencies
- Task 2 依赖 Task 1
- Task 3 依赖 Task 2
- Task 4 依赖 Task 3
- Task 5 依赖 Task 4
- Task 6 依赖 Task 2（与 Task 4 可并行）
- Task 7 依赖 Task 4、Task 5、Task 6
- Task 8 依赖 Task 7
- Task 9 依赖 Task 8
