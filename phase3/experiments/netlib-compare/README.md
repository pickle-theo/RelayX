# netlib-compare：标准库 vs gnet 的独立对比实验

这是一个**实验模块**，不属于 RelayX Phase 3 主线，不参与主 module 的依赖，也不被
`go.work` 收录。它存在的唯一目的是：用同一套 RelayX 帧协议和同一个压测客户端
（`phase3/cmd/loadtest`），对比两种 Go 网络模型的实现成本与吞吐/延迟：

| 目录 | 模型 | 默认地址 |
| --- | --- | --- |
| `stdlib/` | `net.Listen` + 每连接 2 个 goroutine（读/写分离，单 writer） | `127.0.0.1:19000` |
| `gnet/` | `github.com/panjf2000/gnet/v2` event loop（`OnTraffic` 中解析并回写） | `127.0.0.1:19001` |

`frame/` 是最小帧编解码包。它从 `phase3/internal/protocol` 复制而来（未 import，
保持实验模块自包含），字节序与字段偏移与主线完全一致。

## 目录结构

```
experiments/netlib-compare/
├── go.mod                 # 独立 module：relayx/phase3/experiments/netlib-compare
├── frame/frame.go         # 21 字节帧头 + CRC32 的最小实现
├── stdlib/main.go         # 标准库 echo 服务
├── gnet/main.go           # gnet v2 echo 服务
├── bench/compare.sh       # Linux/macOS 压测脚本
├── bench/compare.ps1      # Windows 压测脚本
└── reports/               # 脚本输出（运行时创建，不入库）
```

## 协议（与 Phase 1 echo 语义一致）

固定 21 字节大端帧头：

| 偏移 | 长度 | 字段 |
| --- | --- | --- |
| 0  | 2 | magic = `0x52 0x58`（"RX"，大端 `0x5258`） |
| 2  | 1 | version = `0x01` |
| 3  | 1 | type（DATA = `0x03`） |
| 4  | 1 | flags（必须为 0） |
| 5  | 8 | MsgID |
| 13 | 4 | body length |
| 17 | 4 | CRC32（IEEE，仅覆盖 body） |

服务端行为：收到完整 DATA 帧后原样回显一帧 DATA（MsgID、Body 不变，CRC 按 body
重新计算）。非 DATA 帧被忽略。body 上限 1 MiB，与 `loadtest` 默认 `-max-body`
一致。magic/version/flags/type/CRC 任一不合法即断开连接。

## 构建

由于仓库根目录的 `go.work` 只 `use` 了各 Phase 主 module，本实验 module 不在其中，
所以**必须关闭 workspace 模式**构建（不需要也不允许改根 `go.work`）：

```bash
# Windows PowerShell
$env:GOWORK = "off"
go build ./...
go build -o stdlib-echo.exe ./stdlib
go build -o gnet-echo.exe ./gnet

# Linux / macOS
GOWORK=off go build ./...
```

首次构建需要联网拉取 gnet（`go mod tidy` 已生成 `go.sum`）。若默认代理不可达，
可改用镜像：`$env:GOPROXY = "https://goproxy.cn,direct"`。

## 手工复现压测

1. 构建主项目压测工具（`phase3` 下，可继续使用 workspace）：

   ```bash
   cd phase3
   go build -o loadtest ./cmd/loadtest        # Windows: -o loadtest.exe
   ```

2. 启动任意一个服务（两个终端各跑一个也可以）：

   ```bash
   cd phase3/experiments/netlib-compare
   $env:GOWORK = "off"
   go run ./stdlib -addr 127.0.0.1:19000
   go run ./gnet   -addr 127.0.0.1:19001
   ```

   `-heartbeat` 参数被 stdlib 服务接受但忽略，仅用于和主线 CLI 对齐。

3. `-scenario` 留空即为 echo 模式：客户端发送 DATA 并校验回显。

   ```bash
   ./loadtest -addr 127.0.0.1:19000 -conns 200 -duration 20s \
       -interval 100ms -ramp 5s -payload-size 64 -out stdlib.txt
   ./loadtest -addr 127.0.0.1:19001 -conns 200 -duration 20s \
       -interval 100ms -ramp 5s -payload-size 64 -out gnet.txt
   ```

4. 判断标准：报告里 `sent == echoed`、`decode_errors == 0`、`connect_failures == 0`。

## 一键对比脚本

```powershell
# Windows，在实验 module 根目录执行
./bench/compare.ps1 -Conns 200 -Duration 20s -Interval 100ms
```

```bash
# Linux / macOS
./bench/compare.sh            # 环境变量：CONNS / DURATION / INTERVAL / PAYLOAD_SIZE
```

脚本会构建两个 echo 二进制和 `loadtest`，依次启动、压测（固定 `-ramp 0s`，避免
ramp 影响吞吐口径）、关闭服务，报告写入 `reports/`。脚本用固定端口，请先确保
19000/19001 未被占用。

## 结果记录

`ROUND-RESULTS.md` 记录可复现的运行结果。当前只有 Windows 冒烟（功能验证），
正式对比数据需在容器环境压测后填入；不要用本机冒烟数字做结论。

## 与主线的关系

- 本 module 有自己的 `go.mod` / `go.sum`，`gnet` 只出现在这里。
- 主 module `phase3` 的依赖不含 gnet，可用
  `go list -m all | Select-String -Pattern gnet`（PowerShell）或
  `go list -m all | grep gnet`（bash）确认无输出。
- 实验代码不进入任何生产路径，也不被 `phase3` 的构建或测试引用。
