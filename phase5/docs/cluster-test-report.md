# Phase 4 集群验证报告

## 1. 结论

Phase 4 的集群路径已在本机完成三类端到端验证，全部通过：

- 3 节点冒烟：跨节点投递、跨节点重复登录踢人、路由归属均符合预期。
- 故障注入：强制杀死一个节点（`Stop-Process -Force`，无清理机会）后，客户端在 0.18s 内检测到断线、0.19s 内重连；死亡节点的路由在 2.19s 后被回收。
- Redis 不可用：已登录会话的 SEND 仍然持久化，新登录快速失败；Redis 恢复后客户端通过 PULL 取回离线消息。

同时，`gofmt`、`go vet`、`go test ./...`（真实 PostgreSQL + Redis）与容器内 `go test -race -count=1 ./...` 全部通过。本文只陈述本次单机、短 TTL 测试能证明的结论，不代表生产规模或高可用能力。

## 2. 环境

| 项目 | 值 |
| --- | --- |
| 操作系统 | Windows 10.0.26300 |
| Go | go1.27.3 windows/amd64 |
| 容器 | Docker Desktop |
| PostgreSQL | 容器 `relayx-phase2-pg`，`127.0.0.1:5432` |
| Redis | 容器 `relayx-phase4-redis`，`redis:7-alpine`，`127.0.0.1:6379` |
| 节点 | node1 `5001` / `9101`，node2 `5002` / `9102`，node3 `5003` / `9103` |
| 测试 TTL | heartbeat 500ms、node-ttl 1500ms、cleanup 500ms |
| 测试时间 | 2026-11-11（Asia/Shanghai，最终二进制复跑） |

测试 TTL 远短于生产默认值（5s / 15s / 10s），目的是在十几秒内观察完整的故障窗口。生产默认窗口见第 8 节。

## 3. 脚本

| 脚本 | 作用 | 证据目录 |
| --- | --- | --- |
| `scripts/cluster-common.ps1` | 公共函数：检查 Docker/容器、构建二进制、启动/停止节点、等待端口与 Redis、抓取输出、汇总 | 无独立证据，被其它脚本点源加载 |
| `scripts/cluster-smoke.ps1` | 3 节点冒烟：健康检查、跨节点投递、跨节点重复登录踢人、路由断言 | `docs/bench/raw/cluster/smoke/` |
| `scripts/cluster-fault.ps1` | 故障注入：投递 2 条后 `Stop-Process -Force` 杀死 node3，观察客户端切换与路由回收 | `docs/bench/raw/cluster/fault/` |
| `scripts/cluster-redis-outage.ps1` | `docker stop -t 1` 停 Redis，验证 SEND 持久化、登录失败、恢复后 PULL | `docs/bench/raw/cluster/redis-outage/` |

三个场景脚本都没有参数（`param()` 为空），端口、容器名、TTL 与客户端参数都固化在脚本中。每次运行会覆盖对应证据目录，并在 `summary.txt` 中记录退出码、耗时、进程参数与逐项 PASS/FAIL。

## 4. 场景结果

### 4.1 冒烟（13.27s）

| 检查 | 实测 | 结果 |
| --- | --- | --- |
| 3 个网关的 6 个端口全部可用 | 1.48s | PASS |
| node1 的 bob 向 node3 的 alice 发送 3 条 | 3 个 `SEND_ACK accepted` | PASS |
| node3 收到跨节点 DELIVER | 3 条，耗时 0.02s | PASS |
| alice 在 node2 重复登录 | node3 旧连接收到 `server closed connection` | PASS |
| 旧连接退出 | 0.47s 内退出，退出码 0 | PASS |
| 路由归属 | 最终为 node2，不再包含 node3 | PASS |

说明：重复登录的客户端在退出时会删除自己的路由，因此脚本额外启动一个 node2 的常驻会话，再断言路由指向 node2；这是证据文件中的 INFO 项，不是绕过检查。

### 4.2 故障注入（13.68s）

| 检查 | 实测 | 结果 |
| --- | --- | --- |
| 故障前 node1 向 node3 的 alice 投递 | 2 条全部收到 | PASS |
| 杀死 node3 | `Stop-Process -Force`，强制终止且无清理机会 | PASS |
| 客户端检测断线 | 日志出现 `connection lost, switching gateway node`，0.18s | PASS |
| 客户端重连 | 0.19s 内完成，连接 node1 | PASS |
| 死亡节点回收 | `relayx:node:node3` 与 `relayx:node_routes:node3` 在 2.19s 后消失 | PASS |
| 切换后继续投递 | node1 再发 2 条，alice 全部收到 | PASS |
| 新路由 | 指向 node1，不再指向 node3 | PASS |

### 4.3 Redis 不可用（23.23s）

| 检查 | 实测 | 结果 |
| --- | --- | --- |
| 停止 Redis | `docker stop -t 1`，0.91s | PASS |
| 停 Redis 期间 bob 发送 2 条 | 2 个 `SEND_ACK accepted` | PASS |
| 停 Redis 期间 carol 登录 | 进程退出码 1 | PASS |
| 登录失败原因 | 日志为 login rejected / cluster unavailable（`LOGIN_ACK internal`） | PASS |
| Redis 恢复 | 0.7s 后 PONG | PASS |
| 恢复后 alice 登录并 PULL | 拉到 2 条离线消息（`deliveries=2`，`max pull_done count=2`） | PASS |

### 4.4 Go 测试

| 命令 | 结果 |
| --- | --- |
| `gofmt -l .` | 无输出 |
| `go vet ./...` | 通过 |
| `go test ./...`（真实 PostgreSQL + Redis） | 全部 ok |
| 容器内 `go test -race -count=1 ./...`（`golang:1.27`） | 全部 ok |

`-race` 在本机无法执行，因为 Windows 环境没有 gcc；race 测试在容器内完成。包耗时：

| 包 | 耗时 |
| --- | --- |
| `loadtest` | 1.114s |
| `cluster` | 1.215s |
| `config` | 1.040s |
| `delivery` | 3.870s |
| `gateway` | 12.946s |
| `protocol` | 1.116s |
| `registry` | 2.758s |
| `session` | 1.208s |
| `store` | 6.068s |
| `transport` | 2.698s |

## 5. 证据路径

每个场景目录包含 `summary.txt`、各进程的 `.cmd`、`.out.log`、`.err.log`：

- `phase4/docs/bench/raw/cluster/smoke/summary.txt`
- `phase4/docs/bench/raw/cluster/fault/summary.txt`
- `phase4/docs/bench/raw/cluster/redis-outage/summary.txt`

`summary.txt` 中的 `param:` 行记录了节点地址、TTL 与客户端参数，故障场景另有故障方式记录；`PASS:`/`INFO:` 行记录了逐项断言与实测时间。

## 6. 复现

前置条件：

- Docker Desktop 可用，已启动 PostgreSQL 与 Redis 容器。
- 端口 `5001` 到 `5003`、`9101` 到 `9103` 未被占用。
- 在 `phase4/` 目录执行；脚本会自动构建 `bin/gateway.exe` 与 `bin/client.exe`。

使用 Make（Windows 建议在 Git Bash / MSYS2 / WSL 中执行）：

```bash
cd phase4
make db-up
make redis-up
make cluster-smoke
make cluster-fault
make cluster-redis-outage
```

不使用 Make，直接调用 PowerShell：

```powershell
cd phase4
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\cluster-smoke.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\cluster-fault.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\cluster-redis-outage.ps1
```

三个脚本都没有命令行参数。运行结束后查看对应目录的 `summary.txt`。

Go 测试（真实 PostgreSQL + Redis）：

```powershell
cd phase4
gofmt -l .
go vet ./...
$env:RELAYX_TEST_DSN = 'postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable'
$env:RELAYX_TEST_REDIS_ADDR = '127.0.0.1:6379'
go test ./... -count=1
```

容器内 race 测试（需要 Docker Desktop，`host.docker.internal` 可达宿主机）：

```powershell
cd phase4
docker run --rm -v "$($PWD.Path):/app" -w /app `
  -e RELAYX_TEST_DSN='postgres://relayx:relayx@host.docker.internal:5432/relayx?sslmode=disable' `
  -e RELAYX_TEST_REDIS_ADDR='host.docker.internal:6379' `
  golang:1.27 go test -race -count=1 ./...
```

`make race` 只执行 race，不会自动注入 DSN 与 Redis 地址；需要数据库/Redis 用例时应显式传入环境变量，否则相关测试会跳过。

## 7. 观察与限制

- 所有时间是单次运行的墙钟观测，受 Windows 调度、Docker Desktop 网络与进程启动影响，不应作为吞吐或延迟基准。
- 测试使用 1.5s / 0.5s 的短 TTL，故障窗口约 2.0s，实测回收 2.19s；生产默认 15s / 10s，窗口最长 25s。
- 冒烟场景的路由断言依赖一个常驻 node2 会话，因为重复登录客户端退出时会主动删除路由；这反映的是“会话断开后路由删除”的正常行为。
- 故障场景使用 `Stop-Process -Force`，没有给 node3 任何清理机会，因此回收完全依赖 Redis TTL 与清理任务。
- Redis 不可用场景只验证了秒级的短暂中断（停止 0.91s，恢复 0.7s），没有覆盖中断超过 `node_ttl`、Redis 数据丢失、Redis 主从切换或持久化恢复。该场景也没有断言重启后节点 hash 是否仍存在；不过心跳会重新 `RegisterNode` 重建 hash，registry 集成测试已覆盖节点 key 丢失与 TTL 过期后的自愈。
- PULL 断言的实现接受“投递计数为 2”或“单次 `pull_done count=2`”，本次观察到后者。
- 未覆盖：跨节点顺序、超大 payload（接近 `-max-body`）、长时间稳定性、节点间网络分区、重复 node ID、Redis 认证/TLS。
- 未在集群脚本中单独验证单节点模式；该路径由 `go test ./...` 的既有用例覆盖，且网关在 `-redis-addr` 为空时不会初始化 Registry 与 Hub。

## 8. 与 Phase 4 目标对照

| 目标 | 证据 | 结论 |
| --- | --- | --- |
| 多节点可同时提供服务并被发现 | 3 节点 6 端口在 1.48s 内健康；节点注册与路由可查询 | 达成 |
| 跨节点单播 | node1 到 node3 的 3 条消息在 0.02s 内收到 | 达成 |
| 同设备跨节点重复登录替换旧会话 | node3 旧连接收到关闭并在 0.47s 内退出 | 达成 |
| 节点崩溃后客户端自动切换 | 0.18s 检测、0.19s 重连，切换后 2 条消息全部收到 | 达成 |
| 死亡节点路由自动回收 | 2.19s 后节点 hash 与索引消失 | 达成 |
| Redis 故障时消息不丢 | 停 Redis 期间 2 条 SEND 仍 accepted，恢复后 PULL 取回 2 条 | 达成 |
| Redis 故障时新登录快速失败 | carol 进程退出码 1，日志为 cluster unavailable | 达成 |
| 单节点兼容 | 未配置 `-redis-addr` 时不初始化集群；既有测试通过 | 保持 |

## 9. 结论边界

本次验证证明的是：在当前 3 节点、单机 Docker、短 TTL 环境下，服务发现、跨节点投递、重复登录替换、节点故障切换、路由回收与 Redis 短时故障降级均按设计工作。

以下能力没有被证明，也不应据此对外承诺：

- Redis 高可用、持久化或数据不丢。
- 故障窗口小于 `node_ttl + cleanup_interval`。
- 跨节点消息顺序、exactly-once 或恰好一次踢人。
- 生产规模下的吞吐、延迟与长时间稳定性。
- 节点间链路加密或身份认证。
