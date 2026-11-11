# Tasks

- [x] Task 1: 创建 Phase 4 子项目与本地 Redis 环境
  - [x] SubTask 1.1: 复制 `phase3/` 为 `phase4/`，`go.mod` 改为 `module relayx/phase4`（`go 1.27`）并替换全部 import 前缀；根 `go.work` 增加 `use ./phase4`，根 `Makefile` 的 `PHASES` 增加 `phase4`
  - [x] SubTask 1.2: `phase4/Makefile` 增加 `redis-up` / `redis-down` / `redis-ping` / `cluster-smoke` / `cluster-fault` 目标，Redis 使用 `redis:7-alpine`，容器名 `relayx-phase4-redis`，端口 6379
  - [x] SubTask 1.3: 新增配置项 `-node-id`、`-node-addr`、`-node-advertise-addr`、`-redis-addr`、`-redis-password`、`-redis-db`、`-node-heartbeat-interval`、`-node-ttl`、`-node-cleanup-interval`、`-node-dial-timeout`，含校验与默认值（集群默认关闭）
  - [x] SubTask 1.4: 验证复制基线：`gofmt -l .`、`go vet ./...`、`go test ./...` 通过；确认 `phase0`–`phase3` 无改动

- [x] Task 2: Redis 注册表与在线路由
  - [x] SubTask 2.1: 定义 `registry.Registry` 接口（节点注册/心跳/发现、路由 upsert/lookup、version 分配、CAS 删除、死节点回收）与测试用内存实现
  - [x] SubTask 2.2: 实现 go-redis 版本：`relayx:nodes`、`relayx:node:<id>`、`relayx:route:<user>\x00<device>`、`relayx:ver:*`、`relayx:node_routes:<id>`，含 Lua CAS 删除脚本
  - [x] SubTask 2.3: 实现节点心跳循环与 sweeper（清理锁 + 定向路由回收 + 幂等）
  - [x] SubTask 2.4: 补充 Redis 集成测试（`RELAYX_TEST_REDIS_ADDR` 未设置时 skip），覆盖节点过期、version CAS、并发回收

- [x] Task 3: 节点间直连通道
  - [x] SubTask 3.1: 协议新增 `NODE_HELLO(0x10)`、`NODE_HELLO_ACK(0x11)`、`NODE_DELIVER(0x12)`、`NODE_DELIVER_ACK(0x13)`、`NODE_KICK(0x14)` 及编解码，帧头与既有帧不变
  - [x] SubTask 3.2: 内部监听 `-node-addr`：入站握手校验 node_id、拒绝自连与重复连接、握手超时关闭
  - [x] SubTask 3.3: 出站 PeerHub：按 node_id 懒建立连接、握手、断线退避重连、有界发送队列、优雅关闭
  - [x] SubTask 3.4: 补单元与并发测试：帧编解码边界、自连拒绝、重复连接、断线重连、队列满返回错误

- [x] Task 4: 集群感知登录与跨节点踢人
  - [x] SubTask 4.1: 登录流程接入注册表：INCR 分配 version、注册本地会话、写路由、刷新 `node_routes`
  - [x] SubTask 4.2: 向旧节点异步发送 `NODE_KICK`；旧节点仅在本地会话 version 更小时关闭连接
  - [x] SubTask 4.3: `OnConnClosed` 按 version CAS 删除路由；单节点模式保持原逻辑
  - [x] SubTask 4.4: 测试本地重复登录、跨节点重复登录、旧清理不删新路由、Redis 写失败返回 internal

- [x] Task 5: 集群感知发送与重试
  - [x] SubTask 5.1: SEND 收件人解析合并本地会话与 Redis 路由，区分本地在线 / 远端在线 / 离线
  - [x] SubTask 5.2: 实现 `NODE_DELIVER` 入站处理与 ACK（OK / NOT_FOUND / 内部错误），远端在线时转发
  - [x] SubTask 5.3: 重试 worker 集群感知：本地投递、远端存活跳过不写库、无路由或节点已死清理调度
  - [x] SubTask 5.4: 测试跨节点投递成功、接收端会话迁走回 NOT_FOUND、远端 due delivery 不被误清

- [x] Task 6: 客户端与压测工具多节点支持
  - [x] SubTask 6.1: `cmd/client` 与 `cmd/loadtest` 的 `-addr` 支持逗号分隔列表与故障切换，重连后重新 LOGIN
  - [x] SubTask 6.2: 新增 3 节点集群冒烟脚本：客户端分布登录、跨节点单播、跨节点多设备 fan-out
  - [x] SubTask 6.3: 补充多地址解析与切换的单元测试

- [x] Task 7: 故障与一致性验证
  - [x] SubTask 7.1: 编写 3 节点 + Redis 启动/停止脚本（独立端口、独立 node_id、共享 PostgreSQL）
  - [x] SubTask 7.2: kill -9 注入：节点崩溃后客户端切换重连、路由在 TTL + 清理周期内回收、指向旧节点的消息不再误投
  - [x] SubTask 7.3: Redis 不可用场景：启动失败、运行期登录返回 internal、已受理消息通过 PULL 恢复
  - [x] SubTask 7.4: 记录故障场景的原始输出、耗时与最终状态到测试报告

- [x] Task 8: 文档、ADR 与最终门禁
  - [x] SubTask 8.1: 编写 ADR-013（Redis 作为在线路由注册表）、ADR-014（节点直连数据面 + versioned route）、ADR-015（节点故障检测与路由回收）
  - [x] SubTask 8.2: 编写 `phase4/docs/README.md`、`architecture.md`、`protocol-node.md`、`cluster-test-report.md`
  - [x] SubTask 8.3: 最终 `gofmt`、`go vet`、`go test ./...`、`go test -race ./...`、真实 PostgreSQL + Redis 集成测试全部通过并记录输出
  - [x] SubTask 8.4: 确认 `phase0`–`phase3` 未被修改，清理临时产物

# Task Dependencies

- Task 2 依赖 Task 1
- Task 3 依赖 Task 1（协议与网络层），可与 Task 2 并行
- Task 4 依赖 Task 2、Task 3
- Task 5 依赖 Task 2、Task 3、Task 4
- Task 6 依赖 Task 3
- Task 7 依赖 Task 5、Task 6
- Task 8 依赖 Task 1–7
