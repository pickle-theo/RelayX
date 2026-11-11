# Tasks

- [x] Task 1: 创建 Phase 1 独立子项目
  - [x] SubTask 1.1: 复制 `phase0.5/` 的 `cmd/`、`internal/`、`Makefile` 到 `phase1/`，新建 `phase1/go.mod`（`module relayx/phase1`，`go 1.27`）并替换全部 import 前缀
  - [x] SubTask 1.2: 根 `go.work` 增加 `use ./phase1`，根 `Makefile` 的 `PHASES` 增加 `phase1`
  - [x] SubTask 1.3: 在 `phase1/` 执行 `go build ./... && go vet ./... && go test ./...`，确认复制基线全部通过

- [x] Task 2: 协议扩展与路由头编解码
  - [x] SubTask 2.1: 在 `internal/protocol` 新增 `Login`/`LoginAck`/`Send`/`Deliver` 帧类型常量，扩展 `valid()` 与 `String()`；既有编号与语义不变
  - [x] SubTask 2.2: 新增 `RouteTargetType`（user=0x01、endpoint=0x02）与身份/目标长度上限常量，超限返回哨兵错误
  - [x] SubTask 2.3: 实现 `EncodeLogin/DecodeLogin`、`EncodeLoginAck/DecodeLoginAck`、`EncodeSend/DecodeSend`、`EncodeDeliver/DecodeDeliver`，复用现有 `AppendFrame`/`DecodeInto` 缓冲模型
  - [x] SubTask 2.4: 编写表驱动测试：正常往返、空身份、身份/目标超长、targetType 非法、截断 Body、CRC 错误；并验证既有帧类型编解码结果与 Phase 0.5 一致

- [x] Task 3: Session 管理
  - [x] SubTask 3.1: 定义 `Session{ID, UserID, DeviceID, Conn, RemoteAddr, CreatedAt}` 与 `SessionManager`，提供 `NextSessionID()`、`Register/Unregister`、`FindByUser`、`FindByDevice`、`Get(sessionID)`、`ActiveSessions()`、`Snapshot()`
  - [x] SubTask 3.2: 实现重复登录：同 `(userID, deviceID)` 新登录替换旧 Session，旧连接以 `replaced by new login` 关闭并发出 `CLOSE` 帧
  - [x] SubTask 3.3: 实现断线清理：仅当注册表条目仍指向该连接时才删除，避免误删被替换后的新 Session
  - [x] SubTask 3.4: 测试：并发注册/注销、同用户多设备、重复登录只保留新 Session、断线清理计数回落

- [x] Task 4: 消息路由
  - [x] SubTask 4.1: 定义 `Router` 与路由结果类型（成功投递数 / 目标不存在 / 目标离线）
  - [x] SubTask 4.2: 实现按 user_id fan-out 到全部设备 Session，以及按 Session ID 定向投递
  - [x] SubTask 4.3: 实现失败处理：目标不存在或无在线设备时返回错误，由处理器回 `ERROR` 帧；目标连接队列满时按背压关闭目标连接并清理 Session
  - [x] SubTask 4.4: 测试：多设备 fan-out、定向只达目标、离线返回错误、目标慢消费者被关闭且不影响发送方

- [x] Task 5: Gateway 业务处理器
  - [x] SubTask 5.1: 实现 `internal/gateway` 处理器，按帧类型分发 `LOGIN`/`SEND`/`DATA`，`PING`/`CLOSE` 仍由传输层处理
  - [x] SubTask 5.2: 实现 LOGIN 流程：校验身份 → 注册 Session → 回 `LOGIN_ACK{status=0, sessionID}`；已登录连接重复 LOGIN 回 `status!=0` 且不改动原 Session
  - [x] SubTask 5.3: 实现 SEND 流程：未登录回 `ERROR`；已登录交给 Router，投递时回填原 `MsgID`
  - [x] SubTask 5.4: 保持 `DATA` 回显；连接关闭时清理 Session
  - [x] SubTask 5.5: 在 `cmd/gateway` 接入处理器与 `-stats-interval` 增加 `active_sessions` 指标
  - [x] SubTask 5.6: 测试：登录成功/失败、未登录发送被拒、重复登录、DATA 回显不变

- [x] Task 6: CLI 客户端与压测工具
  - [x] SubTask 6.1: `cmd/client` 增加 `-user`、`-device`、`-to`；连接后自动 LOGIN，处理 `LOGIN_ACK`/`DELIVER`/`ERROR`/`PING`，保留 Ctrl+C 与 `-count` 退出
  - [x] SubTask 6.2: `cmd/loadtest` 增加可选登录与路由模式（`-login`、`-route-to`），未启用时保持原 DATA 回显压测语义；路由模式按 `DELIVER` 统计 RTT 与丢失
  - [x] SubTask 6.3: 手工验证：两个客户端登录不同用户，A `-to bob` 发送后 B 收到 `DELIVER`

- [x] Task 7: 集成测试与质量门禁
  - [x] SubTask 7.1: 使用 `127.0.0.1:0` 真实监听编写集成测试：登录、同用户多设备 fan-out、endpoint 定向、重复登录踢旧、离线错误帧、断线清理
  - [x] SubTask 7.2: 协程泄漏检查（测试前后 `runtime.NumGoroutine` 对比，带重试），确认每连接仍为 2 协程
  - [x] SubTask 7.3: 全量 `gofmt`、`go vet ./...`、`go test ./...` 通过；Docker `golang:1.27` 执行 `go test -race ./...` 通过

- [x] Task 8: 文档与压测
  - [x] SubTask 8.1: 编写 `phase1/docs/README.md`：架构图（Connection→Session→Router）、协议扩展表、配置项、快速开始、与 Phase 0.5 的差异
  - [x] SubTask 8.2: 编写 `phase1/docs/adr/ADR-005.md`（Session 与 Connection 分离、Session ID 语义、重复登录策略）与 `ADR-006.md`（路由模型与失败处理、背压边界）
  - [x] SubTask 8.3: 千连接压测（登录 + 路由模式），记录吞吐、P99、内存、协程数、Session 数，与 Phase 0.5 回显基线对比
  - [x] SubTask 8.4: 最终质量门禁复跑并记录实际输出

- [x] Task 9: 修复被踢连接的 CLOSE 帧投递
  - [x] SubTask 9.1: 在 `transport.Conn` 增加 `CloseWithFrame(f *protocol.Frame, reason string) error`：先设置关闭原因，再入队帧并交由 Writer 在写完后关闭；入队失败时立即关闭
  - [x] SubTask 9.2: `session.CloseReplaced` 改用 `CloseWithFrame(CLOSE, "replaced by new login")`，不再「入队后立即 Close」
  - [x] SubTask 9.3: 同步更新 `session.Conn` 接口与各测试假实现；把 `TestDuplicateLoginKicksOldConnection` 恢复为「旧连接必须先收到 CLOSE 帧」

# Task Dependencies

- Task 2 依赖 Task 1
- Task 3 依赖 Task 1（可与 Task 2 并行）
- Task 4 依赖 Task 2、Task 3
- Task 5 依赖 Task 2、Task 3、Task 4
- Task 6 依赖 Task 2、Task 5
- Task 7 依赖 Task 5
- Task 8 依赖 Task 6、Task 7
- Task 9 依赖 Task 3、Task 7（修复验证发现的问题）
