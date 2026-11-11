# Phase 1 单机实时消息 Gateway Spec

## Why

Phase 0 / 0.5 只解决了「一条连接能可靠收发帧」的传输问题。Phase 1 要在此之上建立 Gateway 的核心领域模型——连接、Session、消息路由——让多个客户端能通过身份互相投递消息，为 Phase 2 的可靠投递打基础。

## What Changes

- 新建独立子项目 `phase1/`（`module relayx/phase1`），以 `phase0.5/` 的优化传输层为基线复制，**不修改 `phase0/` 与 `phase0.5/`**。
- 协议新增 4 种帧类型（`LOGIN`、`LOGIN_ACK`、`SEND`、`DELIVER`），21 字节帧头、CRC32、既有帧类型语义保持不变；新增二进制路由头编解码。
- 新增 Session 领域模型：用户身份、设备身份、Session ID、Session↔Connection 映射、同用户多设备、重复登录踢旧连接、断线清理。
- 新增 Message Router：按 user 单播并 fan-out 到该用户全部设备、按 endpoint（Session ID）定向投递、路由失败回错误帧。
- 新增 Gateway 业务处理器：LOGIN 握手、SEND 路由、DELIVER 下发、未登录拒绝、DATA 回显保留（兼容 Phase 0 压测工具）。
- `cmd/client` 支持 `-user` / `-device` / `-to`，可手工验证登录与投递；`cmd/loadtest` 增加登录与路由模式。
- 根 `go.work` 与根 `Makefile` 纳入 `phase1`；新增 `phase1/docs`（README、ADR-005、ADR-006、压测报告）。

## Impact

- Affected specs: Phase 1 单机实时 Gateway（新增）；复用 Phase 0.5 传输层基线（不改动）
- Affected code:
  - 新增：`phase1/**`（`cmd/{gateway,client,loadtest}`、`internal/{protocol,transport,session,router,gateway,config}`、`docs/`）
  - 修改：`go.work`（新增 `use ./phase1`）、`Makefile`（`PHASES` 增加 `phase1`）
  - 不改动：`phase0/**`、`phase0.5/**`、`docs/通用实时消息 Gateway Platform.md`

## ADDED Requirements

### Requirement: 独立子项目与传输基线

系统 SHALL 提供 `phase1/` 独立 Go module（`relayx/phase1`，零第三方依赖），其传输层从 `phase0.5/` 复制并保持既有行为：每连接 2 协程、有界发送队列（帧数 + 字节预算）、控制帧优先、批量写、写 Deadline、缓冲池与所有权约定。

#### Scenario: 复制后可构建

- **WHEN** 在 `phase1/` 执行 `go build ./... && go vet ./... && go test ./...`
- **THEN** 全部通过，且 `phase0/`、`phase0.5/` 文件未被修改

#### Scenario: 工作区纳入

- **WHEN** 查看根 `go.work` 与根 `Makefile`
- **THEN** 分别包含 `use ./phase1` 与 `PHASES := phase0 phase0.5 phase1`

### Requirement: 协议扩展

系统 SHALL 在保留 21 字节帧头（Magic、Version、Type、Flags、MsgID、BodyLen、CRC32）的前提下新增帧类型：

| 帧类型 | 值 | 方向 | Body 布局（大端） |
| --- | --- | --- | --- |
| `LOGIN` | 0x06 | Client→Gateway | `uint16 userLen + userID + uint16 deviceLen + deviceID` |
| `LOGIN_ACK` | 0x07 | Gateway→Client | `uint8 status + uint64 sessionID + uint16 msgLen + msg` |
| `SEND` | 0x08 | Client→Gateway | `uint8 targetType + uint16 targetLen + target + payload` |
| `DELIVER` | 0x09 | Gateway→Client | `uint64 fromSessionID + uint16 fromUserLen + fromUser + uint16 fromDeviceLen + fromDevice + payload` |

约束：

- `targetType`：`0x01` 表示按 user_id 投递，`0x02` 表示按 endpoint（Session ID 十进制字符串）投递。
- user_id、device_id 长度必须为 `1..64` 字节；target 长度必须为 `1..64` 字节；超限返回哨兵错误，不分配超限内存。
- `DELIVER` 的 `MsgID` 必须回填原 `SEND` 的 `MsgID`，便于发送方关联。
- 既有帧类型（`PING`/`PONG`/`DATA`/`ERROR`/`CLOSE`）的编号与语义不变。

#### Scenario: 编解码往返

- **WHEN** 对每种新帧构造 Body 编码后再解码
- **THEN** 字段逐一相等；Body 为空或长度字段越界时返回明确的哨兵错误

#### Scenario: 线格式兼容

- **WHEN** 用 Phase 0.5 的 `Decode` 读取一个 `PING`/`DATA` 帧
- **THEN** 解析结果与 Phase 0.5 完全一致（帧头布局未变）

### Requirement: Session 管理

系统 SHALL 维护如下领域模型与映射：

- `Session{ID uint64, UserID, DeviceID string, Conn, RemoteAddr, CreatedAt}`，`ID` 在单进程内单调递增且唯一。
- 注册表：`sessionID → *Session`、`userID → deviceID → *Session`，读写受 `RWMutex` 保护。
- 重复登录：同一 `(userID, deviceID)` 的新 `LOGIN` SHALL 使旧连接收到 `CLOSE` 帧并以 `replaced by new login` 原因关闭，新 Session 绑定新连接。
- 断线清理：连接关闭时 SHALL 移除其 Session，且仅当注册表中的 Session 仍指向该连接时才删除（避免误删替换后的新 Session）。

#### Scenario: 登录成功

- **WHEN** 客户端首帧发送合法 `LOGIN`
- **THEN** 服务端回 `LOGIN_ACK{status=0, sessionID>0}`，注册表中可按 user/device 查到该 Session

#### Scenario: 同用户多设备

- **WHEN** 同一 user_id 用 device A、device B 分别登录
- **THEN** 两个 Session 同时存在，互不覆盖

#### Scenario: 重复登录踢旧连接

- **WHEN** 同一 `(user_id, device_id)` 建立第二条连接并发送 `LOGIN`
- **THEN** 旧连接收到 `CLOSE` 帧后断开，注册表中该 device 只指向新 Session

#### Scenario: 断线清理

- **WHEN** 已登录客户端断开 TCP 连接
- **THEN** 该 Session 从两级注册表中移除，`ActiveSessions` 计数回落

### Requirement: 消息路由

系统 SHALL 提供三种路由能力：

1. 按 user_id 单播：投递给该用户当前在线的**全部**设备 Session（fan-out）。
2. 按 endpoint 定向投递：投递给指定 Session ID 的连接。
3. 路由失败处理：目标 user 无在线设备、目标 Session 不存在时，SHALL 向发送方回 `ERROR` 帧（Body 为 UTF-8 描述、复用原 `MsgID`），连接保持打开。

投递通过目标连接既有的有界发送队列完成；若目标连接队列已满/字节预算超限，按既有背压策略关闭该目标连接并清理其 Session，不影响发送方。

#### Scenario: 同用户多设备 fan-out

- **WHEN** 用户 U 有 device A、B 在线，另一用户向 U 发送 `SEND{targetType=user, target=U}`
- **THEN** A、B 各收到一条 `DELIVER`，载荷与发送内容一致，`fromUser`/`fromDevice` 正确

#### Scenario: 按 endpoint 定向

- **WHEN** 发送方使用 `SEND{targetType=endpoint, target=<sessionID>}`
- **THEN** 只有该 Session 收到 `DELIVER`，同用户的其它设备不收到

#### Scenario: 目标离线

- **WHEN** 向不在线的 user_id 发送 `SEND`
- **THEN** 发送方收到 `ERROR` 帧，连接不断开，可继续发送

### Requirement: Gateway 业务处理器

系统 SHALL 实现 `transport.HandlerFunc`，按帧类型分发：

- `LOGIN`：已登录连接重复登录 → 回 `LOGIN_ACK{status!=0}` 并保持连接；未登录则建立 Session 并回 `LOGIN_ACK`。
- `SEND`：未登录 → 回 `ERROR` 且不投递；已登录 → 交给 Router。
- `DATA`：保持 Phase 0 的回显语义（原样回 `DATA`），使既有 `loadtest` 可直接复用。
- `PING`/`CLOSE`：沿用传输层既有处理，不进业务处理器。

#### Scenario: 未登录发送

- **WHEN** 连接未完成 `LOGIN` 就发送 `SEND`
- **THEN** 收到 `ERROR` 帧，且没有任何投递发生

#### Scenario: 重复 LOGIN

- **WHEN** 已登录连接再次发送 `LOGIN`
- **THEN** 回 `LOGIN_ACK{status!=0}`，原 Session 与连接保持有效

#### Scenario: DATA 回显保持

- **WHEN** 客户端发送 `DATA`
- **THEN** 收到内容相同的 `DATA` 回显（与 Phase 0/0.5 行为一致）

### Requirement: CLI 客户端与压测工具

`cmd/client` SHALL 支持 `-user`、`-device`（登录）与 `-to`（目标：user_id 或 `endpoint:<sessionID>`），并自动处理 `LOGIN_ACK`、`DELIVER`、`ERROR`、`PING`。`cmd/loadtest` SHALL 增加可选的登录与路由模式（如 `-login`、`-route-to`），在不启用时保持 Phase 0.5 的 DATA 回显压测行为。

#### Scenario: 手工双端投递

- **WHEN** 启动 gateway，客户端 A（user=alice,device=phone）与 B（user=bob,device=pc）登录，A 用 `-to bob` 发送
- **THEN** B 打印收到的 `DELIVER`，内容与 A 发送一致

### Requirement: 质量门禁

系统 SHALL 满足：`gofmt` 无输出、`go vet ./...` 通过、`go test ./...` 通过、Docker `go test -race ./...` 通过、协程泄漏检查基线偏差 ≤ 5、单 Socket 无并发写。

#### Scenario: 并发与泄漏

- **WHEN** 执行全部测试与 `-race`
- **THEN** 无 data race、无协程泄漏；每连接仍为 2 个协程
