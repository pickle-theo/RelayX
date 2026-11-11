# Phase 4 节点间协议

本文描述 Gateway 节点之间的内部协议。该协议只用于节点到节点通信，客户端连接不得使用；客户端协议仍遵循 Phase 2/3 的帧定义。

## 1. 范围与安全边界

- 传输为明文 TCP，没有 TLS、认证、签名或授权。
- 节点端口（默认 `127.0.0.1:9100`，由 `-node-addr` 指定）必须只暴露在可信内网，不得暴露到公网或不可信网络。
- 任何能连到节点端口并完成握手的进程都可以发送 `NODE_DELIVER` 与 `NODE_KICK`，也可以声明任意 node ID 与通告地址。协议本身不校验对端身份。
- 通告地址会写入 Redis 并被其它节点信任，因此 Redis 与节点端口应放在同一信任域；示例配置没有 Redis TLS 与密码。
- 客户端端口与节点端口必须分离；客户端连接收到任何节点帧都会被立即关闭。

## 2. 帧头

所有帧复用 Phase 1 以来的 21 字节固定帧头，字节序为大端：

| 偏移 | 长度 | 字段 | 取值 |
| --- | --- | --- | --- |
| 0 | 2 | magic | 固定 `0x5258`（ASCII `RX`） |
| 2 | 1 | version | 固定 `0x01` |
| 3 | 1 | type | 帧类型，见下表 |
| 4 | 1 | flags | 必须为 `0x00`，非零将被拒绝 |
| 5 | 8 | msgID | `uint64`，含义随帧类型变化 |
| 13 | 4 | bodyLen | `uint32`，body 字节数 |
| 17 | 4 | crc32 | body 的 CRC-32（IEEE 多项式），body 为空时为 0 |
| 21 | bodyLen | body | 具体布局见下文 |

解码时的校验顺序：magic、version、flags、帧类型是否已知、bodyLen 是否超过 `-max-body`、读取 body、CRC。任一失败都会关闭连接（握手阶段则关闭连接或拒绝握手）。

`-max-body` 默认 1 MiB，最大可配 16 MiB；节点 Hub 与客户端传输层都使用该值。所有节点应配置为兼容的上限，否则大帧会在接收侧被拒绝并导致连接关闭。

## 3. 帧类型

| 类型 | 值 | 方向 | 用途 |
| --- | --- | --- | --- |
| `NODE_HELLO` | `0x10` | 拨号方 -> 监听方 | 握手第一帧，声明 node ID 与通告地址 |
| `NODE_HELLO_ACK` | `0x11` | 监听方 -> 拨号方 | 握手结果 |
| `NODE_DELIVER` | `0x12` | 任意节点 -> owner 节点 | 转发一条 DELIVER |
| `NODE_DELIVER_ACK` | `0x13` | owner 节点 -> 发送方 | 转发处理结果 |
| `NODE_KICK` | `0x14` | 新登录节点 -> 旧节点 | 关闭旧版本会话 |

0x01 到 0x0E 的客户端帧类型不变。节点帧是新增类型，不复用旧类型的语义。

## 4. 线格式

所有字符串使用 `uint16` 大端长度前缀加原始字节，不做 UTF-8 校验；身份字段长度必须在 `[1, 64]`，node ID 在 `[1, 64]`，通告地址在 `[1, 128]`，ACK 文本在 `[0, 128]`。

### 4.1 NODE_HELLO（0x10）

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 1 | protocolVersion | 当前发送 `0x01` |
| 1 | 2 | nodeIDLen | node ID 字节数 |
| 3 | nodeIDLen | nodeID | 发送方节点标识 |
| 3+nodeIDLen | 2 | addrLen | 通告地址字节数 |
| 5+nodeIDLen | addrLen | advertiseAddr | 其它节点回拨时使用的 `host:port` |

- 帧头 `msgID` 固定为 0。
- 接收方只解码，不校验 `protocolVersion`；当前版本协商仅有这个字段，没有拒绝逻辑。
- 必须是连接上的第一帧；其它类型作为首帧会被关闭连接。

### 4.2 NODE_HELLO_ACK（0x11）

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 1 | status | 0 OK、1 INVALID、2 NOT_FOUND、3 INTERNAL |
| 1 | 2 | msgLen | 文本字节数 |
| 3 | msgLen | message | 人类可读原因，可为空 |

- `msgID` 固定为 0。
- 拨号方只有收到 `status = 0` 才继续；其它状态关闭连接并在日志中记录原因。
- 自连接被拒绝时返回 `status = 1`、`message = "self connection rejected"`。

### 4.3 NODE_DELIVER（0x12）

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 2 | userLen | 目标用户 ID 字节数 |
| 2 | userLen | user | 目标用户 |
| 2+userLen | 2 | deviceLen | 目标设备 ID 字节数 |
| 4+userLen | deviceLen | device | 目标设备 |
| 4+userLen+deviceLen | 8 | fromSessionID | 来源节点的本地 session ID |
| 12+userLen+deviceLen | 2 | fromUserLen | 来源用户 ID 字节数 |
| 14+userLen+deviceLen | fromUserLen | fromUser | 来源用户 |
| 14+userLen+deviceLen+fromUserLen | 2 | fromDeviceLen | 来源设备 ID 字节数 |
| 16+userLen+deviceLen+fromUserLen | fromDeviceLen | fromDevice | 来源设备 |
| 剩余 | 0..N | payload | 消息原始字节，可为空 |

- 帧头 `msgID` 是平台消息 ID；目标节点回 ACK 时原样带回。
- `fromSessionID` 仅用于客户端展示来源，不用于跨节点路由。
- body 不携带目标 node ID；连接身份即目标节点。
- `payload` 直接引用解码缓冲区，只在 handler 调用期间有效；handler 不得把切片保存到调用之外。

### 4.4 NODE_DELIVER_ACK（0x13）

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 1 | status | 0 OK、1 INVALID、2 NOT_FOUND、3 INTERNAL |
| 1 | 2 | msgLen | 文本字节数 |
| 3 | msgLen | message | 人类可读原因，可为空 |

- 帧头 `msgID` 与对应的 `NODE_DELIVER` 相同。
- 发送方会读取并校验该帧，但当前不使用它驱动重试；可靠性由 PostgreSQL 的投递状态保证。
- 目标节点没有会话时返回 `status = 2`、`"session not found"`；本地投递失败返回 `3`、`"deliver failed"`；没有安装 handler 时返回 `3`、`"no handler"`。

### 4.5 NODE_KICK（0x14）

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 2 | userLen | 目标用户 ID 字节数 |
| 2 | userLen | user | 目标用户 |
| 2+userLen | 2 | deviceLen | 目标设备 ID 字节数 |
| 4+userLen | deviceLen | device | 目标设备 |
| 4+userLen+deviceLen | 8 | version | 新登录分配的版本号 |

- 帧头 `msgID` 固定为 0。
- 没有 ACK，也没有重试；发送失败只记录警告。
- 接收方只在本地会话版本小于该版本时关闭会话。

## 5. 状态码

| 值 | 名称 | 含义 |
| --- | --- | --- |
| 0 | OK | 成功 |
| 1 | INVALID | 请求无效，例如自连接 |
| 2 | NOT_FOUND | 目标会话不存在 |
| 3 | INTERNAL | 本地处理失败或没有 handler |

未定义的状态值会被解码器拒绝，导致连接关闭（握手阶段则拒绝握手）。

## 6. 握手

### 6.1 拨号方

1. 使用 `-node-dial-timeout`（默认 1s）作为 TCP 拨号超时。
2. 设置总超时为 `2 * dial timeout` 的读写 deadline。
3. 发送 `NODE_HELLO`，携带自己的 node ID 与通告地址。
4. 读取一帧，要求类型为 `NODE_HELLO_ACK`；解码失败、类型不符或状态非 0 都关闭连接。
5. 清除 deadline，把连接注册为 peer，启动读、写两个协程。

### 6.2 监听方

1. 设置读 deadline 为 `2 * dial timeout`。
2. 读取第一帧，要求类型为 `NODE_HELLO`；否则关闭连接。
3. 解码 hello；如果 node ID 等于自己的 node ID，回复 `INVALID "self connection rejected"` 后关闭。
4. 清除读 deadline，回复 `NODE_HELLO_ACK OK`（写超时使用 Hub 的默认 5s）。
5. 注册 peer，启动读、写两个协程。

握手阶段没有应用层心跳；连接建立后的半开检测依赖 TCP 读写错误与 5s 写超时。节点存活由 Redis TTL 判断。

## 7. 重复连接、自连接与 node ID 冲突

- Hub 以 node ID 为键，每个节点最多保留一个 peer。
- 新连接（无论入站还是出站）注册时，如果已有同 ID 的 peer，旧 peer 会以 `replaced by new connection` 关闭，新连接生效。
- 两个节点同时向对方拨号时，两条连接都能完成握手，后注册的一条胜出，另一条被替换；这是正常竞态，不需要额外协调。
- 自连接（通告地址指向自己）会在握手阶段被识别并拒绝，状态为 `INVALID`。
- 两个不同进程使用同一个 `node_id` 时，它们的连接会反复互相替换，路由归属不稳定；协议不检测这种部署错误，唯一性由部署方保证。
- 断开连接只影响当前 peer；下一次转发会按 Redis 中的地址立即重新拨号，只有拨号失败才会进入 100ms 到 2s 的指数退避。

## 8. 节点连接上的帧处理

| 收到的帧 | 处理 |
| --- | --- |
| `NODE_DELIVER` | 在 peer 读协程上调用 handler；handler 返回后发送 `NODE_DELIVER_ACK`，`msgID` 原样带回 |
| `NODE_KICK` | 调用 handler，不回复 |
| `NODE_DELIVER_ACK` | 解码校验后丢弃；发送方不同步等待 |
| `NODE_HELLO` / `NODE_HELLO_ACK` | 握手完成后视为意外帧，关闭连接 |
| 其它类型 | 关闭连接，原因为 `unexpected frame` |

`NODE_DELIVER` 的 handler 运行在 peer 读协程上，必须快速返回；耗时工作应由 handler 自行转交其它协程。body 的 `payload` 复用解码缓冲区，只在调用期间有效。

`NODE_DELIVER_ACK` 的发送也经过同一个有界队列；队列满时会关闭 peer，而不是阻塞读协程。写协程串行写 socket，因此同一 peer 上的帧不会交错。

## 9. 客户端连接收到节点帧

客户端传输层在分发前检查帧类型：

- 如果 `type` 是 0x10 到 0x14，不调用业务 handler，直接以 `node frame on client connection` 为原因关闭连接，并记录警告日志。
- 该行为有单元测试覆盖（`TestNodeFrameRejectedOnClientConn`），保证节点帧不会从客户端路径进入业务层。

## 10. 兼容性

- 21 字节帧头、magic、version、CRC 与 0x01 到 0x0E 的客户端帧完全不变。
- 节点帧是新增类型；Phase 3 的帧类型校验不认识 0x10 到 0x14，会将其视为未知类型并关闭连接。因此不支持 Phase 3 与 Phase 4 节点混跑集群。
- Phase 4 客户端协议不变；客户端不会发送节点帧，收到节点帧会按第 9 节关闭。
- `NODE_HELLO` 的 `protocolVersion` 当前只发送不校验；如果未来需要版本协商，必须在不破坏现有帧头的前提下增加拒绝规则。
- 节点 ID、地址与 ACK 文本的长度上限是硬限制；超过上限的帧在编码或解码阶段被拒绝。
- 所有节点必须使用兼容的 `-max-body`，否则超过接收方上限的 `NODE_DELIVER` 会导致连接关闭。
