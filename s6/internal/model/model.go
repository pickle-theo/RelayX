// Package model 定义 S6 分布式协调层共享的领域类型。
//
// 这些类型在 node / route / forward / dispatch / gateway 之间传递。
// 这里刻意不含任何存储实现细节（Redis 键、Lua 脚本、mTLS 配置都不在此），
// 使「连接在哪」的寻址语义与传输机制解耦（架构方案 I1）。
package model

import (
	"strings"
	"time"
)

// EndpointKey 是端点路由键：(tenant_id, user_id, device_id)。
// 与 S2 投递幂等键的维度一致；String 的结果用作本地缓存与反向索引成员。
type EndpointKey struct {
	TenantID string
	UserID   string
	DeviceID string
}

// String 返回 "tenant:user:device" 形式。
// Valid 保证三段都不含 ':'，因此该形式是单射的、可安全用作 map key。
func (k EndpointKey) String() string {
	return k.TenantID + ":" + k.UserID + ":" + k.DeviceID
}

// Valid 检查三段均非空且不含 ':'。空设备 ID 在 S6 表示"未知端点"，不允许绑定。
func (k EndpointKey) Valid() bool {
	return k.TenantID != "" && k.UserID != "" && k.DeviceID != "" &&
		!strings.Contains(k.TenantID, ":") &&
		!strings.Contains(k.UserID, ":") &&
		!strings.Contains(k.DeviceID, ":")
}

// NodeStatus 是节点在注册表中的生命周期状态。
type NodeStatus string

const (
	NodeAlive   NodeStatus = "ALIVE"
	NodeSuspect NodeStatus = "SUSPECT"
	NodeDead    NodeStatus = "DEAD"
)

// NodeInfo 是集群中一个 Gateway 节点的注册信息。
// Addr 是节点间 mTLS 监听地址（不是客户端 TCP 地址）。
type NodeInfo struct {
	NodeID    string
	Addr      string
	LastHeart time.Time
	Status    NodeStatus
}

// NodeBinding 是端点→节点的路由绑定。
//
// Version 由 Redis 的 per-endpoint INCR 分配（§5.2），是 HASH 的数值字段；
// 解绑必须携带它做 compare-and-delete（§3.3），否则旧连接的延迟清理事件
// 会删除新连接的路由。
type NodeBinding struct {
	NodeID  string
	Version uint64
	BoundAt time.Time
}
