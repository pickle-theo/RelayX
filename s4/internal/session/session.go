// Package session 实现 Session 状态机与 EndpointRegistry（S1 LLD §3 / §7 / §8）。
package session

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Endpoint 是逻辑身份：租户 + 用户 + 设备。一个用户可有多个 Endpoint。
type Endpoint struct {
	TenantID string
	UserID   string
	DeviceID string
}

func (e Endpoint) Key() string {
	return e.TenantID + ":" + e.UserID + ":" + e.DeviceID
}

// SessionState 是会话状态（LLD §8.1）。
type SessionState int

const (
	Authenticating SessionState = iota
	Active
	Replaced
	Closed
)

func (s SessionState) String() string {
	switch s {
	case Authenticating:
		return "AUTHENTICATING"
	case Active:
		return "ACTIVE"
	case Replaced:
		return "REPLACED"
	case Closed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

// Session 是一次登录。Connection 字段由 wiring 层注入，避免包循环依赖：
// session 包只持有会话元数据与状态，连接指针放在 S1 的 gateway 包组装。
type Session struct {
	ID           string
	Endpoint     Endpoint
	ProducerID   string
	ProducerKind string // "user" | "service"（S4 判定 source_device_id 是否为空）
	Capabilities uint32
	Version      uint64 // Connection Version（per-endpoint 单调，S6 用 Redis 分配）

	mu    sync.RWMutex
	state SessionState

	// eventMask 是该会话在 SUBSCRIBE_EVENTS 中获授的事件位（S4）。
	// 未订阅为 0：服务端不发送任何 CLIENT_EVENT。
	eventMask atomic.Uint32
}

// SetEventMask 记录本会话的事件订阅位（服务端授予后的值）。
func (s *Session) SetEventMask(m uint32) { s.eventMask.Store(m) }

// EventMask 返回本会话获授的事件位。
func (s *Session) EventMask() uint32 { return s.eventMask.Load() }

func NewSession(id string, ep Endpoint, producerID string, caps uint32) *Session {
	return &Session{
		ID: id, Endpoint: ep, ProducerID: producerID,
		Capabilities: caps, state: Authenticating,
	}
}

// State 返回当前状态。
func (s *Session) State() SessionState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// transition 在持锁时尝试状态迁移。
func (s *Session) transition(from, to SessionState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != from {
		return false
	}
	s.state = to
	return true
}

// Activate 鉴权成功后 AUTHENTICATING → ACTIVE。
func (s *Session) Activate() bool { return s.transition(Authenticating, Active) }

// MarkReplaced 同 Endpoint 新登录时 ACTIVE → REPLACED。
func (s *Session) MarkReplaced() bool { return s.transition(Active, Replaced) }

// MarkClosed 关闭：REPLACED → CLOSED 或 ACTIVE → CLOSED。
func (s *Session) MarkClosed() bool {
	if s.transition(Replaced, Closed) {
		return true
	}
	return s.transition(Active, Closed)
}

// ErrNotFound 表示 Endpoint 无活动 Session（S1 无离线概念，见 LLD §7.1）。
var ErrNotFound = fmt.Errorf("session: endpoint not found")

// Entry 是注册表中的一条映射（Session + 关闭通知）。
type Entry struct {
	Session *Session
	// OnReplace 在新登录替换本 Session 时调用（由 gateway 注入以发送 CLOSE）。
	OnReplace func(old *Session)
}

// Registry 是 EndpointKey → Entry 的注册表，RWMutex 保护（LLD §10）。
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]*Entry)}
}

// Register 注册/替换 Endpoint 的活动 Session。
// 若原 Entry 存在，返回被替换的旧 Entry（调用方负责标记 REPLACED 并发 CLOSE）。
func (r *Registry) Register(e *Entry) (*Entry, error) {
	if e == nil || e.Session == nil {
		return nil, fmt.Errorf("session: nil entry")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := e.Session.Endpoint.Key()
	old := r.entries[key]
	r.entries[key] = e
	return old, nil
}

// Lookup 精确查找 Endpoint 的活动 Session。
func (r *Registry) Lookup(ep Endpoint) (*Session, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e := r.entries[ep.Key()]
	if e == nil || e.Session.State() != Active {
		return nil, ErrNotFound
	}
	return e.Session, nil
}

// LookupUser 返回该租户 + 用户下所有活动 Session（多设备 fan-out）。
func (r *Registry) LookupUser(tenantID, userID string) []*Session {
	prefix := tenantID + ":" + userID + ":"
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*Session
	for key, e := range r.entries {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix && e.Session.State() == Active {
			out = append(out, e.Session)
		}
	}
	return out
}

// Unregister 注销 Endpoint，仅当当前映射仍为给定 Session 时生效
// （避免断线清理误删已被新登录替换的映射，LLD §8.3 / §10）。
func (r *Registry) Unregister(ep Endpoint, s *Session) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[ep.Key()]
	if e == nil || e.Session != s {
		return false
	}
	delete(r.entries, ep.Key())
	return true
}

// Count 返回活动 Entry 数（供 /metrics）。
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// Replace 原子替换 Entry，返回旧 Entry（供重复登录使用）。
func (r *Registry) Replace(e *Entry) (*Entry, error) {
	return r.Register(e)
}

// Close 保留接口对称；Registry 无后台资源。
func (r *Registry) Close(context.Context) {}
