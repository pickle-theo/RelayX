package broadcast

import (
	"sync"
	"sync/atomic"
)

// IndexLimits 是订阅索引的容量上限（LLD §5.1）。
type IndexLimits struct {
	MaxTopicsPerSession    int
	MaxSubscribersPerTopic int
	MaxTopicBytes          int
	MaxGlobalSubscriptions int
}

// topicKey 是扇出索引的 topic 维度：租户 + topic。
// 广播必须按租户隔离：只按 topic 建索引会让同 topic 名的其他租户订阅者
// 收到本租户的 payload（跨租户数据泄漏），与语义契约「tenant_id 为身份第一维」冲突。
type topicKey struct {
	tenant string
	topic  string
}

// subscription 是一条订阅记录。Version 在订阅时分配、退订/重订阅时换新，
// 用于扇出快照投递前的竞态校验（LLD §9.1）。
type subscription struct {
	sessionID string
	tenantID  string
	topic     string
	version   uint64
	expiresAt int64 // UnixNano，仅观测用；token 过期不主动断开订阅（LLD §4.3）
	sender    P2Sender
}

// Index 是纯内存双向索引。
// byTopic 供扇出 O(订阅者数) 取快照；bySession 供断连 O(该会话订阅数) 清理。
type Index struct {
	mu        sync.RWMutex
	byTopic   map[topicKey]map[string]*subscription // (tenant, topic) → sessID → sub
	bySession map[string]map[string]*subscription   // sessID → topic → sub
	total     int                                   // 全局订阅数（O(1) 容量判断）

	versionSeq atomic.Uint64
	fullEvents atomic.Int64
	limits     IndexLimits
}

func NewIndex(l IndexLimits) *Index {
	return &Index{
		byTopic:   make(map[topicKey]map[string]*subscription),
		bySession: make(map[string]map[string]*subscription),
		limits:    l,
	}
}

// Subscribe 新建订阅；重复订阅幂等返回 (false, nil)。
// 任一容量上限触顶则拒绝并返回对应错误，已有订阅不受影响（LLD §5.3 / B1）。
func (idx *Index) Subscribe(sessID string, claims SubscriptionClaims, senderFactory SenderFactory) (bool, error) {
	topic := claims.Topic
	if claims.TenantID == "" || topic == "" ||
		(idx.limits.MaxTopicBytes > 0 && len(topic) > idx.limits.MaxTopicBytes) {
		return false, ErrBadTopic
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if existing := idx.bySession[sessID]; existing != nil {
		if _, dup := existing[topic]; dup {
			return false, nil // 幂等：重复订阅
		}
		if idx.limits.MaxTopicsPerSession > 0 && len(existing) >= idx.limits.MaxTopicsPerSession {
			idx.fullEvents.Add(1)
			return false, ErrSessionFull
		}
	}

	key := topicKey{tenant: claims.TenantID, topic: topic}
	topicMap := idx.byTopic[key]
	if idx.limits.MaxSubscribersPerTopic > 0 && len(topicMap) >= idx.limits.MaxSubscribersPerTopic {
		idx.fullEvents.Add(1)
		return false, ErrTopicFull
	}
	if idx.limits.MaxGlobalSubscriptions > 0 && idx.total >= idx.limits.MaxGlobalSubscriptions {
		idx.fullEvents.Add(1)
		return false, ErrIndexFull
	}

	version := idx.versionSeq.Add(1)
	sub := &subscription{
		sessionID: sessID,
		tenantID:  claims.TenantID,
		topic:     topic,
		version:   version,
		expiresAt: claims.ExpiresAt.UnixNano(),
		sender:    senderFactory(sessID, topic, version),
	}
	if topicMap == nil {
		topicMap = make(map[string]*subscription)
		idx.byTopic[key] = topicMap
	}
	topicMap[sessID] = sub
	sessionMap := idx.bySession[sessID]
	if sessionMap == nil {
		sessionMap = make(map[string]*subscription)
		idx.bySession[sessID] = sessionMap
	}
	sessionMap[topic] = sub
	idx.total++
	return true, nil
}

// Unsubscribe 删除一条订阅；未订阅返回 (false, ErrNotSubscribed)。
func (idx *Index) Unsubscribe(sessID, topic string) (bool, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	sessionMap := idx.bySession[sessID]
	if sessionMap == nil {
		return false, ErrNotSubscribed
	}
	sub := sessionMap[topic]
	if sub == nil {
		return false, ErrNotSubscribed
	}
	clearDegradation(sub)
	delete(sessionMap, topic)
	idx.total--
	if len(sessionMap) == 0 {
		delete(idx.bySession, sessID)
	}
	key := topicKey{tenant: sub.tenantID, topic: topic}
	if topicMap := idx.byTopic[key]; topicMap != nil {
		delete(topicMap, sessID)
		if len(topicMap) == 0 {
			delete(idx.byTopic, key) // 空 topic 释放内存（LLD §5.3）
		}
	}
	return true, nil
}

// Snapshot 在读锁内复制某租户某 topic 的订阅者快照，释放锁后返回（LLD §5.2）。
// 绝不持有索引锁做网络 I/O。租户维度保证跨租户不串流量。
func (idx *Index) Snapshot(tenantID, topic string) []SubscriberRef {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	m := idx.byTopic[topicKey{tenant: tenantID, topic: topic}]
	out := make([]SubscriberRef, 0, len(m))
	for _, sub := range m {
		out = append(out, SubscriberRef{SessionID: sub.sessionID, Sender: sub.sender, Version: sub.version})
	}
	return out
}

// VersionMatches 在投递前校验订阅仍存在且版本未变（LLD §9.1）。
func (idx *Index) VersionMatches(sessID, topic string, version uint64) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	sessionMap := idx.bySession[sessID]
	if sessionMap == nil {
		return false
	}
	sub := sessionMap[topic]
	return sub != nil && sub.version == version
}

// RemoveSession 断连清理：移除该会话所有订阅，返回被清理的 topic（LLD §5.3 / §9.2）。
func (idx *Index) RemoveSession(sessID string) []string {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	sessionMap := idx.bySession[sessID]
	if sessionMap == nil {
		return nil
	}
	topics := make([]string, 0, len(sessionMap))
	for topic, sub := range sessionMap {
		clearDegradation(sub)
		topics = append(topics, topic)
		key := topicKey{tenant: sub.tenantID, topic: topic}
		if topicMap := idx.byTopic[key]; topicMap != nil {
			delete(topicMap, sessID)
			if len(topicMap) == 0 {
				delete(idx.byTopic, key)
			}
		}
	}
	idx.total -= len(sessionMap)
	delete(idx.bySession, sessID)
	return topics
}

// Stats 返回容量指标。
func (idx *Index) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return IndexStats{
		Subscriptions: idx.total,
		Topics:        len(idx.byTopic),
		Sessions:      len(idx.bySession),
		FullEvents:    idx.fullEvents.Load(),
	}
}

// clearDegradation 在订阅被删除时清除降级状态，避免慢消费者聚合计数泄漏。
func clearDegradation(sub *subscription) {
	if s, ok := sub.sender.(*connSender); ok && s.degraded.Load() {
		s.setDegraded(false)
	}
}

// 编译期接口断言。
var _ SubscriptionIndex = (*Index)(nil)
