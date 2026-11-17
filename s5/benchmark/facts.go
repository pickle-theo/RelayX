package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"sync"
)

// 本文件是 S2 事实写入的 CPU 侧成本模型（S5 §3.1 BenchmarkFactsWrite / §8.2）。
//
// 边界说明（不许含糊）：S5 不重新实现 S2 的 PostgreSQL 事务，也不改
// delivery_facts 的 schema / event_id 派生规则。这里只建模两件可离线测量的
// 事：①确定性 event_id 派生的 CPU 成本；②「同事务内组装事实行 + 幂等插入」
// 的进程内成本。数据库往返（commit latency）不在本基准内，由 S2 契约测试与
// S8 容量报告给出，本包只提供换算公式所需的 CPU 侧常数。
//
// event_id 派生规则与 S2 冻结语义逐字节一致（S2 LLD §3.4）：
//
//	raw = tenant_id + "|" + message_id + "|" + fact_type + "|" + endpoint_id + "|" + version
//	event_id = hex(sha256(raw))[:32]
//
// 这里刻意复刻 S2 的朴素拼接（而不是长度前缀或域分隔哈希），否则基准测到的
// 不是生产路径的 CPU 成本，event_id 也会与 S2 落库值不同。

// FactRecord 是 delivery_facts 的进程内投影（只含成本建模需要的字段）。
type FactRecord struct {
	EventID      string
	TenantID     string
	MessageID    string
	FactType     string
	EndpointID   string
	StateVersion int32
	OccurredAt   int64
}

// DeriveEventID 计算确定性 event_id，与 S2 facts.Derive 完全一致。
func DeriveEventID(tenant, messageID, factType, endpointID string, version int32) string {
	raw := tenant + "|" + messageID + "|" + factType + "|" + endpointID + "|" + strconv.Itoa(int(version))
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])[:32]
}

// MemFactStore 是事实写入的进程内替身：主键 event_id 幂等（重复写被吸收）。
// 它不复刻 S2 的事务/索引实现，只复刻「写放大」与「幂等判定」的 CPU 形态。
type MemFactStore struct {
	mu   sync.RWMutex
	rows []FactRecord
	seen map[string]struct{}
}

// NewMemFactStore 创建内存事实存储。
func NewMemFactStore() *MemFactStore {
	return &MemFactStore{seen: make(map[string]struct{})}
}

// Append 幂等追加一条事实；返回 false 表示已存在（重复被吸收，与 S2 一致）。
func (s *MemFactStore) Append(r FactRecord) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[r.EventID]; ok {
		return false
	}
	s.seen[r.EventID] = struct{}{}
	s.rows = append(s.rows, r)
	return true
}

// Len 返回已写入的事实条数。
func (s *MemFactStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

// BuildFact 组装一条事实（派生 event_id + 填字段），供基准与容量模型直用。
func BuildFact(tenant, messageID, factType, endpointID string, version int32, occurredAt int64) FactRecord {
	return FactRecord{
		EventID:      DeriveEventID(tenant, messageID, factType, endpointID, version),
		TenantID:     tenant,
		MessageID:    messageID,
		FactType:     factType,
		EndpointID:   endpointID,
		StateVersion: version,
		OccurredAt:   occurredAt,
	}
}
