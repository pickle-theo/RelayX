package benchmark

import (
	"strconv"
	"testing"
)

// 事实写入基准（S5 §3.1 BenchmarkFactsWrite / §8.2 DB 写入上限）。
// 只测 CPU 侧：event_id 派生 + 事实行组装 + 幂等插入。
// 数据库往返与事务提交耗时不在本基准内（由 S2 契约测试与 S8 容量报告给出）。

func BenchmarkFactsWrite(b *testing.B) {
	store := NewMemFactStore()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := strconv.Itoa(i)
		r := BuildFact("tenant-a", "msg-"+id, "message_settled", "u:alice:dev1", 1, int64(i))
		if !store.Append(r) {
			b.Fatal("unexpected duplicate")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(store.Len()), "rows")
}

// BenchmarkFactsWrite_Duplicate 测幂等吸收路径的成本（重复事实不得重复落库）。
func BenchmarkFactsWrite_Duplicate(b *testing.B) {
	store := NewMemFactStore()
	r := BuildFact("tenant-a", "msg-1", "message_settled", "u:alice:dev1", 1, 0)
	if !store.Append(r) {
		b.Fatal("first append rejected")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if store.Append(r) {
			b.Fatal("duplicate accepted")
		}
	}
}

// BenchmarkDeriveEventID 单独量化确定性哈希的 CPU 成本，
// 供容量模型换算「每消息事实数 × 哈希成本」。
func BenchmarkDeriveEventID(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DeriveEventID("tenant-a", "msg-1", "delivery_sent", "u:bob:dev2", 3)
	}
}
