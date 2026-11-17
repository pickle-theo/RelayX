package benchmark

import "testing"

// TestDeriveEventIDMatchesS2FrozenRule 冻结 S2 §3.4 的 event_id 派生规则：
// sha256(tenant|message_id|fact_type|endpoint_id|version) 的 hex 前 32 字符。
// 若基准实现与 S2 漂移，容量模型测到的哈希成本与落库键都不再可信。
func TestDeriveEventIDMatchesS2FrozenRule(t *testing.T) {
	got := DeriveEventID("tenant-a", "msg-1", "delivery_sent", "u:bob:dev2", 3)
	const want = "7e104a3932f016e5488c4f4b92f2efdf"
	if got != want {
		t.Fatalf("DeriveEventID=%s, want %s", got, want)
	}
}
