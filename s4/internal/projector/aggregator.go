package projector

import (
	"encoding/json"

	"relayx/s4/protocol"
)

// routeFromSummary 从事实摘要读取路由键。空串是合法值（服务/HTTP 提交），
// 因此调用方用 producerID 是否为空判断"摘要缺失"，而不是用 source 是否为空。
func routeFromSummary(summary map[string]any) (producerID, sourceDeviceID string) {
	if summary == nil {
		return "", ""
	}
	if v, ok := summary["producer_id"]; ok {
		producerID, _ = v.(string)
	}
	if v, ok := summary["source_device_id"]; ok {
		sourceDeviceID, _ = v.(string)
	}
	return producerID, sourceDeviceID
}

// CountsFromSummary 从 payload_summary.counts 提取四元计数。
// 唯一权威来源是 S2 FactWriter 在结算事务内写好的 summary；本函数只是解码，
// 不重新计算。兼容 map[string]int（内存事实）、map[string]any 与 JSON float64。
func CountsFromSummary(summary map[string]any) (protocol.DeliveryCounts, bool) {
	v, ok := summary["counts"]
	if !ok {
		return protocol.DeliveryCounts{}, false
	}
	switch c := v.(type) {
	case map[string]int:
		return protocol.DeliveryCounts{
			Total: c["total"], ReceiptAcked: c["receipt_acked"],
			Failed: c["failed"], Expired: c["expired"],
		}, true
	case map[string]any:
		return protocol.DeliveryCounts{
			Total:        toInt(c["total"]),
			ReceiptAcked: toInt(c["receipt_acked"]),
			Failed:       toInt(c["failed"]),
			Expired:      toInt(c["expired"]),
		}, true
	case protocol.DeliveryCounts:
		return c, true
	default:
		return protocol.DeliveryCounts{}, false
	}
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint32:
		return int(n)
	case uint64:
		return int(n)
	case float64:
		return int(n)
	case float32:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}
