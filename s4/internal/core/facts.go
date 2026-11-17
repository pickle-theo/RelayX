package core

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"relayx/s4/protocol"
)

// DeriveEventID 是 S2 冻结的确定性事实 ID：
// sha256(tenant|message_id|fact_type|endpoint_id|state_version) 前 32 个 hex 字符。
// 重复投影得到同一 event_id，端侧按 event_id 幂等吸收（S4 LLD §5.4）。
func DeriveEventID(tenant, messageID, factType string, endpointID *string, version int) string {
	ep := ""
	if endpointID != nil {
		ep = *endpointID
	}
	raw := tenant + "|" + messageID + "|" + factType + "|" + ep + "|" + strconv.Itoa(version)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])[:32]
}

func countsMap(c protocol.DeliveryCounts) map[string]any {
	return map[string]any{
		"total":         c.Total,
		"receipt_acked": c.ReceiptAcked,
		"failed":        c.Failed,
		"expired":       c.Expired,
	}
}

// messageFact 携带来源端点与终态计数。它是客户端事件路由器无需回查
// 业务库即可恢复"消息由谁发出"的唯一位置（S2 §3.4 / S4 §5.3）。
func messageFact(m *Message, factType string, version int, counts protocol.DeliveryCounts) *Fact {
	return &Fact{
		EventID:      DeriveEventID(m.TenantID, m.ID, factType, nil, version),
		TenantID:     m.TenantID,
		MessageID:    m.ID,
		FactType:     factType,
		StateVersion: version,
		PayloadSummary: map[string]any{
			"producer_id":      m.ProducerID,
			"source_device_id": m.SourceDeviceID,
			"counts":           countsMap(counts),
		},
	}
}

func deliveryFact(d *Delivery, factType string, version int) *Fact {
	ep := d.EndpointID
	return &Fact{
		EventID:      DeriveEventID(d.TenantID, d.MessageID, factType, &ep, version),
		TenantID:     d.TenantID,
		MessageID:    d.MessageID,
		EndpointID:   &ep,
		FactType:     factType,
		StateVersion: version,
		PayloadSummary: map[string]any{
			"user_id":   d.UserID,
			"device_id": d.DeviceID,
			"attempts":  d.Attempts,
		},
	}
}
