package engine

import (
	"relayx/s2/internal/facts"
	"relayx/s2/internal/store"
)

func countsMap(c store.FinalCounts) map[string]any {
	return map[string]any{
		"total":         c.Total,
		"receipt_acked": c.ReceiptAcked,
		"failed":        c.Failed,
		"expired":       c.Expired,
	}
}

// messageFact 携带来源端点与终态计数。它是客户端事件路由器（S4）
// 或远端路由器（S7）无需查询业务库即可恢复
// “消息由谁发出”的唯一位置。
func (e *Engine) messageFact(m *store.Message, factType string, version int, counts store.FinalCounts) *store.Fact {
	return &store.Fact{
		EventID:      facts.Derive(m.TenantID, m.ID, factType, nil, version),
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

// MessageAcceptedFact 在受理事务中写入。此时
// 只知道 total，终态计数出现在结算/过期事实中。
func (e *Engine) MessageAcceptedFact(m *store.Message, total int) *store.Fact {
	f := e.messageFact(m, store.FactMessageAccepted, 0, store.FinalCounts{})
	f.PayloadSummary["counts"] = map[string]any{"total": total}
	return f
}

// DeliveryCreatedFact 对应每条扇出行，state_version 为 0。
func (e *Engine) DeliveryCreatedFact(d *store.Delivery) *store.Fact {
	return e.deliveryFact(d, store.FactDeliveryCreated, 0)
}

func (e *Engine) deliveryFact(d *store.Delivery, factType string, version int) *store.Fact {
	ep := d.EndpointID
	return &store.Fact{
		EventID:      facts.Derive(d.TenantID, d.MessageID, factType, &ep, version),
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
