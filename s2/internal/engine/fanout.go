package engine

import (
	"relayx/s2/internal/idgen"
	"relayx/s2/internal/store"
)

// PlanDeliveries 把解析出的端点转换为每端点一条投递行。
// 集合已由上游 Resolve 与投递唯一键去重。
// 离线端点仍生成 PENDING 行，以便重连后恢复。
func (e *Engine) PlanDeliveries(m *store.Message, eps []store.Endpoint) []*store.Delivery {
	out := make([]*store.Delivery, 0, len(eps))
	seen := make(map[string]bool, len(eps))
	for _, ep := range eps {
		key := ep.UserID + ":" + ep.DeviceID
		if seen[key] {
			continue
		}
		seen[key] = true
		_, online := e.resolver.Lookup(m.TenantID, ep.UserID, ep.DeviceID)
		out = append(out, &store.Delivery{
			ID:            idgen.New("del"),
			TenantID:      m.TenantID,
			MessageID:     m.ID,
			UserID:        ep.UserID,
			DeviceID:      ep.DeviceID,
			EndpointID:    key,
			DeliveryState: store.DeliveryPending,
			Online:        online,
		})
	}
	return out
}
