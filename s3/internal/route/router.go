// Package route 实现 S1 单播路由（LLD §7）：尽力转发、无确认。
// 寻址方式在 S2 保持不变——寻址原语先于可靠性。
package route

import (
	"errors"

	"relayx/s3/internal/protocol"
	"relayx/s3/internal/session"
)

// ErrNoRoute 表示目标 Endpoint 无活动 Session（S1 无离线概念，S2 实现离线）。
var ErrNoRoute = errors.New("route: no active endpoint")

// Destination 是投递目标：一个活动 Session 及其出站投递函数。
// 由 gateway 层注入投递函数，避免 route 依赖 connection/sendpath。
type Destination struct {
	Session *session.Session
	Deliver func(from string, payload []byte) error
}

// Router 基于 EndpointRegistry 做单播寻址。
type Router struct {
	registry  *session.Registry
	itinerary func(*session.Session) (Destination, bool) // Session → 投递目标
}

// New 创建 Router。itinerary 由 gateway 注入：把 Session 解析为可投递目标
// （通常从 Session.ID 查 Connection）。返回 false 表示该 Session 不可投递。
func New(registry *session.Registry, itinerary func(*session.Session) (Destination, bool)) *Router {
	return &Router{registry: registry, itinerary: itinerary}
}

// Resolve 按 SEND body 解析投递目标列表：
//   - DeviceID 非空：精确匹配一个 Endpoint
//   - DeviceID 为空：该 user 的全部 Endpoint（多设备 fan-out）
func (r *Router) Resolve(sb protocol.SendBody) ([]Destination, error) {
	var sessions []*session.Session
	if sb.DeviceID != "" {
		s, err := r.registry.Lookup(session.Endpoint{
			TenantID: sb.TenantID, UserID: sb.UserID, DeviceID: sb.DeviceID,
		})
		if err != nil {
			return nil, ErrNoRoute
		}
		sessions = []*session.Session{s}
	} else {
		sessions = r.registry.LookupUser(sb.TenantID, sb.UserID)
		if len(sessions) == 0 {
			return nil, ErrNoRoute
		}
	}

	dsts := make([]Destination, 0, len(sessions))
	for _, s := range sessions {
		d, ok := r.itinerary(s)
		if !ok {
			continue
		}
		dsts = append(dsts, d)
	}
	if len(dsts) == 0 {
		return nil, ErrNoRoute
	}
	return dsts, nil
}
