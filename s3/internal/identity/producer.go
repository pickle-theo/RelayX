// Package identity 实现令牌鉴权与 Producer 身份派生（S1 LLD §5）。
// producer_id 一律由令牌派生，客户端不可伪造。
package identity

import "fmt"

// Producer 是从令牌派生的身份。
type Producer struct {
	ID       string // "u:alice"（用户）/ "svc:billing"（服务）
	Kind     string // "user" | "service"
	TenantID string
}

// UserID 返回用户/服务名（去掉 u:/svc: 前缀）。
func (p Producer) UserID() string {
	switch p.Kind {
	case "user":
		return p.ID[len("u:"):]
	case "service":
		return p.ID[len("svc:"):]
	default:
		return p.ID
	}
}

func makeProducerID(kind, name string) (string, error) {
	switch kind {
	case "user":
		return "u:" + name, nil
	case "service":
		return "svc:" + name, nil
	default:
		return "", fmt.Errorf("identity: unknown producer kind %q", kind)
	}
}
