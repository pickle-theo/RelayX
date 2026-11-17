// Package query 实现端侧只读查询 API（S4 LLD §8.1）。
//
// 与 S2 的 GET /v1/messages/{id}/deliveries（业务对账）不同：本 API 面向端侧，
// 只查发送者自己的消息、不返回 payload、字段命名使用 platform_state。
// 跨租户与未知消息一律 404，不暴露存在性。
package query

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"relayx/s4/internal/core"
	"relayx/s4/internal/identity"
	"relayx/s4/protocol"
)

// Store 是查询 API 需要的只读能力。
type Store interface {
	GetMessage(messageID string) (core.Message, error)
	Counts(messageID string) protocol.DeliveryCounts
}

// View 是端侧消息视图（不含 payload）。
type View struct {
	MessageID     string                  `json:"message_id"`
	PlatformState string                  `json:"platform_state"`
	StateVersion  int                     `json:"state_version"`
	Counts        protocol.DeliveryCounts `json:"counts"`
	SettledAt     *time.Time              `json:"settled_at,omitempty"`
	ExpiresAt     time.Time               `json:"expires_at"`
}

// Service 是只读查询处理器。它从不修改状态。
type Service struct {
	st      Store
	auth    *identity.Authenticator
	metrics *Metrics
}

func New(st Store, auth *identity.Authenticator) *Service {
	return &Service{st: st, auth: auth, metrics: &Metrics{}}
}

// Metrics 返回查询延迟指标（Gateway 合并进 /metrics）。
func (s *Service) Metrics() *Metrics { return s.metrics }

func (s *Service) authorize(r *http.Request, m core.Message) bool {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return false
	}
	p, err := s.auth.Authenticate(strings.TrimPrefix(h, "Bearer "))
	if err != nil || p.TenantID != m.TenantID {
		return false
	}
	if p.Kind == core.ProducerService {
		return true
	}
	return p.ID == m.ProducerID
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Service) view(m core.Message) View {
	return View{
		MessageID:     m.ID,
		PlatformState: m.PlatformState,
		StateVersion:  m.StateVersion,
		Counts:        s.st.Counts(m.ID),
		SettledAt:     m.SettledAt,
		ExpiresAt:     m.ExpiresAt,
	}
}

// Message 处理 GET /v1/client/messages/{id}。
func (s *Service) Message(w http.ResponseWriter, r *http.Request, id string) {
	start := time.Now()
	defer func() { s.metrics.Observe(time.Since(start).Seconds()) }()

	m, err := s.st.GetMessage(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if !s.authorize(r, m) {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, s.view(m))
}

// Batch 处理 GET /v1/client/messages?ids=a,b,c。
// 空 ids 是 SDK 的能力探测请求，返回 200 与空列表。
func (s *Service) Batch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() { s.metrics.Observe(time.Since(start).Seconds()) }()

	raw := strings.TrimSpace(r.URL.Query().Get("ids"))
	out := make([]View, 0)
	if raw != "" {
		seen := make(map[string]bool)
		ids := strings.Split(raw, ",")
		if len(ids) > 100 {
			ids = ids[:100]
		}
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			m, err := s.st.GetMessage(id)
			if err != nil {
				continue // 未知或已淘汰：批量查询静默跳过，不暴露存在性
			}
			if !s.authorize(r, m) {
				continue // 非自己的消息：与未知消息同样处理
			}
			out = append(out, s.view(m))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

// Handler 注册端侧查询路由。
func (s *Service) Handler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/client/messages", s.Batch)
	mux.HandleFunc("GET /v1/client/messages/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/v1/client/messages/")
		if id == "" || strings.Contains(id, "/") {
			http.NotFound(w, r)
			return
		}
		s.Message(w, r, id)
	})
	return mux
}
