// Package query 实现只读消息查询 API。查询处理器
// 从不修改状态。跨租户请求返回 404 而非 403，
// 以免泄露其他租户消息的存在性。
package query

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"relayx/s2/internal/identity"
	"relayx/s2/internal/store"
)

type Service struct {
	st   store.Store
	auth *identity.Authenticator
}

func New(st store.Store, auth *identity.Authenticator) *Service {
	return &Service{st: st, auth: auth}
}

func (s *Service) authorize(r *http.Request, tenant, producerID string) bool {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return false
	}
	p, err := s.auth.Authenticate(strings.TrimPrefix(header, "Bearer "))
	if err != nil || p.TenantID != tenant {
		return false
	}
	if p.Kind == store.ProducerService {
		return true
	}
	return p.ID == producerID
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Service) Message(w http.ResponseWriter, r *http.Request, id string) {
	m, err := s.st.Messages().GetByID(r.Context(), id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if !s.authorize(r, m.TenantID, m.ProducerID) {
		http.NotFound(w, r)
		return
	}
	ds, err := s.st.Deliveries().ListByMessage(r.Context(), id)
	if err != nil {
		http.Error(w, "internal", http.StatusInternalServerError)
		return
	}
	counts := store.FinalCounts{Total: len(ds)}
	for _, d := range ds {
		switch d.DeliveryState {
		case store.DeliveryReceiptAcked:
			counts.ReceiptAcked++
		case store.DeliveryFailed:
			counts.Failed++
		case store.DeliveryExpired:
			counts.Expired++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message_id":       m.ID,
		"platform_state":   m.PlatformState,
		"state_version":    m.StateVersion,
		"producer_id":      m.ProducerID,
		"source_device_id": m.SourceDeviceID,
		"created_at":       m.CreatedAt,
		"expires_at":       m.ExpiresAt,
		"settled_at":       m.SettledAt,
		"counts":           counts,
	})
}

func (s *Service) Deliveries(w http.ResponseWriter, r *http.Request, id string) {
	m, err := s.st.Messages().GetByID(r.Context(), id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if !s.authorize(r, m.TenantID, m.ProducerID) {
		http.NotFound(w, r)
		return
	}
	ds, err := s.st.Deliveries().ListByMessage(r.Context(), id)
	if err != nil {
		http.Error(w, "internal", http.StatusInternalServerError)
		return
	}
	limit, offset := 50, 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	if offset > len(ds) {
		offset = len(ds)
	}
	// 先钳制上限再求和，避免 offset+limit 整数溢出导致负切片/巨大 make panic。
	if limit > len(ds)-offset {
		limit = len(ds) - offset
	}
	end := offset + limit
	type row struct {
		DeliveryID        string     `json:"delivery_id"`
		EndpointID        string     `json:"endpoint_id"`
		DeliveryState     string     `json:"delivery_state"`
		StateVersion      int        `json:"state_version"`
		Attempts          int32      `json:"attempts"`
		ApplicationStatus *string    `json:"application_status,omitempty"`
		TerminalAt        *time.Time `json:"terminal_at,omitempty"`
	}
	out := make([]row, 0, end-offset)
	for _, d := range ds[offset:end] {
		out = append(out, row{
			DeliveryID: d.ID, EndpointID: d.EndpointID,
			DeliveryState: d.DeliveryState, StateVersion: d.StateVersion,
			Attempts: d.Attempts, ApplicationStatus: d.ApplicationStatus,
			TerminalAt: d.TerminalAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message_id": id, "total": len(ds), "deliveries": out,
	})
}

func (s *Service) Facts(w http.ResponseWriter, r *http.Request, id string) {
	m, err := s.st.Messages().GetByID(r.Context(), id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if !s.authorize(r, m.TenantID, m.ProducerID) {
		http.NotFound(w, r)
		return
	}
	fs, err := s.st.Facts().ListByMessage(r.Context(), id)
	if err != nil {
		http.Error(w, "internal", http.StatusInternalServerError)
		return
	}
	type row struct {
		EventID        string         `json:"event_id"`
		FactType       string         `json:"fact_type"`
		EndpointID     *string        `json:"endpoint_id,omitempty"`
		StateVersion   int            `json:"state_version"`
		OccurredAt     time.Time      `json:"occurred_at"`
		PayloadSummary map[string]any `json:"payload_summary"`
	}
	out := make([]row, 0, len(fs))
	for _, f := range fs {
		out = append(out, row{
			EventID: f.EventID, FactType: f.FactType, EndpointID: f.EndpointID,
			StateVersion: f.StateVersion, OccurredAt: f.OccurredAt,
			PayloadSummary: f.PayloadSummary,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"message_id": id, "facts": out})
}

// Handler 注册只读路由。访问 API 的 POST 路由由
// gateway 装配代码注册在同一 mux 上。
func (s *Service) Handler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/messages/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/messages/")
		parts := strings.Split(rest, "/")
		if len(parts) == 1 && parts[0] != "" {
			s.Message(w, r, parts[0])
			return
		}
		if len(parts) == 2 && parts[0] != "" {
			switch parts[1] {
			case "deliveries":
				s.Deliveries(w, r, parts[0])
				return
			case "facts":
				s.Facts(w, r, parts[0])
				return
			}
		}
		http.NotFound(w, r)
	})
	return mux
}
