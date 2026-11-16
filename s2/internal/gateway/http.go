package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"relayx/s2/internal/config"
	"relayx/s2/internal/ingress"
	"relayx/s2/internal/query"
	"relayx/s2/internal/store"
)

type submitRequest struct {
	ClientMsgID   string         `json:"client_msg_id"`
	TargetUserID  string         `json:"target_user_id"`
	TargetDevices []string       `json:"target_devices"`
	Payload       []byte         `json:"payload"`
	Policy        *config.Policy `json:"policy,omitempty"`
}

// AccessHandler 返回统一 HTTP 接口：POST 通过与 TCP SEND
// 相同的 IngressService 受理消息；GET 路由是只读查询 API。
func (g *Gateway) AccessHandler() http.Handler {
	mux := query.New(g.st, g.auth).Handler()
	if g.cfg.TestHooks {
		mux.HandleFunc("POST /debug/clock", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				OffsetSeconds int64 `json:"offset_seconds"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			ctrl, ok := g.st.(interface{ SetClockOffset(time.Duration) })
			if !ok {
				http.Error(w, "clock hook unavailable", http.StatusNotImplemented)
				return
			}
			ctrl.SetClockOffset(time.Duration(req.OffsetSeconds) * time.Second)
			w.WriteHeader(http.StatusNoContent)
		})
	}
	mux.HandleFunc("POST /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		p, err := g.auth.Authenticate(strings.TrimPrefix(h, "Bearer "))
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req submitRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := dec.Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		policy := config.DefaultPolicy()
		if req.Policy != nil {
			policy = *req.Policy
		}
		// HTTP 提交没有连接上下文：source_device_id 显式为空串，
		// 绝不采用客户端提供的值。
		res, err := g.ing.Submit(r.Context(), ingress.SubmitInput{
			TenantID: p.TenantID, ProducerID: p.ID, ProducerKind: p.Kind,
			Sender: p.UserID(), SourceDeviceID: "",
			ClientMsgID: req.ClientMsgID, TargetUserID: req.TargetUserID,
			TargetDevices: req.TargetDevices, Payload: req.Payload, Policy: policy,
		})
		if err != nil {
			switch {
			case errors.Is(err, store.ErrPolicyLimit):
				http.Error(w, "policy limit", http.StatusBadRequest)
			case errors.Is(err, store.ErrDuplicateMismatch):
				http.Error(w, "idempotency conflict", http.StatusConflict)
			default:
				http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id": res.MessageID, "platform_state": res.State,
			"duplicate": res.Duplicate, "accepted_at": res.AcceptedAt.UTC().Format(time.RFC3339Nano),
		})
	})
	return mux
}
