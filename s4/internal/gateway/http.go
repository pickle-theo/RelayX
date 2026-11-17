package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"relayx/s4/internal/broadcast"
	"relayx/s4/internal/core"
	"relayx/s4/internal/session"
	"relayx/s4/protocol"
)

type submitRequest struct {
	ClientMsgID   string        `json:"client_msg_id"`
	TargetUserID  string        `json:"target_user_id"`
	TargetDevices []string      `json:"target_devices"`
	Payload       []byte        `json:"payload"`
	Policy        *submitPolicy `json:"policy,omitempty"`
}

type submitPolicy struct {
	TTLSeconds   int32  `json:"ttl_seconds"`
	MaxAttempts  int32  `json:"max_attempts"`
	DeliveryMode string `json:"delivery_mode"`
	Fanout       int32  `json:"fanout"`
}

type broadcastRequest struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	IdemKey string `json:"idempotency_key,omitempty"`
}

// AccessHandler 暴露 HTTP 接口（access 端口）：
//   - GET  /v1/client/messages[/{id}]  端侧只读查询（S4 §8.1）
//   - POST /v1/messages                HTTP 受理（source_device_id 恒为空）
//   - POST /v1/broadcasts              S3 广播受理（202）
//   - POST /debug/*                    仅 -test-hooks 开启时注册
func (g *Gateway) AccessHandler() http.Handler {
	mux := g.query.Handler()
	mux.HandleFunc("POST /v1/messages", g.handleSubmitHTTP)
	mux.HandleFunc("POST /v1/broadcasts", g.handleBroadcastHTTP)
	if g.cfg.TestHooks {
		g.registerDebug(mux)
	}
	return mux
}

func (g *Gateway) handleSubmitHTTP(w http.ResponseWriter, r *http.Request) {
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
	var policy core.Policy
	if req.Policy != nil {
		policy = core.Policy{
			TTLSeconds: req.Policy.TTLSeconds, MaxAttempts: req.Policy.MaxAttempts,
			DeliveryMode: req.Policy.DeliveryMode, Fanout: req.Policy.Fanout,
		}
	}
	// HTTP 提交没有连接上下文：source_device_id 显式为空串，
	// 绝不采用客户端提供的值。这类消息不会路由出客户端事件（§5.3）。
	res, err := g.core.Submit(r.Context(), core.Input{
		TenantID: p.TenantID, ProducerID: p.ID, ProducerKind: p.Kind,
		Sender: p.UserID(), SourceDeviceID: "",
		ClientMsgID: req.ClientMsgID, TargetUserID: req.TargetUserID,
		TargetDevices: req.TargetDevices, Payload: req.Payload, Policy: policy,
	})
	if err != nil {
		switch {
		case errors.Is(err, core.ErrPolicyLimit):
			http.Error(w, "policy limit", http.StatusBadRequest)
		case errors.Is(err, core.ErrDuplicateMismatch):
			http.Error(w, "idempotency conflict", http.StatusConflict)
		default:
			http.Error(w, "internal", http.StatusInternalServerError)
		}
		return
	}
	if res.State == core.MessageAccepted {
		go func(id string) {
			if err := g.core.Dispatch(g.ctx, id); err != nil {
				g.log.Warn("dispatch http message failed", "message_id", id, "err", err)
			}
		}(res.MessageID)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message_id": res.MessageID, "platform_state": res.State,
		"duplicate": res.Duplicate, "accepted_at": res.AcceptedAt.UTC().Format(time.RFC3339Nano),
	})
}

func (g *Gateway) handleBroadcastHTTP(w http.ResponseWriter, r *http.Request) {
	hdr := r.Header.Get("Authorization")
	if !strings.HasPrefix(hdr, "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	producer, err := g.auth.Authenticate(strings.TrimPrefix(hdr, "Bearer "))
	if err != nil || producer.Kind != "service" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req broadcastRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Topic == "" || len(req.Payload) > g.bc.MaxPayload() {
		http.Error(w, "invalid topic or payload too large", http.StatusBadRequest)
		return
	}
	accepted, err := g.bc.Publish(r.Context(), broadcast.BroadcastInput{
		TenantID: producer.TenantID, Topic: req.Topic,
		Payload: req.Payload, IdemKey: req.IdemKey,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"accepted_at": accepted.AcceptedAt.UTC().Format(time.RFC3339Nano),
		"duplicate":   accepted.Duplicate,
	})
}

// ---------- 验证钩子（仅 -test-hooks） ----------

func (g *Gateway) registerDebug(mux *http.ServeMux) {
	mux.HandleFunc("POST /debug/clock", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			OffsetSeconds int64 `json:"offset_seconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		skew, ok := g.clock.(*core.SkewClock)
		if !ok {
			http.Error(w, "clock hook unavailable", http.StatusNotImplemented)
			return
		}
		skew.SetOffset(time.Duration(req.OffsetSeconds) * time.Second)
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /debug/kick", func(w http.ResponseWriter, r *http.Request) {
		g.debugCloseEndpoint(w, r, protocol.CloseKicked)
	})
	mux.HandleFunc("POST /debug/expire-token", func(w http.ResponseWriter, r *http.Request) {
		g.debugCloseEndpoint(w, r, protocol.CloseTokenExpired)
	})
	mux.HandleFunc("POST /debug/drain", func(w http.ResponseWriter, _ *http.Request) {
		n := g.Drain()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{"closed": n})
	})
	mux.HandleFunc("POST /debug/legacy-mode", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		g.SetLegacyMode(req.Enabled)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /debug/events/skip", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Count int64 `json:"count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Count <= 0 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		g.SkipNextEvents(req.Count)
		w.WriteHeader(http.StatusNoContent)
	})
}

func (g *Gateway) debugCloseEndpoint(w http.ResponseWriter, r *http.Request, reason protocol.CloseReason) {
	var req struct {
		Tenant string `json:"tenant"`
		User   string `json:"user"`
		Device string `json:"device"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s, err := g.registry.Lookup(session.Endpoint{TenantID: req.Tenant, UserID: req.User, DeviceID: req.Device})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if conn, ok := g.sessionConn(s.ID); ok {
		conn.CloseWithReason(reason, reason.String())
	}
	w.WriteHeader(http.StatusNoContent)
}
