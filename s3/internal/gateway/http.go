package gateway

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"relayx/s3/internal/broadcast"
)

// broadcastRequest 是 POST /v1/broadcasts 的请求体。
// payload 在 JSON 中为 base64 字符串；平台不解析其内容。
type broadcastRequest struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	IdemKey string `json:"idempotency_key,omitempty"`
}

type broadcastResponse struct {
	AcceptedAt string `json:"accepted_at"`
	Duplicate  bool   `json:"duplicate"`
}

// AccessHandler 暴露 HTTP 受理 API。与 /metrics 端口分离（LLD §2.1）。
// 202 Accepted 仅表示已进入扇出流程，不代表任何订阅者已收到。
func (g *Gateway) AccessHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/broadcasts", g.handleBroadcastHTTP)
	return mux
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
		TenantID: producer.TenantID,
		Topic:    req.Topic,
		Payload:  req.Payload,
		IdemKey:  req.IdemKey,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(broadcastResponse{
		AcceptedAt: accepted.AcceptedAt.UTC().Format(time.RFC3339Nano),
		Duplicate:  accepted.Duplicate,
	})
}
