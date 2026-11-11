// Package message implements the shared message-acceptance path used by the
// Gateway SEND handler and the HTTP Message Service. Both entry points produce
// the same message + outbox transaction inside the store.
package message

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"relayx/phase5.5/internal/broker"
	"relayx/phase5.5/internal/store"
)

// ValidationError marks a request rejected before touching the database.
type ValidationError struct{ Message string }

func (e ValidationError) Error() string { return e.Message }

// IsValidation reports whether err is a request validation failure.
func IsValidation(err error) bool {
	var v ValidationError
	return errors.As(err, &v)
}

// Request is one message acceptance.
type Request struct {
	TenantID    string
	ClientMsgID uint64
	FromUser    string
	FromDevice  string
	TargetType  store.TargetType
	Target      string
	Payload     []byte
	// TTL overrides the service default when positive.
	TTL time.Duration
	// TargetUser and TargetDevice may be supplied by the Gateway after it has
	// resolved a node-local session id; they override the parsed target.
	TargetUser   string
	TargetDevice string
}

// Service accepts messages on behalf of one store.
type Service struct {
	store      *store.Store
	defaultTTL time.Duration
	maxBody    int
}

// New returns a Service. maxBody <= 0 disables the payload size check.
func New(st *store.Store, defaultTTL time.Duration, maxBody int) *Service {
	return &Service{store: st, defaultTTL: defaultTTL, maxBody: maxBody}
}

// Accept validates req and writes message + outbox in one transaction. The
// returned Duplicate flag means the (tenant, from_user, client_msg_id) key was
// already accepted and nothing new was written.
func (s *Service) Accept(ctx context.Context, req Request) (store.AcceptResult, error) {
	if s == nil || s.store == nil {
		return store.AcceptResult{}, errors.New("message: store unavailable")
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = store.DefaultTenant
	}
	if strings.TrimSpace(req.FromUser) == "" {
		return store.AcceptResult{}, ValidationError{Message: "from_user must not be empty"}
	}
	if strings.TrimSpace(req.Target) == "" {
		return store.AcceptResult{}, ValidationError{Message: "target must not be empty"}
	}
	if s.maxBody > 0 && len(req.Payload) > s.maxBody {
		return store.AcceptResult{}, ValidationError{
			Message: fmt.Sprintf("payload size %d exceeds max-body %d", len(req.Payload), s.maxBody),
		}
	}
	if req.TTL < 0 {
		return store.AcceptResult{}, ValidationError{Message: "ttl must not be negative"}
	}
	if req.TargetType != store.TargetUser && req.TargetType != store.TargetEndpoint {
		return store.AcceptResult{}, ValidationError{Message: "target_type must be user or endpoint"}
	}

	targetUser, targetDevice := req.TargetUser, req.TargetDevice
	if req.TargetType == store.TargetEndpoint && targetUser == "" {
		user, device, ok := ParseEndpointTarget(req.Target)
		if !ok {
			return store.AcceptResult{}, ValidationError{
				Message: "endpoint target must be user:device",
			}
		}
		targetUser, targetDevice = user, device
	}

	ttl := req.TTL
	if ttl == 0 {
		ttl = s.defaultTTL
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	expireAt := time.Now().Add(ttl)

	clientMsgID := req.ClientMsgID
	if clientMsgID == 0 {
		id, err := randomID()
		if err != nil {
			return store.AcceptResult{}, err
		}
		clientMsgID = id
	}

	event := broker.FanoutEvent{
		TenantID:     tenant,
		FromUser:     req.FromUser,
		FromDevice:   req.FromDevice,
		TargetType:   int16(req.TargetType),
		Target:       req.Target,
		TargetUser:   targetUser,
		TargetDevice: targetDevice,
		Payload:      req.Payload,
		ExpireAt:     expireAt,
	}
	keyUser := req.Target
	if targetUser != "" {
		keyUser = targetUser
	}

	return s.store.AcceptMessage(ctx, store.AcceptRequest{
		TenantID:     tenant,
		ClientMsgID:  clientMsgID,
		FromUser:     req.FromUser,
		FromDevice:   req.FromDevice,
		TargetType:   req.TargetType,
		Target:       req.Target,
		Payload:      req.Payload,
		ExpireAt:     expireAt,
		TargetUser:   targetUser,
		TargetDevice: targetDevice,
		OutboxTopic:  broker.TopicFanout,
		OutboxKey:    broker.FanoutKey(tenant, keyUser),
		OutboxPayload: func(messageID uint64) ([]byte, error) {
			event.MessageID = messageID
			return broker.EncodeFanout(event)
		},
	})
}

// ParseEndpointTarget splits a canonical "user:device" endpoint target.
func ParseEndpointTarget(target string) (user, device string, ok bool) {
	user, device, ok = strings.Cut(target, ":")
	if !ok || strings.TrimSpace(user) == "" || strings.TrimSpace(device) == "" {
		return "", "", false
	}
	return user, device, true
}

func randomID() (uint64, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, fmt.Errorf("message: generate client_msg_id: %w", err)
	}
	id := binary.BigEndian.Uint64(buf[:])
	if id == 0 {
		id = 1
	}
	return id, nil
}
