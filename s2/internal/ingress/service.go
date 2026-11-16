// Package ingress 实现 TCP SEND 与 HTTP POST /v1/messages
// 共用的唯一受理路径。身份始终由调用方从已验证的
// 令牌/会话提供，请求体无法伪造 producer 或来源。
package ingress

import (
	"bytes"
	"context"
	"time"

	"relayx/s2/internal/config"
	"relayx/s2/internal/engine"
	"relayx/s2/internal/idgen"
	"relayx/s2/internal/store"
)

type SubmitInput struct {
	TenantID       string
	ProducerID     string
	ProducerKind   string
	Sender         string
	SourceDeviceID string
	ClientMsgID    string
	TargetUserID   string
	TargetDevices  []string
	Payload        []byte
	Policy         config.Policy
}

type SubmitResult struct {
	MessageID  string
	State      string
	Duplicate  bool
	AcceptedAt time.Time
}

// Service 即 S2 IngressService。
type Service struct {
	st  store.Store
	cfg config.Config
	eng *engine.Engine
}

func New(st store.Store, cfg config.Config, eng *engine.Engine) *Service {
	return &Service{st: st, cfg: cfg, eng: eng}
}

func (s *Service) Submit(ctx context.Context, in SubmitInput) (SubmitResult, error) {
	if in.TenantID == "" || in.ProducerID == "" || in.ClientMsgID == "" || in.TargetUserID == "" {
		return SubmitResult{}, store.ErrPolicyLimit
	}
	if len(in.Payload) == 0 || int32(len(in.Payload)) > s.cfg.MaxPayload {
		return SubmitResult{}, store.ErrPolicyLimit
	}
	policy, ok := config.ClampAndValidate(in.Policy, s.cfg)
	if !ok {
		return SubmitResult{}, store.ErrPolicyLimit
	}
	if in.ProducerKind != store.ProducerUser && in.ProducerKind != store.ProducerService {
		in.ProducerKind = store.ProducerUser
	}

	// 幂等判定放在写事务内由 Insert 原子完成（见 MessageStore.Insert），
	// 不再做「先查后写」，否则并发重放会各自生成 message_id。
	limit := policy.Fanout
	eps, err := s.st.Endpoints().Resolve(ctx, in.TenantID, in.TargetUserID,
		in.TargetDevices, policy.DeliveryMode, limit)
	if err != nil {
		return SubmitResult{}, err
	}

	m := &store.Message{
		ID:             idgen.New("msg"),
		TenantID:       in.TenantID,
		ProducerID:     in.ProducerID,
		ProducerKind:   in.ProducerKind,
		Sender:         in.Sender,
		SourceDeviceID: in.SourceDeviceID,
		ClientMsgID:    in.ClientMsgID,
		Payload:        append([]byte(nil), in.Payload...),
		PayloadSize:    int32(len(in.Payload)),
		DeliveryMode:   policy.DeliveryMode,
		Fanout:         policy.Fanout,
		TTLSeconds:     policy.TTLSeconds,
		MaxAttempts:    policy.MaxAttempts,
		PlatformState:  store.MessageAccepted,
	}
	ds := s.eng.PlanDeliveries(m, eps)

	var duplicate *SubmitResult
	var settled bool
	err = s.st.WithinTx(ctx, func(tx store.Tx) error {
		inserted, existing, err := tx.Messages().Insert(ctx, tx, m)
		if err != nil {
			return err
		}
		if !inserted {
			// 键已被并发请求占用：事务内读到的那条即权威消息。
			// 直接返回重放结果，绝不写本请求的 deliveries/facts。
			if !bytes.Equal(existing.Payload, in.Payload) {
				return store.ErrDuplicateMismatch
			}
			duplicate = &SubmitResult{
				MessageID:  existing.ID,
				State:      existing.PlatformState,
				Duplicate:  true,
				AcceptedAt: existing.CreatedAt,
			}
			return nil
		}
		if err := tx.Deliveries().InsertBatch(ctx, tx, ds); err != nil {
			return err
		}
		facts := []*store.Fact{s.eng.MessageAcceptedFact(m, len(ds))}
		for _, d := range ds {
			facts = append(facts, s.eng.DeliveryCreatedFact(d))
		}
		if err := tx.Facts().InsertBatch(ctx, tx, facts); err != nil {
			return err
		}
		settled, err = s.eng.OnFanoutDone(ctx, tx, m.ID)
		return err
	})
	if err != nil {
		return SubmitResult{}, err
	}
	if duplicate != nil {
		return *duplicate, nil
	}

	state := store.MessageAccepted
	if settled {
		state = store.MessageSettled
	}
	return SubmitResult{
		MessageID:  m.ID,
		State:      state,
		Duplicate:  false,
		AcceptedAt: time.Now().UTC(),
	}, nil
}
