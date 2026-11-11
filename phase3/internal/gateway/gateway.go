// Package gateway implements the Phase 2 reliable-delivery business handler on
// top of the transport: login handshake with endpoint persistence, SEND
// acceptance with idempotency, two-level ACKs and offline PULL recovery.
package gateway

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"relayx/phase3/internal/delivery"
	"relayx/phase3/internal/protocol"
	"relayx/phase3/internal/session"
	"relayx/phase3/internal/store"
	"relayx/phase3/internal/transport"
)

// LOGIN_ACK statuses beyond protocol.LoginStatusOK.
const (
	LoginStatusAlreadyLoggedIn uint8 = 1
	LoginStatusInvalid         uint8 = 2
	LoginStatusInternal        uint8 = 3
)

// Defaults applied when the matching Config field is not positive.
const (
	DefaultMessageTTL   = 24 * time.Hour
	DefaultPullLimit    = 100
	DefaultPullMaxLimit = 500
	// maxPullLimit is the largest page size the PULL_DONE body can report
	// because Count is a uint16.
	maxPullLimit = 65535
	// dbTimeout bounds every database call made from the connection goroutine.
	dbTimeout = 5 * time.Second
)

// Config controls the reliable-delivery behaviour of the handler.
type Config struct {
	// MessageTTL is added to the acceptance time to compute expire_at; values
	// <= 0 fall back to DefaultMessageTTL.
	MessageTTL time.Duration
	// PullLimit is the page size used when a PULL body requests 0; values <= 0
	// fall back to DefaultPullLimit.
	PullLimit int
	// PullMaxLimit caps a client-requested PULL page size; values <= 0 fall
	// back to DefaultPullMaxLimit and the value is never above 65535.
	PullMaxLimit int
}

func (c Config) withDefaults() Config {
	if c.MessageTTL <= 0 {
		c.MessageTTL = DefaultMessageTTL
	}
	if c.PullLimit <= 0 {
		c.PullLimit = DefaultPullLimit
	}
	if c.PullMaxLimit <= 0 {
		c.PullMaxLimit = DefaultPullMaxLimit
	}
	if c.PullMaxLimit > maxPullLimit {
		c.PullMaxLimit = maxPullLimit
	}
	if c.PullMaxLimit < c.PullLimit {
		c.PullMaxLimit = c.PullLimit
	}
	return c
}

// Handler is the Phase 2 gateway business handler. It owns the login state of
// every connection and persists accepted messages through the store.
type Handler struct {
	sessions  *session.Manager
	store     *store.Store
	deliverer *delivery.Deliverer
	cfg       Config
	logger    *slog.Logger

	// conns maps a connection ID to its *session.Session.
	conns sync.Map

	accepted  atomic.Uint64
	duplicate atomic.Uint64
}

// New returns a handler backed by sessions and st. The handler must be wired to
// the transport both as HandlerFunc and as the close callback (SetOnClose) so
// session state is released when a connection goes away.
func New(sessions *session.Manager, st *store.Store, cfg Config, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		sessions:  sessions,
		store:     st,
		deliverer: delivery.NewDeliverer(sessions),
		cfg:       cfg.withDefaults(),
		logger:    logger,
	}
}

// Stats returns the number of accepted and duplicate SEND frames observed
// since the handler was created.
func (h *Handler) Stats() (accepted, duplicate uint64) {
	return h.accepted.Load(), h.duplicate.Load()
}

// Handle dispatches one decoded frame. PING and CLOSE never reach it: the
// transport answers them itself.
func (h *Handler) Handle(c *transport.Conn, f *protocol.Frame) error {
	switch f.Type {
	case protocol.Login:
		return h.handleLogin(c, f)
	case protocol.Send:
		h.handleSend(c, f)
		return nil
	case protocol.Data:
		return c.Send(&protocol.Frame{Type: protocol.Data, MsgID: f.MsgID, Body: f.Body})
	case protocol.ReceiptAck:
		h.handleReceiptAck(c, f)
		return nil
	case protocol.BusinessAck:
		h.handleBusinessAck(c, f)
		return nil
	case protocol.Pull:
		h.handlePull(c, f)
		return nil
	default:
		return nil
	}
}

// OnConnClosed releases the session bound to c. It is registered with the
// transport as the server close callback and must not block.
func (h *Handler) OnConnClosed(c *transport.Conn) {
	if c == nil {
		return
	}
	v, ok := h.conns.LoadAndDelete(c.ID())
	if !ok {
		return
	}
	sess, ok := v.(*session.Session)
	if !ok {
		return
	}
	h.sessions.Unregister(sess)
	h.logger.Info("session closed",
		"session_id", sess.ID,
		"user_id", sess.UserID,
		"device_id", sess.DeviceID)
}

// SessionFor returns the session logged in on c, or nil when the connection
// has not logged in yet.
func (h *Handler) SessionFor(c *transport.Conn) *session.Session {
	if c == nil {
		return nil
	}
	v, ok := h.conns.Load(c.ID())
	if !ok {
		return nil
	}
	sess, _ := v.(*session.Session)
	return sess
}

func (h *Handler) handleLogin(c *transport.Conn, f *protocol.Frame) error {
	body, err := protocol.DecodeLogin(f.Body)
	if err != nil {
		return h.sendLoginAck(c, f.MsgID, &protocol.LoginAckBody{
			Status:  LoginStatusInvalid,
			Message: err.Error(),
		})
	}

	if _, ok := h.conns.Load(c.ID()); ok {
		return h.sendLoginAck(c, f.MsgID, &protocol.LoginAckBody{
			Status:  LoginStatusAlreadyLoggedIn,
			Message: "already logged in",
		})
	}

	if err := h.upsertEndpoint(body.UserID, body.DeviceID); err != nil {
		h.logger.Warn("gateway: upsert endpoint failed",
			"user_id", body.UserID,
			"device_id", body.DeviceID,
			"error", err)
		return h.sendLoginAck(c, f.MsgID, &protocol.LoginAckBody{
			Status:  LoginStatusInternal,
			Message: "store unavailable",
		})
	}

	sess := &session.Session{
		ID:         h.sessions.NextSessionID(),
		UserID:     body.UserID,
		DeviceID:   body.DeviceID,
		Conn:       c,
		RemoteAddr: c.RemoteAddr(),
		CreatedAt:  time.Now(),
	}
	replaced := h.sessions.Register(sess)
	h.conns.Store(c.ID(), sess)
	session.CloseReplaced(replaced)

	return h.sendLoginAck(c, f.MsgID, &protocol.LoginAckBody{
		Status:    protocol.LoginStatusOK,
		SessionID: sess.ID,
	})
}

func (h *Handler) handleSend(c *transport.Conn, f *protocol.Frame) {
	body, err := protocol.DecodeSend(f.Body)
	if err != nil {
		_ = h.sendError(c, f.MsgID, err.Error())
		return
	}

	sess := h.SessionFor(c)
	if sess == nil {
		_ = h.sendError(c, f.MsgID, "not logged in")
		return
	}

	if h.store == nil {
		_ = h.sendSendAck(c, f.MsgID, protocol.SendAckInternal, 0, "store unavailable")
		return
	}

	recipients, targetType, status, ackMessage := h.resolveRecipients(body)
	if status != protocol.SendAckAccepted {
		_ = h.sendSendAck(c, f.MsgID, status, 0, ackMessage)
		return
	}

	// Resolve the live sessions before accepting so the delivery rows of online
	// targets are persisted directly as SENT: the realtime push then needs no
	// second write transaction. A session that goes away in between is covered
	// by the retry worker, exactly like a push that fails after being marked.
	targets := make(map[store.Endpoint]*session.Session, len(recipients))
	online := make(map[store.Endpoint]bool, len(recipients))
	for _, ep := range recipients {
		if target := h.sessions.FindByDevice(ep.UserID, ep.DeviceID); target != nil {
			targets[ep] = target
			online[ep] = true
		}
	}

	ctx, cancel := h.dbContext()
	defer cancel()

	res, err := h.store.AcceptMessage(ctx, store.AcceptRequest{
		ClientMsgID: f.MsgID,
		FromUser:    sess.UserID,
		FromDevice:  sess.DeviceID,
		TargetType:  targetType,
		Target:      body.Target,
		Payload:     body.Payload,
		ExpireAt:    time.Now().Add(h.cfg.MessageTTL),
		Endpoints:   recipients,
		Online:      online,
	})
	if err != nil {
		h.logger.Warn("gateway: accept message failed",
			"client_msg_id", f.MsgID,
			"from_user", sess.UserID,
			"error", err)
		_ = h.sendSendAck(c, f.MsgID, protocol.SendAckInternal, 0, "store unavailable")
		return
	}

	if res.Duplicate {
		h.duplicate.Add(1)
		_ = h.sendSendAck(c, f.MsgID, protocol.SendAckDuplicate, res.MessageID, "duplicate")
		return
	}

	h.accepted.Add(1)
	_ = h.sendSendAck(c, f.MsgID, protocol.SendAckAccepted, res.MessageID, "")

	for _, ep := range recipients {
		target := targets[ep]
		if target == nil {
			// Offline target: the row stays PENDING until PULL or the TTL.
			continue
		}
		if err := h.deliverer.Deliver(target, res.MessageID, sess.UserID, sess.DeviceID, body.Payload); err != nil {
			h.logger.Debug("gateway: realtime deliver failed",
				"message_id", res.MessageID,
				"user_id", ep.UserID,
				"device_id", ep.DeviceID,
				"error", err)
		}
	}
}

// resolveRecipients turns a SEND body into the recipient endpoint set. A status
// other than SendAckAccepted reports a routing failure; the caller forwards it
// verbatim as the SEND_ACK status together with ackMessage.
func (h *Handler) resolveRecipients(body *protocol.SendBody) ([]store.Endpoint, store.TargetType, protocol.SendAckStatus, string) {
	switch body.TargetType {
	case protocol.TargetUser:
		ctx, cancel := h.dbContext()
		defer cancel()
		endpoints, err := h.store.ListEndpoints(ctx, body.Target)
		if err != nil {
			h.logger.Warn("gateway: list endpoints failed",
				"target", body.Target, "error", err)
			return nil, store.TargetUser, protocol.SendAckInternal, "store unavailable"
		}
		if len(endpoints) == 0 {
			return nil, store.TargetUser, protocol.SendAckInvalid, "target has no endpoint"
		}
		return endpoints, store.TargetUser, protocol.SendAckAccepted, ""
	case protocol.TargetEndpoint:
		id, err := strconv.ParseUint(body.Target, 10, 64)
		if err != nil {
			return nil, store.TargetEndpoint, protocol.SendAckInvalid, "target not found"
		}
		target := h.sessions.Get(id)
		if target == nil {
			return nil, store.TargetEndpoint, protocol.SendAckInvalid, "target not found"
		}
		return []store.Endpoint{{UserID: target.UserID, DeviceID: target.DeviceID}}, store.TargetEndpoint, protocol.SendAckAccepted, ""
	default:
		return nil, store.TargetUser, protocol.SendAckInvalid, "bad target type"
	}
}

func (h *Handler) handleReceiptAck(c *transport.Conn, f *protocol.Frame) {
	body, err := protocol.DecodeReceiptAck(f.Body)
	if err != nil {
		_ = h.sendError(c, f.MsgID, err.Error())
		return
	}
	sess := h.SessionFor(c)
	if sess == nil {
		_ = h.sendError(c, f.MsgID, "not logged in")
		return
	}
	if h.store == nil {
		return
	}
	ctx, cancel := h.dbContext()
	defer cancel()
	if _, err := h.store.ApplyReceiptAck(ctx, sess.UserID, sess.DeviceID, body.MessageID); err != nil {
		h.logger.Warn("gateway: apply receipt ack failed",
			"message_id", body.MessageID,
			"user_id", sess.UserID,
			"device_id", sess.DeviceID,
			"error", err)
	}
}

func (h *Handler) handleBusinessAck(c *transport.Conn, f *protocol.Frame) {
	body, err := protocol.DecodeBusinessAck(f.Body)
	if err != nil {
		_ = h.sendError(c, f.MsgID, err.Error())
		return
	}
	sess := h.SessionFor(c)
	if sess == nil {
		_ = h.sendError(c, f.MsgID, "not logged in")
		return
	}
	if h.store == nil {
		return
	}
	ctx, cancel := h.dbContext()
	defer cancel()
	if _, err := h.store.ApplyBusinessAck(ctx, sess.UserID, sess.DeviceID, body.MessageID, body.Status); err != nil {
		h.logger.Warn("gateway: apply business ack failed",
			"message_id", body.MessageID,
			"status", body.Status,
			"user_id", sess.UserID,
			"device_id", sess.DeviceID,
			"error", err)
	}
}

func (h *Handler) handlePull(c *transport.Conn, f *protocol.Frame) {
	body, err := protocol.DecodePull(f.Body)
	if err != nil {
		_ = h.sendError(c, f.MsgID, err.Error())
		return
	}
	sess := h.SessionFor(c)
	if sess == nil {
		_ = h.sendError(c, f.MsgID, "not logged in")
		return
	}
	if h.store == nil {
		_ = h.sendError(c, f.MsgID, "store: unavailable")
		return
	}

	limit := int(body.Limit)
	if limit <= 0 {
		limit = h.cfg.PullLimit
	}
	if limit > h.cfg.PullMaxLimit {
		limit = h.cfg.PullMaxLimit
	}

	ctx, cancel := h.dbContext()
	defer cancel()
	items, hasMore, err := h.store.PullPending(ctx, sess.UserID, sess.DeviceID, body.Cursor, limit)
	if err != nil {
		h.logger.Warn("gateway: pull pending failed",
			"user_id", sess.UserID,
			"device_id", sess.DeviceID,
			"error", err)
		_ = h.sendError(c, f.MsgID, "store: unavailable")
		return
	}

	count := 0
	nextCursor := body.Cursor
	for i := range items {
		if err := h.deliverer.Deliver(sess, items[i].MessageID, items[i].FromUser, items[i].FromDevice, items[i].Payload); err != nil {
			h.logger.Debug("gateway: pull deliver failed",
				"message_id", items[i].MessageID,
				"delivery_id", items[i].ID,
				"error", err)
			hasMore = true
			break
		}
		count++
		nextCursor = items[i].ID
	}

	_ = h.sendPullDone(c, f.MsgID, &protocol.PullDoneBody{
		Count:      uint16(count),
		NextCursor: nextCursor,
		HasMore:    hasMore,
	})
}

func (h *Handler) upsertEndpoint(userID, deviceID string) error {
	if h.store == nil {
		return errStoreUnavailable
	}
	ctx, cancel := h.dbContext()
	defer cancel()
	return h.store.UpsertEndpoint(ctx, userID, deviceID)
}

// dbContext bounds one database call made from a connection goroutine.
func (h *Handler) dbContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), dbTimeout)
}

func (h *Handler) sendLoginAck(c *transport.Conn, msgID uint64, ack *protocol.LoginAckBody) error {
	body, err := protocol.AppendLoginAckBody(nil, ack)
	if err != nil {
		return err
	}
	return c.Send(&protocol.Frame{Type: protocol.LoginAck, MsgID: msgID, Body: body})
}

func (h *Handler) sendSendAck(c *transport.Conn, msgID uint64, status protocol.SendAckStatus, messageID uint64, msg string) error {
	body, err := protocol.AppendSendAckBody(nil, &protocol.SendAckBody{
		Status:    status,
		MessageID: messageID,
		Message:   msg,
	})
	if err != nil {
		return err
	}
	return c.Send(&protocol.Frame{Type: protocol.SendAck, MsgID: msgID, Body: body})
}

func (h *Handler) sendPullDone(c *transport.Conn, msgID uint64, done *protocol.PullDoneBody) error {
	body, err := protocol.AppendPullDoneBody(nil, done)
	if err != nil {
		return err
	}
	return c.Send(&protocol.Frame{Type: protocol.PullDone, MsgID: msgID, Body: body})
}

func (h *Handler) sendError(c *transport.Conn, msgID uint64, msg string) error {
	return c.Send(&protocol.Frame{Type: protocol.Error, MsgID: msgID, Body: []byte(msg)})
}

var errStoreUnavailable = errors.New("gateway: store unavailable")
