// Package gateway implements the Phase 1 business handler on top of the
// transport: login handshake, message routing and legacy DATA echo.
package gateway

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"relayx/phase1/internal/protocol"
	"relayx/phase1/internal/router"
	"relayx/phase1/internal/session"
	"relayx/phase1/internal/transport"
)

// LOGIN_ACK statuses beyond protocol.LoginStatusOK.
const (
	LoginStatusAlreadyLoggedIn uint8 = 1
	LoginStatusInvalid         uint8 = 2
	LoginStatusInternal        uint8 = 3
)

// Handler is the Phase 1 gateway business handler. It owns the login state of
// every connection and routes SEND frames through the router.
type Handler struct {
	sessions *session.Manager
	router   *router.Router
	logger   *slog.Logger

	// conns maps a connection ID to its *session.Session.
	conns sync.Map
}

// New returns a handler backed by sessions. The handler must be wired to the
// transport both as HandlerFunc and as the close callback (SetOnClose) so
// session state is released when a connection goes away.
func New(sessions *session.Manager, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		sessions: sessions,
		router:   router.New(sessions),
		logger:   logger,
	}
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

	var routeErr error
	switch body.TargetType {
	case protocol.TargetUser:
		_, routeErr = h.router.DeliverToUser(sess, body.Target, f.MsgID, body.Payload)
	case protocol.TargetEndpoint:
		_, routeErr = h.router.DeliverToEndpoint(sess, body.Target, f.MsgID, body.Payload)
	default:
		routeErr = fmt.Errorf("%w: target type %d", protocol.ErrBadTarget, uint8(body.TargetType))
	}
	if routeErr != nil {
		_ = h.sendError(c, f.MsgID, routeErrorMessage(routeErr))
	}
}

func (h *Handler) sendLoginAck(c *transport.Conn, msgID uint64, ack *protocol.LoginAckBody) error {
	body, err := protocol.AppendLoginAckBody(nil, ack)
	if err != nil {
		return err
	}
	return c.Send(&protocol.Frame{Type: protocol.LoginAck, MsgID: msgID, Body: body})
}

func (h *Handler) sendError(c *transport.Conn, msgID uint64, msg string) error {
	return c.Send(&protocol.Frame{Type: protocol.Error, MsgID: msgID, Body: []byte(msg)})
}

// routeErrorMessage turns a router failure into a short, client-readable
// ERROR body.
func routeErrorMessage(err error) string {
	switch {
	case errors.Is(err, router.ErrTargetOffline):
		return "route: target offline"
	case errors.Is(err, router.ErrTargetNotFound):
		return "route: target not found"
	default:
		return "route: " + err.Error()
	}
}
