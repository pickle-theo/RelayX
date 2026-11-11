// Package router delivers gateway messages to online sessions. It owns no
// state beyond a reference to the session registry.
package router

import (
	"errors"
	"fmt"
	"strconv"

	"relayx/phase1/internal/protocol"
	"relayx/phase1/internal/session"
)

// Errors returned by the router.
var (
	// ErrTargetOffline reports that a user has no online device able to accept
	// the message.
	ErrTargetOffline = errors.New("router: target offline")
	// ErrTargetNotFound reports that the addressed endpoint does not exist.
	ErrTargetNotFound = errors.New("router: target not found")
)

// Router delivers gateway messages to online sessions. It holds only a
// reference to the concurrent session registry and therefore needs no locking
// of its own.
type Router struct {
	sessions *session.Manager
}

// New returns a Router backed by sessions.
func New(sessions *session.Manager) *Router {
	return &Router{sessions: sessions}
}

// DeliverToUser fans msg out to every online device of userID. It returns the
// number of successful deliveries. When the user has no online device it
// returns ErrTargetOffline.
//
// A device whose Send fails (for example with transport.ErrSendQueueFull or
// transport.ErrConnClosed) is skipped: the transport layer closes that
// connection and the gateway's session hook unregisters it, so one unhealthy
// device never aborts the fan-out. When every delivery fails the router
// reports (0, ErrTargetOffline) rather than a nil error, because the outcome is
// indistinguishable from the user being offline.
func (r *Router) DeliverToUser(from *session.Session, userID string, msgID uint64, payload []byte) (int, error) {
	if from == nil {
		return 0, fmt.Errorf("%w: nil source session", ErrTargetNotFound)
	}
	if userID == "" {
		return 0, fmt.Errorf("%w: empty user id", ErrTargetNotFound)
	}

	targets := r.sessions.FindByUser(userID)
	if len(targets) == 0 {
		return 0, fmt.Errorf("%w: user %q has no online device", ErrTargetOffline, userID)
	}

	body, err := protocol.AppendDeliverBody(nil, &protocol.DeliverBody{
		FromSessionID: from.ID,
		FromUser:      from.UserID,
		FromDevice:    from.DeviceID,
		Payload:       payload,
	})
	if err != nil {
		return 0, fmt.Errorf("router: encode deliver body: %w", err)
	}

	delivered := 0
	for _, target := range targets {
		// The body slice is shared by every frame below. That is safe because
		// Conn.Send encodes the frame synchronously and copies the bytes into
		// its own buffer; it neither retains nor modifies body.
		if target == nil || target.Conn == nil {
			continue
		}
		if err := target.Conn.Send(&protocol.Frame{Type: protocol.Deliver, MsgID: msgID, Body: body}); err != nil {
			// Only this target is lost (see the method comment).
			continue
		}
		delivered++
	}

	if delivered == 0 {
		return 0, fmt.Errorf("%w: all %d deliveries to user %q failed", ErrTargetOffline, len(targets), userID)
	}
	return delivered, nil
}

// DeliverToEndpoint delivers msg to the single session identified by target,
// which is a decimal session ID. Unknown or already closed sessions return
// ErrTargetNotFound.
//
// Delivering to the sender's own session is allowed.
func (r *Router) DeliverToEndpoint(from *session.Session, target string, msgID uint64, payload []byte) (int, error) {
	if from == nil {
		return 0, fmt.Errorf("%w: nil source session", ErrTargetNotFound)
	}

	id, err := strconv.ParseUint(target, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid session id %q", ErrTargetNotFound, target)
	}
	s := r.sessions.Get(id)
	if s == nil || s.Conn == nil {
		return 0, fmt.Errorf("%w: session %d", ErrTargetNotFound, id)
	}

	body, err := protocol.AppendDeliverBody(nil, &protocol.DeliverBody{
		FromSessionID: from.ID,
		FromUser:      from.UserID,
		FromDevice:    from.DeviceID,
		Payload:       payload,
	})
	if err != nil {
		return 0, fmt.Errorf("router: encode deliver body: %w", err)
	}

	if err := s.Conn.Send(&protocol.Frame{Type: protocol.Deliver, MsgID: msgID, Body: body}); err != nil {
		return 0, fmt.Errorf("router: deliver to session %d: %w", id, err)
	}
	return 1, nil
}
