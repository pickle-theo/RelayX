// Package delivery pushes reliable DELIVER frames to online sessions and runs
// the background retry/TTL worker of Phase 2.
package delivery

import (
	"errors"
	"fmt"

	"relayx/phase5/internal/protocol"
	"relayx/phase5/internal/session"
)

// ErrOffline reports that a user has no online device to deliver to.
var ErrOffline = errors.New("delivery: target offline")

// Deliverer builds DELIVER frames and enqueues them on session connections.
// The frame header MsgID is the platform message ID, so clients can echo it in
// their two-level ACKs.
type Deliverer struct {
	sessions *session.Manager
}

// NewDeliverer returns a Deliverer backed by sessions.
func NewDeliverer(sessions *session.Manager) *Deliverer {
	return &Deliverer{sessions: sessions}
}

// Deliver enqueues one DELIVER frame for msgID on sess. FromSessionID is always
// 0 because a retry may happen long after the original sender connection is
// gone; recipients identify the sender by FromUser/FromDevice.
func (d *Deliverer) Deliver(sess *session.Session, msgID uint64, fromUser, fromDevice string, payload []byte) error {
	if sess == nil || sess.Conn == nil {
		return errors.New("delivery: session unavailable")
	}
	body, err := protocol.AppendDeliverBody(nil, &protocol.DeliverBody{
		FromSessionID: 0,
		FromUser:      fromUser,
		FromDevice:    fromDevice,
		Payload:       payload,
	})
	if err != nil {
		return fmt.Errorf("delivery: encode deliver body: %w", err)
	}
	if err := sess.Conn.Send(&protocol.Frame{Type: protocol.Deliver, MsgID: msgID, Body: body}); err != nil {
		return fmt.Errorf("delivery: enqueue deliver frame: %w", err)
	}
	return nil
}

// DeliverToUser fans msg out to every online device of userID and returns the
// number of frames successfully enqueued. A device whose Send fails is skipped
// and never aborts the fan-out. When userID has no online device it returns
// (0, ErrOffline).
func (d *Deliverer) DeliverToUser(userID string, msgID uint64, fromUser, fromDevice string, payload []byte) (int, error) {
	if d == nil || d.sessions == nil {
		return 0, ErrOffline
	}
	targets := d.sessions.FindByUser(userID)
	if len(targets) == 0 {
		return 0, ErrOffline
	}

	delivered := 0
	for _, target := range targets {
		if err := d.Deliver(target, msgID, fromUser, fromDevice, payload); err != nil {
			continue
		}
		delivered++
	}
	return delivered, nil
}
