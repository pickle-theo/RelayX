package router

import (
	"errors"
	"strconv"
	"sync"
	"testing"

	"relayx/phase1/internal/protocol"
	"relayx/phase1/internal/session"
	"relayx/phase1/internal/transport"
)

// fakeConn records the frames a session receives. Send returns err when it is
// set, which lets tests simulate a saturated or closed connection.
type fakeConn struct {
	id uint64

	mu     sync.Mutex
	err    error
	frames []*protocol.Frame
}

func newFakeConn(id uint64) *fakeConn { return &fakeConn{id: id} }

func (c *fakeConn) ID() uint64         { return c.id }
func (c *fakeConn) RemoteAddr() string { return "127.0.0.1:1234" }

func (c *fakeConn) Send(f *protocol.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.frames = append(c.frames, f)
	return nil
}

func (c *fakeConn) Close() {}

func (c *fakeConn) CloseWithReason(string) {}

func (c *fakeConn) CloseWithFrame(f *protocol.Frame, reason string) error {
	if err := c.Send(f); err != nil {
		return err
	}
	c.CloseWithReason(reason)
	return nil
}

func (c *fakeConn) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

func (c *fakeConn) sentFrames() []*protocol.Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*protocol.Frame(nil), c.frames...)
}

// newSession registers a session for user/device backed by conn and returns it.
func newSession(m *session.Manager, user, device string, conn *fakeConn) *session.Session {
	s := &session.Session{
		ID:         m.NextSessionID(),
		UserID:     user,
		DeviceID:   device,
		Conn:       conn,
		RemoteAddr: conn.RemoteAddr(),
	}
	m.Register(s)
	return s
}

// wantDeliver decodes the single frame conn received and checks it is a
// DELIVER frame carrying the expected message.
func wantDeliver(t *testing.T, conn *fakeConn, msgID uint64, from *session.Session, payload []byte) {
	t.Helper()

	frames := conn.sentFrames()
	if len(frames) != 1 {
		t.Fatalf("conn %d received %d frames, want 1", conn.id, len(frames))
	}
	f := frames[0]
	if f.Type != protocol.Deliver {
		t.Fatalf("conn %d frame type = %v, want %v", conn.id, f.Type, protocol.Deliver)
	}
	if f.MsgID != msgID {
		t.Fatalf("conn %d frame MsgID = %d, want %d", conn.id, f.MsgID, msgID)
	}

	body, err := protocol.DecodeDeliver(f.Body)
	if err != nil {
		t.Fatalf("DecodeDeliver() error = %v", err)
	}
	if body.FromSessionID != from.ID {
		t.Fatalf("FromSessionID = %d, want %d", body.FromSessionID, from.ID)
	}
	if body.FromUser != from.UserID {
		t.Fatalf("FromUser = %q, want %q", body.FromUser, from.UserID)
	}
	if body.FromDevice != from.DeviceID {
		t.Fatalf("FromDevice = %q, want %q", body.FromDevice, from.DeviceID)
	}
	if string(body.Payload) != string(payload) {
		t.Fatalf("Payload = %q, want %q", body.Payload, payload)
	}
}

func TestDeliverToUserFanOut(t *testing.T) {
	m := session.NewManager()
	phoneConn := newFakeConn(1)
	laptopConn := newFakeConn(2)
	newSession(m, "alice", "phone", phoneConn)
	newSession(m, "alice", "laptop", laptopConn)
	from := newSession(m, "bob", "desktop", newFakeConn(3))

	r := New(m)
	payload := []byte("hello alice")

	got, err := r.DeliverToUser(from, "alice", 42, payload)
	if err != nil {
		t.Fatalf("DeliverToUser() error = %v, want nil", err)
	}
	if got != 2 {
		t.Fatalf("DeliverToUser() = %d, want 2", got)
	}

	wantDeliver(t, phoneConn, 42, from, payload)
	wantDeliver(t, laptopConn, 42, from, payload)
}

func TestDeliverToEndpointOnlyTarget(t *testing.T) {
	m := session.NewManager()
	targetConn := newFakeConn(1)
	otherConn := newFakeConn(2)
	target := newSession(m, "alice", "phone", targetConn)
	newSession(m, "alice", "laptop", otherConn)
	from := newSession(m, "bob", "desktop", newFakeConn(3))

	r := New(m)
	payload := []byte("direct")

	got, err := r.DeliverToEndpoint(from, strconv.FormatUint(target.ID, 10), 7, payload)
	if err != nil {
		t.Fatalf("DeliverToEndpoint() error = %v, want nil", err)
	}
	if got != 1 {
		t.Fatalf("DeliverToEndpoint() = %d, want 1", got)
	}
	wantDeliver(t, targetConn, 7, from, payload)
	if frames := otherConn.sentFrames(); len(frames) != 0 {
		t.Fatalf("other device received %d frames, want 0", len(frames))
	}
}

func TestDeliverToUserOffline(t *testing.T) {
	m := session.NewManager()
	from := newSession(m, "bob", "desktop", newFakeConn(1))

	r := New(m)

	if got, err := r.DeliverToUser(from, "alice", 1, []byte("x")); got != 0 || !errors.Is(err, ErrTargetOffline) {
		t.Fatalf("DeliverToUser(offline) = (%d, %v), want (0, %v)", got, err, ErrTargetOffline)
	}
}

func TestDeliverToEndpointNotFound(t *testing.T) {
	m := session.NewManager()
	from := newSession(m, "bob", "desktop", newFakeConn(1))
	gone := newSession(m, "alice", "phone", newFakeConn(2))
	m.Unregister(gone)

	r := New(m)

	tests := []struct {
		name   string
		target string
	}{
		{name: "unknown id", target: strconv.FormatUint(gone.ID, 10)},
		{name: "not a number", target: "abc"},
		{name: "zero", target: "0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.DeliverToEndpoint(from, tc.target, 1, []byte("x"))
			if got != 0 || !errors.Is(err, ErrTargetNotFound) {
				t.Fatalf("DeliverToEndpoint(%q) = (%d, %v), want (0, %v)", tc.target, got, err, ErrTargetNotFound)
			}
		})
	}
}

func TestDeliverToUserSkipsFailedTarget(t *testing.T) {
	m := session.NewManager()
	fullConn := newFakeConn(1)
	fullConn.setErr(transport.ErrSendQueueFull)
	okConn := newFakeConn(2)
	newSession(m, "alice", "phone", fullConn)
	newSession(m, "alice", "laptop", okConn)
	from := newSession(m, "bob", "desktop", newFakeConn(3))

	r := New(m)
	payload := []byte("still there?")

	got, err := r.DeliverToUser(from, "alice", 9, payload)
	if err != nil {
		t.Fatalf("DeliverToUser() error = %v, want nil", err)
	}
	if got != 1 {
		t.Fatalf("DeliverToUser() = %d, want 1", got)
	}
	if frames := fullConn.sentFrames(); len(frames) != 0 {
		t.Fatalf("full conn received %d frames, want 0", len(frames))
	}
	wantDeliver(t, okConn, 9, from, payload)
}

func TestDeliverToUserAllTargetsFail(t *testing.T) {
	m := session.NewManager()
	for i, device := range []string{"phone", "laptop"} {
		conn := newFakeConn(uint64(i + 1))
		conn.setErr(transport.ErrConnClosed)
		newSession(m, "alice", device, conn)
	}
	from := newSession(m, "bob", "desktop", newFakeConn(3))

	r := New(m)

	got, err := r.DeliverToUser(from, "alice", 11, []byte("x"))
	if got != 0 || !errors.Is(err, ErrTargetOffline) {
		t.Fatalf("DeliverToUser() = (%d, %v), want (0, %v)", got, err, ErrTargetOffline)
	}
}

func TestDeliverToUserEmptyPayload(t *testing.T) {
	m := session.NewManager()
	conn := newFakeConn(1)
	newSession(m, "alice", "phone", conn)
	from := newSession(m, "bob", "desktop", newFakeConn(2))

	r := New(m)

	got, err := r.DeliverToUser(from, "alice", 12, nil)
	if err != nil {
		t.Fatalf("DeliverToUser() error = %v, want nil", err)
	}
	if got != 1 {
		t.Fatalf("DeliverToUser() = %d, want 1", got)
	}
	wantDeliver(t, conn, 12, from, nil)
}

func TestDeliverToEndpointSelf(t *testing.T) {
	m := session.NewManager()
	conn := newFakeConn(1)
	from := newSession(m, "alice", "phone", conn)

	r := New(m)
	payload := []byte("echo")

	got, err := r.DeliverToEndpoint(from, strconv.FormatUint(from.ID, 10), 13, payload)
	if err != nil {
		t.Fatalf("DeliverToEndpoint(self) error = %v, want nil", err)
	}
	if got != 1 {
		t.Fatalf("DeliverToEndpoint(self) = %d, want 1", got)
	}
	wantDeliver(t, conn, 13, from, payload)
}
