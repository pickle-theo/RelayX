package session

import (
	"fmt"
	"sync"
	"testing"

	"relayx/phase5/internal/protocol"
)

// fakeConn records the calls the session layer makes on a connection.
type fakeConn struct {
	id uint64

	mu          sync.Mutex
	frames      []*protocol.Frame
	closeFrames []*protocol.Frame
	reasons     []string
	closed      int
	closedCh    chan struct{}
}

func newFakeConn(id uint64) *fakeConn {
	return &fakeConn{id: id, closedCh: make(chan struct{}, 1)}
}

func (c *fakeConn) ID() uint64         { return c.id }
func (c *fakeConn) RemoteAddr() string { return fmt.Sprintf("10.0.0.%d:1234", c.id) }

func (c *fakeConn) Send(f *protocol.Frame) error {
	c.mu.Lock()
	c.frames = append(c.frames, f)
	c.mu.Unlock()
	return nil
}

func (c *fakeConn) Close() {
	c.mu.Lock()
	c.closed++
	c.mu.Unlock()
	select {
	case c.closedCh <- struct{}{}:
	default:
	}
}

func (c *fakeConn) CloseWithReason(reason string) {
	c.mu.Lock()
	c.reasons = append(c.reasons, reason)
	c.mu.Unlock()
	c.Close()
}

// CloseWithFrame records the frame and reason, then marks the connection as
// closed, mirroring the real transport: the writer flushes the frame and the
// connection ends afterwards.
func (c *fakeConn) CloseWithFrame(f *protocol.Frame, reason string) error {
	c.mu.Lock()
	c.closeFrames = append(c.closeFrames, f)
	c.reasons = append(c.reasons, reason)
	c.closed++
	c.mu.Unlock()
	select {
	case c.closedCh <- struct{}{}:
	default:
	}
	return nil
}

func (c *fakeConn) sentFrames() []*protocol.Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*protocol.Frame(nil), c.frames...)
}

func (c *fakeConn) closeWithFrameFrames() []*protocol.Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*protocol.Frame(nil), c.closeFrames...)
}

func (c *fakeConn) closeReasons() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.reasons...)
}

func newTestSession(m *Manager, user, device string, conn Conn) *Session {
	return &Session{
		ID:         m.NextSessionID(),
		UserID:     user,
		DeviceID:   device,
		Conn:       conn,
		RemoteAddr: conn.RemoteAddr(),
	}
}

func TestRegisterAndLookup(t *testing.T) {
	m := NewManager()
	conn := newFakeConn(1)
	s := newTestSession(m, "alice", "phone", conn)

	if got := m.NextSessionID(); got != 2 {
		t.Fatalf("NextSessionID() = %d, want 2", got)
	}
	if replaced := m.Register(s); replaced != nil {
		t.Fatalf("Register() replaced = %v, want nil", replaced)
	}
	if got := m.ActiveSessions(); got != 1 {
		t.Fatalf("ActiveSessions() = %d, want 1", got)
	}
	if got := m.Get(s.ID); got != s {
		t.Fatalf("Get(%d) = %v, want session", s.ID, got)
	}
	if got := m.FindByUser("alice"); len(got) != 1 || got[0] != s {
		t.Fatalf("FindByUser(alice) = %v, want [session]", got)
	}
	if got := m.FindByDevice("alice", "phone"); got != s {
		t.Fatalf("FindByDevice(alice, phone) = %v, want session", got)
	}
	if got := m.Snapshot(); len(got) != 1 || got[0] != s {
		t.Fatalf("Snapshot() = %v, want [session]", got)
	}
}

func TestMultipleDevicesPerUser(t *testing.T) {
	m := NewManager()
	phone := newTestSession(m, "alice", "phone", newFakeConn(1))
	laptop := newTestSession(m, "alice", "laptop", newFakeConn(2))

	m.Register(phone)
	m.Register(laptop)

	got := m.FindByUser("alice")
	if len(got) != 2 {
		t.Fatalf("FindByUser(alice) returned %d sessions, want 2", len(got))
	}
	seen := map[*Session]bool{}
	for _, s := range got {
		seen[s] = true
	}
	if !seen[phone] || !seen[laptop] {
		t.Fatalf("FindByUser(alice) = %v, want both phone and laptop", got)
	}
	if m.FindByDevice("alice", "phone") != phone {
		t.Fatal("FindByDevice(alice, phone) did not return phone session")
	}
	if m.FindByDevice("alice", "laptop") != laptop {
		t.Fatal("FindByDevice(alice, laptop) did not return laptop session")
	}
	if got := m.ActiveSessions(); got != 2 {
		t.Fatalf("ActiveSessions() = %d, want 2", got)
	}
}

func TestRegisterReplacesSameDevice(t *testing.T) {
	m := NewManager()
	old := newTestSession(m, "alice", "phone", newFakeConn(1))
	m.Register(old)

	replacement := newTestSession(m, "alice", "phone", newFakeConn(2))
	replaced := m.Register(replacement)

	if replaced != old {
		t.Fatalf("Register() replaced = %v, want old session %v", replaced, old)
	}
	if got := m.ActiveSessions(); got != 1 {
		t.Fatalf("ActiveSessions() = %d, want 1", got)
	}
	if got := m.FindByDevice("alice", "phone"); got != replacement {
		t.Fatalf("FindByDevice(alice, phone) = %v, want replacement", got)
	}
	if got := m.Get(old.ID); got != nil {
		t.Fatalf("Get(old.ID) = %v, want nil", got)
	}
	if got := m.Get(replacement.ID); got != replacement {
		t.Fatalf("Get(replacement.ID) = %v, want replacement", got)
	}
	if got := m.FindByUser("alice"); len(got) != 1 || got[0] != replacement {
		t.Fatalf("FindByUser(alice) = %v, want [replacement]", got)
	}
}

func TestCloseReplaced(t *testing.T) {
	m := NewManager()
	oldConn := newFakeConn(1)
	old := newTestSession(m, "alice", "phone", oldConn)
	m.Register(old)

	replaced := m.Register(newTestSession(m, "alice", "phone", newFakeConn(2)))
	CloseReplaced(replaced)

	frames := oldConn.closeWithFrameFrames()
	if len(frames) != 1 || frames[0].Type != protocol.Close {
		t.Fatalf("CloseReplaced sent close frames %v, want exactly one CLOSE frame via CloseWithFrame", frames)
	}
	if sent := oldConn.sentFrames(); len(sent) != 0 {
		t.Fatalf("CloseReplaced used Send for %v, want CloseWithFrame", sent)
	}
	reasons := oldConn.closeReasons()
	if len(reasons) != 1 || reasons[0] != "replaced by new login" {
		t.Fatalf("old conn close reasons = %v, want [replaced by new login]", reasons)
	}

	// A nil session must be a no-op.
	CloseReplaced(nil)
	CloseReplaced(&Session{})
}

func TestUnregisterProtectsReplacement(t *testing.T) {
	m := NewManager()
	old := newTestSession(m, "alice", "phone", newFakeConn(1))
	m.Register(old)

	replacement := newTestSession(m, "alice", "phone", newFakeConn(2))
	m.Register(replacement)

	if removed := m.Unregister(old); removed {
		t.Fatal("Unregister(old) = true, want false after replacement")
	}
	if got := m.ActiveSessions(); got != 1 {
		t.Fatalf("ActiveSessions() = %d, want 1 after stale Unregister", got)
	}
	if got := m.FindByDevice("alice", "phone"); got != replacement {
		t.Fatalf("FindByDevice(alice, phone) = %v, want replacement", got)
	}

	if removed := m.Unregister(replacement); !removed {
		t.Fatal("Unregister(replacement) = false, want true")
	}
	if got := m.ActiveSessions(); got != 0 {
		t.Fatalf("ActiveSessions() = %d, want 0", got)
	}
	if got := m.FindByUser("alice"); got != nil {
		t.Fatalf("FindByUser(alice) = %v, want nil", got)
	}
}

func TestUnregisterCleansEmptyUserMap(t *testing.T) {
	m := NewManager()
	s := newTestSession(m, "alice", "phone", newFakeConn(1))
	m.Register(s)

	m.mu.RLock()
	usersBefore := len(m.byUser)
	m.mu.RUnlock()
	if usersBefore != 1 {
		t.Fatalf("len(byUser) = %d, want 1 after register", usersBefore)
	}

	if !m.Unregister(s) {
		t.Fatal("Unregister() = false, want true")
	}

	m.mu.RLock()
	usersAfter := len(m.byUser)
	_, exists := m.byUser["alice"]
	m.mu.RUnlock()
	if usersAfter != 0 || exists {
		t.Fatalf("len(byUser) = %d (alice exists = %v), want 0 and false", usersAfter, exists)
	}
}

func TestConcurrentRegisterUnregister(t *testing.T) {
	const workers = 100

	m := NewManager()
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			user := fmt.Sprintf("user-%d", i)
			s := &Session{
				ID:         m.NextSessionID(),
				UserID:     user,
				DeviceID:   "device",
				Conn:       newFakeConn(uint64(i + 1)),
				RemoteAddr: "127.0.0.1:1",
			}
			m.Register(s)
			if got := m.FindByDevice(user, "device"); got != s {
				t.Errorf("FindByDevice(%q, device) = %v, want %v", user, got, s)
			}
			if !m.Unregister(s) {
				t.Errorf("Unregister(%q) = false, want true", user)
			}
		}(i)
	}
	wg.Wait()

	if got := m.ActiveSessions(); got != 0 {
		t.Fatalf("ActiveSessions() = %d, want 0", got)
	}
	if got := len(m.Snapshot()); got != 0 {
		t.Fatalf("len(Snapshot()) = %d, want 0", got)
	}
}
