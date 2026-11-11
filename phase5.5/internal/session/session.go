// Package session tracks the live, authenticated client connections of the
// gateway. It keeps one Session per connection and indexes sessions by
// (user, device) so a repeated login can replace the previous connection.
package session

import (
	"sync"
	"sync/atomic"
	"time"

	"relayx/phase5.5/internal/protocol"
)

// Conn is the subset of *transport.Conn the session layer needs. It is an
// interface so tests can use a lightweight fake instead of a real socket.
type Conn interface {
	ID() uint64
	RemoteAddr() string
	Send(f *protocol.Frame) error
	Close()
	CloseWithReason(reason string)
	CloseWithFrame(f *protocol.Frame, reason string) error
}

// Session is one authenticated client connection. UserID and DeviceID are
// filled in by the login handshake; a user may be online from several devices
// at once, but each (user, device) pair is unique.
type Session struct {
	ID         uint64
	UserID     string
	DeviceID   string
	Conn       Conn
	RemoteAddr string
	CreatedAt  time.Time
	// Version is the cluster route version allocated at login. It is zero in
	// single-node mode and drives the CAS delete on connection close.
	Version uint64
}

// Manager is a concurrent registry of live sessions. The zero value is not
// usable; call NewManager.
type Manager struct {
	mu     sync.RWMutex
	byID   map[uint64]*Session
	byUser map[string]map[string]*Session
	nextID atomic.Uint64
}

// NewManager returns an empty session manager.
func NewManager() *Manager {
	return &Manager{
		byID:   make(map[uint64]*Session),
		byUser: make(map[string]map[string]*Session),
	}
}

// NextSessionID returns a new session identifier. IDs start at 1 and increase
// monotonically, so they are never reused within one manager.
func (m *Manager) NextSessionID() uint64 {
	return m.nextID.Add(1)
}

// Register adds s to the registry. When a session for the same (user, device)
// pair already exists it is removed from both indexes and returned as
// replaced; the caller owns closing that connection (see CloseReplaced).
//
// Register never closes connections itself so it can be called from any
// goroutine without performing blocking work while holding the lock.
func (m *Manager) Register(s *Session) (replaced *Session) {
	if s == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	devices := m.byUser[s.UserID]
	if devices == nil {
		devices = make(map[string]*Session)
		m.byUser[s.UserID] = devices
	}
	if old, ok := devices[s.DeviceID]; ok && old != s {
		replaced = old
		// Drop the superseded session from the ID index as well, but only if
		// the ID still points at it; a stale pointer must not evict a newer
		// session.
		if cur, ok := m.byID[old.ID]; ok && cur == old {
			delete(m.byID, old.ID)
		}
	}

	// A duplicate ID for a different (user, device) key would only happen if a
	// caller reuses an ID by hand; the new registration wins in that case.
	devices[s.DeviceID] = s
	m.byID[s.ID] = s
	return replaced
}

// Unregister removes s from the registry. It reports whether s was still the
// registered session for its ID: after a replacement the old session is no
// longer in the table and Unregister must not evict its successor.
func (m *Manager) Unregister(s *Session) bool {
	if s == nil {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.byID[s.ID]
	if !ok || cur != s {
		return false
	}
	delete(m.byID, s.ID)

	if devices, ok := m.byUser[s.UserID]; ok {
		delete(devices, s.DeviceID)
		if len(devices) == 0 {
			delete(m.byUser, s.UserID)
		}
	}
	return true
}

// Get returns the session registered under id, or nil.
func (m *Manager) Get(id uint64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byID[id]
}

// FindByUser returns all live sessions of userID. The returned slice is a
// copy, so callers may iterate it without holding the manager lock.
func (m *Manager) FindByUser(userID string) []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()

	devices := m.byUser[userID]
	if len(devices) == 0 {
		return nil
	}
	out := make([]*Session, 0, len(devices))
	for _, s := range devices {
		out = append(out, s)
	}
	return out
}

// FindByDevice returns the session of the given (user, device) pair, or nil.
func (m *Manager) FindByDevice(userID, deviceID string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byUser[userID][deviceID]
}

// ActiveSessions returns the number of live sessions.
func (m *Manager) ActiveSessions() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byID)
}

// Snapshot returns a copy of all live sessions.
func (m *Manager) Snapshot() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]*Session, 0, len(m.byID))
	for _, s := range m.byID {
		out = append(out, s)
	}
	return out
}

// CloseReplaced kicks the previous connection of a repeated login: the client
// first receives a CLOSE frame, then the connection is closed with the reason
// "replaced by new login". A nil argument is ignored.
func CloseReplaced(old *Session) {
	if old == nil || old.Conn == nil {
		return
	}
	_ = old.Conn.CloseWithFrame(&protocol.Frame{Type: protocol.Close}, "replaced by new login")
}
