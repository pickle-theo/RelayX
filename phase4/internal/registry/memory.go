package registry

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Memory is an in-process Registry for unit tests. It mirrors the Redis
// implementation's TTL, version and CAS semantics without any external
// dependency.
type Memory struct {
	mu      sync.Mutex
	nodeTTL time.Duration

	nodes      map[string]memoryNode
	versions   map[string]uint64
	routes     map[string]Route
	nodeRoutes map[string]map[string]struct{} // node ID -> route keys
}

type memoryNode struct {
	Node
	expiresAt time.Time
}

var _ Registry = (*Memory)(nil)

// NewMemory returns an empty in-memory registry. A non-positive nodeTTL falls
// back to 15s, like RedisConfig.NodeTTL.
func NewMemory(nodeTTL time.Duration) *Memory {
	if nodeTTL <= 0 {
		nodeTTL = defaultNodeTTL
	}
	return &Memory{
		nodeTTL:    nodeTTL,
		nodes:      make(map[string]memoryNode),
		versions:   make(map[string]uint64),
		routes:     make(map[string]Route),
		nodeRoutes: make(map[string]map[string]struct{}),
	}
}

func memRouteKey(userID, deviceID string) string {
	return userID + "\x00" + deviceID
}

// aliveLocked reports whether id exists and has not expired. Expired entries
// are dropped from the node map, but the route index is kept so SweepDeadNodes
// can still find the routes that belonged to the dead node.
func (m *Memory) aliveLocked(id string) bool {
	n, ok := m.nodes[id]
	if !ok {
		return false
	}
	if time.Now().After(n.expiresAt) {
		delete(m.nodes, id)
		return false
	}
	return true
}

// RegisterNode stores the node with a fresh TTL.
func (m *Memory) RegisterNode(ctx context.Context, n Node) error {
	if n.StartedAt.IsZero() {
		n.StartedAt = time.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodes[n.ID] = memoryNode{Node: n, expiresAt: time.Now().Add(m.nodeTTL)}
	if m.nodeRoutes[n.ID] == nil {
		m.nodeRoutes[n.ID] = make(map[string]struct{})
	}
	return nil
}

// DeregisterNode removes the node and its route index.
func (m *Memory) DeregisterNode(ctx context.Context, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.nodes, nodeID)
	delete(m.nodeRoutes, nodeID)
	return nil
}

// ListNodes returns the live nodes sorted by ID.
func (m *Memory) ListNodes(ctx context.Context) ([]Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodes := make([]Node, 0, len(m.nodes))
	for id := range m.nodes {
		if !m.aliveLocked(id) {
			continue
		}
		nodes = append(nodes, m.nodes[id].Node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes, nil
}

// LookupNode returns a live node entry. An expired node is reported as missing,
// mirroring the Redis hash TTL.
func (m *Memory) LookupNode(ctx context.Context, nodeID string) (Node, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.aliveLocked(nodeID) {
		return Node{}, false, nil
	}
	return m.nodes[nodeID].Node, true, nil
}

// NodeAlive reports whether the node exists and has not expired.
func (m *Memory) NodeAlive(ctx context.Context, nodeID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.aliveLocked(nodeID), nil
}

// NextVersion allocates the next route version, starting at 1.
func (m *Memory) NextVersion(ctx context.Context, userID, deviceID string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := memRouteKey(userID, deviceID)
	m.versions[key]++
	return m.versions[key], nil
}

// UpsertRoute stores the route and moves it between node indexes when needed.
func (m *Memory) UpsertRoute(ctx context.Context, rt Route) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := memRouteKey(rt.UserID, rt.DeviceID)
	if old, ok := m.routes[key]; ok && old.NodeID != rt.NodeID {
		m.removeFromIndexLocked(old.NodeID, key)
	}
	m.routes[key] = rt
	if m.nodeRoutes[rt.NodeID] == nil {
		m.nodeRoutes[rt.NodeID] = make(map[string]struct{})
	}
	m.nodeRoutes[rt.NodeID][key] = struct{}{}
	return nil
}

// LookupRoute returns the stored route, or ok=false when it does not exist.
func (m *Memory) LookupRoute(ctx context.Context, userID, deviceID string) (Route, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.routes[memRouteKey(userID, deviceID)]
	return rt, ok, nil
}

// DeleteRouteIfVersion deletes the route only when version and node match.
func (m *Memory) DeleteRouteIfVersion(ctx context.Context, rt Route) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := memRouteKey(rt.UserID, rt.DeviceID)
	cur, ok := m.routes[key]
	if !ok || cur.Version != rt.Version || cur.NodeID != rt.NodeID {
		return false, nil
	}
	delete(m.routes, key)
	m.removeFromIndexLocked(cur.NodeID, key)
	return true, nil
}

// SweepDeadNodes removes expired nodes and the routes they still own. It is
// idempotent and returns the cleaned node IDs sorted.
func (m *Memory) SweepDeadNodes(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make(map[string]struct{}, len(m.nodes)+len(m.nodeRoutes))
	for id := range m.nodes {
		ids[id] = struct{}{}
	}
	for id := range m.nodeRoutes {
		ids[id] = struct{}{}
	}

	var cleaned []string
	for id := range ids {
		if m.aliveLocked(id) {
			continue
		}
		for key := range m.nodeRoutes[id] {
			if rt, ok := m.routes[key]; ok && rt.NodeID == id {
				delete(m.routes, key)
			}
		}
		delete(m.nodeRoutes, id)
		delete(m.nodes, id)
		cleaned = append(cleaned, id)
	}
	sort.Strings(cleaned)
	return cleaned, nil
}

// Close is a no-op for the memory implementation.
func (m *Memory) Close() error {
	return nil
}

func (m *Memory) removeFromIndexLocked(nodeID, key string) {
	index := m.nodeRoutes[nodeID]
	if index == nil {
		return
	}
	delete(index, key)
	if len(index) == 0 {
		delete(m.nodeRoutes, nodeID)
	}
}
