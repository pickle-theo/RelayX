// Package registry tracks cluster nodes and the (user, device) route table that
// tells the gateway which node currently owns a client session. It ships two
// implementations: Redis for real clusters and an in-memory one for tests.
package registry

import (
	"context"
	"time"
)

// Node is one gateway process in the cluster.
type Node struct {
	ID        string
	Addr      string
	StartedAt time.Time
}

// Route maps a (user, device) pair to the node that owns its live session.
// Version is a monotonically increasing per-route counter used for CAS deletes:
// a connection that was already replaced must not remove its successor's route.
type Route struct {
	UserID    string
	DeviceID  string
	NodeID    string
	SessionID uint64
	Version   uint64
	CreatedAt time.Time
}

// Registry is the cluster membership and routing store.
type Registry interface {
	RegisterNode(ctx context.Context, n Node) error
	DeregisterNode(ctx context.Context, nodeID string) error
	ListNodes(ctx context.Context) ([]Node, error)
	// LookupNode returns the node entry including its advertised address. A
	// missing or expired node is reported as (Node{}, false, nil).
	LookupNode(ctx context.Context, nodeID string) (Node, bool, error)
	NodeAlive(ctx context.Context, nodeID string) (bool, error)

	// NextVersion returns a monotonically increasing version for one route.
	// The first version is 1.
	NextVersion(ctx context.Context, userID, deviceID string) (uint64, error)
	UpsertRoute(ctx context.Context, r Route) error
	LookupRoute(ctx context.Context, userID, deviceID string) (Route, bool, error)
	// DeleteRouteIfVersion removes the route only when both the stored version
	// and node still match r, so a stale session cannot evict its successor.
	DeleteRouteIfVersion(ctx context.Context, r Route) (bool, error)
	// SweepDeadNodes removes nodes whose entry expired and returns their IDs.
	SweepDeadNodes(ctx context.Context) ([]string, error)

	Close() error
}
