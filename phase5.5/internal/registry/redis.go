package registry

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultPrefix     = "relayx:"
	defaultNodeTTL    = 15 * time.Second
	cleanupLockTTL    = 30 * time.Second
	cleanupLockPrefix = "node_cleanup:"
)

// Redis topology modes.
const (
	// RedisModeSingle connects to one Redis instance.
	RedisModeSingle = "single"
	// RedisModeSentinel connects through Redis Sentinel and follows the
	// master when it fails over.
	RedisModeSentinel = "sentinel"
	// RedisModeCluster connects to a Redis Cluster.
	RedisModeCluster = "cluster"
)

// RedisConfig configures the Redis-backed registry.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	Prefix   string        // key namespace, defaults to "relayx:"
	NodeTTL  time.Duration // node entry lifetime, defaults to 15s

	// Mode selects the topology: "single" (default), "sentinel" or "cluster".
	Mode string
	// MasterName is the sentinel master name, required in sentinel mode.
	MasterName string
	// SentinelAddrs lists the sentinel addresses, required in sentinel mode.
	SentinelAddrs []string
}

// Redis stores cluster membership and routes in a single Redis instance.
//
// Keys (with the configured prefix):
//
//	<nodes>                 SET of node IDs
//	<node:<id>>             HASH addr, started_at, TTL NodeTTL
//	<route:<user>\x00<dev>> HASH node, session, version, created_at
//	<ver:<user>\x00<dev>>   INCR counter
//	<node_routes:<id>>      SET of full route keys owned by the node
//	<node_cleanup:<id>>     sweep lock, SET NX PX 30s
type Redis struct {
	client  redis.UniversalClient
	prefix  string
	nodeTTL time.Duration
}

var _ Registry = (*Redis)(nil)

// NewRedis connects to Redis and verifies the connection with a PING.
//
// The topology is selected by Mode: a single instance, a Sentinel setup that
// follows the master during a failover, or a Redis Cluster.
func NewRedis(ctx context.Context, cfg RedisConfig) (*Redis, error) {
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	ttl := cfg.NodeTTL
	if ttl <= 0 {
		ttl = defaultNodeTTL
	}

	client, err := dialRedis(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Redis{client: client, prefix: prefix, nodeTTL: ttl}, nil
}

// dialRedis creates the client for the configured topology and verifies it.
func dialRedis(ctx context.Context, cfg RedisConfig) (redis.UniversalClient, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	switch mode {
	case "", RedisModeSingle:
		client := redis.NewClient(&redis.Options{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
		})
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("registry: ping redis %s: %w", cfg.Addr, err)
		}
		return client, nil

	case RedisModeSentinel:
		if cfg.MasterName == "" || len(cfg.SentinelAddrs) == 0 {
			return nil, fmt.Errorf("registry: sentinel mode needs a master name and sentinel addresses")
		}
		client := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.MasterName,
			SentinelAddrs: cfg.SentinelAddrs,
			Password:      cfg.Password,
			DB:            cfg.DB,
			PoolSize:      32,
			MinIdleConns:  2,
		})
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("registry: ping sentinel master %s: %w", cfg.MasterName, err)
		}
		return client, nil

	case RedisModeCluster:
		addrs := cfg.SentinelAddrs
		if len(addrs) == 0 && cfg.Addr != "" {
			addrs = []string{cfg.Addr}
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("registry: cluster mode needs at least one address")
		}
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    addrs,
			Password: cfg.Password,
			PoolSize: 32,
		})
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("registry: ping redis cluster: %w", err)
		}
		return client, nil

	default:
		return nil, fmt.Errorf("registry: unknown redis mode %q", cfg.Mode)
	}
}

// Client returns the underlying client so other packages can share the
// connection, for example the cluster-wide tenant quota.
func (r *Redis) Client() redis.UniversalClient {
	if r == nil {
		return nil
	}
	return r.client
}

func (r *Redis) nodesKey() string {
	return r.prefix + "nodes"
}

func (r *Redis) nodeKey(nodeID string) string {
	return r.prefix + "node:" + nodeID
}

func (r *Redis) routeKey(userID, deviceID string) string {
	// Redis keys are binary safe, so NUL cleanly separates the two IDs even if
	// they contain the ":" used by the other keys.
	return r.prefix + "route:" + userID + "\x00" + deviceID
}

func (r *Redis) versionKey(userID, deviceID string) string {
	return r.prefix + "ver:" + userID + "\x00" + deviceID
}

func (r *Redis) nodeRoutesKey(nodeID string) string {
	return r.prefix + "node_routes:" + nodeID
}

func (r *Redis) cleanupKey(nodeID string) string {
	return r.prefix + cleanupLockPrefix + nodeID
}

// RegisterNode writes the node hash, sets its TTL and adds it to the node set.
func (r *Redis) RegisterNode(ctx context.Context, n Node) error {
	if n.StartedAt.IsZero() {
		n.StartedAt = time.Now()
	}
	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, r.nodeKey(n.ID), map[string]any{
		"addr":       n.Addr,
		"started_at": n.StartedAt.UTC().Format(time.RFC3339),
	})
	pipe.PExpire(ctx, r.nodeKey(n.ID), r.nodeTTL)
	pipe.SAdd(ctx, r.nodesKey(), n.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("registry: register node %s: %w", n.ID, err)
	}
	return nil
}

// DeregisterNode removes the node entry and its route index. Routes themselves
// are deleted by their gateway on session close (version CAS); deleting the
// index here is only a safety net.
func (r *Redis) DeregisterNode(ctx context.Context, nodeID string) error {
	pipe := r.client.TxPipeline()
	pipe.Del(ctx, r.nodeKey(nodeID))
	pipe.SRem(ctx, r.nodesKey(), nodeID)
	pipe.Del(ctx, r.nodeRoutesKey(nodeID))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("registry: deregister node %s: %w", nodeID, err)
	}
	return nil
}

// ListNodes returns the nodes whose hash has not expired yet.
func (r *Redis) ListNodes(ctx context.Context) ([]Node, error) {
	ids, err := r.client.SMembers(ctx, r.nodesKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("registry: list nodes: %w", err)
	}

	nodes := make([]Node, 0, len(ids))
	for _, id := range ids {
		fields, err := r.client.HGetAll(ctx, r.nodeKey(id)).Result()
		if err != nil {
			return nil, fmt.Errorf("registry: read node %s: %w", id, err)
		}
		if len(fields) == 0 {
			// The hash expired but the set member is still there; the next sweep
			// will remove it.
			continue
		}
		startedAt, err := time.Parse(time.RFC3339, fields["started_at"])
		if err != nil {
			return nil, fmt.Errorf("registry: node %s started_at: %w", id, err)
		}
		nodes = append(nodes, Node{ID: id, Addr: fields["addr"], StartedAt: startedAt})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes, nil
}

// LookupNode returns the node entry, or ok=false when its hash expired or was
// never written.
func (r *Redis) LookupNode(ctx context.Context, nodeID string) (Node, bool, error) {
	fields, err := r.client.HGetAll(ctx, r.nodeKey(nodeID)).Result()
	if err != nil {
		return Node{}, false, fmt.Errorf("registry: lookup node %s: %w", nodeID, err)
	}
	if len(fields) == 0 {
		return Node{}, false, nil
	}
	startedAt, err := time.Parse(time.RFC3339, fields["started_at"])
	if err != nil {
		return Node{}, false, fmt.Errorf("registry: node %s started_at: %w", nodeID, err)
	}
	return Node{ID: nodeID, Addr: fields["addr"], StartedAt: startedAt}, true, nil
}

// NodeAlive reports whether the node hash still exists.
func (r *Redis) NodeAlive(ctx context.Context, nodeID string) (bool, error) {
	n, err := r.client.Exists(ctx, r.nodeKey(nodeID)).Result()
	if err != nil {
		return false, fmt.Errorf("registry: check node %s: %w", nodeID, err)
	}
	return n > 0, nil
}

// NextVersion allocates the next route version, starting at 1.
func (r *Redis) NextVersion(ctx context.Context, userID, deviceID string) (uint64, error) {
	v, err := r.client.Incr(ctx, r.versionKey(userID, deviceID)).Result()
	if err != nil {
		return 0, fmt.Errorf("registry: next version: %w", err)
	}
	return uint64(v), nil
}

// UpsertRoute overwrites the route and keeps the per-node indexes in sync. The
// Lua script makes the HSET, old-node SREM and new-node SADD one atomic step.
func (r *Redis) UpsertRoute(ctx context.Context, rt Route) error {
	key := r.routeKey(rt.UserID, rt.DeviceID)
	_, err := upsertRouteScript.Run(ctx, r.client,
		[]string{key, r.nodeRoutesKey(rt.NodeID)},
		r.prefix, rt.NodeID,
		strconv.FormatUint(rt.SessionID, 10),
		strconv.FormatUint(rt.Version, 10),
		rt.CreatedAt.UTC().Format(time.RFC3339),
	).Result()
	if err != nil {
		return fmt.Errorf("registry: upsert route %s/%s: %w", rt.UserID, rt.DeviceID, err)
	}
	return nil
}

// LookupRoute returns the route for (userID, deviceID), or ok=false.
func (r *Redis) LookupRoute(ctx context.Context, userID, deviceID string) (Route, bool, error) {
	fields, err := r.client.HGetAll(ctx, r.routeKey(userID, deviceID)).Result()
	if err != nil {
		return Route{}, false, fmt.Errorf("registry: lookup route %s/%s: %w", userID, deviceID, err)
	}
	if len(fields) == 0 {
		return Route{}, false, nil
	}

	sessionID, err := parseUintField("session", fields["session"])
	if err != nil {
		return Route{}, false, err
	}
	version, err := parseUintField("version", fields["version"])
	if err != nil {
		return Route{}, false, err
	}
	createdAt, err := time.Parse(time.RFC3339, fields["created_at"])
	if err != nil {
		return Route{}, false, fmt.Errorf("registry: route %s/%s created_at: %w", userID, deviceID, err)
	}
	return Route{
		UserID:    userID,
		DeviceID:  deviceID,
		NodeID:    fields["node"],
		SessionID: sessionID,
		Version:   version,
		CreatedAt: createdAt,
	}, true, nil
}

func parseUintField(name, raw string) (uint64, error) {
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("registry: route field %s: %w", name, err)
	}
	return v, nil
}

// DeleteRouteIfVersion deletes the route only when both version and node match.
func (r *Redis) DeleteRouteIfVersion(ctx context.Context, rt Route) (bool, error) {
	res, err := deleteRouteScript.Run(ctx, r.client,
		[]string{r.routeKey(rt.UserID, rt.DeviceID), r.nodeRoutesKey(rt.NodeID)},
		strconv.FormatUint(rt.Version, 10), rt.NodeID,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("registry: delete route %s/%s: %w", rt.UserID, rt.DeviceID, err)
	}
	return res == 1, nil
}

// SweepDeadNodes removes nodes whose hash expired, together with the routes
// still pointing at them. A cleanup lock keeps concurrent sweepers apart and a
// per-route script re-checks the node field so routes already taken over by a
// live node are left alone.
func (r *Redis) SweepDeadNodes(ctx context.Context) ([]string, error) {
	ids, err := r.client.SMembers(ctx, r.nodesKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("registry: sweep list nodes: %w", err)
	}
	sort.Strings(ids)

	var cleaned []string
	for _, id := range ids {
		alive, err := r.client.Exists(ctx, r.nodeKey(id)).Result()
		if err != nil {
			return nil, fmt.Errorf("registry: sweep check node %s: %w", id, err)
		}
		if alive > 0 {
			continue
		}

		// The lock value is the dead node ID: only the sweeper that created the
		// lock may release it.
		ok, err := r.client.SetNX(ctx, r.cleanupKey(id), id, cleanupLockTTL).Result()
		if err != nil {
			return nil, fmt.Errorf("registry: sweep lock node %s: %w", id, err)
		}
		if !ok {
			continue
		}

		// The node may have refreshed between Exists and SetNX.
		alive, err = r.client.Exists(ctx, r.nodeKey(id)).Result()
		if err != nil || alive > 0 {
			r.releaseCleanup(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("registry: sweep recheck node %s: %w", id, err)
			}
			continue
		}

		routeKeys, err := r.client.SMembers(ctx, r.nodeRoutesKey(id)).Result()
		if err != nil {
			r.releaseCleanup(ctx, id)
			return nil, fmt.Errorf("registry: sweep routes of node %s: %w", id, err)
		}
		for _, key := range routeKeys {
			if _, err := sweepRouteScript.Run(ctx, r.client, []string{key}, id).Result(); err != nil {
				r.releaseCleanup(ctx, id)
				return nil, fmt.Errorf("registry: sweep route %q: %w", key, err)
			}
		}

		if err := r.client.Del(ctx, r.nodeRoutesKey(id)).Err(); err != nil {
			r.releaseCleanup(ctx, id)
			return nil, fmt.Errorf("registry: sweep index of node %s: %w", id, err)
		}
		if err := r.client.SRem(ctx, r.nodesKey(), id).Err(); err != nil {
			r.releaseCleanup(ctx, id)
			return nil, fmt.Errorf("registry: sweep node set %s: %w", id, err)
		}
		r.releaseCleanup(ctx, id)
		cleaned = append(cleaned, id)
	}
	return cleaned, nil
}

// releaseCleanup is best effort: if it fails the lock simply expires.
func (r *Redis) releaseCleanup(ctx context.Context, nodeID string) {
	_ = releaseCleanupScript.Run(ctx, r.client, []string{r.cleanupKey(nodeID)}, nodeID).Err()
}

// Close closes the Redis client.
func (r *Redis) Close() error {
	return r.client.Close()
}

// upsertRouteScript stores the route, moves the index membership when the node
// changed, and adds the route to the new node's index.
//
// KEYS[1] route key, KEYS[2] new node index.
// ARGV: prefix, node, session, version, created_at.
var upsertRouteScript = redis.NewScript(`
local routeKey = KEYS[1]
local newIndex = KEYS[2]
local prefix = ARGV[1]
local node = ARGV[2]
local oldNode = redis.call('HGET', routeKey, 'node')
redis.call('HSET', routeKey,
    'node', node,
    'session', ARGV[3],
    'version', ARGV[4],
    'created_at', ARGV[5])
if oldNode and oldNode ~= node then
    redis.call('SREM', prefix .. 'node_routes:' .. oldNode, routeKey)
end
redis.call('SADD', newIndex, routeKey)
return 1
`)

// deleteRouteScript deletes only when version and node both match.
//
// KEYS[1] route key, KEYS[2] owning node index.
// ARGV: version, node.
var deleteRouteScript = redis.NewScript(`
local routeKey = KEYS[1]
local indexKey = KEYS[2]
if redis.call('HGET', routeKey, 'version') == ARGV[1]
    and redis.call('HGET', routeKey, 'node') == ARGV[2] then
    redis.call('DEL', routeKey)
    redis.call('SREM', indexKey, routeKey)
    return 1
end
return 0
`)

// sweepRouteScript deletes a route only if it still belongs to the dead node.
var sweepRouteScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'node') == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`)

// releaseCleanupScript deletes the lock only if this sweeper owns it.
var releaseCleanupScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`)
