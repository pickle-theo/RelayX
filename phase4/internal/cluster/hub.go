// Package cluster implements the direct node-to-node TCP channel that forwards
// deliveries and kicks between gateway nodes.
//
// Every node owns one Hub. The hub accepts inbound connections, dials outbound
// peers on demand and keeps at most one connection per node ID.
package cluster

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/phase4/internal/protocol"
)

// Hub errors.
var (
	ErrHubClosed  = errors.New("cluster: hub closed")
	ErrPeerClosed = errors.New("cluster: peer closed")
	ErrQueueFull  = errors.New("cluster: peer send queue full")
	ErrBackoff    = errors.New("cluster: dial backoff active")
)

// Hub defaults and dial backoff bounds.
const (
	defaultMaxBody      = 1 << 20
	defaultDialTimeout  = time.Second
	defaultWriteTimeout = 5 * time.Second
	defaultQueueSize    = 256
	defaultQueueBytes   = 4 << 20

	minDialBackoff = 100 * time.Millisecond
	maxDialBackoff = 2 * time.Second
)

// Handler receives frames forwarded from another node.
//
// Calls run on the peer's read goroutine, so HandleNodeDeliver must return
// promptly; long work should be handed to another goroutine. The body's
// Payload aliases a reusable decode buffer and is only valid for the duration
// of the call, mirroring transport.HandlerFunc.
type Handler interface {
	HandleNodeDeliver(p *Peer, msgID uint64, body *protocol.NodeDeliverBody) (status uint8, message string)
	HandleNodeKick(p *Peer, body *protocol.NodeKickBody)
}

// Config configures a Hub. Zero-valued fields are replaced with defaults.
type Config struct {
	NodeID        string
	ListenAddr    string // internal listen address, e.g. 127.0.0.1:9100
	AdvertiseAddr string // written into NODE_HELLO; ListenAddr when empty
	MaxBody       int    // maximum accepted frame body, default 1 MiB
	DialTimeout   time.Duration
	WriteTimeout  time.Duration
	QueueSize     int // frames queued per peer, default 256
	QueueBytes    int // bytes queued per peer, default 4 MiB
	Logger        *slog.Logger
}

// Hub owns the node-to-node listener and the set of connected peers.
type Hub struct {
	cfg           Config
	logger        *slog.Logger
	listener      net.Listener
	addr          string
	advertiseAddr string

	handlerMu sync.RWMutex
	handler   Handler

	// mu guards peers and the dial backoff bookkeeping. It is also the barrier
	// that orders WaitGroup accounting against Close.
	mu         sync.Mutex
	peers      map[string]*Peer
	nextDialAt map[string]time.Time
	backoff    map[string]time.Duration

	startOnce sync.Once
	closeOnce sync.Once
	closed    atomic.Bool
	closeErr  error
	wg        sync.WaitGroup
}

// NewHub listens on cfg.ListenAddr and returns a hub ready to Start.
func NewHub(cfg Config) (*Hub, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("cluster: node id must not be empty")
	}
	if len(cfg.NodeID) > protocol.MaxNodeIDLen {
		return nil, fmt.Errorf("cluster: node id length %d exceeds limit %d", len(cfg.NodeID), protocol.MaxNodeIDLen)
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:0"
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = defaultMaxBody
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.QueueBytes <= 0 {
		cfg.QueueBytes = defaultQueueBytes
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: listen on %s: %w", cfg.ListenAddr, err)
	}

	h := &Hub{
		cfg:        cfg,
		logger:     cfg.Logger,
		listener:   ln,
		addr:       ln.Addr().String(),
		peers:      make(map[string]*Peer),
		nextDialAt: make(map[string]time.Time),
		backoff:    make(map[string]time.Duration),
	}
	h.advertiseAddr = cfg.AdvertiseAddr
	if h.advertiseAddr == "" {
		h.advertiseAddr = h.addr
	}
	return h, nil
}

// SetHandler installs the handler used by inbound peer read loops. It may be
// called before or after Start; the hub reads it on every frame.
func (h *Hub) SetHandler(handler Handler) {
	h.handlerMu.Lock()
	h.handler = handler
	h.handlerMu.Unlock()
}

func (h *Hub) getHandler() Handler {
	h.handlerMu.RLock()
	defer h.handlerMu.RUnlock()
	return h.handler
}

// Addr returns the actual listen address, which is useful when binding to
// 127.0.0.1:0.
func (h *Hub) Addr() string { return h.addr }

// Start begins accepting inbound node connections.
func (h *Hub) Start() {
	h.startOnce.Do(func() {
		h.mu.Lock()
		if h.closed.Load() {
			h.mu.Unlock()
			return
		}
		h.wg.Add(1)
		h.mu.Unlock()
		go h.acceptLoop()
	})
}

// PeerCount returns the number of currently connected peers.
func (h *Hub) PeerCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.peers)
}

// Close stops the listener, closes every peer and waits for all hub
// goroutines to exit. It is safe to call multiple times.
func (h *Hub) Close() error {
	h.closeOnce.Do(func() {
		h.mu.Lock()
		h.closed.Store(true)
		peers := make([]*Peer, 0, len(h.peers))
		for _, p := range h.peers {
			peers = append(peers, p)
		}
		h.mu.Unlock()

		if err := h.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			h.closeErr = err
		}
		for _, p := range peers {
			p.close("hub closed")
		}
		h.wg.Wait()
	})
	return h.closeErr
}

// ForwardDeliver sends a NODE_DELIVER frame to nodeID, dialing addr first when
// no peer is connected. It returns as soon as the frame is queued, not when
// the peer acknowledges it.
func (h *Hub) ForwardDeliver(ctx context.Context, nodeID, addr string, msgID uint64, body *protocol.NodeDeliverBody) error {
	payload, err := protocol.AppendNodeDeliverBody(nil, body)
	if err != nil {
		return err
	}
	p, err := h.peerFor(ctx, nodeID, addr)
	if err != nil {
		return err
	}
	if err := p.send(&protocol.Frame{Type: protocol.NodeDeliver, MsgID: msgID, Body: payload}); err != nil {
		p.close("forward deliver failed")
		h.logger.Warn("node forward deliver failed", "node_id", nodeID, "addr", addr, "msg_id", msgID, "error", err)
		return err
	}
	return nil
}

// Kick sends a NODE_KICK frame to nodeID, dialing addr first when needed.
func (h *Hub) Kick(ctx context.Context, nodeID, addr string, body *protocol.NodeKickBody) error {
	payload, err := protocol.AppendNodeKickBody(nil, body)
	if err != nil {
		return err
	}
	p, err := h.peerFor(ctx, nodeID, addr)
	if err != nil {
		return err
	}
	if err := p.send(&protocol.Frame{Type: protocol.NodeKick, Body: payload}); err != nil {
		p.close("kick failed")
		h.logger.Warn("node kick failed", "node_id", nodeID, "addr", addr, "error", err)
		return err
	}
	return nil
}

// peerFor returns the connected peer for nodeID or dials addr. Dial failures
// arm an exponential backoff so callers fail fast instead of blocking on a
// node that is down.
func (h *Hub) peerFor(ctx context.Context, nodeID, addr string) (*Peer, error) {
	if nodeID == "" {
		return nil, errors.New("cluster: node id must not be empty")
	}

	h.mu.Lock()
	if h.closed.Load() {
		h.mu.Unlock()
		return nil, ErrHubClosed
	}
	if p := h.peers[nodeID]; p != nil {
		h.mu.Unlock()
		return p, nil
	}
	if until := h.nextDialAt[nodeID]; time.Now().Before(until) {
		h.mu.Unlock()
		return nil, fmt.Errorf("%w for node %s until %s", ErrBackoff, nodeID, until.Format(time.RFC3339Nano))
	}
	h.mu.Unlock()

	p, err := h.dial(ctx, nodeID, addr)
	if err != nil {
		h.recordDialFailure(nodeID)
		h.logger.Warn("node dial failed", "node_id", nodeID, "addr", addr, "error", err)
		return nil, err
	}
	if !h.registerPeer(p) {
		p.close("hub closed")
		return nil, ErrHubClosed
	}
	h.logger.Info("node connected", "node_id", nodeID, "addr", addr, "inbound", false)
	p.start()
	return p, nil
}

// recordDialFailure doubles the backoff for nodeID and remembers when the next
// dial is allowed.
func (h *Hub) recordDialFailure(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	backoff := h.backoff[nodeID]
	if backoff < minDialBackoff {
		backoff = minDialBackoff
	} else if backoff *= 2; backoff > maxDialBackoff {
		backoff = maxDialBackoff
	}
	h.backoff[nodeID] = backoff
	h.nextDialAt[nodeID] = time.Now().Add(backoff)
}

// dial opens a TCP connection to addr, performs the NODE_HELLO exchange and
// returns a peer that is not yet registered with the hub.
func (h *Hub) dial(ctx context.Context, nodeID, addr string) (*Peer, error) {
	dialCtx, cancel := context.WithTimeout(ctx, h.cfg.DialTimeout)
	defer cancel()

	var dialer net.Dialer
	nc, err := dialer.DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cluster: dial %s: %w", addr, err)
	}
	closeConn := func() {
		if err := nc.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			h.logger.Debug("close node connection", "addr", addr, "error", err)
		}
	}

	hello, err := protocol.AppendNodeHelloBody(nil, &protocol.NodeHelloBody{
		ProtocolVersion: protocol.Version,
		NodeID:          h.cfg.NodeID,
		AdvertiseAddr:   h.advertiseAddr,
	})
	if err != nil {
		closeConn()
		return nil, err
	}

	if err := nc.SetDeadline(time.Now().Add(2 * h.cfg.DialTimeout)); err != nil {
		closeConn()
		return nil, fmt.Errorf("cluster: set handshake deadline for %s: %w", addr, err)
	}
	if err := protocol.Encode(nc, &protocol.Frame{Type: protocol.NodeHello, Body: hello}, h.cfg.MaxBody); err != nil {
		closeConn()
		return nil, fmt.Errorf("cluster: send node hello to %s: %w", addr, err)
	}

	r := bufio.NewReaderSize(nc, 4096)
	f := &protocol.Frame{}
	if err := protocol.DecodeInto(r, f, h.cfg.MaxBody); err != nil {
		closeConn()
		return nil, fmt.Errorf("cluster: read node hello ack from %s: %w", addr, err)
	}
	if f.Type != protocol.NodeHelloAck {
		closeConn()
		return nil, fmt.Errorf("cluster: node %s answered handshake with %s", addr, f.Type)
	}
	ack, err := protocol.DecodeNodeHelloAck(f.Body)
	if err != nil {
		closeConn()
		return nil, fmt.Errorf("cluster: decode node hello ack from %s: %w", addr, err)
	}
	if ack.Status != protocol.NodeStatusOK {
		closeConn()
		return nil, fmt.Errorf("cluster: node %s rejected handshake with status %d: %s", addr, ack.Status, ack.Message)
	}
	if err := nc.SetDeadline(time.Time{}); err != nil {
		closeConn()
		return nil, fmt.Errorf("cluster: clear handshake deadline for %s: %w", addr, err)
	}
	return newPeer(h, nodeID, addr, nc, r), nil
}

func (h *Hub) acceptLoop() {
	defer h.wg.Done()
	for {
		nc, err := h.listener.Accept()
		if err != nil {
			if !h.closed.Load() {
				h.logger.Warn("node accept failed", "error", err)
			}
			return
		}
		if !h.trackHandshake() {
			_ = nc.Close()
			return
		}
		go func() {
			defer h.wg.Done()
			h.handleConn(nc)
		}()
	}
}

// trackHandshake registers one in-flight handshake goroutine. It takes the
// same lock as Close so WaitGroup.Add can never race WaitGroup.Wait.
func (h *Hub) trackHandshake() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed.Load() {
		return false
	}
	h.wg.Add(1)
	return true
}

// handleConn performs the inbound handshake and then hands the connection over
// to the peer read/write loops.
func (h *Hub) handleConn(nc net.Conn) {
	addr := remoteAddr(nc)
	if err := nc.SetReadDeadline(time.Now().Add(2 * h.cfg.DialTimeout)); err != nil {
		h.logger.Warn("node handshake deadline failed", "remote", addr, "error", err)
		_ = nc.Close()
		return
	}

	r := bufio.NewReaderSize(nc, 4096)
	f := &protocol.Frame{}
	if err := protocol.DecodeInto(r, f, h.cfg.MaxBody); err != nil {
		h.logger.Warn("node handshake read failed", "remote", addr, "error", err)
		_ = nc.Close()
		return
	}
	if f.Type != protocol.NodeHello {
		h.logger.Warn("node handshake rejected", "remote", addr, "frame_type", f.Type.String())
		_ = nc.Close()
		return
	}
	hello, err := protocol.DecodeNodeHello(f.Body)
	if err != nil {
		h.logger.Warn("node handshake rejected", "remote", addr, "error", err)
		_ = nc.Close()
		return
	}
	if hello.NodeID == h.cfg.NodeID {
		if err := h.writeHelloAck(nc, protocol.NodeStatusInvalid, "self connection rejected"); err != nil {
			h.logger.Warn("node handshake ack failed", "remote", addr, "error", err)
		}
		h.logger.Warn("node handshake rejected", "remote", addr, "node_id", hello.NodeID, "reason", "self connection")
		_ = nc.Close()
		return
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		h.logger.Warn("node handshake deadline clear failed", "remote", addr, "error", err)
		_ = nc.Close()
		return
	}
	if err := h.writeHelloAck(nc, protocol.NodeStatusOK, ""); err != nil {
		h.logger.Warn("node handshake ack failed", "remote", addr, "error", err)
		_ = nc.Close()
		return
	}

	p := newPeer(h, hello.NodeID, addr, nc, r)
	if !h.registerPeer(p) {
		_ = nc.Close()
		return
	}
	h.logger.Info("node connected", "node_id", p.nodeID, "addr", addr, "inbound", true)
	p.start()
}

// writeHelloAck writes a NODE_HELLO_ACK directly, before the peer writer
// goroutine exists.
func (h *Hub) writeHelloAck(nc net.Conn, status uint8, message string) error {
	body, err := protocol.AppendNodeHelloAckBody(nil, &protocol.NodeHelloAckBody{Status: status, Message: message})
	if err != nil {
		return err
	}
	if h.cfg.WriteTimeout > 0 {
		if err := nc.SetWriteDeadline(time.Now().Add(h.cfg.WriteTimeout)); err != nil {
			return err
		}
	}
	return protocol.Encode(nc, &protocol.Frame{Type: protocol.NodeHelloAck, Body: body}, h.cfg.MaxBody)
}

// registerPeer makes p the peer for its node ID, replacing and closing any
// previous connection, and accounts for its two goroutines.
func (h *Hub) registerPeer(p *Peer) bool {
	h.mu.Lock()
	if h.closed.Load() {
		h.mu.Unlock()
		return false
	}
	old := h.peers[p.nodeID]
	h.peers[p.nodeID] = p
	delete(h.nextDialAt, p.nodeID)
	delete(h.backoff, p.nodeID)
	h.wg.Add(2)
	h.mu.Unlock()

	if old != nil && old != p {
		old.close("replaced by new connection")
	}
	return true
}

// forget removes p from the peer map if it is still the registered peer.
func (h *Hub) forget(p *Peer) {
	h.mu.Lock()
	if h.peers[p.nodeID] == p {
		delete(h.peers, p.nodeID)
	}
	h.mu.Unlock()
}

func remoteAddr(nc net.Conn) string {
	if addr := nc.RemoteAddr(); addr != nil {
		return addr.String()
	}
	return ""
}

// Peer is one connected peer node. Every peer owns exactly one read goroutine
// and one write goroutine.
type Peer struct {
	hub    *Hub
	nodeID string
	addr   string
	nc     net.Conn
	r      *bufio.Reader

	queue       chan []byte
	done        chan struct{}
	queuedBytes atomic.Int64

	closeOnce sync.Once
	closed    atomic.Bool
}

func newPeer(h *Hub, nodeID, addr string, nc net.Conn, r *bufio.Reader) *Peer {
	return &Peer{
		hub:    h,
		nodeID: nodeID,
		addr:   addr,
		nc:     nc,
		r:      r,
		queue:  make(chan []byte, h.cfg.QueueSize),
		done:   make(chan struct{}),
	}
}

// NodeID returns the node ID announced during the handshake.
func (p *Peer) NodeID() string { return p.nodeID }

// RemoteAddr returns the peer's TCP address.
func (p *Peer) RemoteAddr() string { return p.addr }

// start launches the read and write goroutines. The hub has already accounted
// for both in its WaitGroup.
func (p *Peer) start() {
	go p.readLoop()
	go p.writeLoop()
}

// send encodes f and queues it without blocking. A full queue closes the peer
// so the next forward attempts a fresh dial instead of buffering forever.
func (p *Peer) send(f *protocol.Frame) error {
	if p.closed.Load() {
		return ErrPeerClosed
	}
	b, err := protocol.AppendFrame(nil, f, p.hub.cfg.MaxBody)
	if err != nil {
		return err
	}

	size := int64(len(b))
	if p.queuedBytes.Add(size) > int64(p.hub.cfg.QueueBytes) {
		p.queuedBytes.Add(-size)
		p.close("send queue byte budget exhausted")
		return ErrQueueFull
	}
	select {
	case p.queue <- b:
		return nil
	case <-p.done:
		p.queuedBytes.Add(-size)
		return ErrPeerClosed
	default:
		p.queuedBytes.Add(-size)
		p.close("send queue full")
		return ErrQueueFull
	}
}

// close tears the peer down once and removes it from the hub.
func (p *Peer) close(reason string) {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		close(p.done)
		if err := p.nc.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			p.hub.logger.Debug("close node connection", "node_id", p.nodeID, "error", err)
		}
		p.hub.forget(p)
		p.hub.logger.Info("node disconnected", "node_id", p.nodeID, "addr", p.addr, "reason", reason)
	})
}

func (p *Peer) readLoop() {
	defer p.hub.wg.Done()

	f := &protocol.Frame{}
	for {
		if err := protocol.DecodeInto(p.r, f, p.hub.cfg.MaxBody); err != nil {
			if !p.closed.Load() {
				p.close("read error: " + err.Error())
			}
			return
		}

		switch f.Type {
		case protocol.NodeDeliver:
			body, err := protocol.DecodeNodeDeliver(f.Body)
			if err != nil {
				p.close("bad node deliver: " + err.Error())
				return
			}
			status, message := protocol.NodeStatusInternal, "no handler"
			if hd := p.hub.getHandler(); hd != nil {
				// The forwarded frame keeps the platform message ID so the
				// destination node can deliver it and answer with the same ID.
				status, message = hd.HandleNodeDeliver(p, f.MsgID, body)
			}
			if err := p.sendDeliverAck(f.MsgID, status, message); err != nil {
				return
			}
		case protocol.NodeKick:
			body, err := protocol.DecodeNodeKick(f.Body)
			if err != nil {
				p.close("bad node kick: " + err.Error())
				return
			}
			if hd := p.hub.getHandler(); hd != nil {
				hd.HandleNodeKick(p, body)
			}
		case protocol.NodeDeliverAck:
			// ForwardDeliver is fire-and-forget, but the acknowledgement still
			// travels back over this link and must be consumed.
			if _, err := protocol.DecodeNodeDeliverAck(f.Body); err != nil {
				p.close("bad node deliver ack: " + err.Error())
				return
			}
		default:
			p.close("unexpected frame " + f.Type.String())
			return
		}
	}
}

// sendDeliverAck answers a NODE_DELIVER with the handler outcome. An invalid
// handler reply is downgraded to an internal error instead of closing the
// peer.
func (p *Peer) sendDeliverAck(msgID uint64, status uint8, message string) error {
	body, err := protocol.AppendNodeDeliverAckBody(nil, &protocol.NodeDeliverAckBody{Status: status, Message: message})
	if err != nil {
		body, err = protocol.AppendNodeDeliverAckBody(nil, &protocol.NodeDeliverAckBody{
			Status:  protocol.NodeStatusInternal,
			Message: "invalid handler reply",
		})
		if err != nil {
			p.close("encode node deliver ack: " + err.Error())
			return err
		}
	}
	return p.send(&protocol.Frame{Type: protocol.NodeDeliverAck, MsgID: msgID, Body: body})
}

func (p *Peer) writeLoop() {
	defer p.hub.wg.Done()

	for {
		select {
		case <-p.done:
			return
		case b := <-p.queue:
			p.queuedBytes.Add(-int64(len(b)))
			if p.hub.cfg.WriteTimeout > 0 {
				if err := p.nc.SetWriteDeadline(time.Now().Add(p.hub.cfg.WriteTimeout)); err != nil {
					p.close("set write deadline: " + err.Error())
					return
				}
			}
			if _, err := p.nc.Write(b); err != nil {
				p.close("write error: " + err.Error())
				return
			}
		}
	}
}
