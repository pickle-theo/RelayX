package transport

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/phase1/internal/config"
	"relayx/phase1/internal/protocol"
)

// ErrConnClosed is returned by Conn.Send when the connection has been closed.
var ErrConnClosed = errors.New("transport: connection closed")

// ErrSendQueueFull is returned by Conn.Send when the bounded send queue is
// full. The connection is closed as well to apply backpressure.
var ErrSendQueueFull = errors.New("transport: send queue full")

// Writer and queue defaults. They mirror the flag defaults in
// internal/config so a Conn built directly (tests, benchmarks) behaves like a
// server-configured one.
const (
	defaultSendQueueBytes   = 4 << 20 // 4 MiB
	defaultWriteTimeout     = 10 * time.Second
	defaultWriteBatchFrames = 16
	defaultWriteBatchBytes  = 64 << 10 // 64 KiB
	defaultReadBufferSize   = 4096

	// ctrlQueueSize is the capacity of the dedicated control-frame queue. It
	// is intentionally tiny: control frames are expected to be rare and must
	// never be starved by data traffic.
	ctrlQueueSize = 16
)

// Conn is a single client connection. Every connection owns exactly one
// reader goroutine and one writer goroutine (heartbeat merged into the
// writer), so all socket writes are serialized through sendCh/ctrlCh.
type Conn struct {
	id      uint64
	nc      net.Conn
	handler HandlerFunc
	logger  *slog.Logger

	sendCh chan []byte
	ctrlCh chan []byte
	done   chan struct{}

	// queuedBytes tracks the wire bytes currently sitting in sendCh/ctrlCh. It
	// enforces the SendQueueBytes budget; the writer subtracts each frame as it
	// takes it out of the queue.
	queuedBytes    atomic.Int64
	sendQueueBytes int

	writeTimeout     time.Duration
	writeBatchFrames int
	writeBatchBytes  int
	readBufferSize   int

	closeOnce sync.Once
	closed    atomic.Bool

	lastInbound atomic.Int64 // UnixNano of the last successfully decoded frame
	nextMsgID   atomic.Uint64

	maxBody           int
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	idleTimeout       time.Duration

	onClose func(*Conn)

	reasonMu sync.Mutex
	reason   string

	wg sync.WaitGroup
}

func newConn(id uint64, nc net.Conn, cfg config.ServerConfig, handler HandlerFunc, logger *slog.Logger, onClose func(*Conn)) *Conn {
	c := &Conn{
		id:                id,
		nc:                nc,
		handler:           handler,
		sendCh:            make(chan []byte, cfg.SendQueueSize),
		ctrlCh:            make(chan []byte, ctrlQueueSize),
		done:              make(chan struct{}),
		sendQueueBytes:    cfg.SendQueueBytes,
		writeTimeout:      cfg.WriteTimeout,
		writeBatchFrames:  cfg.WriteBatchFrames,
		writeBatchBytes:   cfg.WriteBatchBytes,
		readBufferSize:    cfg.ReadBufferSize,
		maxBody:           cfg.MaxBody,
		heartbeatInterval: cfg.HeartbeatInterval,
		heartbeatTimeout:  cfg.HeartbeatTimeout,
		idleTimeout:       cfg.IdleTimeout,
		onClose:           onClose,
	}
	if c.sendQueueBytes <= 0 {
		c.sendQueueBytes = defaultSendQueueBytes
	}
	if c.writeTimeout <= 0 {
		c.writeTimeout = defaultWriteTimeout
	}
	if c.writeBatchFrames <= 0 {
		c.writeBatchFrames = defaultWriteBatchFrames
	}
	if c.writeBatchBytes <= 0 {
		c.writeBatchBytes = defaultWriteBatchBytes
	}
	if c.readBufferSize <= 0 {
		c.readBufferSize = defaultReadBufferSize
	}
	c.lastInbound.Store(time.Now().UnixNano())
	c.logger = logger.With("conn_id", id, "remote", c.RemoteAddr())
	return c
}

// ID returns the server-assigned connection identifier.
func (c *Conn) ID() uint64 { return c.id }

// RemoteAddr returns the peer address of the connection.
func (c *Conn) RemoteAddr() string {
	if addr := c.nc.RemoteAddr(); addr != nil {
		return addr.String()
	}
	return ""
}

// Send encodes f and enqueues it for the connection writer. It never blocks
// the caller: when the send queue (by frame count or by byte budget) is full
// the connection is closed and ErrSendQueueFull is returned.
func (c *Conn) Send(f *protocol.Frame) error {
	if c.closed.Load() {
		return ErrConnClosed
	}

	b := getEncodeBuf(protocol.HeaderSize + len(f.Body))
	b, err := protocol.AppendFrame(b[:0], f, c.maxBody)
	if err != nil {
		putEncodeBuf(b)
		return err
	}
	if c.closed.Load() {
		putEncodeBuf(b)
		return ErrConnClosed
	}

	queue := c.sendCh
	if isControlType(f.Type) {
		queue = c.ctrlCh
	}

	size := int64(len(b))
	if c.queuedBytes.Add(size) > int64(c.sendQueueBytes) {
		c.queuedBytes.Add(-size)
		putEncodeBuf(b)
		c.logger.Warn("send queue byte budget exhausted, closing connection",
			"queued_bytes", c.queuedBytes.Load(), "budget", c.sendQueueBytes)
		c.closeWithReason("send queue full")
		return ErrSendQueueFull
	}

	select {
	case queue <- b:
		// The writer goroutine now owns b and returns it to the pool once the
		// frame has been flushed; the caller must not touch b again.
		return nil
	default:
		c.queuedBytes.Add(-size)
		putEncodeBuf(b)
		c.logger.Warn("send queue full, closing connection", "queue_size", cap(queue))
		c.closeWithReason("send queue full")
		return ErrSendQueueFull
	}
}

// isControlType reports whether frames of type t are written through the
// priority control queue. ERROR frames stay on the data queue.
func isControlType(t protocol.FrameType) bool {
	switch t {
	case protocol.Ping, protocol.Pong, protocol.Close:
		return true
	default:
		return false
	}
}

// Close closes the connection once. It is safe to call multiple times and
// from multiple goroutines. The send channel is intentionally never closed.
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		c.setReason("local close")
		c.closed.Store(true)
		close(c.done)
		if err := c.nc.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			c.logger.Debug("close underlying connection", "error", err)
		}
		if c.onClose != nil {
			c.onClose(c)
		}
		c.logger.Info("connection closed", "reason", c.reasonString())
	})
}

// run starts the writer goroutine and then reads on the calling goroutine.
// It returns once both goroutines have exited.
func (c *Conn) run() {
	c.wg.Add(1)
	go c.writeLoop()

	c.readLoop()
	c.Close()
	c.wg.Wait()
}

// readLoop decodes frames, answers PING with PONG and dispatches everything
// else to the handler. A read deadline enforces the idle timeout.
//
// The deadline is not reset on every frame: after each decoded frame it is
// only refreshed once less than half of the idle window remains. When such an
// early deadline fires while no partial frame is buffered, the deadline is
// re-armed to lastInbound+idleTimeout, so the connection still closes exactly
// one idle window after the last received frame (the Phase 0 semantics) while
// SetReadDeadline is called only about once per idle/2 window.
func (c *Conn) readLoop() {
	defer c.Close()

	r := bufio.NewReaderSize(c.nc, c.readBufferSize)
	lastInbound := time.Now()
	var deadline time.Time
	if c.idleTimeout > 0 {
		deadline = lastInbound.Add(c.idleTimeout)
		if err := c.nc.SetReadDeadline(deadline); err != nil {
			c.closeWithReason("set read deadline: " + err.Error())
			return
		}
	}

	for {
		f := getFrame()
		if err := protocol.DecodeInto(r, f, c.maxBody); err != nil {
			putFrame(f)
			var ne net.Error
			if c.idleTimeout > 0 && errors.As(err, &ne) && ne.Timeout() && r.Buffered() == 0 {
				// The deadline was armed before the last frame arrived. Re-arm
				// it to the full idle window measured from that frame instead
				// of closing early; a buffered partial frame must not be
				// retried, so fall through to the error path in that case.
				now := time.Now()
				if remaining := c.idleTimeout - now.Sub(lastInbound); remaining > 0 {
					deadline = lastInbound.Add(c.idleTimeout)
					if setErr := c.nc.SetReadDeadline(deadline); setErr != nil {
						c.closeWithReason("set read deadline: " + setErr.Error())
						return
					}
					continue
				}
			}
			c.handleReadError(err)
			return
		}
		now := time.Now()
		lastInbound = now
		c.lastInbound.Store(now.UnixNano())
		if c.idleTimeout > 0 && now.Add(c.idleTimeout/2).After(deadline) {
			deadline = now.Add(c.idleTimeout)
			if err := c.nc.SetReadDeadline(deadline); err != nil {
				putFrame(f)
				c.closeWithReason("set read deadline: " + err.Error())
				return
			}
		}

		switch f.Type {
		case protocol.Ping:
			// Heartbeat responses take priority over the user handler. Send
			// encodes the frame before returning, so f.Body stays valid here.
			if err := c.Send(&protocol.Frame{Type: protocol.Pong, MsgID: f.MsgID, Body: f.Body}); err != nil {
				putFrame(f)
				return
			}
		case protocol.Close:
			putFrame(f)
			c.closeWithReason("received close frame")
			return
		default:
			if c.handler == nil {
				putFrame(f)
				continue
			}
			err := c.handler(c, f)
			frameType, msgID := f.Type, f.MsgID
			putFrame(f)
			if err != nil {
				c.logger.Warn("handler error", "frame_type", frameType.String(), "msg_id", msgID, "error", err)
				c.closeWithReason("handler error")
				return
			}
		}
	}
}

func (c *Conn) handleReadError(err error) {
	if c.closed.Load() {
		return
	}
	var ne net.Error
	switch {
	case errors.As(err, &ne) && ne.Timeout():
		c.closeWithReason("idle timeout")
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
		c.closeWithReason("peer closed")
	default:
		c.closeWithReason(fmt.Sprintf("read error: %v", err))
	}
}

// writeLoop is the only goroutine that writes to the socket. Control frames
// get strict priority; data frames are coalesced into one Write up to the
// configured frame/byte targets. It also drives the heartbeat timer and so
// replaces a dedicated heartbeat goroutine: PING frames are queued on the
// control channel, which guarantees they are written ahead of data traffic.
// It exits after successfully flushing a CLOSE frame or after any write,
// heartbeat or timeout failure.
func (c *Conn) writeLoop() {
	defer c.wg.Done()

	// The heartbeat timer only exists when heartbeats are enabled. hbC stays
	// nil otherwise, which disables the select case below.
	var hb *time.Timer
	var hbC <-chan time.Time
	if c.heartbeatInterval > 0 {
		hb = time.NewTimer(c.heartbeatInterval)
		hbC = hb.C
		defer hb.Stop()
	}

	for {
		// Priority check: never wait behind data traffic when a control frame
		// is already queued.
		select {
		case <-c.done:
			return
		case frame := <-c.ctrlCh:
			if !c.writeBatch([][]byte{frame}) {
				return
			}
			continue
		default:
		}

		select {
		case <-c.done:
			return
		case frame := <-c.ctrlCh:
			if !c.writeBatch([][]byte{frame}) {
				return
			}
		case frame := <-c.sendCh:
			batch := make([][]byte, 0, c.writeBatchFrames)
			batch = append(batch, frame)
			batch = c.drainData(batch)
			if !c.writeBatch(batch) {
				return
			}
		case <-hbC:
			// drainData is bounded, so returning to this select after each
			// batch keeps the timer responsive under sustained data load.
			if c.heartbeatTimeout > 0 && time.Since(time.Unix(0, c.lastInbound.Load())) > c.heartbeatTimeout {
				c.closeWithReason("heartbeat timeout")
				return
			}
			msgID := c.nextMsgID.Add(1)
			if err := c.Send(&protocol.Frame{Type: protocol.Ping, MsgID: msgID}); err != nil {
				return
			}
			hb.Reset(c.heartbeatInterval)
		}
	}
}

// drainData greedily appends queued data frames to batch until the frame or
// byte target is reached. The byte target may be exceeded by at most one frame
// (the one that was already taken). All frames stay owned by the caller.
func (c *Conn) drainData(batch [][]byte) [][]byte {
	total := 0
	for _, frame := range batch {
		total += len(frame)
	}
	for len(batch) < c.writeBatchFrames && total < c.writeBatchBytes {
		select {
		case frame := <-c.sendCh:
			batch = append(batch, frame)
			total += len(frame)
		default:
			return batch
		}
	}
	return batch
}

// writeBatch concatenates batch into one buffer and writes it with a single
// Write call. Every frame buffer is returned to the pool and its bytes are
// released from the queue budget as soon as it has been copied. It reports
// whether the writer should keep running.
func (c *Conn) writeBatch(batch [][]byte) bool {
	total := 0
	for _, frame := range batch {
		total += len(frame)
	}

	out := getEncodeBuf(total)
	isClose := false
	for _, frame := range batch {
		// Frame type lives at header offset 3; every queued slice was produced
		// by AppendFrame and therefore has a full header.
		if len(frame) >= protocol.HeaderSize && protocol.FrameType(frame[3]) == protocol.Close {
			isClose = true
		}
		out = append(out, frame...)
		c.queuedBytes.Add(-int64(len(frame)))
		putEncodeBuf(frame)
	}

	if c.writeTimeout > 0 {
		if err := c.nc.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			putEncodeBuf(out)
			if !c.closed.Load() {
				c.closeWithReason("set write deadline: " + err.Error())
			}
			return false
		}
	}

	_, err := c.nc.Write(out)
	putEncodeBuf(out)

	if c.writeTimeout > 0 {
		_ = c.nc.SetWriteDeadline(time.Time{})
	}

	if err != nil {
		if !c.closed.Load() {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				c.closeWithReason("write timeout")
			} else {
				c.closeWithReason("write error: " + err.Error())
			}
		}
		return false
	}
	if isClose {
		c.closeWithReason("close frame sent")
		return false
	}
	return true
}

// CloseWithReason closes the connection with an explicit reason that shows up
// in the close log. It is safe to call multiple times; the first reason wins.
func (c *Conn) CloseWithReason(reason string) { c.closeWithReason(reason) }

// CloseWithFrame enqueues f and lets the writer close the connection once f
// has been flushed. The first reason set wins, so the connection is logged
// with reason even though the writer closes it. If enqueueing fails the
// connection is closed immediately.
func (c *Conn) CloseWithFrame(f *protocol.Frame, reason string) error {
	if reason != "" {
		c.setReason(reason)
	}
	if err := c.Send(f); err != nil {
		c.Close()
		return err
	}
	return nil
}

func (c *Conn) closeWithReason(reason string) {
	c.setReason(reason)
	c.Close()
}

func (c *Conn) setReason(reason string) {
	c.reasonMu.Lock()
	if c.reason == "" {
		c.reason = reason
	}
	c.reasonMu.Unlock()
}

func (c *Conn) reasonString() string {
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	return c.reason
}
