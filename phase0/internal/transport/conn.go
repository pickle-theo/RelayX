package transport

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/phase0/internal/config"
	"relayx/phase0/internal/protocol"
)

// ErrConnClosed is returned by Conn.Send when the connection has been closed.
var ErrConnClosed = errors.New("transport: connection closed")

// ErrSendQueueFull is returned by Conn.Send when the bounded send queue is
// full. The connection is closed as well to apply backpressure.
var ErrSendQueueFull = errors.New("transport: send queue full")

// Conn is a single client connection. Every connection owns exactly one
// reader goroutine, one writer goroutine and one heartbeat goroutine, so all
// socket writes are serialized through sendCh.
type Conn struct {
	id      uint64
	nc      net.Conn
	handler HandlerFunc
	logger  *slog.Logger

	sendCh chan []byte
	done   chan struct{}

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
		done:              make(chan struct{}),
		maxBody:           cfg.MaxBody,
		heartbeatInterval: cfg.HeartbeatInterval,
		heartbeatTimeout:  cfg.HeartbeatTimeout,
		idleTimeout:       cfg.IdleTimeout,
		onClose:           onClose,
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
// the caller: when the send queue is full the connection is closed and
// ErrSendQueueFull is returned.
func (c *Conn) Send(f *protocol.Frame) error {
	if c.closed.Load() {
		return ErrConnClosed
	}

	var buf bytes.Buffer
	if err := protocol.Encode(&buf, f, c.maxBody); err != nil {
		return err
	}
	if c.closed.Load() {
		return ErrConnClosed
	}

	select {
	case c.sendCh <- buf.Bytes():
		return nil
	default:
		c.logger.Warn("send queue full, closing connection", "queue_size", cap(c.sendCh))
		c.closeWithReason("send queue full")
		return ErrSendQueueFull
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

// run starts the writer and heartbeat goroutines and then reads on the
// calling goroutine. It returns once all three goroutines have exited.
func (c *Conn) run() {
	c.wg.Add(2)
	go c.writeLoop()
	go c.heartbeatLoop()

	c.readLoop()
	c.Close()
	c.wg.Wait()
}

// readLoop decodes frames, answers PING with PONG and dispatches everything
// else to the handler. A read deadline enforces the idle timeout.
func (c *Conn) readLoop() {
	defer c.Close()

	r := bufio.NewReader(c.nc)
	for {
		if c.idleTimeout > 0 {
			if err := c.nc.SetReadDeadline(time.Now().Add(c.idleTimeout)); err != nil {
				c.closeWithReason("set read deadline: " + err.Error())
				return
			}
		}

		f, err := protocol.Decode(r, c.maxBody)
		if err != nil {
			c.handleReadError(err)
			return
		}
		c.lastInbound.Store(time.Now().UnixNano())

		switch f.Type {
		case protocol.Ping:
			// Heartbeat responses take priority over the user handler.
			if err := c.Send(&protocol.Frame{Type: protocol.Pong, MsgID: f.MsgID, Body: f.Body}); err != nil {
				return
			}
		case protocol.Close:
			c.closeWithReason("received close frame")
			return
		default:
			if c.handler == nil {
				continue
			}
			if err := c.handler(c, f); err != nil {
				c.logger.Warn("handler error", "frame_type", f.Type.String(), "msg_id", f.MsgID, "error", err)
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

// writeLoop is the only goroutine that writes to the socket. It exits after
// successfully flushing a CLOSE frame or after any write failure.
func (c *Conn) writeLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.done:
			return
		case data := <-c.sendCh:
			if _, err := c.nc.Write(data); err != nil {
				if !c.closed.Load() {
					c.closeWithReason("write error: " + err.Error())
				}
				return
			}
			// Frame type lives at header offset 3; every queued slice was
			// produced by protocol.Encode and therefore has a full header.
			if len(data) >= protocol.HeaderSize && protocol.FrameType(data[3]) == protocol.Close {
				c.closeWithReason("close frame sent")
				return
			}
		}
	}
}

// heartbeatLoop periodically sends PING frames and closes the connection when
// no inbound frame has been observed within the heartbeat timeout.
func (c *Conn) heartbeatLoop() {
	defer c.wg.Done()

	if c.heartbeatInterval <= 0 {
		return
	}
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if c.heartbeatTimeout > 0 && time.Since(time.Unix(0, c.lastInbound.Load())) > c.heartbeatTimeout {
				c.closeWithReason("heartbeat timeout")
				return
			}
			msgID := c.nextMsgID.Add(1)
			if err := c.Send(&protocol.Frame{Type: protocol.Ping, MsgID: msgID}); err != nil {
				return
			}
		}
	}
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
