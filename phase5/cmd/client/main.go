// Command client is the Phase 2 RelayX demo client. It connects to a gateway,
// performs the LOGIN handshake, optionally pulls offline deliveries, echoes
// server-side traffic to stdout and acknowledges DELIVER frames.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"relayx/phase5/internal/config"
	"relayx/phase5/internal/protocol"
)

const (
	dialTimeout = 10 * time.Second
	// echoWaitTimeout bounds how long -count mode waits for the final echoes.
	echoWaitTimeout = 10 * time.Second
	// ackWaitTimeout bounds how long -count SEND mode waits for its SEND_ACKs so
	// every acceptance result is printed even when the registry is degraded.
	ackWaitTimeout = 10 * time.Second
	// pullPageLimit is the page size requested by the automatic PULL loop.
	pullPageLimit = 100
)

// payloadAlphabet is repeated and truncated to build fixed-size payloads.
const payloadAlphabet = "abcdefghijklmnopqrstuvwxyz"

var (
	errServerClosed = errors.New("server closed connection")
	errConnClosed   = errors.New("connection closed")
)

// ackMode selects which DELIVER acknowledgements the client sends.
type ackMode struct {
	receipt  bool
	business bool
}

func parseAckMode(s string) (ackMode, error) {
	switch s {
	case "receipt":
		return ackMode{receipt: true}, nil
	case "business":
		return ackMode{business: true}, nil
	case "both":
		return ackMode{receipt: true, business: true}, nil
	case "none":
		return ackMode{}, nil
	default:
		return ackMode{}, fmt.Errorf("ack must be one of receipt, business, both, none; got %q", s)
	}
}

// clientOptions holds the Phase 2 client flags that config.ParseClient does
// not know about.
type clientOptions struct {
	pull bool
	ack  ackMode
}

func main() {
	opts, rest, err := parseClientFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(2)
	}
	cfg, err := config.ParseClient(rest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(cfg, opts, logger); err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(1)
	}
}

// parseClientFlags removes -pull and -ack from args and returns the remaining
// arguments for config.ParseClient. It mirrors the standard flag syntax:
// "-pull", "-pull=false", "-ack=both" and "-ack both" are all accepted.
func parseClientFlags(args []string) (clientOptions, []string, error) {
	opts := clientOptions{pull: true, ack: ackMode{receipt: true, business: true}}
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		name, value, hasValue, ok := splitFlag(args[i])
		if !ok {
			rest = append(rest, args[i])
			continue
		}
		switch name {
		case "pull":
			if hasValue {
				v, err := strconv.ParseBool(value)
				if err != nil {
					return clientOptions{}, nil, fmt.Errorf("invalid -pull value %q", value)
				}
				opts.pull = v
			} else {
				opts.pull = true
			}
		case "ack":
			if !hasValue {
				if i+1 >= len(args) {
					return clientOptions{}, nil, errors.New("-ack requires a value")
				}
				i++
				value = args[i]
			}
			ack, err := parseAckMode(value)
			if err != nil {
				return clientOptions{}, nil, err
			}
			opts.ack = ack
		default:
			rest = append(rest, args[i])
		}
	}
	return opts, rest, nil
}

// splitFlag reports whether arg is a flag and splits "-name=value" (or the
// double-dash form) into its parts.
func splitFlag(arg string) (name, value string, hasValue, ok bool) {
	if len(arg) < 2 || arg[0] != '-' {
		return "", "", false, false
	}
	name = arg[1:]
	if strings.HasPrefix(name, "-") {
		name = name[1:]
	}
	if name == "" {
		return "", "", false, false
	}
	if i := strings.IndexByte(name, '='); i >= 0 {
		return name[:i], name[i+1:], true, true
	}
	return name, "", false, true
}

// frameWriter serializes socket writes so the read loop (ACK/PULL/PONG replies)
// and the main goroutine (DATA/SEND) never interleave frame bytes.
type frameWriter struct {
	mu      sync.Mutex
	w       *bufio.Writer
	maxBody int
}

func (fw *frameWriter) send(f *protocol.Frame) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if err := protocol.Encode(fw.w, f, fw.maxBody); err != nil {
		return err
	}
	return fw.w.Flush()
}

// run connects to the gateway and owns the connection. A client may be given
// several gateway nodes: it starts at the first reachable one and, when the
// connection drops, fails over to the next node and logs in again. All socket
// writes go through frameWriter; the reader goroutine only reads frames and
// signals the main goroutine for PONGs.
func run(cfg config.ClientConfig, opts clientOptions, logger *slog.Logger) error {
	addrs := cfg.Addrs
	if len(addrs) == 0 {
		addrs = []string{cfg.Addr}
	}

	idx, conn, reader, writer, err := dialFirst(addrs, cfg, opts)
	if err != nil {
		return err
	}
	fmt.Printf("connected to %s as %s\n", addrs[idx], cfg.Name)

	for {
		err = session(conn, reader, writer, cfg, opts, logger)
		if err == nil {
			return nil
		}
		// A single-node client keeps the existing behaviour: a server-side
		// close is a clean exit and any other failure is fatal.
		if len(addrs) == 1 {
			if errors.Is(err, errServerClosed) || errors.Is(err, errConnClosed) {
				return nil
			}
			return err
		}

		// The node is gone. Try every other address before giving up so a
		// crashed node does not end the client's workload.
		lostAddr := addrs[idx]
		broken := err
		var lastErr error
		switched := false
		for attempt := 1; attempt < len(addrs); attempt++ {
			idx = config.NextAddrIndex(addrs, idx)
			var cerr error
			conn, reader, writer, cerr = connectAddr(addrs[idx], cfg, opts)
			if cerr != nil {
				lastErr = cerr
				continue
			}
			logger.Warn("connection lost, switching gateway node",
				"from", lostAddr, "to", addrs[idx], "reason", broken)
			switched = true
			break
		}
		if !switched {
			return fmt.Errorf("all gateway addresses failed (last error: %v)", lastErr)
		}
		fmt.Printf("connected to %s as %s\n", addrs[idx], cfg.Name)
	}
}

// dialFirst connects to the first reachable address in list order and returns
// its index together with the ready connection.
func dialFirst(addrs []string, cfg config.ClientConfig, opts clientOptions) (int, net.Conn, *bufio.Reader, *frameWriter, error) {
	var lastErr error
	for i, addr := range addrs {
		conn, reader, writer, err := connectAddr(addr, cfg, opts)
		if err == nil {
			return i, conn, reader, writer, nil
		}
		lastErr = err
	}
	return 0, nil, nil, nil, lastErr
}

// connectAddr dials one address and completes LOGIN plus the initial PULL.
func connectAddr(addr string, cfg config.ClientConfig, opts clientOptions) (net.Conn, *bufio.Reader, *frameWriter, error) {
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("connect to %s: %w", addr, err)
	}

	reader := bufio.NewReader(conn)
	writer := &frameWriter{w: bufio.NewWriter(conn), maxBody: cfg.MaxBody}

	// The handshake and the initial PULL run before the reader goroutine starts
	// so both use the same bufio.Reader without competing for bytes.
	if cfg.User != "" {
		if err := login(conn, reader, writer.send, cfg); err != nil {
			_ = conn.Close()
			return nil, nil, nil, fmt.Errorf("connect to %s: %w", addr, err)
		}
		if opts.pull {
			if err := sendPull(writer, 0); err != nil {
				_ = conn.Close()
				return nil, nil, nil, fmt.Errorf("connect to %s: send pull: %w", addr, err)
			}
		}
	}
	return conn, reader, writer, nil
}

// session runs the traffic loop on an established connection. A non-nil error
// means the connection ended unexpectedly and the caller may fail over.
func session(conn net.Conn, reader *bufio.Reader, writer *frameWriter, cfg config.ClientConfig, opts clientOptions, logger *slog.Logger) error {
	done := make(chan struct{})
	pongCh := make(chan *protocol.Frame, 8)
	dataCh := make(chan uint64, 64)
	ackCh := make(chan struct{}, 64)
	errCh := make(chan error, 1)

	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		readLoop(conn, reader, writer, cfg, opts, pongCh, dataCh, ackCh, errCh, done)
	}()

	ticker := time.NewTicker(cfg.Interval)
	var waitTimer *time.Timer
	defer func() {
		ticker.Stop()
		if waitTimer != nil {
			waitTimer.Stop()
		}
		close(done)
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Warn("close connection", "error", err)
		}
		readerWG.Wait()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	payload := makePayload(cfg.PayloadSize)
	sendMode := cfg.To != ""
	targetType := protocol.TargetUser
	target := cfg.To
	if rest, ok := strings.CutPrefix(cfg.To, "endpoint:"); ok {
		targetType = protocol.TargetEndpoint
		target = rest
	}

	var (
		seq      uint64
		sent     int
		acked    int
		received int
	)
	tickCh := ticker.C
	// waitCh stays nil until -count mode has sent every frame; waitSend marks
	// SEND mode waiting for one SEND_ACK per frame.
	var waitCh <-chan time.Time
	var waitSend bool
	for {
		select {
		case <-tickCh:
			seq++
			body := fmt.Sprintf("%s#%d %s", cfg.Name, seq, payload)
			frame := &protocol.Frame{Type: protocol.Data, MsgID: seq, Body: []byte(body)}
			if sendMode {
				sendBody, err := protocol.AppendSendBody(nil, &protocol.SendBody{
					TargetType: targetType,
					Target:     target,
					Payload:    []byte(body),
				})
				if err != nil {
					return fmt.Errorf("encode send: %w", err)
				}
				frame = &protocol.Frame{Type: protocol.Send, MsgID: seq, Body: sendBody}
			}
			if err := writer.send(frame); err != nil {
				return fmt.Errorf("send data: %w", err)
			}
			sent++
			if cfg.Count > 0 && sent >= cfg.Count {
				ticker.Stop()
				tickCh = nil
				switch {
				case sendMode:
					// Wait for one SEND_ACK per frame so the acceptance result
					// is printed for every message; the timer is a safety net.
					waitTimer = time.NewTimer(ackWaitTimeout)
					waitCh = waitTimer.C
					waitSend = true
				case received >= sent:
					return nil
				default:
					waitTimer = time.NewTimer(echoWaitTimeout)
					waitCh = waitTimer.C
				}
			}
		case <-dataCh:
			received++
			if waitCh != nil && !waitSend && received >= sent {
				return nil
			}
		case f := <-pongCh:
			if err := writer.send(f); err != nil {
				return fmt.Errorf("send pong: %w", err)
			}
		case <-ackCh:
			acked++
			if waitSend && acked >= sent {
				return nil
			}
		case <-waitCh:
			if waitSend {
				logger.Warn("timed out waiting for send acks",
					"sent", sent, "acked", acked, "timeout", ackWaitTimeout)
				return nil
			}
			logger.Warn("timed out waiting for echoes",
				"sent", sent, "received", received, "timeout", echoWaitTimeout)
			return nil
		case <-sigCh:
			fmt.Println("bye")
			return nil
		case err := <-errCh:
			return err
		}
	}
}

// sendPull requests one PULL page starting at cursor.
func sendPull(w *frameWriter, cursor uint64) error {
	body, err := protocol.AppendPullBody(nil, &protocol.PullBody{Cursor: cursor, Limit: pullPageLimit})
	if err != nil {
		return err
	}
	return w.send(&protocol.Frame{Type: protocol.Pull, Body: body})
}

// readLoop decodes frames until the connection ends. PING responses are handed
// to the main goroutine; DELIVER acknowledgements and follow-up PULL requests
// are written here through the shared frameWriter.
func readLoop(conn net.Conn, r *bufio.Reader, w *frameWriter, cfg config.ClientConfig, opts clientOptions, pongCh chan<- *protocol.Frame, dataCh chan<- uint64, ackCh chan<- struct{}, errCh chan<- error, done <-chan struct{}) {
	for {
		if cfg.IdleTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(cfg.IdleTimeout)); err != nil {
				deliverErr(errCh, done, fmt.Errorf("set read deadline: %w", err))
				return
			}
		}

		f, err := protocol.Decode(r, cfg.MaxBody)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
				deliverErr(errCh, done, errConnClosed)
			} else {
				deliverErr(errCh, done, fmt.Errorf("read: %w", err))
			}
			return
		}

		switch f.Type {
		case protocol.Ping:
			pong := &protocol.Frame{Type: protocol.Pong, MsgID: f.MsgID}
			select {
			case pongCh <- pong:
			case <-done:
				return
			}
		case protocol.Data:
			fmt.Printf("echo msg_id=%d body=%s\n", f.MsgID, f.Body)
			select {
			case dataCh <- f.MsgID:
			case <-done:
				return
			}
		case protocol.Deliver:
			d, err := protocol.DecodeDeliver(f.Body)
			if err != nil {
				fmt.Printf("error decoding deliver: %v\n", err)
				continue
			}
			fmt.Printf("deliver from=%s/%s message_id=%d body=%s\n", d.FromUser, d.FromDevice, f.MsgID, d.Payload)
			if err := ackDeliver(w, opts.ack, f.MsgID); err != nil {
				deliverErr(errCh, done, err)
				return
			}
		case protocol.SendAck:
			ack, err := protocol.DecodeSendAck(f.Body)
			if err != nil {
				fmt.Printf("error decoding send_ack: %v\n", err)
				continue
			}
			fmt.Printf("send_ack status=%s message_id=%d msg=%s\n", sendAckStatusName(ack.Status), ack.MessageID, ack.Message)
			select {
			case ackCh <- struct{}{}:
			case <-done:
				return
			}
		case protocol.PullDone:
			page, err := protocol.DecodePullDone(f.Body)
			if err != nil {
				fmt.Printf("error decoding pull_done: %v\n", err)
				continue
			}
			fmt.Printf("pull_done count=%d cursor=%d has_more=%t\n", page.Count, page.NextCursor, page.HasMore)
			if page.HasMore {
				if err := sendPull(w, page.NextCursor); err != nil {
					deliverErr(errCh, done, fmt.Errorf("send pull: %w", err))
					return
				}
			}
		case protocol.LoginAck:
			// The handshake already completed before this loop started.
		case protocol.Error:
			fmt.Printf("error from server: %s\n", f.Body)
		case protocol.Close:
			fmt.Println("server closed connection")
			deliverErr(errCh, done, errServerClosed)
			return
		}
	}
}

// ackDeliver sends the acknowledgements selected by ack for one DELIVER. The
// frame header MsgID of a DELIVER is the platform message ID.
func ackDeliver(w *frameWriter, ack ackMode, messageID uint64) error {
	if ack.receipt {
		body, err := protocol.AppendReceiptAckBody(nil, &protocol.ReceiptAckBody{MessageID: messageID})
		if err != nil {
			return fmt.Errorf("encode receipt ack: %w", err)
		}
		if err := w.send(&protocol.Frame{Type: protocol.ReceiptAck, MsgID: messageID, Body: body}); err != nil {
			return fmt.Errorf("send receipt ack: %w", err)
		}
	}
	if ack.business {
		body, err := protocol.AppendBusinessAckBody(nil, &protocol.BusinessAckBody{MessageID: messageID, Status: 0})
		if err != nil {
			return fmt.Errorf("encode business ack: %w", err)
		}
		if err := w.send(&protocol.Frame{Type: protocol.BusinessAck, MsgID: messageID, Body: body}); err != nil {
			return fmt.Errorf("send business ack: %w", err)
		}
	}
	return nil
}

// sendAckStatusName maps a SEND_ACK status to the lowercase name printed in
// the client log.
func sendAckStatusName(s protocol.SendAckStatus) string {
	switch s {
	case protocol.SendAckAccepted:
		return "accepted"
	case protocol.SendAckDuplicate:
		return "duplicate"
	case protocol.SendAckInvalid:
		return "invalid"
	case protocol.SendAckInternal:
		return "internal"
	default:
		return fmt.Sprintf("status(%d)", uint8(s))
	}
}

// login performs the synchronous LOGIN/LOGIN_ACK handshake on the calling
// goroutine, reusing the shared reader so no bytes are stolen before the read
// loop starts. PING frames received during the handshake are answered at once.
func login(conn net.Conn, r *bufio.Reader, writeFrame func(*protocol.Frame) error, cfg config.ClientConfig) error {
	body, err := protocol.AppendLoginBody(nil, &protocol.LoginBody{UserID: cfg.User, DeviceID: deviceID(cfg)})
	if err != nil {
		return fmt.Errorf("encode login: %w", err)
	}
	if cfg.IdleTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(cfg.IdleTimeout)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}
	}
	if err := writeFrame(&protocol.Frame{Type: protocol.Login, Body: body}); err != nil {
		return fmt.Errorf("send login: %w", err)
	}

	for {
		f, err := protocol.Decode(r, cfg.MaxBody)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
				return errConnClosed
			}
			return fmt.Errorf("read login ack: %w", err)
		}

		switch f.Type {
		case protocol.LoginAck:
			ack, err := protocol.DecodeLoginAck(f.Body)
			if err != nil {
				return fmt.Errorf("decode login ack: %w", err)
			}
			if ack.Status != protocol.LoginStatusOK {
				return fmt.Errorf("login rejected: status=%d message=%q", ack.Status, ack.Message)
			}
			fmt.Printf("logged in as %s/%s session=%d\n", cfg.User, deviceID(cfg), ack.SessionID)
			return nil
		case protocol.Ping:
			if err := writeFrame(&protocol.Frame{Type: protocol.Pong, MsgID: f.MsgID}); err != nil {
				return fmt.Errorf("send pong: %w", err)
			}
		}
	}
}

// deviceID returns the device claimed in LOGIN, defaulting to the client name.
func deviceID(cfg config.ClientConfig) string {
	if cfg.Device != "" {
		return cfg.Device
	}
	return cfg.Name
}

// deliverErr reports a read-loop outcome without blocking shutdown.
func deliverErr(errCh chan<- error, done <-chan struct{}, err error) {
	select {
	case errCh <- err:
	case <-done:
	}
}

// makePayload returns a lowercase payload of exactly n bytes.
func makePayload(n int) string {
	if n <= 0 {
		return ""
	}
	repeats := (n + len(payloadAlphabet) - 1) / len(payloadAlphabet)
	return strings.Repeat(payloadAlphabet, repeats)[:n]
}
