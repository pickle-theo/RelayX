// Command client is the Phase 0 RelayX demo client. It connects to a gateway,
// periodically sends DATA frames and echoes server-side traffic to stdout.
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
	"strings"
	"sync"
	"time"

	"relayx/phase1/internal/config"
	"relayx/phase1/internal/protocol"
)

const (
	dialTimeout = 10 * time.Second
	// echoWaitTimeout bounds how long -count mode waits for the final echoes.
	echoWaitTimeout = 10 * time.Second
	// deliverWaitTimeout lets pending DELIVER frames print before -count SEND
	// mode exits.
	deliverWaitTimeout = 300 * time.Millisecond
)

// payloadAlphabet is repeated and truncated to build fixed-size payloads.
const payloadAlphabet = "abcdefghijklmnopqrstuvwxyz"

var (
	errServerClosed = errors.New("server closed connection")
	errConnClosed   = errors.New("connection closed")
)

func main() {
	cfg, err := config.ParseClient(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(cfg, logger); err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(1)
	}
}

// run owns the connection. All socket writes happen on the calling goroutine;
// the reader goroutine only reads and forwards PONG frames through pongCh.
func run(cfg config.ClientConfig, logger *slog.Logger) error {
	conn, err := net.DialTimeout("tcp", cfg.Addr, dialTimeout)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", cfg.Addr, err)
	}
	fmt.Printf("connected to %s as %s\n", cfg.Addr, cfg.Name)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	writeFrame := func(f *protocol.Frame) error {
		if err := protocol.Encode(writer, f, cfg.MaxBody); err != nil {
			return err
		}
		return writer.Flush()
	}

	// The handshake runs before the reader goroutine starts so both use the
	// same bufio.Reader without competing for bytes.
	if cfg.User != "" {
		if err := login(conn, reader, writeFrame, cfg); err != nil {
			_ = conn.Close()
			return err
		}
	}

	done := make(chan struct{})
	pongCh := make(chan *protocol.Frame, 8)
	dataCh := make(chan uint64, 64)
	errCh := make(chan error, 1)

	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		readLoop(conn, reader, cfg, pongCh, dataCh, errCh, done)
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
		received int
	)
	tickCh := ticker.C
	// waitCh stays nil until -count mode has sent every frame; waitSend marks
	// the short SEND-mode grace period.
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
			if err := writeFrame(frame); err != nil {
				return fmt.Errorf("send data: %w", err)
			}
			sent++
			if cfg.Count > 0 && sent >= cfg.Count {
				ticker.Stop()
				tickCh = nil
				switch {
				case sendMode:
					// SEND has no echo; give DELIVER frames time to print.
					waitTimer = time.NewTimer(deliverWaitTimeout)
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
			if err := writeFrame(f); err != nil {
				return fmt.Errorf("send pong: %w", err)
			}
		case <-waitCh:
			if waitSend {
				return nil
			}
			logger.Warn("timed out waiting for echoes",
				"sent", sent, "received", received, "timeout", echoWaitTimeout)
			return nil
		case <-sigCh:
			fmt.Println("bye")
			return nil
		case err := <-errCh:
			if errors.Is(err, errServerClosed) || errors.Is(err, errConnClosed) {
				return nil
			}
			return err
		}
	}
}

// readLoop decodes frames until the connection ends. PING responses are handed
// to the main goroutine instead of being written here.
func readLoop(conn net.Conn, r *bufio.Reader, cfg config.ClientConfig, pongCh chan<- *protocol.Frame, dataCh chan<- uint64, errCh chan<- error, done <-chan struct{}) {
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
			fmt.Printf("deliver from=%s/%s msg_id=%d body=%s\n", d.FromUser, d.FromDevice, f.MsgID, d.Payload)
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
