// Command loadtest drives many concurrent RelayX connections against a
// gateway and reports connection success, echo latency and resource usage.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"relayx/phase2/internal/protocol"
)

// options holds every load test setting.
type options struct {
	addr        string
	conns       int
	duration    time.Duration
	interval    time.Duration
	payloadSize int
	maxBody     int
	ramp        time.Duration
	timeout     time.Duration
	out         string
	login       bool
	route       bool
	routeTo     string
	reliable    bool
	ack         string
}

func main() {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadtest: %v\n", err)
		os.Exit(2)
	}

	report, ok := run(opts)
	fmt.Print(report)

	if opts.out != "" {
		if err := os.WriteFile(opts.out, []byte(report), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "loadtest: write %s: %v\n", opts.out, err)
			os.Exit(1)
		}
	}
	if !ok {
		os.Exit(1)
	}
}

func parseFlags(args []string) (options, error) {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var o options
	fs.StringVar(&o.addr, "addr", "127.0.0.1:9000", "gateway address")
	fs.IntVar(&o.conns, "conns", 1000, "number of concurrent connections")
	fs.DurationVar(&o.duration, "duration", 60*time.Second, "test duration")
	fs.DurationVar(&o.interval, "interval", time.Second, "send interval per connection")
	fs.IntVar(&o.payloadSize, "payload-size", 64, "fixed payload size in bytes")
	fs.IntVar(&o.maxBody, "max-body", 1<<20, "maximum frame body size in bytes")
	fs.DurationVar(&o.ramp, "ramp", 5*time.Second, "time over which connections are established")
	fs.DurationVar(&o.timeout, "timeout", 5*time.Second, "per-message echo timeout")
	fs.StringVar(&o.out, "out", "", "optional report file path")
	fs.BoolVar(&o.login, "login", false, "send LOGIN and wait for LOGIN_ACK per connection")
	fs.BoolVar(&o.route, "route", false, "send SEND instead of DATA (implies -login)")
	fs.StringVar(&o.routeTo, "route-to", "", "route target user id (empty means self; implies -route)")
	fs.BoolVar(&o.reliable, "reliable", false, "send SEND and wait for SEND_ACK/DELIVER (implies -login)")
	fs.StringVar(&o.ack, "ack", "both", "DELIVER acknowledgements in reliable mode: receipt, business, both or none")

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if o.reliable {
		// Reliable mode replaces the Phase 1 route pattern; -route-to only
		// selects the target user.
		o.route = false
	} else if o.routeTo != "" {
		o.route = true
	}
	if o.route || o.reliable {
		o.login = true
	}
	if !validAckMode(o.ack) {
		return options{}, fmt.Errorf("ack must be one of receipt, business, both, none; got %q", o.ack)
	}
	if err := o.validate(); err != nil {
		return options{}, err
	}
	return o, nil
}

// mode names the traffic pattern derived from the flags.
func (o options) mode() string {
	switch {
	case o.reliable:
		return "reliable"
	case o.route:
		return "route"
	case o.login:
		return "login"
	default:
		return "echo"
	}
}

// target reports the route target, or "-" when the frame is not routed.
func (o options) target() string {
	if !o.route && !o.reliable {
		return "-"
	}
	if o.routeTo != "" {
		return o.routeTo
	}
	return "self"
}

// ackReceipt reports whether receipt acknowledgements are enabled.
func (o options) ackReceipt() bool {
	return o.ack == "receipt" || o.ack == "both"
}

// ackBusiness reports whether business acknowledgements are enabled.
func (o options) ackBusiness() bool {
	return o.ack == "business" || o.ack == "both"
}

func validAckMode(s string) bool {
	switch s {
	case "receipt", "business", "both", "none":
		return true
	default:
		return false
	}
}

func (o options) validate() error {
	switch {
	case o.conns <= 0:
		return fmt.Errorf("conns must be positive, got %d", o.conns)
	case o.duration <= 0:
		return fmt.Errorf("duration must be positive, got %s", o.duration)
	case o.interval <= 0:
		return fmt.Errorf("interval must be positive, got %s", o.interval)
	case o.timeout <= 0:
		return fmt.Errorf("timeout must be positive, got %s", o.timeout)
	case o.ramp < 0:
		return fmt.Errorf("ramp must not be negative, got %s", o.ramp)
	case o.maxBody <= 0:
		return fmt.Errorf("max-body must be positive, got %d", o.maxBody)
	case o.payloadSize < 0:
		return fmt.Errorf("payload-size must not be negative, got %d", o.payloadSize)
	case o.payloadSize > o.maxBody:
		return fmt.Errorf("payload-size %d exceeds max-body %d", o.payloadSize, o.maxBody)
	}
	return nil
}

// stats aggregates counters shared by every connection goroutine.
type stats struct {
	connsOK       atomic.Int64
	connsFailed   atomic.Int64
	loginFailures atomic.Int64
	sent          atomic.Int64
	echoed        atomic.Int64
	delivered     atomic.Int64
	sendAcks      atomic.Int64
	duplicateAcks atomic.Int64
	receiptAcked  atomic.Int64
	businessAcked atomic.Int64
	routeErrors   atomic.Int64
	timeouts      atomic.Int64
	decodeErrs    atomic.Int64
	unexpected    atomic.Int64
	writeErrs     atomic.Int64

	errMu    sync.Mutex
	firstErr error

	mu          sync.Mutex
	rtts        []time.Duration
	deliverRTTs []time.Duration
}

func (s *stats) recordErr(err error) {
	s.errMu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	s.errMu.Unlock()
}

func (s *stats) firstErrString() string {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.firstErr == nil {
		return ""
	}
	return s.firstErr.Error()
}

func (s *stats) flushRTTs(v []time.Duration) {
	if len(v) == 0 {
		return
	}
	s.mu.Lock()
	s.rtts = append(s.rtts, v...)
	s.mu.Unlock()
}

func (s *stats) rttSnapshot() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Duration, len(s.rtts))
	copy(out, s.rtts)
	return out
}

func (s *stats) flushDeliverRTTs(v []time.Duration) {
	if len(v) == 0 {
		return
	}
	s.mu.Lock()
	s.deliverRTTs = append(s.deliverRTTs, v...)
	s.mu.Unlock()
}

func (s *stats) deliverRTTSnapshot() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Duration, len(s.deliverRTTs))
	copy(out, s.deliverRTTs)
	return out
}

// connSet tracks live connections so stuck sockets can be force closed.
type connSet struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newConnSet() *connSet {
	return &connSet{conns: make(map[net.Conn]struct{})}
}

func (cs *connSet) add(c net.Conn) {
	cs.mu.Lock()
	cs.conns[c] = struct{}{}
	cs.mu.Unlock()
}

func (cs *connSet) remove(c net.Conn) {
	cs.mu.Lock()
	delete(cs.conns, c)
	cs.mu.Unlock()
}

func (cs *connSet) closeAll() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for c := range cs.conns {
		_ = c.Close()
	}
}

// client serializes socket writes so the reader (PONG replies) and the sender
// (DATA frames) never interleave bytes.
type client struct {
	conn net.Conn
	mu   sync.Mutex
}

func (c *client) send(f *protocol.Frame, maxBody int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return protocol.Encode(c.conn, f, maxBody)
}

func run(o options) (string, bool) {
	st := &stats{}
	live := newConnSet()
	stop := make(chan struct{})

	start := time.Now()
	step := time.Duration(0)
	if o.ramp > 0 {
		step = o.ramp / time.Duration(o.conns)
	}

	var wg sync.WaitGroup
	for i := 0; i < o.conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if delay := time.Duration(id) * step; delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-stop:
					// Ramp incomplete: this connection was never dialed.
					st.connsFailed.Add(1)
					return
				}
			}
			runConn(o, st, live, id, stop)
		}(i)
	}

	time.Sleep(o.duration)
	preShutdownGoroutines := runtime.NumGoroutine()
	close(stop)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Connections close their own sockets once sending stops; force close the
	// remainder if they overrun the grace period.
	grace := o.timeout
	if grace > 2*time.Second {
		grace = 2 * time.Second
	}
	select {
	case <-done:
	case <-time.After(grace):
		live.closeAll()
		select {
		case <-done:
		case <-time.After(o.timeout + time.Second):
		}
	}

	elapsed := time.Since(start)
	return formatReport(o, st, elapsed, preShutdownGoroutines), st.isHealthy()
}

func (s *stats) isHealthy() bool {
	missed := s.sent.Load() - s.responded()
	return s.connsFailed.Load() == 0 && s.loginFailures.Load() == 0 && missed <= 0 && s.routeErrors.Load() == 0
}

// responded reports the number of messages answered by the gateway: DELIVER
// frames in route and reliable modes, DATA echoes otherwise.
func (s *stats) responded() int64 {
	return s.echoed.Load() + s.delivered.Load()
}

// ackMissed reports sends that never produced a DELIVER frame.
func (s *stats) ackMissed() int64 {
	missed := s.sent.Load() - s.delivered.Load()
	if missed < 0 {
		return 0
	}
	return missed
}

func runConn(o options, st *stats, live *connSet, id int, stop <-chan struct{}) {
	conn, err := net.DialTimeout("tcp", o.addr, o.timeout)
	if err != nil {
		st.connsFailed.Add(1)
		st.recordErr(fmt.Errorf("conn %d: dial: %w", id, err))
		return
	}
	st.connsOK.Add(1)
	live.add(conn)
	defer live.remove(conn)

	cl := &client{conn: conn}
	r := bufio.NewReader(conn)
	if o.login {
		if err := login(cl, r, o, id); err != nil {
			st.loginFailures.Add(1)
			st.recordErr(fmt.Errorf("conn %d: login: %w", id, err))
			_ = conn.Close()
			return
		}
	}

	echoCh := make(chan reply, 64)
	readDone := make(chan struct{})

	go func() {
		defer close(readDone)
		readLoop(cl, r, st, echoCh, o)
	}()

	if o.reliable {
		sendReliable(cl, st, echoCh, o, id, stop)
	} else {
		sendLoop(cl, st, echoCh, o, id, stop)
	}

	_ = conn.Close()
	select {
	case <-readDone:
	case <-time.After(time.Second):
	}
}

// loginDevice is the device id every -login connection claims.
const loginDevice = "d0"

// login sends LOGIN and waits for a successful LOGIN_ACK, answering PING while
// it waits. It runs before the read loop starts and shares its reader.
func login(cl *client, r *bufio.Reader, o options, id int) error {
	body, err := protocol.AppendLoginBody(nil, &protocol.LoginBody{
		UserID:   fmt.Sprintf("load-%d", id),
		DeviceID: loginDevice,
	})
	if err != nil {
		return err
	}
	if err := cl.conn.SetReadDeadline(time.Now().Add(o.timeout)); err != nil {
		return err
	}
	if err := cl.send(&protocol.Frame{Type: protocol.Login, Body: body}, o.maxBody); err != nil {
		return err
	}

	for {
		f, err := protocol.Decode(r, o.maxBody)
		if err != nil {
			return err
		}
		switch f.Type {
		case protocol.LoginAck:
			ack, err := protocol.DecodeLoginAck(f.Body)
			if err != nil {
				return err
			}
			if ack.Status != protocol.LoginStatusOK {
				return fmt.Errorf("status=%d message=%q", ack.Status, ack.Message)
			}
			return nil
		case protocol.Ping:
			if err := cl.send(&protocol.Frame{Type: protocol.Pong, MsgID: f.MsgID, Body: f.Body}, o.maxBody); err != nil {
				return err
			}
		case protocol.Close:
			return errors.New("connection closed during login")
		}
	}
}

// replyKind classifies a gateway reply forwarded to the sender goroutine.
type replyKind int

const (
	replyEcho replyKind = iota
	replySendAck
	replyDeliver
)

// reply is one reply handed to the sender for RTT accounting.
type reply struct {
	kind      replyKind
	msgID     uint64 // frame header: echo/DELIVER message ID or SEND idempotency key
	messageID uint64 // SEND_ACK body message ID (platform message ID)
	status    protocol.SendAckStatus
}

// readLoop decodes frames, answers PING with PONG, acknowledges DELIVER frames
// in reliable mode and forwards matched replies to the sender.
func readLoop(cl *client, r *bufio.Reader, st *stats, echoCh chan<- reply, o options) {
	for {
		if err := cl.conn.SetReadDeadline(time.Now().Add(o.timeout)); err != nil {
			return
		}

		f, err := protocol.Decode(r, o.maxBody)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
				// Peer closed: normal during shutdown.
			default:
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					st.timeouts.Add(1)
				} else {
					st.decodeErrs.Add(1)
					st.recordErr(err)
				}
			}
			return
		}

		switch f.Type {
		case protocol.Ping:
			if err := cl.send(&protocol.Frame{Type: protocol.Pong, MsgID: f.MsgID, Body: f.Body}, o.maxBody); err != nil {
				if !errors.Is(err, net.ErrClosed) {
					st.writeErrs.Add(1)
				}
				return
			}
		case protocol.Data:
			if o.route || o.reliable {
				st.unexpected.Add(1)
				continue
			}
			pushReply(echoCh, st, reply{kind: replyEcho, msgID: f.MsgID})
		case protocol.Deliver:
			if !o.route && !o.reliable {
				st.unexpected.Add(1)
				continue
			}
			if _, err := protocol.DecodeDeliver(f.Body); err != nil {
				st.decodeErrs.Add(1)
				st.recordErr(fmt.Errorf("decode deliver: %w", err))
				continue
			}
			if o.reliable {
				st.delivered.Add(1)
				if err := ackDeliver(cl, st, o, f.MsgID); err != nil {
					if !errors.Is(err, net.ErrClosed) {
						st.writeErrs.Add(1)
					}
					st.recordErr(fmt.Errorf("ack deliver: %w", err))
					return
				}
			}
			pushReply(echoCh, st, reply{kind: replyDeliver, msgID: f.MsgID})
		case protocol.SendAck:
			if !o.reliable {
				st.unexpected.Add(1)
				continue
			}
			ack, err := protocol.DecodeSendAck(f.Body)
			if err != nil {
				st.decodeErrs.Add(1)
				st.recordErr(fmt.Errorf("decode send_ack: %w", err))
				continue
			}
			st.sendAcks.Add(1)
			if ack.Status == protocol.SendAckDuplicate {
				st.duplicateAcks.Add(1)
			}
			pushReply(echoCh, st, reply{
				kind:      replySendAck,
				msgID:     f.MsgID,
				messageID: ack.MessageID,
				status:    ack.Status,
			})
		case protocol.Error:
			if !o.route && !o.reliable {
				st.unexpected.Add(1)
				continue
			}
			st.routeErrors.Add(1)
			st.recordErr(fmt.Errorf("route error: %s", f.Body))
		case protocol.Close:
			return
		default:
			st.unexpected.Add(1)
		}
	}
}

// pushReply forwards a reply without blocking the read loop. Replies dropped on
// a full channel are counted as unexpected frames.
func pushReply(echoCh chan<- reply, st *stats, r reply) {
	select {
	case echoCh <- r:
	default:
		st.unexpected.Add(1)
	}
}

// ackDeliver sends the acknowledgements selected by -ack for one DELIVER.
func ackDeliver(cl *client, st *stats, o options, messageID uint64) error {
	if o.ackReceipt() {
		body, err := protocol.AppendReceiptAckBody(nil, &protocol.ReceiptAckBody{MessageID: messageID})
		if err != nil {
			return err
		}
		if err := cl.send(&protocol.Frame{Type: protocol.ReceiptAck, MsgID: messageID, Body: body}, o.maxBody); err != nil {
			return err
		}
		st.receiptAcked.Add(1)
	}
	if o.ackBusiness() {
		body, err := protocol.AppendBusinessAckBody(nil, &protocol.BusinessAckBody{MessageID: messageID})
		if err != nil {
			return err
		}
		if err := cl.send(&protocol.Frame{Type: protocol.BusinessAck, MsgID: messageID, Body: body}, o.maxBody); err != nil {
			return err
		}
		st.businessAcked.Add(1)
	}
	return nil
}

// sendLoop sends one frame per interval, matches replies by MsgID and records
// the round-trip time.
func sendLoop(cl *client, st *stats, echoCh <-chan reply, o options, id int, stop <-chan struct{}) {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	payload := strings.Repeat("x", o.payloadSize)
	target := fmt.Sprintf("load-%d", id)
	if o.routeTo != "" {
		target = o.routeTo
	}
	pending := make(map[uint64]time.Time)
	rtts := make([]time.Duration, 0, 8)
	var seq uint64

	record := func(r reply) {
		sentAt, ok := pending[r.msgID]
		if !ok {
			// Late or unmatched reply.
			st.unexpected.Add(1)
			return
		}
		delete(pending, r.msgID)
		rtts = append(rtts, time.Since(sentAt))
		if o.route {
			st.delivered.Add(1)
		} else {
			st.echoed.Add(1)
		}
	}

send:
	for {
		select {
		case <-stop:
			break send
		case <-ticker.C:
			now := time.Now()
			for msgID, sentAt := range pending {
				if now.Sub(sentAt) > o.timeout {
					delete(pending, msgID)
					st.timeouts.Add(1)
				}
			}
			seq++
			pending[seq] = time.Now()
			payloadBody := fmt.Sprintf("load#%d#%d %s", id, seq, payload)
			frame := &protocol.Frame{Type: protocol.Data, MsgID: seq, Body: []byte(payloadBody)}
			if o.route {
				body, err := protocol.AppendSendBody(nil, &protocol.SendBody{
					TargetType: protocol.TargetUser,
					Target:     target,
					Payload:    []byte(payloadBody),
				})
				if err != nil {
					delete(pending, seq)
					st.writeErrs.Add(1)
					st.recordErr(fmt.Errorf("conn %d: encode send: %w", id, err))
					st.flushRTTs(rtts)
					return
				}
				frame = &protocol.Frame{Type: protocol.Send, MsgID: seq, Body: body}
			}
			if err := cl.send(frame, o.maxBody); err != nil {
				delete(pending, seq)
				st.writeErrs.Add(1)
				st.recordErr(fmt.Errorf("conn %d: send: %w", id, err))
				st.flushRTTs(rtts)
				return
			}
			st.sent.Add(1)
		case r := <-echoCh:
			record(r)
		}
	}

	// Drain in-flight echoes; anything still pending after the timeout is lost.
	drain := time.NewTimer(o.timeout)
	defer drain.Stop()
	for len(pending) > 0 {
		select {
		case r := <-echoCh:
			record(r)
		case <-drain.C:
			for msgID := range pending {
				delete(pending, msgID)
				st.timeouts.Add(1)
			}
			st.flushRTTs(rtts)
			return
		}
	}
	st.flushRTTs(rtts)
}

// sendReliable sends SEND frames with a per-connection monotonically increasing
// idempotency key and matches SEND_ACK and DELIVER replies by ID.
func sendReliable(cl *client, st *stats, echoCh <-chan reply, o options, id int, stop <-chan struct{}) {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	payload := strings.Repeat("x", o.payloadSize)
	target := fmt.Sprintf("load-%d", id)
	if o.routeTo != "" {
		target = o.routeTo
	}

	// pending keyed by the SEND frame MsgID until SEND_ACK arrives; deliveries
	// keyed by the platform message ID from SEND_ACK until DELIVER arrives.
	pending := make(map[uint64]time.Time)
	deliveries := make(map[uint64]time.Time)
	acceptRTTs := make([]time.Duration, 0, 16)
	deliverRTTs := make([]time.Duration, 0, 16)
	var seq uint64

	handle := func(r reply) {
		switch r.kind {
		case replySendAck:
			sentAt, ok := pending[r.msgID]
			if !ok {
				st.unexpected.Add(1)
				return
			}
			delete(pending, r.msgID)
			acceptRTTs = append(acceptRTTs, time.Since(sentAt))
			if r.status == protocol.SendAckAccepted && r.messageID != 0 {
				deliveries[r.messageID] = sentAt
			}
		case replyDeliver:
			sentAt, ok := deliveries[r.msgID]
			if !ok {
				st.unexpected.Add(1)
				return
			}
			delete(deliveries, r.msgID)
			deliverRTTs = append(deliverRTTs, time.Since(sentAt))
		default:
			st.unexpected.Add(1)
		}
	}

	// expire removes entries that exceeded the per-message timeout.
	expire := func(m map[uint64]time.Time, now time.Time) {
		for msgID, sentAt := range m {
			if now.Sub(sentAt) > o.timeout {
				delete(m, msgID)
				st.timeouts.Add(1)
			}
		}
	}

send:
	for {
		select {
		case <-stop:
			break send
		case <-ticker.C:
			now := time.Now()
			expire(pending, now)
			expire(deliveries, now)
			// MsgID starts at 1 and never wraps: uint64 is far beyond any run.
			seq++
			payloadBody := fmt.Sprintf("load#%d#%d %s", id, seq, payload)
			body, err := protocol.AppendSendBody(nil, &protocol.SendBody{
				TargetType: protocol.TargetUser,
				Target:     target,
				Payload:    []byte(payloadBody),
			})
			if err != nil {
				st.writeErrs.Add(1)
				st.recordErr(fmt.Errorf("conn %d: encode send: %w", id, err))
				st.flushRTTs(acceptRTTs)
				st.flushDeliverRTTs(deliverRTTs)
				return
			}
			pending[seq] = time.Now()
			if err := cl.send(&protocol.Frame{Type: protocol.Send, MsgID: seq, Body: body}, o.maxBody); err != nil {
				delete(pending, seq)
				st.writeErrs.Add(1)
				st.recordErr(fmt.Errorf("conn %d: send: %w", id, err))
				st.flushRTTs(acceptRTTs)
				st.flushDeliverRTTs(deliverRTTs)
				return
			}
			st.sent.Add(1)
		case r := <-echoCh:
			handle(r)
		}
	}

	// Drain in-flight acknowledgements; anything still pending after the timeout
	// is counted as lost.
	drain := time.NewTimer(o.timeout)
	defer drain.Stop()
	for len(pending) > 0 || len(deliveries) > 0 {
		select {
		case r := <-echoCh:
			handle(r)
		case <-drain.C:
			st.timeouts.Add(int64(len(pending) + len(deliveries)))
			st.flushRTTs(acceptRTTs)
			st.flushDeliverRTTs(deliverRTTs)
			return
		}
	}
	st.flushRTTs(acceptRTTs)
	st.flushDeliverRTTs(deliverRTTs)
}

func formatReport(o options, st *stats, elapsed time.Duration, preShutdownGoroutines int) string {
	ok := st.connsOK.Load()
	failed := st.connsFailed.Load()
	total := ok + failed
	successRate := 0.0
	if total > 0 {
		successRate = float64(ok) / float64(total) * 100
	}

	sent := st.sent.Load()
	echoed := st.echoed.Load()
	delivered := st.delivered.Load()
	missed := sent - st.responded()
	if missed < 0 {
		missed = 0
	}

	rtts := st.rttSnapshot()
	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })

	var b strings.Builder
	fmt.Fprintf(&b, "RelayX Phase 2 load test\n")
	fmt.Fprintf(&b, "finished_at : %s\n", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	fmt.Fprintf(&b, "elapsed     : %s\n", elapsed.Round(time.Millisecond))
	fmt.Fprintf(&b, "addr        : %s\n", o.addr)
	fmt.Fprintf(&b, "mode        : %s\n", o.mode())
	fmt.Fprintf(&b, "target      : %s\n", o.target())
	fmt.Fprintf(&b, "conns       : %d (ramp %s)\n", o.conns, o.ramp)
	fmt.Fprintf(&b, "duration    : %s\n", o.duration)
	fmt.Fprintf(&b, "interval    : %s\n", o.interval)
	fmt.Fprintf(&b, "payload_size: %d bytes\n", o.payloadSize)
	fmt.Fprintf(&b, "max_body    : %d bytes\n", o.maxBody)
	fmt.Fprintf(&b, "timeout     : %s\n\n", o.timeout)

	fmt.Fprintf(&b, "[connections]\n")
	fmt.Fprintf(&b, "succeeded    : %d\n", ok)
	fmt.Fprintf(&b, "failed       : %d\n", failed)
	fmt.Fprintf(&b, "success_rate : %.2f%%\n\n", successRate)

	fmt.Fprintf(&b, "[messages]\n")
	fmt.Fprintf(&b, "sent   : %d\n", sent)
	if o.reliable {
		fmt.Fprintf(&b, "send_ack      : %d\n", st.sendAcks.Load())
		fmt.Fprintf(&b, "duplicate_ack : %d\n", st.duplicateAcks.Load())
		fmt.Fprintf(&b, "delivered     : %d\n", delivered)
		fmt.Fprintf(&b, "receipt_acked : %d\n", st.receiptAcked.Load())
		fmt.Fprintf(&b, "business_acked: %d\n", st.businessAcked.Load())
		fmt.Fprintf(&b, "ack_missed    : %d\n\n", st.ackMissed())
	} else {
		if o.route {
			fmt.Fprintf(&b, "delivered : %d\n", delivered)
		} else {
			fmt.Fprintf(&b, "echoed : %d\n", echoed)
		}
		fmt.Fprintf(&b, "missed : %d\n\n", missed)
	}

	fmt.Fprintf(&b, "[errors]\n")
	fmt.Fprintf(&b, "connect_failures  : %d\n", failed)
	fmt.Fprintf(&b, "login_failures    : %d\n", st.loginFailures.Load())
	fmt.Fprintf(&b, "route_errors      : %d\n", st.routeErrors.Load())
	fmt.Fprintf(&b, "timeouts          : %d\n", st.timeouts.Load())
	fmt.Fprintf(&b, "decode_errors     : %d\n", st.decodeErrs.Load())
	fmt.Fprintf(&b, "unexpected_frames : %d\n", st.unexpected.Load())
	fmt.Fprintf(&b, "write_errors      : %d\n", st.writeErrs.Load())
	if msg := st.firstErrString(); msg != "" {
		fmt.Fprintf(&b, "first_error       : %s\n", msg)
	}
	fmt.Fprintf(&b, "\n")

	if o.reliable {
		deliverRTTs := st.deliverRTTSnapshot()
		sort.Slice(deliverRTTs, func(i, j int) bool { return deliverRTTs[i] < deliverRTTs[j] })
		writeRTTStats(&b, "accept_rtt", rtts)
		writeRTTStats(&b, "deliver_rtt", deliverRTTs)
	} else {
		writeRTTStats(&b, "rtt", rtts)
	}

	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	fmt.Fprintf(&b, "[client process]\n")
	fmt.Fprintf(&b, "goroutines_pre_shutdown : %d\n", preShutdownGoroutines)
	fmt.Fprintf(&b, "goroutines              : %d\n", runtime.NumGoroutine())
	fmt.Fprintf(&b, "heap_alloc_mb           : %.2f\n", float64(m.HeapAlloc)/(1<<20))
	fmt.Fprintf(&b, "heap_sys_mb             : %.2f\n", float64(m.HeapSys)/(1<<20))

	return b.String()
}

// writeRTTStats appends one latency section. sorted must be ascending.
func writeRTTStats(b *strings.Builder, name string, sorted []time.Duration) {
	fmt.Fprintf(b, "[%s ms]\n", name)
	if len(sorted) == 0 {
		fmt.Fprintf(b, "samples: 0\n\n")
		return
	}
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	fmt.Fprintf(b, "samples : %d\n", len(sorted))
	fmt.Fprintf(b, "p50     : %.3f\n", toMillis(percentile(sorted, 50)))
	fmt.Fprintf(b, "p90     : %.3f\n", toMillis(percentile(sorted, 90)))
	fmt.Fprintf(b, "p99     : %.3f\n", toMillis(percentile(sorted, 99)))
	fmt.Fprintf(b, "max     : %.3f\n", toMillis(sorted[len(sorted)-1]))
	fmt.Fprintf(b, "avg     : %.3f\n\n", toMillis(sum/time.Duration(len(sorted))))
}

// percentile returns the nearest-rank percentile of an ascending slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func toMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
