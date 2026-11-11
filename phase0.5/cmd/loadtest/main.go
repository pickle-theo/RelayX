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

	"relayx/phase0.5/internal/protocol"
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

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if err := o.validate(); err != nil {
		return options{}, err
	}
	return o, nil
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
	connsOK     atomic.Int64
	connsFailed atomic.Int64
	sent        atomic.Int64
	echoed      atomic.Int64
	timeouts    atomic.Int64
	decodeErrs  atomic.Int64
	unexpected  atomic.Int64
	writeErrs   atomic.Int64

	errMu    sync.Mutex
	firstErr error

	mu   sync.Mutex
	rtts []time.Duration
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
	missed := s.sent.Load() - s.echoed.Load()
	return s.connsFailed.Load() == 0 && missed <= 0
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
	echoCh := make(chan uint64, 64)
	readDone := make(chan struct{})

	go func() {
		defer close(readDone)
		readLoop(cl, st, echoCh, o)
	}()

	sendLoop(cl, st, echoCh, o, id, stop)

	_ = conn.Close()
	select {
	case <-readDone:
	case <-time.After(time.Second):
	}
}

// readLoop decodes frames, answers PING with PONG and forwards DATA MsgIDs to
// the sender for RTT accounting.
func readLoop(cl *client, st *stats, echoCh chan<- uint64, o options) {
	r := bufio.NewReader(cl.conn)
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
			select {
			case echoCh <- f.MsgID:
			default:
				st.unexpected.Add(1)
			}
		case protocol.Close:
			return
		default:
			st.unexpected.Add(1)
		}
	}
}

// sendLoop sends one DATA frame per interval, matches echoes by MsgID and
// records the round-trip time.
func sendLoop(cl *client, st *stats, echoCh <-chan uint64, o options, id int, stop <-chan struct{}) {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	payload := strings.Repeat("x", o.payloadSize)
	pending := make(map[uint64]time.Time)
	rtts := make([]time.Duration, 0, 8)
	var seq uint64

	record := func(msgID uint64) {
		sentAt, ok := pending[msgID]
		if !ok {
			// Late or unmatched echo.
			st.unexpected.Add(1)
			return
		}
		delete(pending, msgID)
		rtts = append(rtts, time.Since(sentAt))
		st.echoed.Add(1)
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
			body := fmt.Sprintf("load#%d#%d %s", id, seq, payload)
			if err := cl.send(&protocol.Frame{Type: protocol.Data, MsgID: seq, Body: []byte(body)}, o.maxBody); err != nil {
				delete(pending, seq)
				st.writeErrs.Add(1)
				st.recordErr(fmt.Errorf("conn %d: send: %w", id, err))
				st.flushRTTs(rtts)
				return
			}
			st.sent.Add(1)
		case msgID := <-echoCh:
			record(msgID)
		}
	}

	// Drain in-flight echoes; anything still pending after the timeout is lost.
	drain := time.NewTimer(o.timeout)
	defer drain.Stop()
	for len(pending) > 0 {
		select {
		case msgID := <-echoCh:
			record(msgID)
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
	missed := sent - echoed
	if missed < 0 {
		missed = 0
	}

	rtts := st.rttSnapshot()
	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })

	var b strings.Builder
	fmt.Fprintf(&b, "RelayX Phase 0 load test\n")
	fmt.Fprintf(&b, "finished_at : %s\n", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	fmt.Fprintf(&b, "elapsed     : %s\n", elapsed.Round(time.Millisecond))
	fmt.Fprintf(&b, "addr        : %s\n", o.addr)
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
	fmt.Fprintf(&b, "echoed : %d\n", echoed)
	fmt.Fprintf(&b, "missed : %d\n\n", missed)

	fmt.Fprintf(&b, "[errors]\n")
	fmt.Fprintf(&b, "connect_failures  : %d\n", failed)
	fmt.Fprintf(&b, "timeouts          : %d\n", st.timeouts.Load())
	fmt.Fprintf(&b, "decode_errors     : %d\n", st.decodeErrs.Load())
	fmt.Fprintf(&b, "unexpected_frames : %d\n", st.unexpected.Load())
	fmt.Fprintf(&b, "write_errors      : %d\n", st.writeErrs.Load())
	if msg := st.firstErrString(); msg != "" {
		fmt.Fprintf(&b, "first_error       : %s\n", msg)
	}
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "[rtt ms]\n")
	if len(rtts) == 0 {
		fmt.Fprintf(&b, "samples: 0\n")
	} else {
		var sum time.Duration
		for _, d := range rtts {
			sum += d
		}
		fmt.Fprintf(&b, "samples : %d\n", len(rtts))
		fmt.Fprintf(&b, "p50     : %.3f\n", toMillis(percentile(rtts, 50)))
		fmt.Fprintf(&b, "p90     : %.3f\n", toMillis(percentile(rtts, 90)))
		fmt.Fprintf(&b, "p99     : %.3f\n", toMillis(percentile(rtts, 99)))
		fmt.Fprintf(&b, "max     : %.3f\n", toMillis(rtts[len(rtts)-1]))
		fmt.Fprintf(&b, "avg     : %.3f\n", toMillis(sum/time.Duration(len(rtts))))
	}
	fmt.Fprintf(&b, "\n")

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
