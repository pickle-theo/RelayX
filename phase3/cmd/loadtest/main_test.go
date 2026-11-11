package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseFlagsScenario(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		want      string
		wantLogin bool
		wantMode  string
	}{
		{name: "empty keeps legacy behaviour", args: nil, want: "", wantLogin: false, wantMode: "echo"},
		{name: "idle", args: []string{"-scenario", "idle"}, want: "idle", wantLogin: true, wantMode: "login"},
		{name: "heartbeat", args: []string{"-scenario", "heartbeat"}, want: "heartbeat", wantLogin: true, wantMode: "login"},
		{name: "steady-low", args: []string{"-scenario", "steady-low"}, want: "steady-low", wantLogin: false, wantMode: "echo"},
		{name: "steady-high", args: []string{"-scenario", "steady-high"}, want: "steady-high", wantLogin: false, wantMode: "echo"},
		{name: "slow-consumer", args: []string{"-scenario", "slow-consumer"}, want: "slow-consumer", wantLogin: true, wantMode: "login"},
		{name: "large-payload", args: []string{"-scenario", "large-payload", "-payload-size", "65536"}, want: "large-payload", wantLogin: false, wantMode: "echo"},
		{name: "reconnect-storm", args: []string{"-scenario", "reconnect-storm"}, want: "reconnect-storm", wantLogin: true, wantMode: "login"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFlags(tt.args)
			if err != nil {
				t.Fatalf("parseFlags(%v) error = %v", tt.args, err)
			}
			if got.scenario != tt.want {
				t.Fatalf("scenario = %q, want %q", got.scenario, tt.want)
			}
			if got.login != tt.wantLogin {
				t.Fatalf("login = %v, want %v", got.login, tt.wantLogin)
			}
			if got.mode() != tt.wantMode {
				t.Fatalf("mode() = %q, want %q", got.mode(), tt.wantMode)
			}
		})
	}
}

func TestParseFlagsScenarioInvalid(t *testing.T) {
	for _, scenario := range []string{"idle2", "steady", "reconnect", "IDLE"} {
		t.Run(scenario, func(t *testing.T) {
			_, err := parseFlags([]string{"-scenario", scenario})
			if err == nil {
				t.Fatalf("parseFlags(-scenario %q) error = nil, want failure", scenario)
			}
			for _, name := range scenarioNames {
				if !strings.Contains(err.Error(), name) {
					t.Fatalf("error %q does not list legal scenario %q", err, name)
				}
			}
		})
	}
}

func TestParseFlagsSlowRatioBounds(t *testing.T) {
	tests := []struct {
		ratio   string
		wantErr bool
	}{
		{ratio: "-0.1", wantErr: true},
		{ratio: "-1", wantErr: true},
		{ratio: "0", wantErr: false},
		{ratio: "0.1", wantErr: false},
		{ratio: "0.999", wantErr: false},
		{ratio: "1", wantErr: true},
		{ratio: "1.5", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.ratio, func(t *testing.T) {
			_, err := parseFlags([]string{"-scenario", "slow-consumer", "-slow-ratio", tt.ratio})
			if tt.wantErr && err == nil {
				t.Fatalf("slow-ratio %s: error = nil, want failure", tt.ratio)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("slow-ratio %s: error = %v, want success", tt.ratio, err)
			}
		})
	}
}

func TestParseFlagsReconnectDurations(t *testing.T) {
	if _, err := parseFlags([]string{"-scenario", "reconnect-storm", "-reconnect-after", "-1s"}); err == nil {
		t.Fatal("negative reconnect-after accepted")
	}
	if _, err := parseFlags([]string{"-scenario", "reconnect-storm", "-reconnect-window", "0s"}); err == nil {
		t.Fatal("zero reconnect-window accepted")
	}
	got, err := parseFlags([]string{"-scenario", "reconnect-storm", "-reconnect-after", "5s", "-reconnect-window", "2s"})
	if err != nil {
		t.Fatalf("parseFlags() error = %v", err)
	}
	if got.reconnectAfter != 5*time.Second || got.reconnectWindow != 2*time.Second {
		t.Fatalf("reconnect timings = %s/%s, want 5s/2s", got.reconnectAfter, got.reconnectWindow)
	}
}

func TestIsSlow(t *testing.T) {
	o := options{conns: 100, slowRatio: 0.1}
	slow := 0
	for id := 0; id < o.conns; id++ {
		if o.isSlow(id) {
			slow++
		}
	}
	if slow != 10 {
		t.Fatalf("slow connections = %d, want 10", slow)
	}
	if (options{conns: 100, slowRatio: 0}).isSlow(0) {
		t.Fatal("ratio 0 selected a slow connection")
	}
}

func TestReadIdleTimeoutCoversInterval(t *testing.T) {
	base := options{timeout: 5 * time.Second, interval: time.Second}
	if got := base.readIdleTimeout(); got != 6*time.Second {
		t.Fatalf("readIdleTimeout = %s, want 6s", got)
	}
	slow := options{timeout: 5 * time.Second, interval: 10 * time.Second}
	if got := slow.readIdleTimeout(); got != 21*time.Second {
		t.Fatalf("readIdleTimeout = %s, want 21s", got)
	}
}

func TestFormatReportNoRTTSamples(t *testing.T) {
	o := options{
		addr:            "127.0.0.1:9000",
		conns:           100,
		duration:        time.Second,
		interval:        time.Second,
		payloadSize:     64,
		maxBody:         1 << 20,
		timeout:         time.Second,
		scenario:        scenarioIdle,
		slowRatio:       0.1,
		reconnectWindow: 5 * time.Second,
	}

	report := formatReport(o, &stats{}, time.Second, 100)

	if strings.Contains(report, "NaN") {
		t.Fatalf("report contains NaN:\n%s", report)
	}
	for _, want := range []string{
		"[scenario]",
		"name                : idle",
		"payload_size        : 64 bytes",
		"reconnect_in_window : n/a",
		"[rtt ms]",
		"samples: 0",
		"p50     : 0.000",
		"p99     : 0.000",
		"rss_mb",
		"rss_per_conn_kb",
		"goroutines",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q:\n%s", want, report)
		}
	}
}

func TestReconnectWindowResult(t *testing.T) {
	o := options{scenario: scenarioReconnectStorm}

	var st stats
	if got := reconnectWindowResult(o, &st); got != "n/a" {
		t.Fatalf("no attempts: got %q, want n/a", got)
	}

	st.reconnectAttempts.Add(2)
	st.reconnectSucceeded.Add(2)
	st.reconnectDone.Add(2)
	if got := reconnectWindowResult(o, &st); got != "yes" {
		t.Fatalf("all in window: got %q, want yes", got)
	}

	st.reconnectLate.Add(1)
	if got := reconnectWindowResult(o, &st); got != "no" {
		t.Fatalf("late reconnect: got %q, want no", got)
	}

	if got := reconnectWindowResult(options{}, &st); got != "n/a" {
		t.Fatalf("non-storm scenario: got %q, want n/a", got)
	}
}
