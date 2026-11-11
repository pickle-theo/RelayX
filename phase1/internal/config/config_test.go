package config

import "testing"

func TestParseServerReadBufferSize(t *testing.T) {
	cfg, err := ParseServer([]string{"-read-buffer-size", "8192"})
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	if cfg.ReadBufferSize != 8192 {
		t.Fatalf("ReadBufferSize = %d, want 8192", cfg.ReadBufferSize)
	}
}

func TestParseServerReadBufferSizeDefault(t *testing.T) {
	cfg, err := ParseServer(nil)
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	if cfg.ReadBufferSize != 4096 {
		t.Fatalf("ReadBufferSize = %d, want 4096", cfg.ReadBufferSize)
	}
}

func TestServerConfigReadBufferSizeValidation(t *testing.T) {
	valid, err := ParseServer(nil)
	if err != nil {
		t.Fatalf("ParseServer() error = %v", err)
	}
	for _, v := range []int{0, -1} {
		cfg := valid
		cfg.ReadBufferSize = v
		if err := cfg.Validate(); err == nil {
			t.Fatalf("Validate() with ReadBufferSize = %d succeeded, want error", v)
		}
	}
}
