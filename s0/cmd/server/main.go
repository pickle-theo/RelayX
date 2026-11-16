// Command server 启动 S0 TCP Server。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"relayx/s0/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9700", "监听地址")
	enableEcho := flag.Bool("enable-echo", false, "是否注册 ECHO(0x0C) 测试帧 handler")
	flag.Parse()

	cfg := server.DefaultConfig()
	cfg.Addr = *addr
	cfg.EnableEcho = *enableEcho

	srv := server.New(cfg, log.New(os.Stderr, "[s0] ", log.LstdFlags|log.Lshortfile))

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received %s, shutting down gracefully", sig)
	case err := <-errCh:
		if err != nil {
			log.Fatalf("server exited: %v", err)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("shutdown: %v", err)
	}
	log.Printf("server stopped")
}
