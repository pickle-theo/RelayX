// Command stdlib is a minimal RelayX-protocol echo server built on the
// standard library: net.Listen plus two goroutines per connection (a reader
// and a single writer), matching the main project's "one socket, one writer"
// transport model.
package main

import (
	"bufio"
	"errors"
	"flag"
	"io"
	"log"
	"net"

	"relayx/phase3/experiments/netlib-compare/frame"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19000", "listen address")
	flag.Duration("heartbeat", 0, "ignored; kept for CLI parity with the main project")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("stdlib echo: listen %s: %v", *addr, err)
	}
	log.Printf("stdlib echo listening on %s", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			log.Printf("stdlib echo: accept: %v", err)
			return
		}
		go serve(conn)
	}
}

// serve owns one connection: the reader decodes frames and hands encoded echo
// frames to a single writer goroutine so writes never interleave.
func serve(conn net.Conn) {
	defer conn.Close()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	out := make(chan []byte, 64)
	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)
		w := bufio.NewWriterSize(conn, 32<<10)
		for buf := range out {
			if _, err := w.Write(buf); err != nil {
				return
			}
			if err := w.Flush(); err != nil {
				return
			}
		}
	}()

	r := bufio.NewReaderSize(conn, 32<<10)
read:
	for {
		typ, msgID, body, err := frame.ReadFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("stdlib echo: %s: %v", conn.RemoteAddr(), err)
			}
			break
		}
		if typ != frame.TypeData {
			continue
		}
		select {
		case out <- frame.Append(nil, frame.TypeData, msgID, body):
		case <-writerDone:
			break read
		}
	}

	close(out)
	<-writerDone
}
