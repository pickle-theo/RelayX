// Command gnet is the same RelayX-protocol echo server implemented on top of
// github.com/panjf2000/gnet/v2's event-loop model: frames are parsed and echoed
// inside OnTraffic, without a goroutine per connection.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/panjf2000/gnet/v2"

	"relayx/phase4/experiments/netlib-compare/frame"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19001", "listen address")
	flag.Parse()

	if err := gnet.Run(&echoServer{addr: *addr}, "tcp://"+*addr,
		gnet.WithMulticore(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
	); err != nil {
		log.Fatalf("gnet echo: %v", err)
	}
}

type echoServer struct {
	gnet.BuiltinEventEngine
	addr string
}

func (s *echoServer) OnBoot(eng gnet.Engine) gnet.Action {
	log.Printf("gnet echo listening on %s", s.addr)
	return gnet.None
}

// OnTraffic parses every complete frame currently buffered on the connection
// and queues a DATA echo for each one. Partial frames stay in the inbound
// buffer until more bytes arrive.
func (s *echoServer) OnTraffic(c gnet.Conn) gnet.Action {
	for {
		buffered := c.InboundBuffered()
		if buffered < frame.HeaderSize {
			return gnet.None
		}
		head, err := c.Peek(frame.HeaderSize)
		if err != nil {
			return gnet.Close
		}
		h, err := frame.ParseHeader(head)
		if err != nil {
			log.Printf("gnet echo: %v", err)
			return gnet.Close
		}
		total := frame.HeaderSize + h.BodyLen
		if buffered < total {
			return gnet.None
		}
		raw, err := c.Peek(total)
		if err != nil {
			return gnet.Close
		}
		typ, msgID, body, err := frame.ParseFrame(raw)
		if err != nil {
			log.Printf("gnet echo: %v", err)
			return gnet.Close
		}
		// Encode before Discard: body aliases the inbound buffer.
		var echo []byte
		if typ == frame.TypeData {
			echo = frame.Append(nil, frame.TypeData, msgID, body)
		}
		c.Discard(total)
		if echo != nil {
			if _, err := c.Write(echo); err != nil {
				return gnet.Close
			}
		}
	}
}

// OnTick is disabled: the echo server has no periodic work.
func (s *echoServer) OnTick() (time.Duration, gnet.Action) {
	return -1, gnet.None
}
