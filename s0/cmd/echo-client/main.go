// Command echo-client 是 S0 的验证客户端，支持原始帧收发与异常帧构造。
//
// 用法：
//
//	echo-client -addr 127.0.0.1:9700 -mode roundtrip -count 3
//	echo-client -mode fragmented     # 半包：把一帧拆成多次 TCP 写
//	echo-client -mode coalesced      # 粘包：多帧合并为一次 TCP 写
//	echo-client -mode truncated      # 截断帧：Length 声明大于实际 Body
//	echo-client -mode oversized      # 超大帧：Length 超上限
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"relayx/s0/internal/protocol"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9700", "服务端地址")
	mode := flag.String("mode", "roundtrip", "roundtrip|fragmented|coalesced|truncated|oversized")
	count := flag.Int("count", 1, "发送帧数")
	payload := flag.String("payload", "hello", "ECHO 负载")
	timeout := flag.Duration("timeout", 5*time.Second, "读超时")
	flag.Parse()

	nc, err := net.DialTimeout("tcp", *addr, *timeout)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer nc.Close()

	switch *mode {
	case "roundtrip":
		runRoundtrip(nc, *count, []byte(*payload), *timeout)
	case "fragmented":
		runFragmented(nc, []byte(*payload), *timeout)
	case "coalesced":
		runCoalesced(nc, *count, []byte(*payload), *timeout)
	case "truncated":
		runTruncated(nc, *timeout)
	case "oversized":
		runOversized(nc)
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

func mustEncode(t protocol.Frame) []byte {
	b, err := protocol.Encode(t, nil)
	if err != nil {
		log.Fatalf("encode: %v", err)
	}
	return b
}

// runRoundtrip：逐帧发送 PING + ECHO，校验响应。
func runRoundtrip(nc net.Conn, count int, body []byte, timeout time.Duration) {
	var tracker protocol.SequenceTracker
	_ = nc.SetDeadline(time.Now().Add(timeout))

	for i := 0; i < count; i++ {
		seq, _ := tracker.Next()
		ping := mustEncode(protocol.Frame{Type: protocol.TypePing, Sequence: seq})
		if _, err := nc.Write(ping); err != nil {
			log.Fatalf("write ping: %v", err)
		}
		resp := readOne(nc)
		if resp.Type != protocol.TypePong {
			log.Fatalf("expect PONG, got 0x%02X", resp.Type)
		}
		fmt.Printf("PING seq=%d -> PONG seq=%d\n", seq, resp.Sequence)

		seq, _ = tracker.Next()
		echo := mustEncode(protocol.Frame{Type: protocol.TypeEcho, Sequence: seq, Body: body})
		if _, err := nc.Write(echo); err != nil {
			log.Fatalf("write echo: %v", err)
		}
		resp = readOne(nc)
		if resp.Type != protocol.TypeEcho || string(resp.Body) != string(body) {
			log.Fatalf("echo mismatch: type=0x%02X body=%q", resp.Type, resp.Body)
		}
		fmt.Printf("ECHO seq=%d -> %q\n", seq, resp.Body)
	}
}

// runFragmented：把一帧拆成 Header / 部分 Body / 剩余 Body 三次写，验证半包重组。
func runFragmented(nc net.Conn, body []byte, timeout time.Duration) {
	_ = nc.SetDeadline(time.Now().Add(timeout))
	frame := mustEncode(protocol.Frame{Type: protocol.TypeEcho, Sequence: 1, Body: body})
	cut := protocol.HeaderSize + len(body)/2
	writeParts(nc, frame[:protocol.HeaderSize], frame[protocol.HeaderSize:cut], frame[cut:])
	fmt.Printf("fragmented echo (%d parts) -> %q\n", 3, readOne(nc).Body)
}

// runCoalesced：多帧合并为一次 TCP 写，验证粘包拆分。
func runCoalesced(nc net.Conn, count int, body []byte, timeout time.Duration) {
	_ = nc.SetDeadline(time.Now().Add(timeout))
	var all []byte
	var tracker protocol.SequenceTracker
	for i := 0; i < count; i++ {
		seq, _ := tracker.Next()
		all = append(all, mustEncode(protocol.Frame{Type: protocol.TypeEcho, Sequence: seq, Body: body})...)
	}
	if _, err := nc.Write(all); err != nil {
		log.Fatalf("write coalesced: %v", err)
	}
	buf := make([]byte, protocol.MaxFrameSize)
	for i := 0; i < count; i++ {
		resp := readOneInto(nc, buf)
		fmt.Printf("coalesced frame %d: type=0x%02X seq=%d body=%q\n", i+1, resp.Type, resp.Sequence, resp.Body)
	}
}

// runTruncated：声明 Length=100 但只发 10 字节 Body。
// 服务端将未凑齐的帧视为半包、继续等待，不会主动发帧；本端读应在 deadline 超时。
func runTruncated(nc net.Conn, timeout time.Duration) {
	_ = nc.SetDeadline(time.Now().Add(timeout))
	header := make([]byte, protocol.HeaderSize)
	header[0], header[1], header[2], header[3] = protocol.Magic0, protocol.Magic1, protocol.Version, protocol.TypeEcho
	binary.BigEndian.PutUint32(header[4:8], 100) // 声明 100 字节 Body
	binary.BigEndian.PutUint32(header[8:12], 1)
	binary.BigEndian.PutUint16(header[12:14], protocol.CRC16(header[:12]))
	if _, err := nc.Write(header); err != nil {
		log.Fatalf("write truncated header: %v", err)
	}
	if _, err := nc.Write([]byte("0123456789")); err != nil { // 仅 10 字节
		log.Fatalf("write truncated body: %v", err)
	}
	// 半包不应得到响应：读必须以超时/EOF 结束，否则说明服务端异常响应了截断帧。
	buf := make([]byte, 16)
	_, err := nc.Read(buf)
	if err == nil {
		log.Fatalf("truncated frame: unexpected response from server")
	}
	fmt.Printf("truncated frame: no response before deadline as expected (err=%v)\n", err)
}

// runOversized：声明 Length > maxBodySize，服务端应先校验 Length 再拒绝，不分配内存。
func runOversized(nc net.Conn) {
	header := make([]byte, protocol.HeaderSize)
	header[0], header[1], header[2], header[3] = protocol.Magic0, protocol.Magic1, protocol.Version, protocol.TypeEcho
	binary.BigEndian.PutUint32(header[4:8], protocol.MaxBodySize+1)
	binary.BigEndian.PutUint32(header[8:12], 1)
	binary.BigEndian.PutUint16(header[12:14], protocol.CRC16(header[:12]))
	if _, err := nc.Write(header); err != nil {
		log.Fatalf("write oversized header: %v", err)
	}
	resp := readOne(nc)
	if resp.Type != protocol.TypeError {
		log.Fatalf("expect ERROR for oversized frame, got 0x%02X", resp.Type)
	}
	fmt.Printf("oversized frame -> ERROR code=0x%02X%02X\n", resp.Body[0], resp.Body[1])
}

func writeParts(nc net.Conn, parts ...[]byte) {
	for _, p := range parts {
		if _, err := nc.Write(p); err != nil {
			log.Fatalf("write part: %v", err)
		}
		time.Sleep(5 * time.Millisecond) // 确保 TCP 分段
	}
}

func readOne(nc net.Conn) protocol.Frame {
	return readOneInto(nc, make([]byte, protocol.MaxFrameSize))
}

func readOneInto(nc net.Conn, buf []byte) protocol.Frame {
	// 先精确读固定头，再按 Length 读 body：一次调用只读一帧，
	// 避免粘包时把后续帧吞进缓冲导致后续读取超时。
	if _, err := io.ReadFull(nc, buf[:protocol.HeaderSize]); err != nil {
		log.Fatalf("read header: %v", err)
	}
	length := binary.BigEndian.Uint32(buf[4:8])
	if length > protocol.MaxBodySize {
		log.Fatalf("declared body length %d exceeds max", length)
	}
	total := protocol.HeaderSize + int(length)
	frame := buf[:total]
	if _, err := io.ReadFull(nc, frame[protocol.HeaderSize:]); err != nil {
		log.Fatalf("read body: %v", err)
	}
	f, used, derr := protocol.Decode(frame)
	if derr != nil || used != total {
		log.Fatalf("decode response: used=%d err=%v", used, derr)
	}
	return f
}
