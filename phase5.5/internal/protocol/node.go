package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Node-to-node body limits.
const (
	// MaxNodeIDLen is the maximum length in bytes of a node ID.
	MaxNodeIDLen = 64
	// MaxNodeAddrLen is the maximum length in bytes of an advertised node
	// address.
	MaxNodeAddrLen = 128
	// MaxNodeMsgLen is the maximum length in bytes of a human readable message
	// carried by a NODE_HELLO_ACK or NODE_DELIVER_ACK body.
	MaxNodeMsgLen = 128
)

// Node statuses carried by node acknowledgements.
const (
	NodeStatusOK       uint8 = 0
	NodeStatusInvalid  uint8 = 1
	NodeStatusNotFound uint8 = 2
	NodeStatusInternal uint8 = 3
)

// ErrBadNodeStatus is returned when a node acknowledgement body carries a
// status outside the defined range.
var ErrBadNodeStatus = errors.New("protocol: invalid node status")

// NodeHelloBody is the NODE_HELLO body: the identity a connecting node
// announces together with the address other nodes should dial back.
type NodeHelloBody struct {
	ProtocolVersion uint8
	NodeID          string
	AdvertiseAddr   string
}

// NodeHelloAckBody is the NODE_HELLO_ACK body.
type NodeHelloAckBody struct {
	Status  uint8
	Message string
}

// NodeDeliverBody is the NODE_DELIVER body: a client DELIVER forwarded to the
// node that owns the destination device.
type NodeDeliverBody struct {
	UserID        string
	DeviceID      string
	FromSessionID uint64
	FromUser      string
	FromDevice    string
	Payload       []byte
}

// NodeDeliverAckBody is the NODE_DELIVER_ACK body.
type NodeDeliverAckBody struct {
	Status  uint8
	Message string
}

// NodeKickBody is the NODE_KICK body: a request to close every session of the
// device unless it is newer than Version.
type NodeKickBody struct {
	UserID   string
	DeviceID string
	Version  uint64
}

// AppendNodeHelloBody appends the wire representation of h to dst. The layout
// is uint8 protocolVersion + uint16 nodeIDLen + nodeID + uint16 addrLen + addr.
//
// The advertise address may be empty: a dial-only node never accepts inbound
// connections and therefore has no address to publish.
//
// On error the returned slice is dst unchanged.
func AppendNodeHelloBody(dst []byte, h *NodeHelloBody) ([]byte, error) {
	if h == nil {
		return dst, errors.New("protocol: nil node hello")
	}
	if err := checkNodeString(h.NodeID, MaxNodeIDLen); err != nil {
		return dst, err
	}
	if err := checkNodeAddr(h.AdvertiseAddr, MaxNodeAddrLen); err != nil {
		return dst, err
	}

	dst = append(dst, h.ProtocolVersion)
	dst = appendSized(dst, h.NodeID)
	return appendSized(dst, h.AdvertiseAddr), nil
}

// DecodeNodeHello decodes a NODE_HELLO body. The returned strings are copies
// and do not alias body.
func DecodeNodeHello(body []byte) (*NodeHelloBody, error) {
	if len(body) < 1 {
		return nil, fmt.Errorf("%w: missing protocol version", ErrShortBody)
	}
	nodeID, off, err := readSized(body, 1, MaxNodeIDLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	addr, _, err := readNodeAddr(body, off, MaxNodeAddrLen)
	if err != nil {
		return nil, err
	}
	return &NodeHelloBody{ProtocolVersion: body[0], NodeID: nodeID, AdvertiseAddr: addr}, nil
}

// AppendNodeHelloAckBody appends the wire representation of a to dst. The
// layout is uint8 status + uint16 msgLen + msg. Message may be empty and is
// limited to MaxNodeMsgLen bytes.
//
// On error the returned slice is dst unchanged.
func AppendNodeHelloAckBody(dst []byte, a *NodeHelloAckBody) ([]byte, error) {
	if a == nil {
		return dst, errors.New("protocol: nil node hello ack")
	}
	return appendNodeAckBody(dst, a.Status, a.Message)
}

// DecodeNodeHelloAck decodes a NODE_HELLO_ACK body.
func DecodeNodeHelloAck(body []byte) (*NodeHelloAckBody, error) {
	status, message, err := decodeNodeAckBody(body)
	if err != nil {
		return nil, err
	}
	return &NodeHelloAckBody{Status: status, Message: message}, nil
}

// AppendNodeDeliverBody appends the wire representation of d to dst. The
// layout is uint16 userLen + user + uint16 deviceLen + device + uint64
// fromSessionID + uint16 fromUserLen + fromUser + uint16 fromDeviceLen +
// fromDevice + payload.
//
// On error the returned slice is dst unchanged.
func AppendNodeDeliverBody(dst []byte, d *NodeDeliverBody) ([]byte, error) {
	if d == nil {
		return dst, errors.New("protocol: nil node deliver")
	}
	if err := checkIdentity(d.UserID); err != nil {
		return dst, err
	}
	if err := checkIdentity(d.DeviceID); err != nil {
		return dst, err
	}
	if err := checkIdentity(d.FromUser); err != nil {
		return dst, err
	}
	if err := checkIdentity(d.FromDevice); err != nil {
		return dst, err
	}

	dst = appendSized(dst, d.UserID)
	dst = appendSized(dst, d.DeviceID)
	dst = binary.BigEndian.AppendUint64(dst, d.FromSessionID)
	dst = appendSized(dst, d.FromUser)
	dst = appendSized(dst, d.FromDevice)
	return append(dst, d.Payload...), nil
}

// DecodeNodeDeliver decodes a NODE_DELIVER body. Payload aliases body: it
// shares the same underlying array and is valid only as long as body is not
// modified, mirroring DecodeDeliver.
func DecodeNodeDeliver(body []byte) (*NodeDeliverBody, error) {
	userID, off, err := readSized(body, 0, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	deviceID, off, err := readSized(body, off, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	if len(body)-off < 8 {
		return nil, fmt.Errorf("%w: missing from session id", ErrShortBody)
	}
	fromSessionID := binary.BigEndian.Uint64(body[off:])
	off += 8
	fromUser, off, err := readSized(body, off, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	fromDevice, off, err := readSized(body, off, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	return &NodeDeliverBody{
		UserID:        userID,
		DeviceID:      deviceID,
		FromSessionID: fromSessionID,
		FromUser:      fromUser,
		FromDevice:    fromDevice,
		Payload:       body[off:],
	}, nil
}

// AppendNodeDeliverAckBody appends the wire representation of a to dst. The
// layout is uint8 status + uint16 msgLen + msg.
//
// On error the returned slice is dst unchanged.
func AppendNodeDeliverAckBody(dst []byte, a *NodeDeliverAckBody) ([]byte, error) {
	if a == nil {
		return dst, errors.New("protocol: nil node deliver ack")
	}
	return appendNodeAckBody(dst, a.Status, a.Message)
}

// DecodeNodeDeliverAck decodes a NODE_DELIVER_ACK body.
func DecodeNodeDeliverAck(body []byte) (*NodeDeliverAckBody, error) {
	status, message, err := decodeNodeAckBody(body)
	if err != nil {
		return nil, err
	}
	return &NodeDeliverAckBody{Status: status, Message: message}, nil
}

// AppendNodeKickBody appends the wire representation of k to dst. The layout
// is uint16 userLen + user + uint16 deviceLen + device + uint64 version.
//
// On error the returned slice is dst unchanged.
func AppendNodeKickBody(dst []byte, k *NodeKickBody) ([]byte, error) {
	if k == nil {
		return dst, errors.New("protocol: nil node kick")
	}
	if err := checkIdentity(k.UserID); err != nil {
		return dst, err
	}
	if err := checkIdentity(k.DeviceID); err != nil {
		return dst, err
	}

	dst = appendSized(dst, k.UserID)
	dst = appendSized(dst, k.DeviceID)
	return binary.BigEndian.AppendUint64(dst, k.Version), nil
}

// DecodeNodeKick decodes a NODE_KICK body. The returned strings are copies and
// do not alias body.
func DecodeNodeKick(body []byte) (*NodeKickBody, error) {
	userID, off, err := readSized(body, 0, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	deviceID, off, err := readSized(body, off, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	if len(body)-off < 8 {
		return nil, fmt.Errorf("%w: missing version", ErrShortBody)
	}
	return &NodeKickBody{UserID: userID, DeviceID: deviceID, Version: binary.BigEndian.Uint64(body[off:])}, nil
}

// appendNodeAckBody appends the shared status + message layout used by both
// node acknowledgement bodies.
func appendNodeAckBody(dst []byte, status uint8, message string) ([]byte, error) {
	if err := checkNodeStatus(status); err != nil {
		return dst, err
	}
	if len(message) > MaxNodeMsgLen {
		return dst, fmt.Errorf("%w: message length %d exceeds limit %d", ErrFrameTooLarge, len(message), MaxNodeMsgLen)
	}

	dst = append(dst, status)
	return appendSized(dst, message), nil
}

// decodeNodeAckBody decodes the shared status + message layout used by both
// node acknowledgement bodies.
func decodeNodeAckBody(body []byte) (uint8, string, error) {
	if len(body) < 1 {
		return 0, "", fmt.Errorf("%w: missing status", ErrShortBody)
	}
	status := body[0]
	if err := checkNodeStatus(status); err != nil {
		return 0, "", err
	}
	off := 1
	if len(body)-off < 2 {
		return 0, "", fmt.Errorf("%w: missing message length", ErrShortBody)
	}
	msgLen := int(binary.BigEndian.Uint16(body[off:]))
	off += 2
	if msgLen > MaxNodeMsgLen {
		return 0, "", fmt.Errorf("%w: message length %d exceeds limit %d", ErrFrameTooLarge, msgLen, MaxNodeMsgLen)
	}
	if len(body)-off < msgLen {
		return 0, "", fmt.Errorf("%w: want %d message bytes at offset %d, have %d", ErrShortBody, msgLen, off, len(body)-off)
	}
	return status, string(body[off : off+msgLen]), nil
}

// checkNodeStatus reports whether status is a defined node status.
func checkNodeStatus(status uint8) error {
	switch status {
	case NodeStatusOK, NodeStatusInvalid, NodeStatusNotFound, NodeStatusInternal:
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrBadNodeStatus, status)
	}
}

// checkNodeString reports whether s is a valid node string of at most limit
// bytes. Node IDs and addresses share the identity error because they are
// opaque identifiers on the wire.
func checkNodeString(s string, limit int) error {
	if len(s) < 1 || len(s) > limit {
		return fmt.Errorf("%w: length %d not in [1,%d]", ErrBadIdentity, len(s), limit)
	}
	return nil
}

// checkNodeAddr reports whether s is a valid advertise address of at most
// limit bytes. Unlike a node ID the address may be empty, which marks a
// dial-only node that cannot be called back.
func checkNodeAddr(s string, limit int) error {
	if len(s) > limit {
		return fmt.Errorf("%w: length %d not in [0,%d]", ErrBadIdentity, len(s), limit)
	}
	return nil
}

// readNodeAddr reads the advertise address of a NODE_HELLO body. The address
// may be empty, so it cannot use readSized.
func readNodeAddr(body []byte, off, limit int) (string, int, error) {
	if len(body)-off < 2 {
		return "", 0, fmt.Errorf("%w: missing address length prefix", ErrShortBody)
	}
	n := int(binary.BigEndian.Uint16(body[off:]))
	off += 2
	if n > limit {
		return "", 0, fmt.Errorf("%w: length %d not in [0,%d]", ErrBadIdentity, n, limit)
	}
	if len(body)-off < n {
		return "", 0, fmt.Errorf("%w: want %d bytes at offset %d, have %d", ErrShortBody, n, off, len(body)-off)
	}
	return string(body[off : off+n]), off + n, nil
}
