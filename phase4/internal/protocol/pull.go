package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// PullBody is the PULL body: the cursor returned by the previous PULL_DONE
// (0 to restart from the beginning) and the page size requested by the client.
// A zero Limit asks the server to apply its default.
type PullBody struct {
	Cursor uint64
	Limit  uint16
}

// PullDoneBody is the PULL_DONE body: the number of DELIVER frames sent for the
// page, the cursor to resume from and whether more records remain.
type PullDoneBody struct {
	Count      uint16
	NextCursor uint64
	HasMore    bool
}

// AppendPullBody appends the wire representation of p to dst. The layout is
// uint64 cursor + uint16 limit.
//
// On error the returned slice is dst unchanged.
func AppendPullBody(dst []byte, p *PullBody) ([]byte, error) {
	if p == nil {
		return dst, errors.New("protocol: nil pull")
	}
	dst = binary.BigEndian.AppendUint64(dst, p.Cursor)
	return binary.BigEndian.AppendUint16(dst, p.Limit), nil
}

// DecodePull decodes a PULL body.
func DecodePull(body []byte) (*PullBody, error) {
	if len(body) < 8 {
		return nil, fmt.Errorf("%w: missing cursor", ErrShortBody)
	}
	if len(body) < 10 {
		return nil, fmt.Errorf("%w: missing limit", ErrShortBody)
	}
	return &PullBody{Cursor: binary.BigEndian.Uint64(body), Limit: binary.BigEndian.Uint16(body[8:])}, nil
}

// AppendPullDoneBody appends the wire representation of p to dst. The layout is
// uint16 count + uint64 nextCursor + uint8 hasMore (1=true, 0=false). Count is a
// uint16 and therefore bounded by the wire format itself.
//
// On error the returned slice is dst unchanged.
func AppendPullDoneBody(dst []byte, p *PullDoneBody) ([]byte, error) {
	if p == nil {
		return dst, errors.New("protocol: nil pull done")
	}
	dst = binary.BigEndian.AppendUint16(dst, p.Count)
	dst = binary.BigEndian.AppendUint64(dst, p.NextCursor)
	var hasMore byte
	if p.HasMore {
		hasMore = 1
	}
	return append(dst, hasMore), nil
}

// DecodePullDone decodes a PULL_DONE body.
func DecodePullDone(body []byte) (*PullDoneBody, error) {
	if len(body) < 2 {
		return nil, fmt.Errorf("%w: missing count", ErrShortBody)
	}
	if len(body) < 10 {
		return nil, fmt.Errorf("%w: missing next cursor", ErrShortBody)
	}
	if len(body) < 11 {
		return nil, fmt.Errorf("%w: missing has more", ErrShortBody)
	}
	return &PullDoneBody{
		Count:      binary.BigEndian.Uint16(body),
		NextCursor: binary.BigEndian.Uint64(body[2:]),
		HasMore:    body[10] != 0,
	}, nil
}
