package engine

import (
	"context"
	"testing"

	"relayx/phase5.5/internal/broker"
)

func TestFanoutRejectsMalformedEventAsPermanent(t *testing.T) {
	f := NewFanout(nil, nil, 10, testLogger())

	err := f.Handle(context.Background(), []byte("this is not json"))
	if err == nil {
		t.Fatal("a malformed event must be reported as an error")
	}
	if !broker.IsPermanent(err) {
		t.Fatal("a malformed event must be permanent so it reaches the dead-letter topic")
	}
}

func TestFanoutRejectsUnknownTargetTypeAsPermanent(t *testing.T) {
	body, err := broker.EncodeFanout(broker.FanoutEvent{
		TenantID:   "default",
		MessageID:  7,
		TargetType: 99,
		Target:     "whoever",
	})
	if err != nil {
		t.Fatalf("encode fan-out event: %v", err)
	}

	f := NewFanout(nil, nil, 10, testLogger())
	err = f.Handle(context.Background(), body)
	if err == nil {
		t.Fatal("an unknown target type must be reported as an error")
	}
	if !broker.IsPermanent(err) {
		t.Fatal("an unknown target type must be permanent so it reaches the dead-letter topic")
	}
}

func TestFanoutRejectsUnresolvableEndpointAsPermanent(t *testing.T) {
	body, err := broker.EncodeFanout(broker.FanoutEvent{
		TenantID:   "default",
		MessageID:  8,
		TargetType: int16(2),
		Target:     "not-an-endpoint-target",
	})
	if err != nil {
		t.Fatalf("encode fan-out event: %v", err)
	}

	f := NewFanout(nil, nil, 10, testLogger())
	err = f.Handle(context.Background(), body)
	if err == nil {
		t.Fatal("an unresolvable endpoint target must be reported as an error")
	}
	if !broker.IsPermanent(err) {
		t.Fatal("an unresolvable endpoint target must be permanent so it reaches the dead-letter topic")
	}
}
