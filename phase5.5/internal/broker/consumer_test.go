package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestPermanentMarksError(t *testing.T) {
	base := errors.New("malformed payload")
	wrapped := Permanent(base)
	if wrapped == nil {
		t.Fatal("Permanent(nil) must stay nil")
	}
	if !IsPermanent(wrapped) {
		t.Fatal("IsPermanent must report a wrapped error as permanent")
	}
	if !errors.Is(wrapped, base) {
		t.Fatal("Permanent must keep the original cause reachable through errors.Is")
	}
	if IsPermanent(base) {
		t.Fatal("a plain error must not be classified as permanent")
	}
	if IsPermanent(nil) {
		t.Fatal("nil must not be classified as permanent")
	}
}

func TestPermanentSurvivesWrapping(t *testing.T) {
	wrapped := fmt.Errorf("engine: fan-out failed: %w", Permanent(errors.New("bad target")))
	if !IsPermanent(wrapped) {
		t.Fatal("a permanent error wrapped by another error must stay permanent")
	}
}

func TestSafeHandleRecoversPanic(t *testing.T) {
	c := &Consumer{}
	c.opts.applyDefaults()
	c.opts.Logger = testLogger()

	err := c.safeHandle(context.Background(), Message{}, func(context.Context, Message) error {
		panic("handler exploded")
	})
	if err == nil {
		t.Fatal("a panicking handler must produce an error")
	}
	if IsPermanent(err) {
		t.Fatal("a panic is a retryable failure, not a permanent one")
	}
}

func TestSafeHandlePassesHandlerError(t *testing.T) {
	c := &Consumer{}
	c.opts.applyDefaults()
	c.opts.Logger = testLogger()

	sentinel := errors.New("database down")
	err := c.safeHandle(context.Background(), Message{}, func(context.Context, Message) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("safeHandle must return the handler error, got %v", err)
	}
}

func TestNextDelayCapsAtMax(t *testing.T) {
	base := 100 * time.Millisecond
	max := time.Second
	delay := base
	for i := 0; i < 10; i++ {
		delay = nextDelay(delay, max)
		if delay > max {
			t.Fatalf("delay %v exceeded the cap %v", delay, max)
		}
	}
}

func TestJitterStaysInRange(t *testing.T) {
	d := 100 * time.Millisecond
	for i := 0; i < 100; i++ {
		out := jitter(d)
		if out < d-d/5 || out > d+d/5 {
			t.Fatalf("jitter %v outside +/-20 percent of %v", out, d)
		}
	}
}

func TestDeadLetterWithoutPublisherIsSafe(t *testing.T) {
	c := &Consumer{}
	c.opts.applyDefaults()
	c.opts.Logger = testLogger()

	err := c.deadLetter(context.Background(), Message{Topic: TopicFanout}, errors.New("boom"), "permanent")
	if err != nil {
		t.Fatalf("a missing dead-letter publisher must not fail the record, got %v", err)
	}
}

func TestDeadLetterUsesTheConfiguredTopic(t *testing.T) {
	pub := &recordingPublisher{}
	c := &Consumer{}
	c.opts.applyDefaults()
	c.opts.Logger = testLogger()
	c.opts.DeadLetterTopic = TopicDLQ
	c.opts.DeadLetterPublisher = pub

	msg := Message{Topic: TopicFanout, Partition: 2, Offset: 7, Key: []byte("k"), Value: []byte("v")}
	if err := c.deadLetter(context.Background(), msg, errors.New("boom"), "permanent"); err != nil {
		t.Fatalf("deadLetter failed: %v", err)
	}
	if pub.topic != TopicDLQ {
		t.Fatalf("dead letter published to %q, want %q", pub.topic, TopicDLQ)
	}
	if len(pub.body) == 0 {
		t.Fatal("dead letter body must not be empty")
	}
	ev, err := DecodeDeadLetter(pub.body)
	if err != nil {
		t.Fatalf("decode dead letter: %v", err)
	}
	if ev.SourceTopic != TopicFanout || ev.Partition != 2 || ev.Offset != 7 {
		t.Fatalf("dead letter lost the source coordinates: %+v", ev)
	}
	if ev.ErrorKind != "permanent" {
		t.Fatalf("error kind = %q, want permanent", ev.ErrorKind)
	}
}

// recordingPublisher captures the last published record.
type recordingPublisher struct {
	topic string
	key   string
	body  []byte
}

func (p *recordingPublisher) Publish(_ context.Context, topic, key string, value []byte) error {
	p.topic = topic
	p.key = key
	p.body = append([]byte(nil), value...)
	return nil
}
