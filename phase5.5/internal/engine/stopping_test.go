package engine

import (
	"context"
	"testing"
	"time"
)

func TestEngineStopEndsTheRunLoop(t *testing.T) {
	eng := New(Config{
		WorkerID:     "worker-a",
		Batch:        10,
		LeaseTTL:     time.Minute,
		ScanInterval: 10 * time.Millisecond,
		Logger:       testLogger(),
	}, nil, nil, nil, nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	eng.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the run loop did not stop")
	}
}

func TestEngineWaitReturnsAfterTheRunLoopStopped(t *testing.T) {
	eng := New(Config{
		WorkerID:     "worker-a",
		ScanInterval: 10 * time.Millisecond,
		Logger:       testLogger(),
	}, nil, nil, nil, nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()
	eng.Stop()
	<-done

	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	if err := eng.Wait(waitCtx); err != nil {
		t.Fatalf("Wait() error = %v, want nil", err)
	}
}

func TestEngineWaitHonoursTheContext(t *testing.T) {
	eng := New(Config{WorkerID: "worker-a", Logger: testLogger()},
		nil, nil, nil, nil, nil, nil, nil)

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelWait()
	if err := eng.Wait(waitCtx); err == nil {
		t.Fatal("Wait() must fail when the run loop is still running")
	}
}

func TestEngineTracksTheInFlightDeliveries(t *testing.T) {
	eng := New(Config{WorkerID: "worker-a", Logger: testLogger()},
		nil, nil, nil, nil, nil, nil, nil)

	if ids := eng.inFlightIDs(); len(ids) != 0 {
		t.Fatalf("in-flight deliveries = %d, want 0", len(ids))
	}
	eng.track(7)
	eng.track(9)
	ids := eng.inFlightIDs()
	if len(ids) != 2 {
		t.Fatalf("in-flight deliveries = %d, want 2", len(ids))
	}
	eng.untrack(7)
	ids = eng.inFlightIDs()
	if len(ids) != 1 || ids[0] != 9 {
		t.Fatalf("in-flight deliveries = %v, want [9]", ids)
	}
}
