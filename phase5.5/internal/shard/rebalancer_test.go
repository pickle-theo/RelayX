package shard

import (
	"testing"

	"relayx/phase5.5/internal/store"
)

func TestComputeAssignmentsCoversTheWholeSpace(t *testing.T) {
	workers := []string{"w3", "w1", "w2"}
	got := ComputeAssignments(workers, 1024)
	if len(got) != 3 {
		t.Fatalf("got %d assignments, want 3", len(got))
	}
	if got[0].WorkerID != "w1" || got[1].WorkerID != "w2" || got[2].WorkerID != "w3" {
		t.Fatalf("assignments are not sorted by worker id: %+v", got)
	}
	if got[0].ShardStart != 0 {
		t.Fatalf("first interval starts at %d, want 0", got[0].ShardStart)
	}
	if got[len(got)-1].ShardEnd != 1024 {
		t.Fatalf("last interval ends at %d, want 1024", got[len(got)-1].ShardEnd)
	}
	for i := 1; i < len(got); i++ {
		if got[i].ShardStart != got[i-1].ShardEnd {
			t.Fatalf("interval %d starts at %d but the previous one ends at %d",
				i, got[i].ShardStart, got[i-1].ShardEnd)
		}
	}
}

func TestComputeAssignmentsIsDeterministic(t *testing.T) {
	a := ComputeAssignments([]string{"b", "a", "c"}, 100)
	b := ComputeAssignments([]string{"c", "b", "a"}, 100)
	if len(a) != len(b) {
		t.Fatalf("different lengths: %d and %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("assignment %d differs: %+v and %+v", i, a[i], b[i])
		}
	}
}

func TestComputeAssignmentsSplitsTheRemainder(t *testing.T) {
	got := ComputeAssignments([]string{"a", "b", "c"}, 10)
	if len(got) != 3 {
		t.Fatalf("got %d assignments, want 3", len(got))
	}
	sizes := []int{
		got[0].ShardEnd - got[0].ShardStart,
		got[1].ShardEnd - got[1].ShardStart,
		got[2].ShardEnd - got[2].ShardStart,
	}
	if sizes[0] != 4 || sizes[1] != 3 || sizes[2] != 3 {
		t.Fatalf("unexpected sizes %v", sizes)
	}
}

func TestComputeAssignmentsSkipsEmptyIntervals(t *testing.T) {
	got := ComputeAssignments([]string{"a", "b", "c"}, 2)
	if len(got) != 2 {
		t.Fatalf("got %d assignments, want 2", len(got))
	}
	for _, o := range got {
		if o.ShardEnd <= o.ShardStart {
			t.Fatalf("assignment %+v has an empty interval", o)
		}
	}
}

func TestComputeAssignmentsWithoutWorkers(t *testing.T) {
	if got := ComputeAssignments(nil, 1024); got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
	if got := ComputeAssignments([]string{"a"}, 0); got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}

func TestShardOwnerContains(t *testing.T) {
	owner := store.ShardOwner{WorkerID: "w1", ShardStart: 100, ShardEnd: 200}
	if !owner.Contains(100) || !owner.Contains(199) {
		t.Fatal("the interval must include its bounds")
	}
	if owner.Contains(99) || owner.Contains(200) {
		t.Fatal("the interval must be half open")
	}
}
