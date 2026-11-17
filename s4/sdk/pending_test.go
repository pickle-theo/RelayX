package sdk

import "testing"

func TestPendingTrackerFIFOEviction(t *testing.T) {
	p := NewPendingTracker(2)
	if e := p.Add("m1", "c1"); e != "" {
		t.Fatalf("unexpected eviction %q", e)
	}
	p.Add("m2", "c2")
	if e := p.Add("m3", "c3"); e != "m1" {
		t.Fatalf("evicted=%q, want m1", e)
	}
	if p.Len() != 2 || p.Evicted() != 1 {
		t.Fatalf("len=%d evicted=%d", p.Len(), p.Evicted())
	}
	if ids := p.IDs(); len(ids) != 2 || ids[0] != "m2" || ids[1] != "m3" {
		t.Fatalf("ids=%v", ids)
	}
	if !p.Remove("m2") {
		t.Fatal("remove should succeed")
	}
	if p.Remove("m2") {
		t.Fatal("second remove should be false")
	}
	if ids := p.IDs(); len(ids) != 1 || ids[0] != "m3" {
		t.Fatalf("ids after remove=%v", ids)
	}
	// 重复 Add 不得产生重复项。
	if e := p.Add("m3", "c3"); e != "" {
		t.Fatalf("duplicate add evicted %q", e)
	}
	if p.Len() != 1 {
		t.Fatalf("duplicate add changed len=%d", p.Len())
	}
}
