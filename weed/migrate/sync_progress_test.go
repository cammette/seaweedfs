package migrate

import (
	"testing"
	"time"
)

func TestSyncPointAdvanceHoldsPartialSecond(t *testing.T) {
	live := []LiveNeedle{
		{Key: "a", Unix: 10, HasTime: true},
		{Key: "b", Unix: 10, HasTime: true},
		{Key: "c", Unix: 12, HasTime: true},
	}
	p := SyncPoint{}
	ok := map[string]struct{}{"a": {}, "c": {}}
	next := p.Advance(live, ok)
	if next.CommittedUnix != 0 {
		t.Fatalf("watermark jumped: %+v", next)
	}
	if next.PartialUnix != 10 {
		t.Fatalf("partial second: %+v", next)
	}
	if _, ok := next.PartialFIDs["a"]; !ok {
		t.Fatal("fid a should be recorded in the open second")
	}
	pending := next.Pending(live)
	if len(pending) != 2 || pending[0].Key != "b" && pending[1].Key != "b" {
		t.Fatalf("pending = %+v", pending)
	}

	done := next.Advance(live, map[string]struct{}{"b": {}})
	if done.CommittedUnix != 10 || done.PartialUnix != 0 {
		t.Fatalf("second 10 should commit, got %+v", done)
	}
	// c succeeded in an earlier pass but that second was ahead of a hole.
	// It must be synced again from the caller's point of view only if not in ok.
	// Here ok is only b, so c is still pending and must not be committed.
	if got := done.Pending(live); len(got) != 1 || got[0].Key != "c" {
		t.Fatalf("c still pending, got %+v", got)
	}

	final := done.Advance(live, map[string]struct{}{"c": {}})
	if final.CommittedUnix != 12 || len(final.Pending(live)) != 0 {
		t.Fatalf("final %+v pending %d", final, len(final.Pending(live)))
	}
}

func TestSyncPointLag(t *testing.T) {
	live := []LiveNeedle{{Key: "a", Unix: 1_000, HasTime: true}}
	lag := (SyncPoint{}).Lag(live, time.Unix(1_030, 0))
	if lag != 30*time.Second {
		t.Fatalf("lag %s", lag)
	}
	done := (SyncPoint{CommittedUnix: 1000}).Lag(live, time.Unix(1_030, 0))
	if done != 0 {
		t.Fatalf("caught up lag %s", done)
	}
}

func TestSyncPointUntimedOnce(t *testing.T) {
	live := []LiveNeedle{{Key: "n", HasTime: false}}
	p := (SyncPoint{}).Advance(live, map[string]struct{}{"n": {}})
	if len(p.Pending(live)) != 0 {
		t.Fatal("untimed fid should be skipped after success")
	}
}
