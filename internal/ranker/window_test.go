package ranker

import (
	"testing"
	"time"
)

func TestWindowCountsUniqueUsers(t *testing.T) {
	w := newWindow(10*time.Second, 30)
	base := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	w.add("u1", base)
	w.add("u1", base.Add(time.Second))
	w.add("u2", base.Add(2*time.Second))
	w.add("u2", base.Add(20*time.Second))

	if got := w.count(base.Add(30 * time.Second)); got != 2 {
		t.Fatalf("expected 2 unique users, got %d", got)
	}
}

func TestWindowExpiresOldBuckets(t *testing.T) {
	w := newWindow(10*time.Second, 30)
	base := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	w.add("old", base)

	later := base.Add(6 * time.Minute)
	if got := w.count(later); got != 0 {
		t.Fatalf("expected 0 after window expired, got %d", got)
	}
}

func TestWindowIsEmpty(t *testing.T) {
	w := newWindow(10*time.Second, 30)
	base := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	if !w.isEmpty(base) {
		t.Fatal("fresh window must be empty")
	}
	w.add("u1", base)
	if w.isEmpty(base) {
		t.Fatal("must not be empty after add")
	}
	if !w.isEmpty(base.Add(10 * time.Minute)) {
		t.Fatal("must be empty after window passes")
	}
}

func TestWindowBucketReuse(t *testing.T) {
	w := newWindow(10*time.Second, 6)
	base := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	w.add("u1", base)
	if got := w.count(base); got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}

	future := base.Add(time.Minute + 5*time.Second)
	w.add("u2", future)

	if got := w.count(future); got != 1 {
		t.Fatalf("expected reused bucket to drop u1, got %d", got)
	}
}
