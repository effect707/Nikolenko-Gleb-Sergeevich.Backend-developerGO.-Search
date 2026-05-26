package ranker

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebnikolenko9/wb-search-trends/internal/stoplist"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock(t time.Time) *clock        { return &clock{t: t} }
func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Set(t time.Time)         { c.mu.Lock(); c.t = t; c.mu.Unlock() }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func testRanker(t *testing.T, clk *clock) (*Ranker, *stoplist.StopList) {
	t.Helper()
	sl := stoplist.New()
	r := New(Config{
		WindowDur:    time.Minute,
		BucketDur:    10 * time.Second,
		SnapshotSize: 100,
		Now:          clk.Now,
	}, sl)
	return r, sl
}

func TestRankerTopByDistinctUsers(t *testing.T) {
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	clk := newClock(now)
	r, _ := testRanker(t, clk)

	for i := range 5 {
		r.Ingest("hot", "u"+strconv.Itoa(i), now)
	}
	r.Ingest("cold", "u0", now)

	r.Refresh()
	snap := r.Top(10)
	if len(snap.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d: %+v", len(snap.Entries), snap.Entries)
	}
	if snap.Entries[0].Query != "hot" || snap.Entries[0].Count != 5 {
		t.Fatalf("hot must be first with 5, got %+v", snap.Entries[0])
	}
	if snap.Entries[1].Query != "cold" || snap.Entries[1].Count != 1 {
		t.Fatalf("cold must be second with 1, got %+v", snap.Entries[1])
	}
}

func TestRankerAntiFraudSameUserCountsOnce(t *testing.T) {
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	clk := newClock(now)
	r, _ := testRanker(t, clk)

	for i := range 10000 {
		r.Ingest("spam", "bot-1", now.Add(time.Duration(i)*time.Millisecond))
	}
	r.Ingest("organic", "u-1", now)
	r.Ingest("organic", "u-2", now)

	r.Refresh()
	snap := r.Top(10)
	if len(snap.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(snap.Entries))
	}
	if snap.Entries[0].Query != "organic" {
		t.Fatalf("organic must beat spam, got top=%+v", snap.Entries[0])
	}
	for _, e := range snap.Entries {
		if e.Query == "spam" && e.Count != 1 {
			t.Fatalf("spam from one user must count as 1, got %d", e.Count)
		}
	}
}

func TestRankerStopListFiltering(t *testing.T) {
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	clk := newClock(now)
	r, sl := testRanker(t, clk)

	r.Ingest("badword", "u1", now)
	r.Ingest("badword", "u2", now)
	r.Ingest("good", "u1", now)
	sl.Add("badword")

	r.Refresh()
	snap := r.Top(10)
	if len(snap.Entries) != 1 || snap.Entries[0].Query != "good" {
		t.Fatalf("stop-list must hide badword: %+v", snap.Entries)
	}
}

func TestRankerWindowExpires(t *testing.T) {
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	clk := newClock(now)
	r, _ := testRanker(t, clk)

	r.Ingest("ephemeral", "u1", now)
	r.Refresh()
	if got := r.Top(10); len(got.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got.Entries))
	}

	clk.Advance(2 * time.Minute)
	r.Refresh()
	if got := r.Top(10); len(got.Entries) != 0 {
		t.Fatalf("entry must expire, got %+v", got.Entries)
	}
}

func TestRankerGC(t *testing.T) {
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	clk := newClock(now)
	r, _ := testRanker(t, clk)

	for i := range 100 {
		r.Ingest("q"+strconv.Itoa(i), "u1", now)
	}

	tracked := 0
	for _, s := range r.shards {
		s.mu.RLock()
		tracked += len(s.queries)
		s.mu.RUnlock()
	}
	if tracked != 100 {
		t.Fatalf("expected 100 tracked queries, got %d", tracked)
	}

	clk.Advance(10 * time.Minute)
	r.GC()

	tracked = 0
	for _, s := range r.shards {
		s.mu.RLock()
		tracked += len(s.queries)
		s.mu.RUnlock()
	}
	if tracked != 0 {
		t.Fatalf("expected GC to remove all, got %d remaining", tracked)
	}
}

func TestRankerConcurrentIngestAndRead(t *testing.T) {
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	clk := newClock(now)
	r, _ := testRanker(t, clk)

	const writers = 8
	const readers = 8
	const perWriter = 5000

	var stop atomic.Bool
	var writersWG sync.WaitGroup
	var bgWG sync.WaitGroup

	for w := range writers {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			for i := range perWriter {
				r.Ingest("q"+strconv.Itoa(i%50), "u"+strconv.Itoa(w*1000+i%100), now)
			}
		}(w)
	}

	for range readers {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			for !stop.Load() {
				_ = r.Top(20)
			}
		}()
	}

	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		for !stop.Load() {
			r.Refresh()
		}
	}()

	writersWG.Wait()
	stop.Store(true)
	bgWG.Wait()

	r.Refresh()
	snap := r.Top(50)
	if len(snap.Entries) == 0 {
		t.Fatal("expected snapshot to be non-empty after concurrent ingest")
	}
}
