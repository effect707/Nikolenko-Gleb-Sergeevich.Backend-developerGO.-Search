package ranker

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glebnikolenko9/wb-search-trends/internal/metrics"
	"github.com/glebnikolenko9/wb-search-trends/internal/model"
	"github.com/glebnikolenko9/wb-search-trends/internal/stoplist"
)

const shardCount = 16

type Snapshot struct {
	GeneratedAt time.Time
	WindowDur   time.Duration
	Entries     []model.TopEntry
}

type shard struct {
	mu      sync.RWMutex
	queries map[string]*window
}

type Config struct {
	WindowDur    time.Duration
	BucketDur    time.Duration
	SnapshotSize int
	Now          func() time.Time
}

type Ranker struct {
	bucketDur    time.Duration
	bucketCount  int
	windowDur    time.Duration
	snapshotSize int

	shards   [shardCount]*shard
	stopList *stoplist.StopList
	snapshot atomic.Pointer[Snapshot]
	now      func() time.Time
}

func New(cfg Config, sl *stoplist.StopList) *Ranker {
	if cfg.BucketDur <= 0 {
		cfg.BucketDur = 10 * time.Second
	}
	if cfg.WindowDur <= 0 {
		cfg.WindowDur = 5 * time.Minute
	}
	if cfg.SnapshotSize <= 0 {
		cfg.SnapshotSize = 1000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	bucketCount := int(cfg.WindowDur / cfg.BucketDur)
	if bucketCount < 1 {
		bucketCount = 1
	}

	r := &Ranker{
		bucketDur:    cfg.BucketDur,
		bucketCount:  bucketCount,
		windowDur:    time.Duration(bucketCount) * cfg.BucketDur,
		snapshotSize: cfg.SnapshotSize,
		stopList:     sl,
		now:          cfg.Now,
	}
	for i := range r.shards {
		r.shards[i] = &shard{queries: make(map[string]*window)}
	}
	r.snapshot.Store(&Snapshot{
		GeneratedAt: r.now(),
		WindowDur:   r.windowDur,
		Entries:     []model.TopEntry{},
	})
	return r
}

func (r *Ranker) WindowDuration() time.Duration { return r.windowDur }

func (r *Ranker) shardFor(q string) *shard {
	return r.shards[fnv32(q)%shardCount]
}

func fnv32(s string) uint32 {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}

func (r *Ranker) Ingest(query, userID string, at time.Time) {
	if query == "" || userID == "" {
		return
	}
	s := r.shardFor(query)

	s.mu.RLock()
	w, ok := s.queries[query]
	s.mu.RUnlock()
	if !ok {
		s.mu.Lock()
		if w, ok = s.queries[query]; !ok {
			w = newWindow(r.bucketDur, r.bucketCount)
			s.queries[query] = w
		}
		s.mu.Unlock()
	}
	w.add(userID, at)
}

func (r *Ranker) Top(n int) Snapshot {
	snap := r.snapshot.Load()
	out := Snapshot{
		GeneratedAt: snap.GeneratedAt,
		WindowDur:   snap.WindowDur,
		Entries:     snap.Entries,
	}
	if n <= 0 {
		out.Entries = nil
		return out
	}
	if n < len(snap.Entries) {
		out.Entries = snap.Entries[:n]
	}
	return out
}

type heapEntry struct {
	query string
	count int64
}

type minHeap []heapEntry

func (h minHeap) Len() int { return len(h) }
func (h minHeap) Less(i, j int) bool {
	if h[i].count != h[j].count {
		return h[i].count < h[j].count
	}
	return h[i].query > h[j].query
}
func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)   { *h = append(*h, x.(heapEntry)) }
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type collectedQuery struct {
	query string
	w     *window
}

func (r *Ranker) Refresh() {
	start := time.Now()
	now := r.now()

	var all []collectedQuery
	for _, s := range r.shards {
		s.mu.RLock()
		for q, w := range s.queries {
			all = append(all, collectedQuery{q, w})
		}
		s.mu.RUnlock()
	}

	h := &minHeap{}
	for _, e := range all {
		if r.stopList.Contains(e.query) {
			continue
		}
		c := e.w.count(now)
		if c == 0 {
			continue
		}
		if h.Len() < r.snapshotSize {
			heap.Push(h, heapEntry{query: e.query, count: c})
		} else if (*h)[0].count < c {
			(*h)[0] = heapEntry{query: e.query, count: c}
			heap.Fix(h, 0)
		}
	}

	size := h.Len()
	entries := make([]model.TopEntry, size)
	for i := size - 1; i >= 0; i-- {
		e := heap.Pop(h).(heapEntry)
		entries[i] = model.TopEntry{Query: e.query, Count: e.count}
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Count != entries[j].Count {
			return entries[i].Count > entries[j].Count
		}
		return entries[i].Query < entries[j].Query
	})

	r.snapshot.Store(&Snapshot{
		GeneratedAt: now,
		WindowDur:   r.windowDur,
		Entries:     entries,
	})

	metrics.SnapshotEntries.Set(float64(len(entries)))
	metrics.SnapshotRefreshSeconds.Observe(time.Since(start).Seconds())
}

func (r *Ranker) GC() {
	now := r.now()
	tracked := 0
	for _, s := range r.shards {
		s.mu.Lock()
		for q, w := range s.queries {
			if w.isEmpty(now) {
				delete(s.queries, q)
			}
		}
		tracked += len(s.queries)
		s.mu.Unlock()
	}
	metrics.UniqueQueriesTracked.Set(float64(tracked))
}

func (r *Ranker) Run(ctx context.Context, refreshEvery, gcEvery time.Duration) {
	if refreshEvery <= 0 {
		refreshEvery = time.Second
	}
	if gcEvery <= 0 {
		gcEvery = time.Minute
	}
	rt := time.NewTicker(refreshEvery)
	gct := time.NewTicker(gcEvery)
	defer rt.Stop()
	defer gct.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rt.C:
			r.Refresh()
		case <-gct.C:
			r.GC()
		}
	}
}
