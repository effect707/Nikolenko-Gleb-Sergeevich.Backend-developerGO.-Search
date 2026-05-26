package ranker

import (
	"sync"
	"time"
)

type bucket struct {
	num   int64
	users map[string]struct{}
}

type window struct {
	bucketDur   time.Duration
	bucketCount int

	mu      sync.Mutex
	buckets []bucket
}

func newWindow(bucketDur time.Duration, bucketCount int) *window {
	return &window{
		bucketDur:   bucketDur,
		bucketCount: bucketCount,
		buckets:     make([]bucket, bucketCount),
	}
}

func (w *window) bucketNum(at time.Time) int64 {
	return at.UnixNano() / int64(w.bucketDur)
}

func (w *window) slotFor(num int64) int {
	s := num % int64(w.bucketCount)
	if s < 0 {
		s += int64(w.bucketCount)
	}
	return int(s)
}

func (w *window) add(userID string, at time.Time) {
	num := w.bucketNum(at)
	slot := w.slotFor(num)

	w.mu.Lock()
	b := &w.buckets[slot]
	if b.num != num || b.users == nil {
		b.num = num
		b.users = make(map[string]struct{})
	}
	b.users[userID] = struct{}{}
	w.mu.Unlock()
}

func (w *window) count(now time.Time) int64 {
	cur := w.bucketNum(now)
	min := cur - int64(w.bucketCount-1)

	w.mu.Lock()
	defer w.mu.Unlock()

	union := make(map[string]struct{})
	for i := range w.buckets {
		b := &w.buckets[i]
		if b.users == nil || b.num < min || b.num > cur {
			continue
		}
		for u := range b.users {
			union[u] = struct{}{}
		}
	}
	return int64(len(union))
}

func (w *window) isEmpty(now time.Time) bool {
	cur := w.bucketNum(now)
	min := cur - int64(w.bucketCount-1)

	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.buckets {
		b := &w.buckets[i]
		if len(b.users) == 0 {
			continue
		}
		if b.num >= min && b.num <= cur {
			return false
		}
	}
	return true
}
