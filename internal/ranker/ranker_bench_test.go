package ranker

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebnikolenko9/wb-search-trends/internal/model"
	"github.com/glebnikolenko9/wb-search-trends/internal/stoplist"
)

var benchSink atomic.Pointer[[]model.TopEntry]

func benchRanker(b *testing.B) *Ranker {
	b.Helper()
	sl := stoplist.New()
	return New(Config{
		WindowDur:    5 * time.Minute,
		BucketDur:    10 * time.Second,
		SnapshotSize: 1000,
	}, sl)
}

func BenchmarkIngest(b *testing.B) {
	r := benchRanker(b)
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			id := atomic.AddUint64(&i, 1)
			r.Ingest("query "+strconv.Itoa(int(id%1000)), "u-"+strconv.Itoa(int(id%50000)), now)
		}
	})
}

func BenchmarkTopRead(b *testing.B) {
	r := benchRanker(b)
	now := time.Now()
	for i := range 50000 {
		r.Ingest("query "+strconv.Itoa(i%1000), "u-"+strconv.Itoa(i%10000), now)
	}
	r.Refresh()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var last []model.TopEntry
		for pb.Next() {
			snap := r.Top(100)
			last = snap.Entries
		}
		benchSink.Store(&last)
	})
}

func BenchmarkRefresh10k(b *testing.B) {
	r := benchRanker(b)
	now := time.Now()
	for i := range 10000 {
		r.Ingest("q"+strconv.Itoa(i), "u-"+strconv.Itoa(i%100), now)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.Refresh()
	}
}

func BenchmarkRefresh100k(b *testing.B) {
	r := benchRanker(b)
	now := time.Now()
	for i := range 100000 {
		r.Ingest("q"+strconv.Itoa(i), "u-"+strconv.Itoa(i%100), now)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.Refresh()
	}
}
