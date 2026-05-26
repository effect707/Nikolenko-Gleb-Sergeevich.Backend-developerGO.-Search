package stoplist

import (
	"strings"
	"sync/atomic"
)

type StopList struct {
	words atomic.Pointer[map[string]struct{}]
}

func New() *StopList {
	sl := &StopList{}
	empty := make(map[string]struct{})
	sl.words.Store(&empty)
	return sl
}

func NewWithInitial(words []string) *StopList {
	sl := New()
	sl.Add(words...)
	return sl
}

func normalize(w string) string {
	return strings.ToLower(strings.TrimSpace(w))
}

func (sl *StopList) Contains(q string) bool {
	m := *sl.words.Load()
	_, ok := m[normalize(q)]
	return ok
}

func (sl *StopList) Add(words ...string) int {
	for {
		old := sl.words.Load()
		next := make(map[string]struct{}, len(*old)+len(words))
		for k := range *old {
			next[k] = struct{}{}
		}
		for _, w := range words {
			n := normalize(w)
			if n == "" {
				continue
			}
			next[n] = struct{}{}
		}
		if sl.words.CompareAndSwap(old, &next) {
			return len(next)
		}
	}
}

func (sl *StopList) Remove(words ...string) int {
	for {
		old := sl.words.Load()
		next := make(map[string]struct{}, len(*old))
		for k := range *old {
			next[k] = struct{}{}
		}
		for _, w := range words {
			delete(next, normalize(w))
		}
		if sl.words.CompareAndSwap(old, &next) {
			return len(next)
		}
	}
}

func (sl *StopList) List() []string {
	m := *sl.words.Load()
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func (sl *StopList) Size() int {
	return len(*sl.words.Load())
}
