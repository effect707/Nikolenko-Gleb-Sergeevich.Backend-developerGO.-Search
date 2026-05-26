package stoplist

import (
	"sort"
	"sync"
	"testing"
)

func TestAddRemoveContains(t *testing.T) {
	sl := New()
	if sl.Size() != 0 {
		t.Fatalf("expected empty, got size %d", sl.Size())
	}
	if sl.Contains("casino") {
		t.Fatal("must not contain casino on empty list")
	}

	sl.Add("casino", "  Bet  ", "")
	if sl.Size() != 2 {
		t.Fatalf("expected size 2, got %d", sl.Size())
	}
	if !sl.Contains("CASINO") {
		t.Fatal("Contains must be case-insensitive")
	}
	if !sl.Contains("bet") {
		t.Fatal("trim+lowercase must work")
	}

	sl.Remove("casino")
	if sl.Contains("casino") {
		t.Fatal("after Remove must not contain")
	}
	if sl.Size() != 1 {
		t.Fatalf("expected size 1, got %d", sl.Size())
	}
}

func TestListIsCopy(t *testing.T) {
	sl := New()
	sl.Add("a", "b", "c")
	list := sl.List()
	sort.Strings(list)
	want := []string{"a", "b", "c"}
	for i := range want {
		if list[i] != want[i] {
			t.Fatalf("list mismatch at %d: %v vs %v", i, list, want)
		}
	}
	list[0] = "MUTATED"
	if sl.Contains("MUTATED") {
		t.Fatal("mutation of returned list must not affect stop-list")
	}
}

func TestNewWithInitial(t *testing.T) {
	sl := NewWithInitial([]string{"foo", "bar", " baz "})
	if sl.Size() != 3 {
		t.Fatalf("expected 3 initial, got %d", sl.Size())
	}
	if !sl.Contains("BAZ") {
		t.Fatal("initial words must be normalized")
	}
}

func TestConcurrentAddRemoveContains(t *testing.T) {
	sl := New()
	const goroutines = 32
	const iter = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			for i := range iter {
				w := "w" + string(rune('a'+(g%26)))
				if i%2 == 0 {
					sl.Add(w)
				} else {
					sl.Remove(w)
				}
				_ = sl.Contains(w)
			}
		}(g)
	}
	wg.Wait()
}
