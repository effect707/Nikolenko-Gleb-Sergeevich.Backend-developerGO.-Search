package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/glebnikolenko9/wb-search-trends/internal/ranker"
	"github.com/glebnikolenko9/wb-search-trends/internal/stoplist"
)

func newTestServer(t *testing.T) (*Server, *ranker.Ranker, *stoplist.StopList) {
	t.Helper()
	sl := stoplist.New()
	r := ranker.New(ranker.Config{
		WindowDur:    time.Minute,
		BucketDur:    10 * time.Second,
		SnapshotSize: 100,
	}, sl)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := NewServer(r, sl, log, 10, 100)
	return srv, r, sl
}

func do(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealth(t *testing.T) {
	srv, _, _ := newTestServer(t)
	h := srv.Handler()
	rec := do(t, h, http.MethodGet, "/health", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestTopEndpoint(t *testing.T) {
	srv, r, _ := newTestServer(t)
	h := srv.Handler()

	now := time.Now()
	r.Ingest("foo", "u1", now)
	r.Ingest("foo", "u2", now)
	r.Ingest("bar", "u1", now)
	r.Refresh()

	rec := do(t, h, http.MethodGet, "/api/v1/trends/top?n=5", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp topResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(resp.Items))
	}
	if resp.Items[0].Query != "foo" || resp.Items[0].Count != 2 {
		t.Fatalf("foo must be first with 2, got %+v", resp.Items[0])
	}
	if resp.Items[0].Rank != 1 {
		t.Fatalf("rank must be 1, got %d", resp.Items[0].Rank)
	}
}

func TestTopValidatesN(t *testing.T) {
	srv, _, _ := newTestServer(t)
	h := srv.Handler()

	rec := do(t, h, http.MethodGet, "/api/v1/trends/top?n=-1", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for n=-1, got %d", rec.Code)
	}

	rec = do(t, h, http.MethodGet, "/api/v1/trends/top?n=abc", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for n=abc, got %d", rec.Code)
	}
}

func TestStopListCRUD(t *testing.T) {
	srv, _, sl := newTestServer(t)
	h := srv.Handler()

	rec := do(t, h, http.MethodPost, "/api/v1/stop-list", `{"words":["casino","18+"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if sl.Size() != 2 {
		t.Fatalf("stop list size 2 expected, got %d", sl.Size())
	}

	rec = do(t, h, http.MethodGet, "/api/v1/stop-list", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET expected 200, got %d", rec.Code)
	}
	var resp stopListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Size != 2 {
		t.Fatalf("expected size 2, got %d", resp.Size)
	}

	rec = do(t, h, http.MethodDelete, "/api/v1/stop-list?word=casino", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE expected 200, got %d", rec.Code)
	}
	if sl.Size() != 1 {
		t.Fatalf("expected size 1, got %d", sl.Size())
	}

	rec = do(t, h, http.MethodDelete, "/api/v1/stop-list", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("DELETE without ?word should be 400, got %d", rec.Code)
	}
}

func TestTopRejectsTooLargeN(t *testing.T) {
	srv, _, _ := newTestServer(t)
	h := srv.Handler()
	rec := do(t, h, http.MethodGet, "/api/v1/trends/top?n=10000", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for n>max, got %d", rec.Code)
	}
}

func TestStopListRequiresWords(t *testing.T) {
	srv, _, _ := newTestServer(t)
	h := srv.Handler()

	rec := do(t, h, http.MethodPost, "/api/v1/stop-list", `{}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestStopListAffectsTop(t *testing.T) {
	srv, r, sl := newTestServer(t)
	h := srv.Handler()

	now := time.Now()
	r.Ingest("hot", "u1", now)
	r.Ingest("hot", "u2", now)
	r.Ingest("cool", "u1", now)
	sl.Add("hot")
	r.Refresh()

	rec := do(t, h, http.MethodGet, "/api/v1/trends/top?n=10", "")
	var resp topResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Items) != 1 || resp.Items[0].Query != "cool" {
		t.Fatalf("stop-list must hide hot, got %+v", resp.Items)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv, _, _ := newTestServer(t)
	h := srv.Handler()
	rec := do(t, h, http.MethodGet, "/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "trendsd_") {
		t.Fatal("metrics body must contain trendsd_ counters")
	}
}
