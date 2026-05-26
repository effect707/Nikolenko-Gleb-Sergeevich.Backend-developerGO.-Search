package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/glebnikolenko9/wb-search-trends/internal/metrics"
	"github.com/glebnikolenko9/wb-search-trends/internal/ranker"
	"github.com/glebnikolenko9/wb-search-trends/internal/stoplist"
)

const maxStopListBody = 1 << 20

type Server struct {
	ranker   *ranker.Ranker
	stopList *stoplist.StopList
	log      *slog.Logger
	defaultN int
	maxN     int
}

func NewServer(r *ranker.Ranker, sl *stoplist.StopList, log *slog.Logger, defaultN, maxN int) *Server {
	if defaultN <= 0 {
		defaultN = 10
	}
	if maxN <= 0 {
		maxN = 1000
	}
	return &Server{
		ranker:   r,
		stopList: sl,
		log:      log,
		defaultN: defaultN,
		maxN:     maxN,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /ready", s.health)
	mux.HandleFunc("GET /api/v1/trends/top", s.top)
	mux.HandleFunc("GET /api/v1/stop-list", s.listStop)
	mux.HandleFunc("POST /api/v1/stop-list", s.addStop)
	mux.HandleFunc("DELETE /api/v1/stop-list", s.removeStop)
	mux.Handle("GET /metrics", promhttp.Handler())
	return s.recoverer(mux)
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic in handler", "panic", rec, "path", r.URL.Path)
				writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type errorBody struct {
	Error string `json:"error"`
}

type topItem struct {
	Rank  int    `json:"rank"`
	Query string `json:"query"`
	Count int64  `json:"count"`
}

type topResponse struct {
	GeneratedAt time.Time `json:"generated_at"`
	Window      string    `json:"window"`
	Items       []topItem `json:"items"`
}

type stopListRequest struct {
	Words []string `json:"words"`
}

type stopListResponse struct {
	Size  int      `json:"size"`
	Words []string `json:"words,omitempty"`
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) top(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics.TopRequests.Inc()
	defer func() {
		metrics.TopRequestSeconds.Observe(time.Since(start).Seconds())
	}()

	n := s.defaultN
	if raw := r.URL.Query().Get("n"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid n"})
			return
		}
		if parsed > s.maxN {
			parsed = s.maxN
		}
		n = parsed
	}

	snap := s.ranker.Top(n)
	items := make([]topItem, len(snap.Entries))
	for i, e := range snap.Entries {
		items[i] = topItem{Rank: i + 1, Query: e.Query, Count: e.Count}
	}

	writeJSON(w, http.StatusOK, topResponse{
		GeneratedAt: snap.GeneratedAt,
		Window:      snap.WindowDur.String(),
		Items:       items,
	})
}

func (s *Server) listStop(w http.ResponseWriter, _ *http.Request) {
	words := s.stopList.List()
	writeJSON(w, http.StatusOK, stopListResponse{Size: len(words), Words: words})
}

func (s *Server) addStop(w http.ResponseWriter, r *http.Request) {
	req, err := readStop(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	if len(req.Words) == 0 {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "words is required"})
		return
	}
	size := s.stopList.Add(req.Words...)
	metrics.StopListSize.Set(float64(size))
	writeJSON(w, http.StatusOK, stopListResponse{Size: size})
}

func (s *Server) removeStop(w http.ResponseWriter, r *http.Request) {
	req, err := readStop(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	if len(req.Words) == 0 {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "words is required"})
		return
	}
	size := s.stopList.Remove(req.Words...)
	metrics.StopListSize.Set(float64(size))
	writeJSON(w, http.StatusOK, stopListResponse{Size: size})
}

func readStop(r *http.Request) (stopListRequest, error) {
	var req stopListRequest
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxStopListBody))
	if err != nil {
		return req, err
	}
	if len(body) == 0 {
		return req, nil
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return req, err
	}
	return req, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
