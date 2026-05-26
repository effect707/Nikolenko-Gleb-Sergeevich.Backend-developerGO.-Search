package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsIngested = promauto.NewCounter(prometheus.CounterOpts{
		Name: "trendsd_events_ingested_total",
		Help: "Total number of search events successfully ingested into the ranker.",
	})

	EventsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "trendsd_events_dropped_total",
		Help: "Total number of search events dropped, labeled by reason.",
	}, []string{"reason"})

	ConsumerErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "trendsd_consumer_errors_total",
		Help: "Total number of broker consumer errors.",
	})

	TopRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "trendsd_top_requests_total",
		Help: "Total number of Top-N requests served.",
	})

	TopRequestSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "trendsd_top_request_seconds",
		Help:    "Latency of Top-N requests.",
		Buckets: prometheus.ExponentialBuckets(0.00001, 4, 10),
	})

	SnapshotRefreshSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "trendsd_snapshot_refresh_seconds",
		Help:    "Duration of a single Top-N snapshot rebuild.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 4, 10),
	})

	SnapshotEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "trendsd_snapshot_entries",
		Help: "Number of entries in the latest Top-N snapshot.",
	})

	StopListSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "trendsd_stoplist_size",
		Help: "Current size of the stop list.",
	})

	UniqueQueriesTracked = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "trendsd_unique_queries_tracked",
		Help: "Number of unique queries currently tracked in the sliding window.",
	})
)
