package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr        string
	KafkaBrokers    []string
	KafkaTopic      string
	KafkaGroupID    string
	KafkaMinBytes   int
	KafkaMaxBytes   int
	WindowDur       time.Duration
	BucketDur       time.Duration
	SnapshotSize    int
	RefreshEvery    time.Duration
	GCEvery         time.Duration
	DefaultTopN     int
	MaxTopN         int
	ShutdownTimeout time.Duration
	InitialStopList []string
}

func Load() Config {
	cfg := Config{
		HTTPAddr:        getStr("HTTP_ADDR", ":8080"),
		KafkaBrokers:    splitCSV(getStr("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:      getStr("KAFKA_TOPIC", "search.events"),
		KafkaGroupID:    getStr("KAFKA_GROUP_ID", "trendsd"),
		KafkaMinBytes:   getInt("KAFKA_MIN_BYTES", 1),
		KafkaMaxBytes:   getInt("KAFKA_MAX_BYTES", 10<<20),
		WindowDur:       getDur("WINDOW", 5*time.Minute),
		BucketDur:       getDur("BUCKET", 10*time.Second),
		SnapshotSize:    getInt("SNAPSHOT_SIZE", 1000),
		RefreshEvery:    getDur("REFRESH", time.Second),
		GCEvery:         getDur("GC", time.Minute),
		DefaultTopN:     getInt("DEFAULT_TOP_N", 10),
		MaxTopN:         getInt("MAX_TOP_N", 1000),
		ShutdownTimeout: getDur("SHUTDOWN_TIMEOUT", 15*time.Second),
		InitialStopList: splitCSV(getStr("INITIAL_STOP_LIST", "")),
	}
	return cfg
}

func getStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func getDur(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func splitCSV(v string) []string {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
