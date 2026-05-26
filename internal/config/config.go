package config

import (
	"errors"
	"fmt"
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

func (c Config) Validate() error {
	if c.WindowDur <= 0 {
		return errors.New("WINDOW must be positive")
	}
	if c.BucketDur <= 0 {
		return errors.New("BUCKET must be positive")
	}
	if c.WindowDur%c.BucketDur != 0 {
		return fmt.Errorf("WINDOW (%s) must be a multiple of BUCKET (%s)", c.WindowDur, c.BucketDur)
	}
	if c.WindowDur/c.BucketDur < 2 {
		return fmt.Errorf("WINDOW/BUCKET must yield at least 2 buckets, got %d", c.WindowDur/c.BucketDur)
	}
	if c.SnapshotSize <= 0 {
		return errors.New("SNAPSHOT_SIZE must be positive")
	}
	if c.RefreshEvery <= 0 {
		return errors.New("REFRESH must be positive")
	}
	if c.GCEvery <= 0 {
		return errors.New("GC must be positive")
	}
	if c.DefaultTopN <= 0 {
		return errors.New("DEFAULT_TOP_N must be positive")
	}
	if c.MaxTopN <= 0 {
		return errors.New("MAX_TOP_N must be positive")
	}
	if c.DefaultTopN > c.MaxTopN {
		return fmt.Errorf("DEFAULT_TOP_N (%d) must not exceed MAX_TOP_N (%d)", c.DefaultTopN, c.MaxTopN)
	}
	if c.ShutdownTimeout <= 0 {
		return errors.New("SHUTDOWN_TIMEOUT must be positive")
	}
	if len(c.KafkaBrokers) == 0 {
		return errors.New("KAFKA_BROKERS must not be empty")
	}
	if c.KafkaTopic == "" {
		return errors.New("KAFKA_TOPIC must not be empty")
	}
	if c.KafkaGroupID == "" {
		return errors.New("KAFKA_GROUP_ID must not be empty")
	}
	if c.HTTPAddr == "" {
		return errors.New("HTTP_ADDR must not be empty")
	}
	return nil
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
