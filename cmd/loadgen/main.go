package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated list of Kafka brokers")
	topic := flag.String("topic", envOr("KAFKA_TOPIC", "search.events"), "Kafka topic")
	rps := flag.Int("rps", 5000, "target events per second")
	users := flag.Int("users", 50000, "size of synthetic user pool")
	queries := flag.Int("queries", 2000, "size of synthetic query dictionary")
	zipfS := flag.Float64("zipf-s", 1.2, "Zipf distribution s parameter (>1)")
	zipfV := flag.Float64("zipf-v", 1.0, "Zipf distribution v parameter (>=1)")
	duration := flag.Duration("d", 0, "test duration (0 = infinite)")
	workers := flag.Int("workers", 8, "number of producer goroutines")
	flag.Parse()

	if *rps <= 0 {
		log.Fatal("rps must be > 0")
	}

	addrs := strings.Split(*brokers, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(addrs...),
		Topic:        *topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    1000,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        true,
	}
	defer writer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if *duration > 0 {
		var c context.CancelFunc
		ctx, c = context.WithTimeout(ctx, *duration)
		defer c()
	}

	dict := make([]string, *queries)
	for i := range dict {
		dict[i] = "query " + strconv.Itoa(i)
	}

	interval := time.Second / time.Duration(*rps)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	var sent atomic.Uint64
	var failed atomic.Uint64

	jobs := make(chan struct{}, (*workers)*4)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		seed := time.Now().UnixNano() + int64(i)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			zipf := rand.NewZipf(rng, *zipfS, *zipfV, uint64(*queries-1))
			for range jobs {
				idx := zipf.Uint64()
				if int(idx) >= len(dict) {
					idx = uint64(len(dict) - 1)
				}
				q := dict[idx]
				u := "u-" + strconv.Itoa(rng.Intn(*users))
				payload := map[string]any{
					"query":   q,
					"user_id": u,
					"ts":      time.Now().UTC().Format(time.RFC3339Nano),
				}
				data, _ := json.Marshal(payload)
				if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(q), Value: data}); err != nil {
					failed.Add(1)
					continue
				}
				sent.Add(1)
			}
		}(seed)
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()

	report := time.NewTicker(time.Second)
	defer report.Stop()

	start := time.Now()
	var lastSent uint64

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-tick.C:
			select {
			case jobs <- struct{}{}:
			default:
			}
		case <-report.C:
			cur := sent.Load()
			fmt.Fprintf(os.Stderr, "[loadgen] sent=%d failed=%d rps=%d elapsed=%s\n",
				cur, failed.Load(), cur-lastSent, time.Since(start).Round(time.Second))
			lastSent = cur
		}
	}

	close(jobs)
	wg.Wait()
	fmt.Fprintf(os.Stderr, "[loadgen] done sent=%d failed=%d elapsed=%s\n",
		sent.Load(), failed.Load(), time.Since(start).Round(time.Millisecond))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
