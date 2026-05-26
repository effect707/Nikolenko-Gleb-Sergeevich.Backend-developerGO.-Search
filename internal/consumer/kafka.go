package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/glebnikolenko9/wb-search-trends/internal/metrics"
	"github.com/glebnikolenko9/wb-search-trends/internal/model"
)

type Ingester interface {
	Ingest(query, userID string, at time.Time)
}

type Config struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
}

type Consumer struct {
	reader   *kafka.Reader
	ingester Ingester
	log      *slog.Logger
}

func New(cfg Config, ing Ingester, log *slog.Logger) *Consumer {
	min := cfg.MinBytes
	if min <= 0 {
		min = 1
	}
	max := cfg.MaxBytes
	if max <= 0 {
		max = 10 << 20
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		MinBytes:    min,
		MaxBytes:    max,
		StartOffset: kafka.LastOffset,
		MaxWait:     500 * time.Millisecond,
	})
	return &Consumer{reader: r, ingester: ing, log: log}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			metrics.ConsumerErrors.Inc()
			c.log.Error("kafka fetch", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		c.handle(m.Value)

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			metrics.ConsumerErrors.Inc()
			c.log.Warn("kafka commit", "err", err)
		}
	}
}

func (c *Consumer) handle(value []byte) {
	var ev model.SearchEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		metrics.EventsDropped.WithLabelValues("decode").Inc()
		c.log.Warn("decode event", "err", err)
		return
	}

	q := strings.ToLower(strings.TrimSpace(ev.Query))
	user := strings.TrimSpace(ev.UserID)
	if q == "" {
		metrics.EventsDropped.WithLabelValues("empty_query").Inc()
		return
	}
	if user == "" {
		metrics.EventsDropped.WithLabelValues("empty_user").Inc()
		return
	}

	at := ev.Timestamp
	if at.IsZero() {
		at = time.Now()
	}

	c.ingester.Ingest(q, user, at)
	metrics.EventsIngested.Inc()
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
