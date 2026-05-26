package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/glebnikolenko9/wb-search-trends/internal/api"
	"github.com/glebnikolenko9/wb-search-trends/internal/config"
	"github.com/glebnikolenko9/wb-search-trends/internal/consumer"
	"github.com/glebnikolenko9/wb-search-trends/internal/metrics"
	"github.com/glebnikolenko9/wb-search-trends/internal/ranker"
	"github.com/glebnikolenko9/wb-search-trends/internal/stoplist"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := config.Load()

	log.Info("starting trendsd",
		"http_addr", cfg.HTTPAddr,
		"kafka_brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"group", cfg.KafkaGroupID,
		"window", cfg.WindowDur,
		"bucket", cfg.BucketDur,
		"refresh", cfg.RefreshEvery,
	)

	sl := stoplist.NewWithInitial(cfg.InitialStopList)
	metrics.StopListSize.Set(float64(sl.Size()))

	rnk := ranker.New(ranker.Config{
		WindowDur:    cfg.WindowDur,
		BucketDur:    cfg.BucketDur,
		SnapshotSize: cfg.SnapshotSize,
	}, sl)

	rootCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		rnk.Run(rootCtx, cfg.RefreshEvery, cfg.GCEvery)
	}()

	cons := consumer.New(consumer.Config{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaTopic,
		GroupID:  cfg.KafkaGroupID,
		MinBytes: cfg.KafkaMinBytes,
		MaxBytes: cfg.KafkaMaxBytes,
	}, rnk, log)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := cons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("consumer stopped", "err", err)
		}
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(rnk, sl, log, cfg.DefaultTopN, cfg.MaxTopN).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			cancel()
		}
	}()

	<-rootCtx.Done()
	log.Info("shutdown signal received")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutCancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	if err := cons.Close(); err != nil {
		log.Error("consumer close", "err", err)
	}

	wg.Wait()
	log.Info("trendsd stopped")
}
