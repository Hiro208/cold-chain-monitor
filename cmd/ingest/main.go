// Command ingest consumes container telemetry from Kafka, stores it in
// Postgres, keeps the latest state in Redis and publishes breach events.
package main

import (
	"fmt"
	"os"

	"golang.org/x/sync/errgroup"

	"github.com/guangjiandong/cold-chain-monitor/internal/cache"
	"github.com/guangjiandong/cold-chain-monitor/internal/config"
	"github.com/guangjiandong/cold-chain-monitor/internal/ingest"
	"github.com/guangjiandong/cold-chain-monitor/internal/platform"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ingest:", err)
		os.Exit(1)
	}
}

func run() error {
	log := platform.NewLogger("ingest")
	cfg, err := config.LoadIngest()
	if err != nil {
		return err
	}
	ctx, stop := platform.SignalContext()
	defer stop()

	pool, err := store.Connect(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)

	rdb, err := cache.Dial(ctx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		return err
	}
	defer rdb.Close()

	reg := platform.NewMetricsRegistry()
	metrics := ingest.NewMetrics(reg)

	worker, err := ingest.NewWorker(ingest.WorkerConfig{
		Brokers:       cfg.Kafka.Brokers,
		Group:         cfg.Kafka.ConsumerGroup,
		ReadingsTopic: cfg.Kafka.ReadingsTopic,
		BreachesTopic: cfg.Kafka.BreachesTopic,
		DLQTopic:      cfg.Kafka.DLQTopic,
		BatchSize:     cfg.BatchSize,
		BatchWait:     cfg.BatchWait,
	}, log, metrics)
	if err != nil {
		return err
	}

	topics := stream.DefaultTopics(cfg.Kafka.ReadingsTopic, cfg.Kafka.BreachesTopic, cfg.Kafka.Partitions)
	topics = append(topics, stream.TopicSpec{Name: cfg.Kafka.DLQTopic, Partitions: 3, Replication: -1})
	if err := stream.EnsureTopics(ctx, worker.Client(), topics...); err != nil {
		return err
	}

	registry := ingest.NewRegistry(st)
	n, err := registry.Warm(ctx)
	if err != nil {
		return fmt.Errorf("warm container registry: %w", err)
	}
	log.Info("container registry warmed", "containers", n)

	proc := ingest.NewProcessor(st, cache.New(rdb, cfg.Redis.StateTTL), registry, worker, cfg.HysteresisC, log).WithMetrics(metrics)
	worker.SetProcessor(proc)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return platform.Serve(ctx, platform.NewHTTPServer(cfg.MetricsAddr, platform.MetricsHandler(reg)), platform.DefaultShutdownTimeout, log)
	})
	g.Go(func() error {
		log.Info("consuming", "topic", cfg.Kafka.ReadingsTopic, "group", cfg.Kafka.ConsumerGroup)
		return worker.Run(ctx)
	})
	return g.Wait()
}
