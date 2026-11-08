// Command incidents projects the breaches topic into a MongoDB incident log.
package main

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/sync/errgroup"

	"github.com/guangjiandong/cold-chain-monitor/internal/config"
	"github.com/guangjiandong/cold-chain-monitor/internal/incident"
	"github.com/guangjiandong/cold-chain-monitor/internal/platform"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "incidents:", err)
		os.Exit(1)
	}
}

func run() error {
	log := platform.NewLogger("incidents")
	cfg, err := config.LoadIncidents()
	if err != nil {
		return err
	}
	ctx, stop := platform.SignalContext()
	defer stop()

	mc, err := incident.Connect(ctx, cfg.Mongo.URI)
	if err != nil {
		return err
	}
	defer func() { _ = mc.Disconnect(context.Background()) }()

	st := incident.NewStore(mc.Database(cfg.Mongo.Database), cfg.Mongo.Retention)
	if err := st.EnsureIndexes(ctx); err != nil {
		return err
	}

	reg := platform.NewMetricsRegistry()
	consumer, err := incident.NewConsumer(stream.Conn(cfg.Kafka.Brokers, cfg.Kafka.TLS),
		cfg.Kafka.ConsumerGroup, cfg.Kafka.BreachesTopic, cfg.BatchSize, st, log, incident.NewMetrics(reg))
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return platform.Serve(ctx, platform.NewHTTPServer(cfg.MetricsAddr, platform.MetricsHandler(reg)), platform.DefaultShutdownTimeout, log)
	})
	g.Go(func() error {
		log.Info("projecting incidents", "topic", cfg.Kafka.BreachesTopic, "database", cfg.Mongo.Database)
		return consumer.Run(ctx)
	})
	return g.Wait()
}
