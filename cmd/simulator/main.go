// Command simulator registers a fleet of simulated reefers through the REST
// API and streams their telemetry into Kafka at a fixed cadence.
//
// Sends are spread evenly across the interval (2,000 units every 5 s is a
// steady 400 msg/s) instead of bursting once per tick, which is closer to
// what a real fleet looks like.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/guangjiandong/cold-chain-monitor/internal/config"
	"github.com/guangjiandong/cold-chain-monitor/internal/platform"
	"github.com/guangjiandong/cold-chain-monitor/internal/simulator"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "simulator:", err)
		os.Exit(1)
	}
}

func run() error {
	log := platform.NewLogger("simulator")
	cfg, err := config.LoadSimulator()
	if err != nil {
		return err
	}
	ctx, stop := platform.SignalContext()
	defer stop()

	fleet := simulator.NewFleet(cfg.Containers, cfg.BreachRate, 2026)

	if cfg.Register {
		if err := register(ctx, log, cfg.APIBaseURL, fleet); err != nil {
			return err
		}
	}

	cl, err := stream.NewProducer(stream.Conn(cfg.Kafka.Brokers, cfg.Kafka.TLS), kgo.DefaultProduceTopic(cfg.Kafka.ReadingsTopic))
	if err != nil {
		return err
	}
	defer cl.Close()

	var sent, failed atomic.Int64
	go reportProgress(ctx, log, &sent, &failed)

	n := len(fleet.Units)
	gap := cfg.Interval / time.Duration(n)
	log.Info("streaming telemetry", "containers", n, "interval", cfg.Interval, "rate_per_s", float64(n)/cfg.Interval.Seconds())

	ticker := time.NewTicker(gap)
	defer ticker.Stop()
	for i := 0; ; i = (i + 1) % n {
		select {
		case <-ctx.Done():
			log.Info("flushing producer")
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return cl.Flush(flushCtx)
		case now := <-ticker.C:
			r := fleet.Step(i, now)
			val, err := stream.EncodeReading(r)
			if err != nil {
				return err
			}
			cl.Produce(ctx, &kgo.Record{Key: []byte(r.ContainerID), Value: val}, func(_ *kgo.Record, err error) {
				if err != nil {
					failed.Add(1)
					return
				}
				sent.Add(1)
			})
		}
	}
}

// register creates every simulated container via the API. 409 Conflict means
// it already exists from a previous run, which is fine.
func register(ctx context.Context, log *slog.Logger, baseURL string, fleet *simulator.Fleet) error {
	client := &http.Client{Timeout: 10 * time.Second}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	var created atomic.Int64
	var mu sync.Mutex
	var firstErr error
	for _, u := range fleet.Units {
		c := u.Container
		g.Go(func() error {
			body, _ := json.Marshal(map[string]any{
				"id": c.ID, "owner": c.Owner, "commodity": c.Commodity,
				"setpoint_c": c.SetpointC, "min_temp_c": c.MinTempC, "max_temp_c": c.MaxTempC,
			})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/containers", bytes.NewReader(body))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("register %s: %w", c.ID, err)
			}
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusCreated:
				created.Add(1)
			case http.StatusConflict:
			default:
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("register %s: unexpected status %d", c.ID, resp.StatusCode)
				}
				mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if firstErr != nil {
		return firstErr
	}
	log.Info("registered fleet", "total", len(fleet.Units), "new", created.Load())
	return nil
}

func reportProgress(ctx context.Context, log *slog.Logger, sent, failed *atomic.Int64) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := sent.Load()
			log.Info("progress", "sent", s, "failed", failed.Load(), "rate_per_s", float64(s-last)/10)
			last = s
		}
	}
}
