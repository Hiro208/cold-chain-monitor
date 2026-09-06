//go:build integration

// Package e2e runs the ingest pipeline against real Kafka, Postgres and
// Redis and checks the end-to-end breach detection objective.
package e2e

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/guangjiandong/cold-chain-monitor/internal/cache"
	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/ingest"
	"github.com/guangjiandong/cold-chain-monitor/internal/simulator"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
	"github.com/guangjiandong/cold-chain-monitor/internal/testenv"
)

const (
	readingsTopic = "reefer.readings.v1"
	breachesTopic = "reefer.breaches.v1"
	dlqTopic      = "reefer.readings.dlq.v1"
	fleetSize     = 2000
	// detectionSLO is the resume/README claim this test guards.
	detectionSLO = 2 * time.Second
)

func TestPipelineDetectsBreachesWithinSLO(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	log := testenv.Logger(t)

	pool := testenv.Postgres(t)
	rdb := testenv.Redis(t)
	brokers := testenv.Kafka(t)
	st := store.New(pool)

	// Register the fleet.
	fleet := simulator.NewFleet(fleetSize, 0, 99)
	for _, u := range fleet.Units {
		if _, err := st.CreateContainer(ctx, u.Container); err != nil {
			t.Fatal(err)
		}
	}

	producer, err := stream.NewProducer(stream.Conn(brokers, false), kgo.DefaultProduceTopic(readingsTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	topics := append(stream.DefaultTopics(readingsTopic, breachesTopic, 6), stream.TopicSpec{Name: dlqTopic, Partitions: 1, Replication: -1})
	if err := stream.EnsureTopics(ctx, producer, topics...); err != nil {
		t.Fatal(err)
	}

	// Start the ingest worker.
	metrics := ingest.NewMetrics(prometheus.NewRegistry())
	worker, err := ingest.NewWorker(ingest.WorkerConfig{
		Conn: stream.Conn(brokers, false), Group: "e2e-ingest",
		ReadingsTopic: readingsTopic, BreachesTopic: breachesTopic, DLQTopic: dlqTopic,
		BatchSize: 500, BatchWait: 100 * time.Millisecond,
	}, log, metrics)
	if err != nil {
		t.Fatal(err)
	}
	registry := ingest.NewRegistry(st)
	if _, err := registry.Warm(ctx); err != nil {
		t.Fatal(err)
	}
	worker.SetProcessor(ingest.NewProcessor(st, cache.New(rdb, time.Hour), registry, worker, 0.5, log).WithMetrics(metrics))
	workerCtx, stopWorker := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(workerCtx) }()
	defer func() { stopWorker(); <-done }()

	// Consume breach events.
	events, err := kgo.NewClient(append(stream.Conn(brokers, false),
		kgo.ConsumeTopics(breachesTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()

	// Phase 1: one healthy reading per container (all in band).
	start := time.Now().UTC().Truncate(time.Millisecond)
	produceAll(ctx, t, producer, fleet, func(i int) domain.Reading {
		return domain.Reading{ContainerID: fleet.Units[i].Container.ID, RecordedAt: start, TemperatureC: fleet.Units[i].Container.SetpointC, HumidityPct: 60}
	})

	// Phase 2: every 10th container goes too warm; stamp each reading at send time.
	var expected []string
	for i := 0; i < fleetSize; i += 10 {
		expected = append(expected, fleet.Units[i].Container.ID)
	}
	produceAll(ctx, t, producer, fleet, func(i int) domain.Reading {
		c := fleet.Units[i].Container
		temp := c.SetpointC
		if i%10 == 0 {
			temp = c.MaxTempC + 2
		}
		return domain.Reading{ContainerID: c.ID, RecordedAt: time.Now().UTC(), TemperatureC: temp, HumidityPct: 60}
	})

	// Collect breach.opened events.
	latencies := map[string]time.Duration{}
	deadline := time.Now().Add(60 * time.Second)
	for len(latencies) < len(expected) && time.Now().Before(deadline) {
		pollCtx, cancelPoll := context.WithTimeout(ctx, 2*time.Second)
		fetches := events.PollFetches(pollCtx)
		cancelPoll()
		fetches.EachRecord(func(r *kgo.Record) {
			ev, err := stream.DecodeBreachEvent(r.Value)
			if err != nil {
				t.Errorf("bad breach event: %v", err)
				return
			}
			if ev.Type == stream.BreachOpened {
				latencies[ev.Breach.ContainerID] = time.Duration(ev.LatencyMs) * time.Millisecond
			}
		})
	}
	if len(latencies) != len(expected) {
		t.Fatalf("got %d breach events, want %d", len(latencies), len(expected))
	}
	for _, id := range expected {
		if _, ok := latencies[id]; !ok {
			t.Fatalf("no breach event for %s", id)
		}
	}

	all := make([]time.Duration, 0, len(latencies))
	for _, d := range latencies {
		all = append(all, d)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p50, p99, worst := all[len(all)/2], all[len(all)*99/100], all[len(all)-1]
	t.Logf("breach detection latency over %d breaches: p50=%v p99=%v max=%v", len(all), p50, p99, worst)
	if p99 > detectionSLO {
		t.Fatalf("p99 detection latency %v exceeds %v", p99, detectionSLO)
	}

	// Redis reflects the latest state and Postgres has every reading.
	c := cache.New(rdb, time.Hour)
	stState, err := c.Latest(ctx, expected[0])
	if err != nil || !stState.Breach.Open {
		t.Fatalf("cached state for %s: %+v %v", expected[0], stState, err)
	}
	var rows int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM readings").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2*fleetSize {
		t.Fatalf("readings rows = %d, want %d", rows, 2*fleetSize)
	}
}

func produceAll(ctx context.Context, t *testing.T, cl *kgo.Client, fleet *simulator.Fleet, mk func(i int) domain.Reading) {
	t.Helper()
	recs := make([]*kgo.Record, 0, len(fleet.Units))
	for i := range fleet.Units {
		r := mk(i)
		v, err := stream.EncodeReading(r)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, &kgo.Record{Key: []byte(r.ContainerID), Value: v})
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}
