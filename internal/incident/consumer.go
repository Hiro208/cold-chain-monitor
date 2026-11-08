package incident

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

// Applier persists a batch of breach events.
type Applier interface {
	Apply(ctx context.Context, events []stream.BreachEvent) (BulkResult, error)
}

// Metrics instruments the consumer.
type Metrics struct {
	Events    *prometheus.CounterVec
	Malformed prometheus.Counter
	BatchSecs prometheus.Histogram
}

// NewMetrics registers consumer metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "coldchain_incident_events_total", Help: "Breach events applied to the incident store.",
		}, []string{"type"}),
		Malformed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "coldchain_incident_malformed_total", Help: "Breach-topic messages that could not be decoded.",
		}),
		BatchSecs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "coldchain_incident_batch_seconds", Help: "Time to apply one batch to MongoDB.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}),
	}
	reg.MustRegister(m.Events, m.Malformed, m.BatchSecs)
	return m
}

// Consumer reads the breaches topic in its own consumer group and projects
// it into MongoDB. Offsets are committed only after a batch is durable, so
// delivery is at-least-once; Apply makes replays harmless.
type Consumer struct {
	cl    *kgo.Client
	store Applier
	log   *slog.Logger
	m     *Metrics
	batch int
}

// NewConsumer creates the Kafka client.
func NewConsumer(conn []kgo.Opt, group, topic string, batch int, st Applier, log *slog.Logger, m *Metrics) (*Consumer, error) {
	opts := append(append([]kgo.Opt{}, conn...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl, store: st, log: log, m: m, batch: batch}, nil
}

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	// BlockRebalanceOnPoll makes Close wait for a rebalance that only
	// AllowRebalance can release, so allow it first or shutdown hangs.
	defer func() {
		c.cl.AllowRebalance()
		c.cl.Close()
	}()
	for {
		fetches := c.cl.PollRecords(ctx, c.batch)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				c.log.Error("fetch error", "topic", t, "partition", p, "err", err)
			}
		})
		records := fetches.Records()
		if len(records) > 0 {
			if err := c.handle(ctx, records); err != nil {
				return err
			}
		}
		c.cl.AllowRebalance()
	}
}

func (c *Consumer) handle(ctx context.Context, records []*kgo.Record) error {
	events := Decode(records, c.log, c.m.Malformed)
	start := time.Now()
	backoff := 200 * time.Millisecond
	for {
		res, err := c.store.Apply(ctx, events)
		if err == nil {
			c.log.Debug("applied incidents", "events", len(events), "upserted", res.Upserted, "modified", res.Modified)
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		c.log.Error("apply incidents failed; retrying", "err", err, "events", len(events), "backoff", backoff)
		sleep(ctx, backoff)
		backoff = min(backoff*2, 10*time.Second)
	}
	c.m.BatchSecs.Observe(time.Since(start).Seconds())
	for _, ev := range events {
		c.m.Events.WithLabelValues(string(ev.Type)).Inc()
	}
	if err := c.cl.CommitRecords(ctx, records...); err != nil {
		// Not fatal: the batch is redelivered and re-applied idempotently.
		c.log.Warn("commit offsets failed", "err", err)
	}
	return nil
}

// Decode parses breach events, skipping (and counting) malformed messages.
// A message the ingest service could not have produced is not worth
// blocking the partition for; the source of truth is still Postgres.
func Decode(records []*kgo.Record, log *slog.Logger, malformed prometheus.Counter) []stream.BreachEvent {
	out := make([]stream.BreachEvent, 0, len(records))
	for _, rec := range records {
		ev, err := stream.DecodeBreachEvent(rec.Value)
		if err != nil {
			malformed.Inc()
			log.Warn("skip malformed breach event", "partition", rec.Partition, "offset", rec.Offset, "err", err)
			continue
		}
		out = append(out, ev)
	}
	return out
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
