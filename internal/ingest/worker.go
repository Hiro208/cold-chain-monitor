package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

// WorkerConfig configures the Kafka consumer loop.
type WorkerConfig struct {
	Conn          []kgo.Opt
	Group         string
	ReadingsTopic string
	BreachesTopic string
	DLQTopic      string
	BatchSize     int
	BatchWait     time.Duration
}

// Worker consumes the readings topic as part of a consumer group and feeds
// batches into a Processor. Offsets are committed manually after a batch has
// been fully processed, giving at-least-once semantics; the processor makes
// replays harmless.
type Worker struct {
	cfg  WorkerConfig
	cl   *kgo.Client
	proc *Processor
	log  *slog.Logger
	m    *Metrics

	// partitionOf remembers which partition each container came from so we
	// can drop in-memory state when that partition is revoked.
	partitionOf map[string]int32

	// Revocations arrive on the client's goroutine; the poll loop, which owns
	// the processor state, applies them between batches.
	revokedMu sync.Mutex
	revoked   map[int32]bool
}

// NewWorker creates the Kafka client. The processor's event publisher is
// usually the Worker itself (see PublishBreachEvents).
func NewWorker(cfg WorkerConfig, log *slog.Logger, m *Metrics) (*Worker, error) {
	w := &Worker{cfg: cfg, log: log, m: m, partitionOf: map[string]int32{}, revoked: map[int32]bool{}}
	opts := append(append([]kgo.Opt{}, cfg.Conn...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.ReadingsTopic),
		kgo.DisableAutoCommit(),
		// Rebalances wait until we call AllowRebalance after a batch, so a
		// partition is never handed to another member mid-batch.
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(cfg.BatchWait),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.OnPartitionsRevoked(w.onRevoked),
		kgo.OnPartitionsLost(w.onRevoked),
		// Producer side, for breach events and the DLQ.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	w.cl = cl
	return w, nil
}

// Client exposes the underlying client (for topic setup and health checks).
func (w *Worker) Client() *kgo.Client { return w.cl }

// SetProcessor attaches the processor (two-phase init because the processor
// publishes through the worker's client).
func (w *Worker) SetProcessor(p *Processor) { w.proc = p }

func (w *Worker) onRevoked(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	w.revokedMu.Lock()
	defer w.revokedMu.Unlock()
	for _, p := range lost[w.cfg.ReadingsTopic] {
		w.revoked[p] = true
	}
}

// applyRevocations drops detector state for containers on partitions this
// member no longer owns. If they come back, state is reloaded from Redis,
// which by then reflects whatever the other member processed.
func (w *Worker) applyRevocations() {
	w.revokedMu.Lock()
	gone := w.revoked
	w.revoked = map[int32]bool{}
	w.revokedMu.Unlock()
	if len(gone) == 0 {
		return
	}
	var ids []string
	for id, p := range w.partitionOf {
		if gone[p] {
			ids = append(ids, id)
			delete(w.partitionOf, id)
		}
	}
	w.proc.Forget(ids)
	w.log.Info("partitions revoked", "partitions", len(gone), "containers_forgotten", len(ids))
}

// Run polls until ctx is cancelled. A processing error is retried with
// backoff without committing, so no reading is lost if Postgres or Redis
// blips.
func (w *Worker) Run(ctx context.Context) error {
	defer w.cl.Close()
	for {
		fetches := w.cl.PollRecords(ctx, w.cfg.BatchSize)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		var fetchErr error
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				w.log.Error("fetch error", "topic", topic, "partition", p, "err", err)
				fetchErr = err
			}
		})
		w.applyRevocations()

		records := fetches.Records()
		if len(records) > 0 {
			if err := w.handle(ctx, records); err != nil {
				return err
			}
		}
		w.cl.AllowRebalance()
		if fetchErr != nil && len(records) == 0 {
			sleep(ctx, time.Second)
		}
	}
}

func (w *Worker) handle(ctx context.Context, records []*kgo.Record) error {
	start := time.Now()
	batch := make([]domain.Reading, 0, len(records))
	var poison []*kgo.Record
	for _, rec := range records {
		r, err := stream.DecodeReading(rec.Value)
		if err != nil {
			poison = append(poison, w.dlqRecord(rec.Value, rec.Key, err.Error()))
			continue
		}
		w.partitionOf[r.ContainerID] = rec.Partition
		batch = append(batch, r)
	}

	backoff := 200 * time.Millisecond
	for {
		res, err := w.proc.Process(ctx, batch)
		if err == nil {
			for _, rj := range res.Rejected {
				val, _ := stream.EncodeReading(rj.Reading)
				poison = append(poison, w.dlqRecord(val, []byte(rj.Reading.ContainerID), rj.Reason))
			}
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		w.log.Error("process batch failed; retrying", "err", err, "batch", len(batch), "backoff", backoff)
		sleep(ctx, backoff)
		backoff = min(backoff*2, 10*time.Second)
	}

	if len(poison) > 0 {
		if err := w.cl.ProduceSync(ctx, poison...).FirstErr(); err != nil {
			return fmt.Errorf("produce to DLQ: %w", err)
		}
	}
	if err := w.cl.CommitRecords(ctx, records...); err != nil {
		// Not fatal: the batch will be redelivered and skipped as a replay.
		w.log.Warn("commit offsets failed", "err", err)
	}
	w.m.BatchDuration.Observe(time.Since(start).Seconds())
	w.m.BatchSize.Observe(float64(len(records)))
	return nil
}

func (w *Worker) dlqRecord(value, key []byte, reason string) *kgo.Record {
	return &kgo.Record{
		Topic:   w.cfg.DLQTopic,
		Key:     key,
		Value:   value,
		Headers: []kgo.RecordHeader{{Key: "reject-reason", Value: []byte(reason)}},
	}
}

// PublishBreachEvents implements EventPublisher by producing synchronously to
// the breaches topic, so offsets are only committed once events are durable.
func (w *Worker) PublishBreachEvents(ctx context.Context, events []stream.BreachEvent) error {
	recs := make([]*kgo.Record, 0, len(events))
	for _, e := range events {
		val, err := stream.EncodeBreachEvent(e)
		if err != nil {
			return err
		}
		recs = append(recs, &kgo.Record{Topic: w.cfg.BreachesTopic, Key: []byte(e.Breach.ContainerID), Value: val})
	}
	return w.cl.ProduceSync(ctx, recs...).FirstErr()
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
