// Package ingest turns raw telemetry from Kafka into persisted history,
// cached latest state and breach events.
package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/cache"
	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

// ReadingStore persists readings and breaches.
type ReadingStore interface {
	InsertReadings(ctx context.Context, batch []domain.Reading) (int64, error)
	ApplyBreachTransitions(ctx context.Context, ts []store.BreachTransition) ([]domain.Breach, error)
}

// StateCache holds the latest state per container.
type StateCache interface {
	Upsert(ctx context.Context, entries []cache.Entry) (int, error)
	States(ctx context.Context, ids []string) (map[string]cache.State, error)
}

// EventPublisher emits breach events (to Kafka in production).
type EventPublisher interface {
	PublishBreachEvents(ctx context.Context, events []stream.BreachEvent) error
}

// Rejected is a reading that could not be processed, with the reason. The
// worker forwards these to a dead-letter topic.
type Rejected struct {
	Reading domain.Reading
	Reason  string
}

// Result summarises one processed batch.
type Result struct {
	Inserted int64
	Skipped  int // replayed or out-of-order readings already evaluated
	Rejected []Rejected
	Events   []stream.BreachEvent
}

// tracked is the detector's in-memory view of one container.
type tracked struct {
	breach domain.BreachState
	lastAt time.Time
}

// Processor applies batches of readings. Batches for a given container must
// be processed sequentially, which Kafka's per-partition ordering plus a
// single goroutine per worker guarantees.
type Processor struct {
	store       ReadingStore
	cache       StateCache
	registry    *Registry
	events      EventPublisher
	hysteresisC float64
	now         func() time.Time
	log         *slog.Logger
	metrics     *Metrics

	states map[string]*tracked
}

// NewProcessor wires a processor.
func NewProcessor(st ReadingStore, sc StateCache, reg *Registry, ev EventPublisher, hysteresisC float64, log *slog.Logger) *Processor {
	return &Processor{
		store: st, cache: sc, registry: reg, events: ev,
		hysteresisC: hysteresisC, now: time.Now, log: log,
		metrics: nopMetrics(),
		states:  map[string]*tracked{},
	}
}

// Forget drops in-memory state for containers, e.g. after their partitions
// were revoked in a consumer-group rebalance. State will be reloaded from
// Redis if the container comes back.
func (p *Processor) Forget(ids []string) {
	for _, id := range ids {
		delete(p.states, id)
	}
}

// Process handles one batch. The steps are ordered so that a crash at any
// point followed by a replay of the same batch converges to the same state:
//
//  1. insert readings (idempotent on primary key)
//  2. persist breach transitions (idempotent, see store)
//  3. update Redis (compare-and-set on timestamp)
//  4. publish breach events (at-least-once; consumers dedupe on breach id)
//
// The caller commits Kafka offsets only after Process returns nil.
func (p *Processor) Process(ctx context.Context, batch []domain.Reading) (Result, error) {
	var res Result
	now := p.now()

	// Validate and resolve containers.
	valid := make([]domain.Reading, 0, len(batch))
	containers := map[string]domain.Container{}
	for _, r := range batch {
		if err := r.Validate(now); err != nil {
			res.Rejected = append(res.Rejected, Rejected{r, err.Error()})
			continue
		}
		c, ok, err := p.registry.Lookup(ctx, r.ContainerID)
		if err != nil {
			return res, fmt.Errorf("lookup container: %w", err)
		}
		if !ok {
			res.Rejected = append(res.Rejected, Rejected{r, "unknown container"})
			continue
		}
		containers[c.ID] = c
		valid = append(valid, r)
	}

	// Evaluate in time order per container. A stable sort keeps arrival
	// order for identical timestamps.
	sort.SliceStable(valid, func(i, j int) bool {
		if valid[i].ContainerID != valid[j].ContainerID {
			return valid[i].ContainerID < valid[j].ContainerID
		}
		return valid[i].RecordedAt.Before(valid[j].RecordedAt)
	})

	if err := p.recoverStates(ctx, containers); err != nil {
		return res, err
	}

	// Evaluate against a staged copy of the detector state. It is committed
	// to p.states only after every side effect succeeded; otherwise a failed
	// batch would be treated as "already seen" when Kafka redelivers it.
	var (
		transitions []store.BreachTransition
		latest      = map[string]cache.Entry{}
		staged      = map[string]tracked{}
	)
	for _, r := range valid {
		st, ok := staged[r.ContainerID]
		if !ok {
			st = *p.states[r.ContainerID]
		}
		if !r.RecordedAt.After(st.lastAt) {
			res.Skipped++
			continue
		}
		band := domain.BandOf(containers[r.ContainerID], p.hysteresisC)
		next, tr := domain.Evaluate(band, st.breach, r)
		switch tr {
		case domain.TransitionOpened:
			transitions = append(transitions, store.BreachTransition{
				Type: tr, ContainerID: r.ContainerID, Kind: next.Kind,
				At: r.RecordedAt, PeakTempC: next.PeakTempC, LimitC: band.Limit(next.Kind),
			})
		case domain.TransitionClosed:
			transitions = append(transitions, store.BreachTransition{
				Type: tr, ContainerID: r.ContainerID, Kind: st.breach.Kind,
				At: r.RecordedAt, PeakTempC: st.breach.PeakTempC, LimitC: band.Limit(st.breach.Kind),
			})
		}
		st.breach, st.lastAt = next, r.RecordedAt
		staged[r.ContainerID] = st
		latest[r.ContainerID] = cache.Entry{Reading: r, Breach: next}
	}

	inserted, err := p.store.InsertReadings(ctx, valid)
	if err != nil {
		return res, err
	}
	res.Inserted = inserted

	breaches, err := p.store.ApplyBreachTransitions(ctx, transitions)
	if err != nil {
		return res, err
	}

	entries := make([]cache.Entry, 0, len(latest))
	for _, e := range latest {
		entries = append(entries, e)
	}
	if _, err := p.cache.Upsert(ctx, entries); err != nil {
		return res, fmt.Errorf("update cache: %w", err)
	}

	detectedAt := p.now()
	for _, b := range breaches {
		typ := stream.BreachOpened
		at := b.StartedAt
		if b.EndedAt != nil {
			typ = stream.BreachClosed
			at = *b.EndedAt
		}
		c := containers[b.ContainerID]
		ev := stream.BreachEvent{
			Type: typ, Breach: b, Owner: c.Owner, Commodity: c.Commodity,
			DetectedAt: detectedAt, LatencyMs: detectedAt.Sub(at).Milliseconds(),
		}
		res.Events = append(res.Events, ev)
		if typ == stream.BreachOpened {
			p.metrics.DetectionLatency.Observe(detectedAt.Sub(at).Seconds())
		}
		p.metrics.BreachTransitions.WithLabelValues(string(typ), string(b.Kind)).Inc()
	}
	if len(res.Events) > 0 {
		if err := p.events.PublishBreachEvents(ctx, res.Events); err != nil {
			return res, fmt.Errorf("publish breach events: %w", err)
		}
	}

	for id, st := range staged {
		*p.states[id] = st
	}

	p.metrics.ReadingsProcessed.Add(float64(len(valid) - res.Skipped))
	p.metrics.ReadingsRejected.Add(float64(len(res.Rejected)))
	p.metrics.ReadingsSkipped.Add(float64(res.Skipped))
	for _, r := range valid {
		p.metrics.IngestLag.Observe(detectedAt.Sub(r.RecordedAt).Seconds())
	}
	return res, nil
}

// recoverStates loads detector state from Redis for containers this
// processor has not seen yet.
func (p *Processor) recoverStates(ctx context.Context, containers map[string]domain.Container) error {
	var missing []string
	for id := range containers {
		if _, ok := p.states[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	cached, err := p.cache.States(ctx, missing)
	if err != nil {
		return fmt.Errorf("recover detector state: %w", err)
	}
	for _, id := range missing {
		st := cached[id]
		p.states[id] = &tracked{breach: st.Breach, lastAt: st.Latest.RecordedAt}
	}
	return nil
}
