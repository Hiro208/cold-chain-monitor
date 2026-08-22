package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/guangjiandong/cold-chain-monitor/internal/cache"
	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

// memStore implements ReadingStore and ContainerSource in memory, mimicking
// the idempotency guarantees of the SQL implementation.
type memStore struct {
	mu         sync.Mutex
	containers map[string]domain.Container
	readings   map[string]domain.Reading // key: id|ts
	breaches   []domain.Breach
	failInsert error
	lookups    int
}

func newMemStore(cs ...domain.Container) *memStore {
	m := &memStore{containers: map[string]domain.Container{}, readings: map[string]domain.Reading{}}
	for _, c := range cs {
		m.containers[c.ID] = c
	}
	return m
}

func (m *memStore) GetContainer(_ context.Context, id string) (domain.Container, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lookups++
	c, ok := m.containers[id]
	if !ok {
		return domain.Container{}, store.ErrNotFound
	}
	return c, nil
}

func (m *memStore) ListContainers(_ context.Context, f store.ContainerFilter) ([]domain.Container, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []domain.Container
	for _, c := range m.containers {
		if c.ID > f.After {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

func (m *memStore) InsertReadings(_ context.Context, batch []domain.Reading) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failInsert != nil {
		return 0, m.failInsert
	}
	var n int64
	for _, r := range batch {
		k := r.ContainerID + "|" + r.RecordedAt.String()
		if _, dup := m.readings[k]; !dup {
			m.readings[k] = r
			n++
		}
	}
	return n, nil
}

func (m *memStore) ApplyBreachTransitions(_ context.Context, ts []store.BreachTransition) ([]domain.Breach, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []domain.Breach
	for _, t := range ts {
		switch t.Type {
		case domain.TransitionOpened:
			for i := range m.breaches {
				b := &m.breaches[i]
				if b.ContainerID == t.ContainerID && b.StartedAt.Equal(t.At) {
					out = append(out, *b)
					goto next
				}
			}
			for i := range m.breaches {
				if b := &m.breaches[i]; b.ContainerID == t.ContainerID && b.EndedAt == nil {
					at := t.At
					b.EndedAt = &at
				}
			}
			m.breaches = append(m.breaches, domain.Breach{
				ID: int64(len(m.breaches) + 1), ContainerID: t.ContainerID, Kind: t.Kind,
				StartedAt: t.At, PeakTempC: t.PeakTempC, LimitC: t.LimitC,
			})
			out = append(out, m.breaches[len(m.breaches)-1])
		case domain.TransitionClosed:
			for i := range m.breaches {
				if b := &m.breaches[i]; b.ContainerID == t.ContainerID && b.EndedAt == nil {
					at := t.At
					b.EndedAt, b.PeakTempC = &at, t.PeakTempC
					out = append(out, *b)
				}
			}
		}
	next:
	}
	return out, nil
}

type memPublisher struct{ events []stream.BreachEvent }

func (p *memPublisher) PublishBreachEvents(_ context.Context, ev []stream.BreachEvent) error {
	p.events = append(p.events, ev...)
	return nil
}

var (
	t0      = time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)
	vaccine = domain.Container{ID: "CSQU3054383", Owner: "acme", Commodity: "vaccines", SetpointC: 5, MinTempC: 2, MaxTempC: 8}
)

type harness struct {
	p     *Processor
	store *memStore
	pub   *memPublisher
	cache *cache.Cache
	clock time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	h := &harness{store: newMemStore(vaccine), pub: &memPublisher{}, cache: cache.New(rdb, 0), clock: t0}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h.p = NewProcessor(h.store, h.cache, NewRegistry(h.store), h.pub, 0.5, log)
	h.p.now = func() time.Time { return h.clock }
	return h
}

func rd(sec int, temp float64) domain.Reading {
	return domain.Reading{ContainerID: vaccine.ID, RecordedAt: t0.Add(time.Duration(sec) * time.Second), TemperatureC: temp, HumidityPct: 70}
}

func TestProcessDetectsBreachAndRecovery(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	h.clock = t0.Add(3*time.Second + 800*time.Millisecond)
	res, err := h.p.Process(ctx, []domain.Reading{rd(3, 9.2), rd(1, 5), rd(2, 7.9)}) // out of order on purpose
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 3 || len(res.Events) != 1 {
		t.Fatalf("res = %+v", res)
	}
	ev := res.Events[0]
	if ev.Type != stream.BreachOpened || ev.Breach.Kind != domain.BreachTooWarm || ev.Owner != "acme" {
		t.Fatalf("unexpected event %+v", ev)
	}
	if ev.LatencyMs != 800 {
		t.Fatalf("latency = %dms, want 800", ev.LatencyMs)
	}
	st, err := h.cache.Latest(ctx, vaccine.ID)
	if err != nil || !st.Breach.Open || st.Latest.TemperatureC != 9.2 {
		t.Fatalf("cache state %+v err %v", st, err)
	}

	h.clock = t0.Add(10 * time.Second)
	res, err = h.p.Process(ctx, []domain.Reading{rd(4, 9.8), rd(5, 7.6), rd(6, 7.4)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Events) != 1 || res.Events[0].Type != stream.BreachClosed || res.Events[0].Breach.PeakTempC != 9.8 {
		t.Fatalf("expected close with peak 9.8, got %+v", res.Events)
	}
}

func TestProcessReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	batch := []domain.Reading{rd(1, 5), rd(2, 9)}
	if _, err := h.p.Process(ctx, batch); err != nil {
		t.Fatal(err)
	}
	res, err := h.p.Process(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 0 || res.Skipped != 2 || len(res.Events) != 0 {
		t.Fatalf("replay produced side effects: %+v", res)
	}
	if len(h.store.breaches) != 1 {
		t.Fatalf("breaches = %d, want 1", len(h.store.breaches))
	}
}

func TestProcessRecoversStateFromRedisAfterRestart(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	if _, err := h.p.Process(ctx, []domain.Reading{rd(1, 9)}); err != nil {
		t.Fatal(err)
	}
	// New processor (e.g. pod restart or rebalance) sharing the same Redis.
	h.p.Forget([]string{vaccine.ID})
	res, err := h.p.Process(ctx, []domain.Reading{rd(2, 9.5)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Events) != 0 {
		t.Fatalf("breach re-opened after restart: %+v", res.Events)
	}
}

func TestProcessRejectsUnknownAndInvalid(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	unknown := rd(1, 5)
	unknown.ContainerID = "ABCU1234560"
	invalid := rd(2, 5)
	invalid.HumidityPct = 150

	for i := 0; i < 3; i++ {
		res, err := h.p.Process(ctx, []domain.Reading{unknown, invalid, rd(3, 5)})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Rejected) != 2 {
			t.Fatalf("rejected = %+v", res.Rejected)
		}
	}
	// Unknown container is negatively cached: one lookup for it, one for vaccine.
	if h.store.lookups != 2 {
		t.Fatalf("lookups = %d, want 2", h.store.lookups)
	}
}

func TestProcessFailureDoesNotAdvanceState(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.store.failInsert = errors.New("db down")
	if _, err := h.p.Process(ctx, []domain.Reading{rd(1, 9)}); err == nil {
		t.Fatal("expected error")
	}
	h.store.failInsert = nil
	res, err := h.p.Process(ctx, []domain.Reading{rd(1, 9)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 1 || len(res.Events) != 1 {
		t.Fatalf("redelivered batch was not reprocessed: %+v", res)
	}
}
