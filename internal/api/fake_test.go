package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/cache"
	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

// fakeStore is an in-memory Store used by handler tests. Behaviour that
// depends on real SQL is covered by the integration tests.
type fakeStore struct {
	mu         sync.Mutex
	containers map[string]domain.Container
	readings   []domain.Reading
	breaches   []domain.Breach
}

func newFakeStore() *fakeStore {
	return &fakeStore{containers: map[string]domain.Container{}}
}

func (f *fakeStore) Ping(context.Context) error { return nil }

func (f *fakeStore) CreateContainer(_ context.Context, c domain.Container) (domain.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.containers[c.ID]; ok {
		return domain.Container{}, store.ErrConflict
	}
	c.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.containers[c.ID] = c
	return c, nil
}

func (f *fakeStore) GetContainer(_ context.Context, id string) (domain.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[id]
	if !ok {
		return domain.Container{}, store.ErrNotFound
	}
	return c, nil
}

func (f *fakeStore) ListContainers(_ context.Context, flt store.ContainerFilter) ([]domain.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.Container
	for _, c := range f.containers {
		if (flt.Owner == "" || c.Owner == flt.Owner) && c.ID > flt.After {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > flt.Limit {
		out = out[:flt.Limit]
	}
	return out, nil
}

func (f *fakeStore) ReadingsInRange(_ context.Context, q store.ReadingRange) ([]domain.Reading, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.Reading
	for _, r := range f.readings {
		if r.ContainerID == q.ContainerID && !r.RecordedAt.Before(q.From) && r.RecordedAt.Before(q.To) && r.RecordedAt.After(q.After) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RecordedAt.Before(out[j].RecordedAt) })
	if len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

func (f *fakeStore) ListBreaches(_ context.Context, flt store.BreachFilter) ([]domain.Breach, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.Breach
	for i := len(f.breaches) - 1; i >= 0; i-- { // newest (highest id) first
		b := f.breaches[i]
		if (flt.ContainerID == "" || b.ContainerID == flt.ContainerID) &&
			(!flt.OpenOnly || b.EndedAt == nil) &&
			!b.StartedAt.Before(flt.Since) &&
			(flt.BeforeID == 0 || b.ID < flt.BeforeID) {
			out = append(out, b)
		}
	}
	if len(out) > flt.Limit {
		out = out[:flt.Limit]
	}
	return out, nil
}

// fakeCache is an in-memory StateCache.
type fakeCache struct {
	mu     sync.Mutex
	states map[string]cache.State
}

func newFakeCache() *fakeCache { return &fakeCache{states: map[string]cache.State{}} }

func (f *fakeCache) Ping(context.Context) error { return nil }

func (f *fakeCache) Latest(_ context.Context, id string) (cache.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.states[id]
	if !ok {
		return cache.State{}, cache.ErrMiss
	}
	return st, nil
}

var testNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func newTestServer(t *testing.T, st Store, opts ...Option) http.Handler {
	t.Helper()
	return newTestServerWithCache(t, st, newFakeCache(), opts...)
}

func newTestServerWithCache(t *testing.T, st Store, sc StateCache, opts ...Option) http.Handler {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts = append([]Option{WithClock(func() time.Time { return testNow })}, opts...)
	return NewServer(st, sc, log, opts...).Handler()
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}
