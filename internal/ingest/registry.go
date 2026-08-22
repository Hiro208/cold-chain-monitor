package ingest

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

// ContainerSource is where the registry loads container metadata from.
type ContainerSource interface {
	GetContainer(ctx context.Context, id string) (domain.Container, error)
	ListContainers(ctx context.Context, f store.ContainerFilter) ([]domain.Container, error)
}

// Registry is an in-memory read-through cache of container metadata (band,
// owner, commodity). Registrations are rare and bands never change after
// registration, so entries do not expire. Unknown IDs are negatively cached
// for a short time so a misconfigured device cannot hammer Postgres.
type Registry struct {
	src        ContainerSource
	negTTL     time.Duration
	now        func() time.Time
	mu         sync.RWMutex
	containers map[string]domain.Container
	unknown    map[string]time.Time
}

// NewRegistry returns an empty registry.
func NewRegistry(src ContainerSource) *Registry {
	return &Registry{
		src:        src,
		negTTL:     time.Minute,
		now:        time.Now,
		containers: map[string]domain.Container{},
		unknown:    map[string]time.Time{},
	}
}

// Warm preloads every registered container, paging through the table.
func (r *Registry) Warm(ctx context.Context) (int, error) {
	after := ""
	n := 0
	for {
		page, err := r.src.ListContainers(ctx, store.ContainerFilter{After: after, Limit: 1000})
		if err != nil {
			return n, err
		}
		r.mu.Lock()
		for _, c := range page {
			r.containers[c.ID] = c
		}
		r.mu.Unlock()
		n += len(page)
		if len(page) < 1000 {
			return n, nil
		}
		after = page[len(page)-1].ID
	}
}

// Lookup returns the container, loading it on a miss. ok is false for
// containers that are not registered.
func (r *Registry) Lookup(ctx context.Context, id string) (c domain.Container, ok bool, err error) {
	r.mu.RLock()
	c, ok = r.containers[id]
	negUntil, neg := r.unknown[id]
	r.mu.RUnlock()
	if ok {
		return c, true, nil
	}
	if neg && r.now().Before(negUntil) {
		return domain.Container{}, false, nil
	}

	c, err = r.src.GetContainer(ctx, id)
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case errors.Is(err, store.ErrNotFound):
		r.unknown[id] = r.now().Add(r.negTTL)
		return domain.Container{}, false, nil
	case err != nil:
		return domain.Container{}, false, err
	}
	delete(r.unknown, id)
	r.containers[id] = c
	return c, true, nil
}
