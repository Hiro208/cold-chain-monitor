// Package api implements the public REST API.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/cache"
	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

// Store is the subset of the persistence layer the API needs. Declaring it
// here keeps handlers unit-testable with an in-memory fake.
type Store interface {
	Ping(ctx context.Context) error
	CreateContainer(ctx context.Context, c domain.Container) (domain.Container, error)
	GetContainer(ctx context.Context, id string) (domain.Container, error)
	ListContainers(ctx context.Context, f store.ContainerFilter) ([]domain.Container, error)
	ReadingsInRange(ctx context.Context, q store.ReadingRange) ([]domain.Reading, error)
}

// StateCache serves the latest state of a container.
type StateCache interface {
	Ping(ctx context.Context) error
	Latest(ctx context.Context, id string) (cache.State, error)
}

// Server wires handlers to their dependencies.
type Server struct {
	store Store
	cache StateCache
	log   *slog.Logger
	now   func() time.Time
}

// Option customises a Server.
type Option func(*Server)

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option { return func(s *Server) { s.now = now } }

// NewServer constructs a Server.
func NewServer(st Store, sc StateCache, log *slog.Logger, opts ...Option) *Server {
	s := &Server{store: st, cache: sc, log: log, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Handler returns the root HTTP handler with middleware applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)

	mux.HandleFunc("POST /v1/containers", s.createContainer)
	mux.HandleFunc("GET /v1/containers", s.listContainers)
	mux.HandleFunc("GET /v1/containers/{id}", s.getContainer)
	mux.HandleFunc("GET /v1/containers/{id}/readings", s.listReadings)
	mux.HandleFunc("GET /v1/containers/{id}/state", s.getState)

	return withRequestContext(s.log, recoverPanics(mux))
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// readyz fails when a hard dependency is unreachable so Kubernetes stops
// routing traffic to this pod.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "postgres unavailable")
		return
	}
	if err := s.cache.Ping(ctx); err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "redis unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
