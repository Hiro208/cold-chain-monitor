//go:build integration

// Package testenv starts throwaway Postgres, Redis, Kafka and MongoDB containers for
// integration tests via testcontainers-go. Requires a Docker daemon.
package testenv

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

// Logger discards output unless the test runs with -v.
func Logger(t *testing.T) *slog.Logger {
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(testWriter{t}, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }

// Postgres starts Postgres 16, applies migrations and returns a pool.
func Postgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("coldchain"),
		tcpostgres.WithUsername("coldchain"),
		tcpostgres.WithPassword("coldchain"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := store.Connect(ctx, dsn, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := store.Migrate(ctx, pool, Logger(t)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

// Redis starts Redis 7 and returns a client.
func Redis(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	uri, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// Kafka starts a single-node KRaft broker and returns its bootstrap address.
func Kafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("coldchain-test"))
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return brokers
}

// Mongo starts a single-node MongoDB 7.0 replica set (so majority writes
// behave as in production) and returns its connection string. MongoDB 8.x
// refuses to start on Linux 6.19+ kernels (SERVER-121912), as on current CI
// runners, so tests pin 7.0.
func Mongo(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcmongo.Run(ctx, "mongo:7.0", tcmongo.WithReplicaSet("rs0"))
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start mongo: %v", err)
	}
	uri, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return uri
}
