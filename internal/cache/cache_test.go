package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

func newTestCache(t *testing.T) (*Cache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, time.Hour), mr
}

func reading(id string, at time.Time, temp float64) domain.Reading {
	return domain.Reading{ContainerID: id, RecordedAt: at, TemperatureC: temp, HumidityPct: 60}
}

func TestUpsertIgnoresStaleReadings(t *testing.T) {
	ctx := context.Background()
	c, _ := newTestCache(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	n, err := c.Upsert(ctx, []Entry{
		{Reading: reading("A", t0.Add(time.Minute), 5)},
		{Reading: reading("B", t0, -18)},
	})
	if err != nil || n != 2 {
		t.Fatalf("first upsert: n=%d err=%v", n, err)
	}

	// Older reading for A arrives late: must be ignored.
	n, err = c.Upsert(ctx, []Entry{{Reading: reading("A", t0, 99)}})
	if err != nil || n != 0 {
		t.Fatalf("stale upsert: n=%d err=%v", n, err)
	}
	st, err := c.Latest(ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	if st.Latest.TemperatureC != 5 {
		t.Fatalf("stale reading overwrote cache: %+v", st.Latest)
	}

	// Newer reading with an open breach is applied.
	bs := domain.BreachState{Open: true, Kind: domain.BreachTooWarm, StartedAt: t0.Add(2 * time.Minute), PeakTempC: 9}
	if _, err := c.Upsert(ctx, []Entry{{Reading: reading("A", t0.Add(2*time.Minute), 9), Breach: bs}}); err != nil {
		t.Fatal(err)
	}
	st, _ = c.Latest(ctx, "A")
	if st.Latest.TemperatureC != 9 || !st.Breach.Open || st.Breach.Kind != domain.BreachTooWarm {
		t.Fatalf("unexpected state %+v", st)
	}
}

func TestLatestMiss(t *testing.T) {
	c, _ := newTestCache(t)
	if _, err := c.Latest(context.Background(), "nope"); !errors.Is(err, ErrMiss) {
		t.Fatalf("err = %v, want ErrMiss", err)
	}
}

func TestStateExpires(t *testing.T) {
	ctx := context.Background()
	c, mr := newTestCache(t)
	if _, err := c.Upsert(ctx, []Entry{{Reading: reading("A", time.Now(), 4)}}); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(2 * time.Hour)
	if _, err := c.Latest(ctx, "A"); !errors.Is(err, ErrMiss) {
		t.Fatalf("expected expiry, got %v", err)
	}
}

func TestScriptReloadAfterFlush(t *testing.T) {
	ctx := context.Background()
	c, _ := newTestCache(t)
	if _, err := c.Upsert(ctx, []Entry{{Reading: reading("A", time.Now(), 4)}}); err != nil {
		t.Fatal(err)
	}
	// Simulate a Redis restart / failover that drops the script cache.
	if err := c.rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Upsert(ctx, []Entry{{Reading: reading("A", time.Now(), 5)}}); err != nil || n != 1 {
		t.Fatalf("upsert after script flush: n=%d err=%v", n, err)
	}
}

func TestStates(t *testing.T) {
	ctx := context.Background()
	c, _ := newTestCache(t)
	bs := domain.BreachState{Open: true, Kind: domain.BreachTooCold, PeakTempC: -30}
	if _, err := c.Upsert(ctx, []Entry{{Reading: reading("A", time.Now(), -30), Breach: bs}}); err != nil {
		t.Fatal(err)
	}
	got, err := c.States(ctx, []string{"A", "B"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got["A"].Breach.Open || got["A"].Breach.Kind != domain.BreachTooCold || got["A"].Latest.TemperatureC != -30 {
		t.Fatalf("States = %+v", got)
	}
}
