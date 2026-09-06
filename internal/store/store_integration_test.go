//go:build integration

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
	"github.com/guangjiandong/cold-chain-monitor/internal/testenv"
)

var vaccine = domain.Container{ID: "CSQU3054383", Owner: "acme", Commodity: "vaccines", SetpointC: 5, MinTempC: 2, MaxTempC: 8}

func TestStoreAgainstPostgres(t *testing.T) {
	ctx := context.Background()
	pool := testenv.Postgres(t)
	st := store.New(pool)

	// Migrations are idempotent.
	if err := store.Migrate(ctx, pool, testenv.Logger(t)); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	t.Run("containers", func(t *testing.T) {
		if _, err := st.CreateContainer(ctx, vaccine); err != nil {
			t.Fatal(err)
		}
		if _, err := st.CreateContainer(ctx, vaccine); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("duplicate: err = %v", err)
		}
		got, err := st.GetContainer(ctx, vaccine.ID)
		if err != nil || got.MaxTempC != 8 || got.CreatedAt.IsZero() {
			t.Fatalf("get = %+v, %v", got, err)
		}
		if _, err := st.GetContainer(ctx, "ABCU0000000"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("missing: err = %v", err)
		}
	})

	t0 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	t.Run("readings range and idempotent insert", func(t *testing.T) {
		var batch []domain.Reading
		for i := 0; i < 100; i++ {
			batch = append(batch, domain.Reading{ContainerID: vaccine.ID, RecordedAt: t0.Add(time.Duration(i) * time.Minute), TemperatureC: 4 + float64(i%3), HumidityPct: 70})
		}
		n, err := st.InsertReadings(ctx, batch)
		if err != nil || n != 100 {
			t.Fatalf("insert: n=%d err=%v", n, err)
		}
		n, err = st.InsertReadings(ctx, batch[:10])
		if err != nil || n != 0 {
			t.Fatalf("replay insert: n=%d err=%v", n, err)
		}

		page1, err := st.ReadingsInRange(ctx, store.ReadingRange{ContainerID: vaccine.ID, From: t0.Add(10 * time.Minute), To: t0.Add(30 * time.Minute), Limit: 15})
		if err != nil || len(page1) != 15 || !page1[0].RecordedAt.Equal(t0.Add(10*time.Minute)) {
			t.Fatalf("page1: len=%d err=%v", len(page1), err)
		}
		page2, err := st.ReadingsInRange(ctx, store.ReadingRange{ContainerID: vaccine.ID, From: t0.Add(10 * time.Minute), To: t0.Add(30 * time.Minute), After: page1[14].RecordedAt, Limit: 15})
		if err != nil || len(page2) != 5 {
			t.Fatalf("page2: len=%d err=%v", len(page2), err)
		}

		// The range query must be served by the primary-key index.
		var plan string
		if err := pool.QueryRow(ctx, `EXPLAIN SELECT * FROM readings WHERE container_id = $1 AND recorded_at >= $2 AND recorded_at < $3 ORDER BY recorded_at LIMIT 10`,
			vaccine.ID, t0, t0.Add(time.Hour)).Scan(&plan); err != nil {
			t.Fatal(err)
		}
		t.Logf("plan: %s", plan)
	})

	t.Run("breach transitions are idempotent", func(t *testing.T) {
		open := store.BreachTransition{Type: domain.TransitionOpened, ContainerID: vaccine.ID, Kind: domain.BreachTooWarm, At: t0.Add(time.Hour), PeakTempC: 8.6, LimitC: 8}
		closeT := store.BreachTransition{Type: domain.TransitionClosed, ContainerID: vaccine.ID, Kind: domain.BreachTooWarm, At: t0.Add(2 * time.Hour), PeakTempC: 11.2, LimitC: 8}

		first, err := st.ApplyBreachTransitions(ctx, []store.BreachTransition{open})
		if err != nil || len(first) != 1 {
			t.Fatalf("open: %v %v", first, err)
		}
		again, err := st.ApplyBreachTransitions(ctx, []store.BreachTransition{open})
		if err != nil || len(again) != 1 || again[0].ID != first[0].ID {
			t.Fatalf("replayed open created a new breach: %v %v", again, err)
		}
		closed, err := st.ApplyBreachTransitions(ctx, []store.BreachTransition{closeT})
		if err != nil || len(closed) != 1 || closed[0].EndedAt == nil || closed[0].PeakTempC != 11.2 {
			t.Fatalf("close: %+v %v", closed, err)
		}
		if replay, err := st.ApplyBreachTransitions(ctx, []store.BreachTransition{closeT}); err != nil || len(replay) != 0 {
			t.Fatalf("replayed close returned %v %v", replay, err)
		}

		// A kind flip closes the previous breach and opens a new one.
		_, err = st.ApplyBreachTransitions(ctx, []store.BreachTransition{
			{Type: domain.TransitionOpened, ContainerID: vaccine.ID, Kind: domain.BreachTooWarm, At: t0.Add(3 * time.Hour), PeakTempC: 9, LimitC: 8},
			{Type: domain.TransitionOpened, ContainerID: vaccine.ID, Kind: domain.BreachTooCold, At: t0.Add(4 * time.Hour), PeakTempC: 1, LimitC: 2},
		})
		if err != nil {
			t.Fatal(err)
		}
		openNow, err := st.ListBreaches(ctx, store.BreachFilter{ContainerID: vaccine.ID, OpenOnly: true, Limit: 10})
		if err != nil || len(openNow) != 1 || openNow[0].Kind != domain.BreachTooCold {
			t.Fatalf("open breaches = %+v %v", openNow, err)
		}
		all, _ := st.ListBreaches(ctx, store.BreachFilter{ContainerID: vaccine.ID, Limit: 10})
		if len(all) != 3 {
			t.Fatalf("total breaches = %d, want 3", len(all))
		}
	})

	t.Run("webhooks require a grant", func(t *testing.T) {
		sub := store.WebhookSubscription{ID: "6f1c1d1e-2a3b-4c5d-8e9f-0a1b2c3d4e5f", PartnerID: "key-1", Owner: "acme",
			URL: "https://hooks.example/x", Events: []string{"breach.opened"}, SecretEnc: []byte{1, 2, 3}}
		if _, err := st.CreateWebhook(ctx, sub); !errors.Is(err, store.ErrForbidden) {
			t.Fatalf("ungranted create: %v", err)
		}
		if _, err := pool.Exec(ctx, "INSERT INTO partner_grants (partner_id, owner) VALUES ('key-1', 'acme')"); err != nil {
			t.Fatal(err)
		}
		if _, err := st.CreateWebhook(ctx, sub); err != nil {
			t.Fatal(err)
		}
		hits, err := st.WebhooksForEvent(ctx, "acme", "breach.opened")
		if err != nil || len(hits) != 1 || string(hits[0].SecretEnc) != "\x01\x02\x03" {
			t.Fatalf("for event: %+v %v", hits, err)
		}
		if miss, _ := st.WebhooksForEvent(ctx, "acme", "breach.closed"); len(miss) != 0 {
			t.Fatalf("event filter ignored: %+v", miss)
		}
		if err := st.DeleteWebhook(ctx, "key-2", sub.ID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("cross-partner delete: %v", err)
		}
		if err := st.DeleteWebhook(ctx, "key-1", sub.ID); err != nil {
			t.Fatal(err)
		}
	})
}
