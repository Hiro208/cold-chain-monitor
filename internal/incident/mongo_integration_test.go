//go:build integration

package incident_test

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/incident"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
	"github.com/guangjiandong/cold-chain-monitor/internal/testenv"
)

var t0 = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func event(typ stream.BreachEventType, id int64, container, owner, commodity string, start time.Time, dur time.Duration, peak float64, detectedAfter time.Duration) stream.BreachEvent {
	b := domain.Breach{ID: id, ContainerID: container, Kind: domain.BreachTooWarm, StartedAt: start, PeakTempC: peak, LimitC: 8}
	at := start
	if typ == stream.BreachClosed {
		end := start.Add(dur)
		b.EndedAt = &end
		at = end
	}
	return stream.BreachEvent{Type: typ, Breach: b, Owner: owner, Commodity: commodity,
		DetectedAt: at.Add(detectedAfter), LatencyMs: detectedAfter.Milliseconds()}
}

func TestIncidentStoreAgainstMongo(t *testing.T) {
	ctx := context.Background()
	cl, err := incident.Connect(ctx, testenv.Mongo(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Disconnect(context.Background()) })
	db := cl.Database("coldchain_test")
	st := incident.NewStore(db, 24*time.Hour)
	if err := st.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes must be idempotent: %v", err)
	}

	opened := event(stream.BreachOpened, 1, "CSQU3054383", "acme", "vaccines", t0, 0, 8.6, time.Second)
	closed := event(stream.BreachClosed, 1, "CSQU3054383", "acme", "vaccines", t0, 30*time.Minute, 10.1, time.Second)

	t.Run("closed before opened converges", func(t *testing.T) {
		// A replay after a consumer restart can deliver the close first.
		if _, err := st.Apply(ctx, []stream.BreachEvent{closed}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Apply(ctx, []stream.BreachEvent{opened}); err != nil {
			t.Fatal(err)
		}
		inc, err := st.Get(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if inc.Status != incident.StatusClosed || inc.PeakTempC != 10.1 || inc.EndedAt == nil || *inc.DurationS != 1800 {
			t.Fatalf("incident = %+v", inc)
		}
		if inc.Timeline.Opened == nil || inc.Timeline.Closed == nil || inc.Owner != "acme" || inc.Commodity != "vaccines" {
			t.Fatalf("timeline/identity = %+v", inc)
		}
		if inc.ExpireAt == nil || !inc.ExpireAt.Equal(t0.Add(30*time.Minute+24*time.Hour)) {
			t.Fatalf("expire_at = %v", inc.ExpireAt)
		}
	})

	t.Run("replays are no-ops and keep the first detection", func(t *testing.T) {
		before, _ := st.Get(ctx, 1)
		late := opened
		late.DetectedAt = opened.DetectedAt.Add(time.Minute) // re-published after an ingest crash
		res, err := st.Apply(ctx, []stream.BreachEvent{opened, closed, late})
		if err != nil {
			t.Fatal(err)
		}
		if res.Upserted != 0 || res.Modified != 0 {
			t.Fatalf("replay modified documents: %+v", res)
		}
		after, _ := st.Get(ctx, 1)
		if !after.Timeline.Opened.DetectedAt.Equal(before.Timeline.Opened.DetectedAt) {
			t.Fatalf("first detection overwritten: %v -> %v", before.Timeline.Opened.DetectedAt, after.Timeline.Opened.DetectedAt)
		}
		if n, _ := db.Collection(incident.CollectionName).CountDocuments(ctx, bson.D{}); n != 1 {
			t.Fatalf("documents = %d, want 1", n)
		}
	})

	// More incidents for listing and aggregation.
	batch := []stream.BreachEvent{
		event(stream.BreachOpened, 2, "CSQU3054383", "acme", "vaccines", t0.Add(time.Hour), 0, 9, time.Second),
		event(stream.BreachOpened, 3, "OTHU0000001", "globex", "seafood", t0.Add(2*time.Hour), 0, 9, time.Second),
		event(stream.BreachClosed, 3, "OTHU0000001", "globex", "seafood", t0.Add(2*time.Hour), 10*time.Minute, 12.5, time.Second),
		event(stream.BreachOpened, 4, "OTHU0000002", "globex", "", t0.Add(3*time.Hour), 0, 8.2, time.Second),
	}
	if _, err := st.Apply(ctx, batch); err != nil {
		t.Fatal(err)
	}

	t.Run("list filters and pages newest first", func(t *testing.T) {
		all, err := st.List(ctx, incident.Filter{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 4 || all[0].BreachID != 4 || all[3].BreachID != 1 {
			t.Fatalf("all = %v", ids(all))
		}
		open, _ := st.List(ctx, incident.Filter{Status: incident.StatusOpen, Limit: 10})
		if got := ids(open); len(got) != 2 || got[0] != 4 || got[1] != 2 {
			t.Fatalf("open = %v", got)
		}
		mine, _ := st.List(ctx, incident.Filter{ContainerID: "CSQU3054383", Limit: 10})
		if got := ids(mine); len(got) != 2 {
			t.Fatalf("container = %v", got)
		}
		page2, _ := st.List(ctx, incident.Filter{Before: all[1].StartedAt, BeforeID: all[1].BreachID, Limit: 10})
		if got := ids(page2); len(got) != 2 || got[0] != 2 {
			t.Fatalf("page 2 = %v", got)
		}
	})

	t.Run("summarize aggregates per group", func(t *testing.T) {
		rows, err := st.Summarize(ctx, incident.ByOwner, t0.Add(-time.Hour), 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 2 {
			t.Fatalf("rows = %+v", rows)
		}
		byKey := map[string]incident.SummaryRow{}
		for _, r := range rows {
			byKey[r.Key] = r
		}
		acme, globex := byKey["acme"], byKey["globex"]
		if acme.Incidents != 2 || acme.Open != 1 || acme.AvgDurationS != 1800 || acme.MaxPeakTempC != 10.1 {
			t.Fatalf("acme = %+v", acme)
		}
		if globex.Incidents != 2 || globex.Open != 1 || globex.TotalDurationS != 600 || globex.MaxPeakTempC != 12.5 {
			t.Fatalf("globex = %+v", globex)
		}
		byCommodity, _ := st.Summarize(ctx, incident.ByCommodity, t0.Add(-time.Hour), 10)
		found := false
		for _, r := range byCommodity {
			if r.Key == "unknown" && r.Incidents == 1 {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing commodity must group as unknown: %+v", byCommodity)
		}
	})

	t.Run("ttl index exists", func(t *testing.T) {
		cur, err := db.Collection(incident.CollectionName).Indexes().List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var specs []bson.M
		if err := cur.All(ctx, &specs); err != nil {
			t.Fatal(err)
		}
		for _, s := range specs {
			if _, ok := s["expireAfterSeconds"]; ok {
				return
			}
		}
		t.Fatalf("no TTL index in %v", specs)
	})
}

func ids(in []incident.Incident) []int64 {
	out := make([]int64, len(in))
	for i, x := range in {
		out[i] = x.BreachID
	}
	return out
}
