package incident

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

var (
	t0    = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ended = t0.Add(42 * time.Minute)
)

func openedEvent() stream.BreachEvent {
	return stream.BreachEvent{
		Type:   stream.BreachOpened,
		Breach: domain.Breach{ID: 7, ContainerID: "CSQU3054383", Kind: domain.BreachTooWarm, StartedAt: t0, PeakTempC: 8.4, LimitC: 8},
		Owner:  "acme", Commodity: "vaccines", DetectedAt: t0.Add(900 * time.Millisecond), LatencyMs: 900,
	}
}

func closedEvent() stream.BreachEvent {
	ev := openedEvent()
	ev.Type = stream.BreachClosed
	ev.Breach.EndedAt = &ended
	ev.Breach.PeakTempC = 11.2
	ev.DetectedAt = ended.Add(time.Second)
	ev.LatencyMs = 1000
	return ev
}

// operatorPaths maps each top-level path to the update operators naming it.
func operatorPaths(t *testing.T, update bson.D) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, op := range update {
		fields, ok := op.Value.(bson.D)
		if !ok {
			t.Fatalf("%s: value is %T, want bson.D", op.Key, op.Value)
		}
		for _, f := range fields {
			out[f.Key] = append(out[f.Key], op.Key)
		}
	}
	return out
}

func field(t *testing.T, update bson.D, op, key string) (any, bool) {
	t.Helper()
	for _, o := range update {
		if o.Key != op {
			continue
		}
		for _, f := range o.Value.(bson.D) {
			if f.Key == key {
				return f.Value, true
			}
		}
	}
	return nil, false
}

func TestUpdateForOpened(t *testing.T) {
	filter, update := UpdateFor(openedEvent(), 90*24*time.Hour)
	if len(filter) != 1 || filter[0].Key != "_id" || filter[0].Value != int64(7) {
		t.Fatalf("filter = %v", filter)
	}
	for path, ops := range operatorPaths(t, update) {
		if len(ops) > 1 {
			t.Fatalf("path %q in several operators %v: MongoDB rejects the update", path, ops)
		}
	}
	if v, ok := field(t, update, "$setOnInsert", "status"); !ok || v != StatusOpen {
		t.Fatalf("opened must insert status=open, got %v %v", v, ok)
	}
	if _, ok := field(t, update, "$set", "status"); ok {
		t.Fatal("opened must not $set status, or a late replay would reopen a closed incident")
	}
	ev, ok := field(t, update, "$min", "timeline.opened")
	if !ok || ev.(Event).DetectedAt != t0.Add(900*time.Millisecond) {
		t.Fatalf("timeline.opened = %v %v", ev, ok)
	}
	if _, ok := field(t, update, "$set", "expire_at"); ok {
		t.Fatal("open incidents must not expire")
	}
}

func TestUpdateForClosed(t *testing.T) {
	_, update := UpdateFor(closedEvent(), time.Hour)
	for path, ops := range operatorPaths(t, update) {
		if len(ops) > 1 {
			t.Fatalf("path %q in several operators %v", path, ops)
		}
	}
	want := map[string]any{
		"status":      StatusClosed,
		"ended_at":    ended,
		"duration_s":  (42 * time.Minute).Seconds(),
		"peak_temp_c": 11.2,
		"expire_at":   ended.Add(time.Hour),
	}
	for k, v := range want {
		if got, ok := field(t, update, "$set", k); !ok || got != v {
			t.Errorf("$set.%s = %v (%v), want %v", k, got, ok, v)
		}
	}
	// Identity fields still upsert, so a closed event that arrives first
	// (replay after a consumer restart) creates a complete document.
	for _, k := range []string{"container_id", "owner", "commodity", "kind", "started_at", "limit_c"} {
		if _, ok := field(t, update, "$setOnInsert", k); !ok {
			t.Errorf("$setOnInsert.%s missing", k)
		}
	}
	if _, ok := field(t, update, "$min", "timeline.closed"); !ok {
		t.Error("timeline.closed missing")
	}
}

func TestUpdateForNoRetention(t *testing.T) {
	_, update := UpdateFor(closedEvent(), 0)
	if _, ok := field(t, update, "$set", "expire_at"); ok {
		t.Fatal("retention 0 must keep incidents forever")
	}
}

// The $min merge relies on documents comparing field by field, starting
// with At and then DetectedAt; keep the struct order in sync.
func TestEventFieldOrder(t *testing.T) {
	raw, err := bson.Marshal(Event{At: t0, DetectedAt: t0, LatencyMs: 1})
	if err != nil {
		t.Fatal(err)
	}
	elems, err := bson.Raw(raw).Elements()
	if err != nil {
		t.Fatal(err)
	}
	if len(elems) < 2 || elems[0].Key() != "at" || elems[1].Key() != "detected_at" {
		t.Fatalf("event field order = %v", elems)
	}
}

func TestSummaryPipeline(t *testing.T) {
	p := SummaryPipeline(ByCommodity, t0, 5)
	if len(p) != 5 {
		t.Fatalf("stages = %d", len(p))
	}
	stage := func(i int) string { return p[i].(bson.D)[0].Key }
	for i, want := range []string{"$match", "$group", "$set", "$sort", "$limit"} {
		if stage(i) != want {
			t.Fatalf("stage %d = %s, want %s", i, stage(i), want)
		}
	}
	group := p[1].(bson.D)[0].Value.(bson.D)
	id := group[0].Value.(bson.D)[0].Value.(bson.A)
	if id[0] != "$commodity" {
		t.Fatalf("group key = %v", id)
	}
	if p[4].(bson.D)[0].Value != 5 {
		t.Fatalf("limit = %v", p[4])
	}
}

func TestGroupByValid(t *testing.T) {
	for _, g := range []GroupBy{ByOwner, ByCommodity, ByContainer, ByKind} {
		if !g.Valid() {
			t.Errorf("%s should be valid", g)
		}
	}
	if GroupBy("$where").Valid() {
		t.Error("arbitrary field names must be rejected")
	}
}

func TestDecodeSkipsMalformed(t *testing.T) {
	good, err := stream.EncodeBreachEvent(openedEvent())
	if err != nil {
		t.Fatal(err)
	}
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "m"})
	recs := []*kgo.Record{{Value: good}, {Value: []byte("not json")}, {Value: []byte(`{"v":99}`)}}
	out := Decode(recs, slog.New(slog.NewTextHandler(io.Discard, nil)), c)
	if len(out) != 1 || out[0].Breach.ID != 7 {
		t.Fatalf("decoded = %+v", out)
	}
	if got := testutil.ToFloat64(c); got != 2 {
		t.Fatalf("malformed = %v", got)
	}
}
