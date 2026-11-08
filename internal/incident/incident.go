// Package incident keeps a document-per-breach incident log in MongoDB.
//
// Postgres remains the system of record for readings and breaches. The
// incident log is a read-optimised projection of the breaches topic: each
// breach becomes one document that embeds its lifecycle events alongside the
// container's owner and commodity, so the operations console can list and
// aggregate incidents without joins.
//
// Writes are idempotent and order-independent. Kafka delivers at least once
// and a consumer restart can replay an "opened" event after the matching
// "closed" one; the update built by UpdateFor converges to the same document
// either way.
package incident

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

// Status is the lifecycle state of an incident.
type Status string

const (
	StatusOpen   Status = "open"
	StatusClosed Status = "closed"
)

// Event is one lifecycle transition embedded in an incident document. Field
// order matters: UpdateFor merges events with $min, which compares embedded
// documents field by field, so the earliest detection of a transition wins.
type Event struct {
	At         time.Time `bson:"at" json:"at"`
	DetectedAt time.Time `bson:"detected_at" json:"detected_at"`
	LatencyMs  int64     `bson:"latency_ms" json:"latency_ms"`
}

// Timeline holds the transitions of one incident.
type Timeline struct {
	Opened *Event `bson:"opened,omitempty" json:"opened,omitempty"`
	Closed *Event `bson:"closed,omitempty" json:"closed,omitempty"`
}

// Incident is the MongoDB document for one breach. The breach ID from
// Postgres is the natural key and is used as _id.
type Incident struct {
	BreachID    int64             `bson:"_id" json:"breach_id"`
	ContainerID string            `bson:"container_id" json:"container_id"`
	Owner       string            `bson:"owner" json:"owner"`
	Commodity   string            `bson:"commodity,omitempty" json:"commodity,omitempty"`
	Kind        domain.BreachKind `bson:"kind" json:"kind"`
	Status      Status            `bson:"status" json:"status"`
	StartedAt   time.Time         `bson:"started_at" json:"started_at"`
	EndedAt     *time.Time        `bson:"ended_at,omitempty" json:"ended_at,omitempty"`
	DurationS   *float64          `bson:"duration_s,omitempty" json:"duration_s,omitempty"`
	PeakTempC   float64           `bson:"peak_temp_c" json:"peak_temp_c"`
	LimitC      float64           `bson:"limit_c" json:"limit_c"`
	Timeline    Timeline          `bson:"timeline" json:"timeline"`
	// ExpireAt is set when an incident closes; a TTL index removes closed
	// incidents after the retention period. Open incidents never expire.
	ExpireAt *time.Time `bson:"expire_at,omitempty" json:"-"`
}

// UpdateFor returns the filter and update document that apply ev to its
// incident with upsert semantics:
//
//   - identity fields go in $setOnInsert, so whichever event arrives first
//     creates the document and later events never rewrite it;
//   - the transition is merged into timeline.<opened|closed> with $min. A
//     replayed message is a no-op, and if ingest re-publishes a transition
//     after a crash (same time, later detected_at) the first detection is
//     kept;
//   - only a "closed" event sets the closing fields. An "opened" event never
//     touches status, so a late replay cannot reopen a closed incident.
//
// MongoDB rejects an update that names the same path in two operators, so
// status and peak_temp_c live either in $setOnInsert or in $set, never both.
func UpdateFor(ev stream.BreachEvent, retention time.Duration) (filter, update bson.D) {
	b := ev.Breach
	at := b.StartedAt
	if ev.Type == stream.BreachClosed && b.EndedAt != nil {
		at = b.EndedAt.UTC()
	}
	event := Event{At: at.UTC(), DetectedAt: ev.DetectedAt.UTC(), LatencyMs: ev.LatencyMs}
	slot := "timeline.opened"
	if ev.Type == stream.BreachClosed {
		slot = "timeline.closed"
	}

	insert := bson.D{
		{Key: "container_id", Value: b.ContainerID},
		{Key: "owner", Value: ev.Owner},
		{Key: "kind", Value: b.Kind},
		{Key: "started_at", Value: b.StartedAt.UTC()},
		{Key: "limit_c", Value: b.LimitC},
	}
	if ev.Commodity != "" {
		insert = append(insert, bson.E{Key: "commodity", Value: ev.Commodity})
	}

	update = bson.D{{Key: "$min", Value: bson.D{{Key: slot, Value: event}}}}
	if ev.Type == stream.BreachClosed && b.EndedAt != nil {
		ended := b.EndedAt.UTC()
		set := bson.D{
			{Key: "status", Value: StatusClosed},
			{Key: "ended_at", Value: ended},
			{Key: "duration_s", Value: ended.Sub(b.StartedAt).Seconds()},
			// The closed event carries the final peak of the whole breach.
			{Key: "peak_temp_c", Value: b.PeakTempC},
		}
		if retention > 0 {
			set = append(set, bson.E{Key: "expire_at", Value: ended.Add(retention)})
		}
		update = append(update, bson.E{Key: "$set", Value: set})
	} else {
		insert = append(insert,
			bson.E{Key: "status", Value: StatusOpen},
			bson.E{Key: "peak_temp_c", Value: b.PeakTempC},
		)
	}
	update = append(update, bson.E{Key: "$setOnInsert", Value: insert})
	return bson.D{{Key: "_id", Value: b.ID}}, update
}

// Filter selects incidents for List.
type Filter struct {
	ContainerID string
	Owner       string
	Status      Status // "" means any
	Since       time.Time
	// Before pages backwards: only incidents that started strictly before
	// it (or at it with a smaller breach ID) are returned.
	Before   time.Time
	BeforeID int64
	Limit    int
}

// GroupBy is the dimension Summarize aggregates on.
type GroupBy string

const (
	ByOwner     GroupBy = "owner"
	ByCommodity GroupBy = "commodity"
	ByContainer GroupBy = "container_id"
	ByKind      GroupBy = "kind"
)

// Valid reports whether g is a supported grouping.
func (g GroupBy) Valid() bool {
	switch g {
	case ByOwner, ByCommodity, ByContainer, ByKind:
		return true
	}
	return false
}

// SummaryRow is one group of the incident summary.
type SummaryRow struct {
	Key            string  `bson:"_id" json:"key"`
	Incidents      int64   `bson:"incidents" json:"incidents"`
	Open           int64   `bson:"open" json:"open"`
	AvgDurationS   float64 `bson:"avg_duration_s" json:"avg_duration_s"`
	TotalDurationS float64 `bson:"total_duration_s" json:"total_duration_s"`
	MaxPeakTempC   float64 `bson:"max_peak_temp_c" json:"max_peak_temp_c"`
	MinPeakTempC   float64 `bson:"min_peak_temp_c" json:"min_peak_temp_c"`
}

// SummaryPipeline builds the aggregation behind Summarize: incidents that
// started since `since`, grouped by `by`, worst groups first. Open incidents
// count towards Open but not towards durations, which are unknown yet.
func SummaryPipeline(by GroupBy, since time.Time, limit int) bson.A {
	return bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "started_at", Value: bson.D{{Key: "$gte", Value: since.UTC()}}}}}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$" + string(by), "unknown"}}}},
			{Key: "incidents", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "open", Value: bson.D{{Key: "$sum", Value: bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$eq", Value: bson.A{"$status", StatusOpen}}}, 1, 0,
			}}}}}},
			{Key: "avg_duration_s", Value: bson.D{{Key: "$avg", Value: "$duration_s"}}},
			{Key: "total_duration_s", Value: bson.D{{Key: "$sum", Value: "$duration_s"}}},
			{Key: "max_peak_temp_c", Value: bson.D{{Key: "$max", Value: "$peak_temp_c"}}},
			{Key: "min_peak_temp_c", Value: bson.D{{Key: "$min", Value: "$peak_temp_c"}}},
		}}},
		// $avg over no closed incidents yields null; report 0 instead.
		bson.D{{Key: "$set", Value: bson.D{{Key: "avg_duration_s", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$avg_duration_s", 0}}}}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "incidents", Value: -1}, {Key: "_id", Value: 1}}}},
		bson.D{{Key: "$limit", Value: limit}},
	}
}
