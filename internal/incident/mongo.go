package incident

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

// CollectionName is the collection that holds incident documents.
const CollectionName = "incidents"

// Connect opens a client and verifies the deployment is reachable.
func Connect(ctx context.Context, uri string) (*mongo.Client, error) {
	cl, err := mongo.Connect(options.Client().ApplyURI(uri).SetAppName("cold-chain-monitor"))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cl.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = cl.Disconnect(context.Background())
		return nil, fmt.Errorf("mongo ping: %w", err)
	}
	return cl, nil
}

// Store reads and writes incident documents.
type Store struct {
	coll      *mongo.Collection
	retention time.Duration
}

// NewStore returns a store on db.incidents. Writes use majority write
// concern: the consumer commits Kafka offsets only after BulkWrite returns,
// so an acknowledged write must survive a primary failover.
func NewStore(db *mongo.Database, retention time.Duration) *Store {
	coll := db.Collection(CollectionName, options.Collection().SetWriteConcern(writeconcern.Majority()))
	return &Store{coll: coll, retention: retention}
}

// EnsureIndexes creates the indexes the read paths rely on. CreateMany is
// a no-op for indexes that already exist with the same definition.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		// Per-container history, newest first (GET /v1/incidents?container=).
		{Keys: bson.D{{Key: "container_id", Value: 1}, {Key: "started_at", Value: -1}, {Key: "_id", Value: -1}}},
		// Per-owner console view, optionally filtered by status.
		{Keys: bson.D{{Key: "owner", Value: 1}, {Key: "status", Value: 1}, {Key: "started_at", Value: -1}, {Key: "_id", Value: -1}}},
		// Fleet-wide "open incidents" and time-window scans for Summarize.
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "started_at", Value: -1}, {Key: "_id", Value: -1}}},
		{Keys: bson.D{{Key: "started_at", Value: -1}, {Key: "_id", Value: -1}}},
		// TTL: documents are removed once expire_at passes. Open incidents
		// have no expire_at and are never removed.
		{Keys: bson.D{{Key: "expire_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
	}
	if _, err := s.coll.Indexes().CreateMany(ctx, models); err != nil {
		return fmt.Errorf("create incident indexes: %w", err)
	}
	return nil
}

// Ping checks the deployment is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.coll.Database().Client().Ping(ctx, readpref.Primary())
}

// Apply upserts a batch of breach events in one unordered bulk write. Every
// update is idempotent (see UpdateFor), so replaying a batch after a crash
// is safe, and an unordered write lets the server apply them in parallel.
func (s *Store) Apply(ctx context.Context, events []stream.BreachEvent) (BulkResult, error) {
	if len(events) == 0 {
		return BulkResult{}, nil
	}
	models := make([]mongo.WriteModel, 0, len(events))
	for _, ev := range events {
		f, u := UpdateFor(ev, s.retention)
		models = append(models, mongo.NewUpdateOneModel().SetFilter(f).SetUpdate(u).SetUpsert(true))
	}
	res, err := s.coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return BulkResult{}, fmt.Errorf("apply incidents: %w", err)
	}
	return BulkResult{Upserted: res.UpsertedCount, Modified: res.ModifiedCount, Matched: res.MatchedCount}, nil
}

// BulkResult summarises one Apply call.
type BulkResult struct {
	Upserted, Modified, Matched int64
}

// ErrNotFound is returned by Get for an unknown breach ID.
var ErrNotFound = errors.New("incident not found")

// Get returns one incident by breach ID.
func (s *Store) Get(ctx context.Context, breachID int64) (Incident, error) {
	var inc Incident
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: breachID}}).Decode(&inc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Incident{}, ErrNotFound
	}
	return inc, err
}

// List returns incidents matching f, newest first. Paging is keyset-based
// on (started_at, _id) so it stays stable while new incidents arrive.
func (s *Store) List(ctx context.Context, f Filter) ([]Incident, error) {
	q := bson.D{}
	if f.ContainerID != "" {
		q = append(q, bson.E{Key: "container_id", Value: f.ContainerID})
	}
	if f.Owner != "" {
		q = append(q, bson.E{Key: "owner", Value: f.Owner})
	}
	if f.Status != "" {
		q = append(q, bson.E{Key: "status", Value: f.Status})
	}
	if !f.Since.IsZero() {
		q = append(q, bson.E{Key: "started_at", Value: bson.D{{Key: "$gte", Value: f.Since.UTC()}}})
	}
	if !f.Before.IsZero() {
		before := f.Before.UTC()
		q = append(q, bson.E{Key: "$or", Value: bson.A{
			bson.D{{Key: "started_at", Value: bson.D{{Key: "$lt", Value: before}}}},
			bson.D{{Key: "started_at", Value: before}, {Key: "_id", Value: bson.D{{Key: "$lt", Value: f.BeforeID}}}},
		}})
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "started_at", Value: -1}, {Key: "_id", Value: -1}}).
		SetLimit(int64(f.Limit))
	cur, err := s.coll.Find(ctx, q, opts)
	if err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	out := []Incident{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	return out, nil
}

// Summarize aggregates incidents started since `since` by one dimension.
func (s *Store) Summarize(ctx context.Context, by GroupBy, since time.Time, limit int) ([]SummaryRow, error) {
	if !by.Valid() {
		return nil, fmt.Errorf("summarize: unsupported group %q", by)
	}
	cur, err := s.coll.Aggregate(ctx, SummaryPipeline(by, since, limit))
	if err != nil {
		return nil, fmt.Errorf("summarize incidents: %w", err)
	}
	out := []SummaryRow{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("summarize incidents: %w", err)
	}
	return out, nil
}
