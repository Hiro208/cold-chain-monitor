package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

// InsertReadings writes a batch of readings in a single round trip using
// array parameters + unnest. Duplicates (same container and timestamp, e.g.
// from a Kafka redelivery) are ignored. It returns the number of new rows.
//
// Readings for unknown containers would violate the foreign key and fail the
// whole batch, so callers must filter them out first.
func (s *Store) InsertReadings(ctx context.Context, batch []domain.Reading) (int64, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	ids := make([]string, len(batch))
	ts := make([]time.Time, len(batch))
	temps := make([]float64, len(batch))
	hums := make([]float32, len(batch))
	doors := make([]bool, len(batch))
	for i, r := range batch {
		ids[i], ts[i], temps[i], hums[i], doors[i] = r.ContainerID, r.RecordedAt, r.TemperatureC, float32(r.HumidityPct), r.DoorOpen
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO readings (container_id, recorded_at, temperature_c, humidity_pct, door_open)
		SELECT * FROM unnest($1::text[], $2::timestamptz[], $3::float8[], $4::float4[], $5::bool[])
		ON CONFLICT (container_id, recorded_at) DO NOTHING`,
		ids, ts, temps, hums, doors)
	if err != nil {
		return 0, fmt.Errorf("insert readings: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ReadingRange selects a time window of one container's history. Results are
// ordered by recorded_at ascending; to page, pass the last returned timestamp
// as After (exclusive). From is inclusive and To is exclusive.
type ReadingRange struct {
	ContainerID string
	From, To    time.Time
	After       time.Time
	Limit       int
}

// ReadingsInRange returns readings in the window. The query is an index range
// scan on the (container_id, recorded_at) primary key.
func (s *Store) ReadingsInRange(ctx context.Context, q ReadingRange) ([]domain.Reading, error) {
	from := q.From
	if q.After.After(from) {
		from = q.After.Add(time.Microsecond) // Postgres timestamps have µs precision
	}
	rows, err := s.pool.Query(ctx, `
		SELECT container_id, recorded_at, temperature_c, humidity_pct, door_open
		FROM readings
		WHERE container_id = $1 AND recorded_at >= $2 AND recorded_at < $3
		ORDER BY recorded_at
		LIMIT $4`, q.ContainerID, from, q.To, q.Limit)
	if err != nil {
		return nil, fmt.Errorf("query readings: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Reading, error) {
		var rd domain.Reading
		var hum float32
		err := r.Scan(&rd.ContainerID, &rd.RecordedAt, &rd.TemperatureC, &hum, &rd.DoorOpen)
		rd.HumidityPct = float64(hum)
		rd.RecordedAt = rd.RecordedAt.UTC()
		return rd, err
	})
}
