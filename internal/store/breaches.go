package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

// BreachTransition is a breach state change detected by the ingest worker.
type BreachTransition struct {
	Type        domain.TransitionType
	ContainerID string
	Kind        domain.BreachKind // for Opened
	At          time.Time         // recorded_at of the triggering reading
	PeakTempC   float64
	LimitC      float64
}

// ApplyBreachTransitions persists a batch of transitions in one transaction
// and returns the affected breach rows (in input order, skipping no-ops).
//
// It is idempotent so that a replayed Kafka batch does not create duplicate
// breaches:
//   - Opened upserts on (container_id, started_at); any other open breach for
//     the container is closed at the new start time first.
//   - Closed only touches a breach that is still open; closing twice is a
//     no-op and returns nothing, so no duplicate "closed" event is emitted.
func (s *Store) ApplyBreachTransitions(ctx context.Context, ts []BreachTransition) ([]domain.Breach, error) {
	if len(ts) == 0 {
		return nil, nil
	}
	var out []domain.Breach
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, t := range ts {
			var (
				b   domain.Breach
				err error
			)
			switch t.Type {
			case domain.TransitionOpened:
				b, err = openBreach(ctx, tx, t)
			case domain.TransitionClosed:
				b, err = closeBreach(ctx, tx, t)
				if errors.Is(err, pgx.ErrNoRows) {
					continue
				}
			default:
				continue
			}
			if err != nil {
				return fmt.Errorf("%s breach for %s: %w", t.Type, t.ContainerID, err)
			}
			out = append(out, b)
		}
		return nil
	})
	return out, err
}

const breachColumns = "id, container_id, kind, started_at, ended_at, peak_temp_c, limit_c"

func scanBreach(row pgx.Row) (domain.Breach, error) {
	var b domain.Breach
	var kind string
	err := row.Scan(&b.ID, &b.ContainerID, &kind, &b.StartedAt, &b.EndedAt, &b.PeakTempC, &b.LimitC)
	b.Kind = domain.BreachKind(kind)
	b.StartedAt = b.StartedAt.UTC()
	if b.EndedAt != nil {
		e := b.EndedAt.UTC()
		b.EndedAt = &e
	}
	return b, err
}

func openBreach(ctx context.Context, tx pgx.Tx, t BreachTransition) (domain.Breach, error) {
	if _, err := tx.Exec(ctx, `
		UPDATE breaches SET ended_at = $2
		WHERE container_id = $1 AND ended_at IS NULL AND started_at < $2`,
		t.ContainerID, t.At); err != nil {
		return domain.Breach{}, err
	}
	return scanBreach(tx.QueryRow(ctx, `
		INSERT INTO breaches (container_id, kind, started_at, peak_temp_c, limit_c)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (container_id, started_at) DO UPDATE SET kind = EXCLUDED.kind
		RETURNING `+breachColumns,
		t.ContainerID, string(t.Kind), t.At, t.PeakTempC, t.LimitC))
}

func closeBreach(ctx context.Context, tx pgx.Tx, t BreachTransition) (domain.Breach, error) {
	return scanBreach(tx.QueryRow(ctx, `
		UPDATE breaches SET ended_at = $2, peak_temp_c = $3
		WHERE container_id = $1 AND ended_at IS NULL
		RETURNING `+breachColumns,
		t.ContainerID, t.At, t.PeakTempC))
}

// BreachFilter selects breaches for listing, newest first. Pagination is
// keyset-based on id (pass the last id as BeforeID).
type BreachFilter struct {
	ContainerID string
	OpenOnly    bool
	Since       time.Time
	BeforeID    int64
	Limit       int
}

// ListBreaches returns breaches matching the filter.
func (s *Store) ListBreaches(ctx context.Context, f BreachFilter) ([]domain.Breach, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+breachColumns+` FROM breaches
		WHERE ($1 = '' OR container_id = $1)
		  AND (NOT $2 OR ended_at IS NULL)
		  AND started_at >= $3
		  AND ($4 = 0 OR id < $4)
		ORDER BY id DESC
		LIMIT $5`, f.ContainerID, f.OpenOnly, f.Since, f.BeforeID, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list breaches: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Breach, error) { return scanBreach(r) })
}
