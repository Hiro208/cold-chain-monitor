package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

const containerColumns = "id, owner, commodity, setpoint_c, min_temp_c, max_temp_c, created_at"

func scanContainer(row pgx.Row) (domain.Container, error) {
	var c domain.Container
	err := row.Scan(&c.ID, &c.Owner, &c.Commodity, &c.SetpointC, &c.MinTempC, &c.MaxTempC, &c.CreatedAt)
	return c, err
}

// CreateContainer registers a container. It returns ErrConflict if the ID is
// already registered.
func (s *Store) CreateContainer(ctx context.Context, c domain.Container) (domain.Container, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO containers (id, owner, commodity, setpoint_c, min_temp_c, max_temp_c)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING `+containerColumns,
		c.ID, c.Owner, c.Commodity, c.SetpointC, c.MinTempC, c.MaxTempC)
	out, err := scanContainer(row)
	if pgCode(err) == pgUniqueViolation {
		return domain.Container{}, ErrConflict
	}
	if err != nil {
		return domain.Container{}, fmt.Errorf("insert container: %w", err)
	}
	return out, nil
}

// GetContainer fetches one container by ID.
func (s *Store) GetContainer(ctx context.Context, id string) (domain.Container, error) {
	c, err := scanContainer(s.pool.QueryRow(ctx, "SELECT "+containerColumns+" FROM containers WHERE id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Container{}, ErrNotFound
	}
	if err != nil {
		return domain.Container{}, fmt.Errorf("get container: %w", err)
	}
	return c, nil
}

// ContainerFilter selects a page of containers. Pagination is keyset-based on
// the ID: pass the last ID of the previous page as After.
type ContainerFilter struct {
	Owner string
	After string
	Limit int
}

// ListContainers returns containers ordered by ID.
func (s *Store) ListContainers(ctx context.Context, f ContainerFilter) ([]domain.Container, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+containerColumns+` FROM containers
		WHERE ($1 = '' OR owner = $1) AND id > $2
		ORDER BY id
		LIMIT $3`, f.Owner, f.After, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Container, error) { return scanContainer(r) })
}
