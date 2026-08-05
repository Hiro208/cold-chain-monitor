package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.up.sql
var migrationFS embed.FS

// migrationLockID is an arbitrary constant used with pg_advisory_lock so that
// several replicas starting at once do not race to apply the same migration.
const migrationLockID = 0x636f6c64 // "cold"

type migration struct {
	version int
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.Glob(migrationFS, "migrations/*.up.sql")
	if err != nil {
		return nil, err
	}
	var out []migration
	for _, path := range entries {
		base := strings.TrimPrefix(path, "migrations/")
		num, _, ok := strings.Cut(base, "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: expected NNNN_name.up.sql", base)
		}
		v, err := strconv.Atoi(num)
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", base, err)
		}
		body, err := migrationFS.ReadFile(path)
		if err != nil {
			return nil, err
		}
		out = append(out, migration{version: v, name: base, sql: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	for i := 1; i < len(out); i++ {
		if out[i].version == out[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %d", out[i].version)
		}
	}
	return out, nil
}

// Migrate applies all pending migrations, each in its own transaction, while
// holding a session-level advisory lock.
func Migrate(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	migs, err := loadMigrations()
	if err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer func() {
		// Use a fresh context so the unlock still runs if ctx was cancelled.
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    INTEGER PRIMARY KEY,
		name       TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[int]bool{}
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return err
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return err
	}
	for _, v := range versions {
		applied[v] = true
	}

	for _, m := range migs {
		if applied[m.version] {
			continue
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.sql); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", m.version, m.name)
			return err
		})
		if err != nil {
			return fmt.Errorf("apply %s: %w", m.name, err)
		}
		log.Info("applied migration", "version", m.version, "name", m.name)
	}
	return nil
}
