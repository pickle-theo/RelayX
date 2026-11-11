// Package store provides the PostgreSQL connection pool and the embedded
// schema migrations used by the Phase 2 reliable delivery pipeline.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Defaults applied by Open when the matching Config field is not positive.
const (
	DefaultMaxOpenConns     = 64
	DefaultMaxIdleConns     = 8
	DefaultConnectTimeout   = 5 * time.Second
	DefaultRetryBackoffBase = 2 * time.Second
	DefaultRetryBackoffMax  = time.Minute
)

// migrateLockKey serializes migration runs across processes. It is the ASCII
// string "relay" and is only meaningful inside this project.
const migrateLockKey int64 = 0x72656c6179

const createMigrationsTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Config controls how Store opens and maintains its connection pool.
type Config struct {
	// DSN is the PostgreSQL connection string, for example
	// postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable.
	DSN string
	// MaxOpenConns limits the number of open connections; values <= 0 fall
	// back to DefaultMaxOpenConns.
	MaxOpenConns int
	// MaxIdleConns limits the number of idle connections; values <= 0 fall
	// back to DefaultMaxIdleConns.
	MaxIdleConns int
	// ConnectTimeout bounds the initial PingContext; values <= 0 fall back to
	// DefaultConnectTimeout.
	ConnectTimeout time.Duration
	// RetryBackoffBase is the first retry delay; values <= 0 fall back to
	// DefaultRetryBackoffBase.
	RetryBackoffBase time.Duration
	// RetryBackoffMax caps the exponential retry delay; values <= 0 fall back
	// to DefaultRetryBackoffMax.
	RetryBackoffMax time.Duration
}

// Store owns the PostgreSQL connection pool shared by the gateway.
type Store struct {
	db        *sql.DB
	retryBase time.Duration
	retryMax  time.Duration
}

// Open connects to PostgreSQL, applies pool limits and verifies the connection
// with a PingContext bounded by Config.ConnectTimeout.
//
// The returned error names the database host but never the DSN password, and
// points at `make db-up` because Phase 2 expects the local Docker PostgreSQL.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, errors.New("store: empty DSN")
	}

	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", dsnHost(cfg.DSN), err)
	}

	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = DefaultMaxOpenConns
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = DefaultMaxIdleConns
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	timeout := cfg.ConnectTimeout
	if timeout <= 0 {
		timeout = DefaultConnectTimeout
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: connect to %s failed: %w; start PostgreSQL first with `make db-up`", dsnHost(cfg.DSN), err)
	}

	retryBase := cfg.RetryBackoffBase
	if retryBase <= 0 {
		retryBase = DefaultRetryBackoffBase
	}
	retryMax := cfg.RetryBackoffMax
	if retryMax <= 0 {
		retryMax = DefaultRetryBackoffMax
	}
	return &Store{db: db, retryBase: retryBase, retryMax: retryMax}, nil
}

// Migrate applies every embedded migration that is not yet recorded in
// schema_migrations. Each version runs in its own transaction under a
// PostgreSQL advisory lock, so a second start on the same database is a no-op
// and concurrent starts cannot apply the same version twice.
func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("store: not open")
	}
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	var appliedCount, skippedCount int
	for _, m := range migrations {
		applied, err := s.applyMigration(ctx, m)
		if err != nil {
			return err
		}
		if applied {
			appliedCount++
		} else {
			skippedCount++
		}
	}
	slog.Info("store: migrations complete", "applied", appliedCount, "skipped", skippedCount)
	return nil
}

// Ping verifies that the database is still reachable.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("store: not open")
	}
	return s.db.PingContext(ctx)
}

// Close releases every pooled connection.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// DB exposes the underlying pool for the query layer.
func (s *Store) DB() *sql.DB {
	return s.db
}

// withTx runs fn in a transaction and commits only when fn returns nil.
func (s *Store) withTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (s *Store) applyMigration(ctx context.Context, m migration) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("store: begin migration %04d: %w", m.version, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", migrateLockKey); err != nil {
		return false, fmt.Errorf("store: lock migration %04d: %w", m.version, err)
	}
	if _, err := tx.ExecContext(ctx, createMigrationsTable); err != nil {
		return false, fmt.Errorf("store: create schema_migrations: %w", err)
	}

	var applied bool
	if err := tx.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", m.version).Scan(&applied); err != nil {
		return false, fmt.Errorf("store: check migration %04d: %w", m.version, err)
	}
	if applied {
		return false, tx.Commit()
	}

	// The whole file runs as one implicit multi-statement query. pgx uses the
	// simple protocol when no arguments are passed, which allows this.
	if _, err := tx.ExecContext(ctx, m.sql); err != nil {
		return false, fmt.Errorf("store: apply migration %04d (%s): %w", m.version, m.name, err)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT (version) DO NOTHING", m.version); err != nil {
		return false, fmt.Errorf("store: record migration %04d: %w", m.version, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit migration %04d: %w", m.version, err)
	}
	return true, nil
}

type migration struct {
	version int
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("store: read embedded migrations: %w", err)
	}

	migrations := make([]migration, 0, len(entries))
	seen := make(map[int]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".sql" {
			continue
		}
		version, err := parseMigrationName(entry.Name())
		if err != nil {
			return nil, err
		}
		if other, ok := seen[version]; ok {
			return nil, fmt.Errorf("store: duplicate migration version %d in %q and %q", version, other, entry.Name())
		}
		seen[version] = entry.Name()

		body, err := migrationFS.ReadFile(path.Join("migrations", entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("store: read migration %s: %w", entry.Name(), err)
		}
		migrations = append(migrations, migration{
			version: version,
			name:    entry.Name(),
			sql:     string(body),
		})
	}

	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })
	return migrations, nil
}

// parseMigrationName extracts the numeric prefix of "<version>_<name>.sql".
func parseMigrationName(name string) (int, error) {
	base := strings.TrimSuffix(name, ".sql")
	prefix, _, ok := strings.Cut(base, "_")
	if !ok {
		return 0, fmt.Errorf("store: migration %q must be named <version>_<name>.sql", name)
	}
	version, err := strconv.Atoi(prefix)
	if err != nil || version <= 0 {
		return 0, fmt.Errorf("store: migration %q has invalid version %q", name, prefix)
	}
	return version, nil
}

// dsnHost returns the host:port of dsn without exposing credentials.
func dsnHost(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "database"
	}
	return u.Host
}
