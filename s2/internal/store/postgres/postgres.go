// Package postgres 在 PostgreSQL 上实现 S2 store 契约。
// 所有过期/重试/结算谓词都在 SQL now() 中；Go 层
// 从不计算判定时间戳。
package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"relayx/s2/internal/store"
)

type dbtx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// Store 是基于 pgx 的 Store 实现。
type Store struct {
	pool  *pgxpool.Pool
	clock store.Clock
}

// Open 创建连接池，并在 schema 不存在时应用迁移。
func Open(ctx context.Context, dsn, migrationPath string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", store.ErrDBUnavailable)
	}
	s := &Store{pool: pool, clock: systemClock{}}
	if migrationPath != "" {
		if err := Migrate(ctx, pool, migrationPath); err != nil {
			pool.Close()
			return nil, err
		}
	}
	return s, nil
}

// Migrate 是幂等的：仅当 messages 表不存在时执行 DDL 文件。
func Migrate(ctx context.Context, pool *pgxpool.Pool, path string) error {
	var exists any
	err := pool.QueryRow(ctx, `SELECT to_regclass('messages')`).Scan(&exists)
	if err != nil {
		return fmt.Errorf("postgres: check schema: %w", err)
	}
	if exists != nil {
		return nil
	}
	ddl, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("postgres: read migration %s: %w", path, err)
	}
	if _, err := pool.Exec(ctx, string(ddl)); err != nil {
		return fmt.Errorf("postgres: apply migration: %w", err)
	}
	return nil
}

type txn struct {
	tx   pgx.Tx
	done bool
}

func (t *txn) Messages() store.MessageStore    { return &messageView{view{s: nil, tx: t.tx}} }
func (t *txn) Deliveries() store.DeliveryStore { return &deliveryView{view{s: nil, tx: t.tx}} }
func (t *txn) Facts() store.FactStore          { return &factView{view{s: nil, tx: t.tx}} }
func (t *txn) Commit() error {
	t.done = true
	return t.tx.Commit(context.Background())
}
func (t *txn) Rollback() error {
	if !t.done {
		t.done = true
		return t.tx.Rollback(context.Background())
	}
	return nil
}

func (s *Store) WithinTx(ctx context.Context, fn func(store.Tx) error) error {
	ptx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", store.ErrDBUnavailable)
	}
	t := &txn{tx: ptx}
	if err := fn(t); err != nil {
		_ = t.Rollback()
		return err
	}
	if !t.done {
		return t.Commit()
	}
	return nil
}

func (s *Store) Messages() store.MessageStore      { return &messageView{view{s: s}} }
func (s *Store) Deliveries() store.DeliveryStore   { return &deliveryView{view{s: s}} }
func (s *Store) Endpoints() store.EndpointRegistry { return &endpointView{view{s: s}} }
func (s *Store) Facts() store.FactStore            { return &factView{view{s: s}} }
func (s *Store) Close() error                      { s.pool.Close(); return nil }

type view struct {
	s  *Store
	tx pgx.Tx
}

func (v *view) q() dbtx {
	if v.tx != nil {
		return v.tx
	}
	return v.s.pool
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func nullableString(s string, present bool) any {
	if !present {
		return nil
	}
	return s
}
