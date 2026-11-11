package store

import (
	"context"
	"fmt"
	"time"
)

// DeadLetter is one record that could never be processed. It is kept so an
// operator can inspect it and replay it into the source topic.
type DeadLetter struct {
	ID          uint64
	SourceTopic string
	Partition   int
	Offset      int64
	Key         []byte
	Payload     []byte
	Error       string
	ErrorKind   string
	Attempts    int
	CreatedAt   time.Time
	ReplayedAt  *time.Time
}

// maxDeadLetterError bounds the stored error text.
const maxDeadLetterError = 1024

// RecordDeadLetter stores a dead-lettered record. A record that is already
// known is updated instead of duplicated.
func (s *Store) RecordDeadLetter(ctx context.Context, dl DeadLetter) error {
	errText := dl.Error
	if len(errText) > maxDeadLetterError {
		errText = errText[:maxDeadLetterError]
	}
	attempts := dl.Attempts
	if attempts <= 0 {
		attempts = 1
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO dead_letters (
    source_topic, partition, offset, record_key, payload, error, error_kind, attempts, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, COALESCE($9::timestamptz, now()))
ON CONFLICT (source_topic, partition, offset) DO UPDATE SET
    error = EXCLUDED.error,
    error_kind = EXCLUDED.error_kind,
    attempts = dead_letters.attempts + 1,
    payload = EXCLUDED.payload`,
		dl.SourceTopic, dl.Partition, dl.Offset, dl.Key, dl.Payload,
		errText, dl.ErrorKind, attempts, nullableTime(dl.CreatedAt))
	if err != nil {
		return fmt.Errorf("store: record dead letter: %w", err)
	}
	return nil
}

// CountDeadLetters returns the number of dead letters waiting for a replay.
func (s *Store) CountDeadLetters(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM dead_letters WHERE replayed_at IS NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count dead letters: %w", err)
	}
	return n, nil
}

// MarkDeadLetterReplayed flags a dead letter as replayed.
func (s *Store) MarkDeadLetterReplayed(ctx context.Context, id uint64) error {
	if _, err := s.db.ExecContext(ctx, `
UPDATE dead_letters SET replayed_at = now() WHERE id = $1`, int64(id)); err != nil {
		return fmt.Errorf("store: mark dead letter %d replayed: %w", id, err)
	}
	return nil
}

// ListDeadLetters returns the dead letters that have not been replayed yet,
// oldest first.
func (s *Store) ListDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, source_topic, partition, offset, record_key, payload, error, error_kind, attempts, created_at, replayed_at
FROM dead_letters
WHERE replayed_at IS NULL
ORDER BY id
LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list dead letters: %w", err)
	}
	defer rows.Close()

	var out []DeadLetter
	for rows.Next() {
		var dl DeadLetter
		if err := rows.Scan(&dl.ID, &dl.SourceTopic, &dl.Partition, &dl.Offset, &dl.Key,
			&dl.Payload, &dl.Error, &dl.ErrorKind, &dl.Attempts, &dl.CreatedAt, &dl.ReplayedAt); err != nil {
			return nil, fmt.Errorf("store: scan dead letter: %w", err)
		}
		out = append(out, dl)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate dead letters: %w", err)
	}
	return out, nil
}

// DeleteDeadLetters removes dead letters that were replayed before the given
// time and returns the number of removed rows.
func (s *Store) DeleteDeadLetters(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	res, err := s.db.ExecContext(ctx, `
DELETE FROM dead_letters
WHERE id IN (
    SELECT id FROM dead_letters
    WHERE replayed_at IS NOT NULL AND replayed_at < $1
    ORDER BY id
    LIMIT $2
)`, before, limit)
	if err != nil {
		return 0, fmt.Errorf("store: delete dead letters: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: count deleted dead letters: %w", err)
	}
	return n, nil
}

// nullableTime maps the zero time to SQL NULL.
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
