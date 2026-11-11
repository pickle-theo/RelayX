package engine

import (
	"context"

	"relayx/phase5.5/internal/broker"
	"relayx/phase5.5/internal/store"
)

// DeadLetterRecorder persists the records that the consumer could not process.
// It adapts the broker contract to the store layer.
type DeadLetterRecorder struct {
	store *store.Store
}

// NewDeadLetterRecorder returns a recorder that writes to the given store.
func NewDeadLetterRecorder(st *store.Store) *DeadLetterRecorder {
	return &DeadLetterRecorder{store: st}
}

// RecordDeadLetter stores one dead-lettered record.
func (r *DeadLetterRecorder) RecordDeadLetter(ctx context.Context, dl broker.DeadLetter) error {
	if r == nil || r.store == nil {
		return nil
	}
	return r.store.RecordDeadLetter(ctx, store.DeadLetter{
		SourceTopic: dl.SourceTopic,
		Partition:   dl.Partition,
		Offset:      dl.Offset,
		Key:         dl.Key,
		Payload:     dl.Payload,
		Error:       dl.Error,
		ErrorKind:   dl.ErrorKind,
		Attempts:    dl.Attempts,
		CreatedAt:   dl.CreatedAt,
	})
}
