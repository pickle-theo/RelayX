package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

// Handler consumes one message. Returning nil commits the offset. A permanent
// error moves the record to the dead-letter topic; any other error is retried
// with a backoff before the record is dead-lettered.
type Handler func(ctx context.Context, msg Message) error

// Publisher publishes one record to a topic. *Producer satisfies it.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// DeadLetterRecorder persists a record that could not be processed, so an
// operator can inspect and replay it. The store layer satisfies it.
type DeadLetterRecorder interface {
	RecordDeadLetter(ctx context.Context, dl DeadLetter) error
}

// Defaults for the supervision behaviour of a consumer.
const (
	defaultMaxHandlerAttempts = 5
	defaultRetryBackoff       = 500 * time.Millisecond
	defaultRetryBackoffMax    = 30 * time.Second
	defaultFetchBackoff       = 200 * time.Millisecond
	defaultFetchBackoffMax    = 30 * time.Second
)

// ConsumerOptions tunes how a consumer supervises its own work. The zero value
// is valid and selects the defaults.
type ConsumerOptions struct {
	// DeadLetterTopic receives the records that can never be processed. When
	// it is empty a failing record is logged and dropped instead.
	DeadLetterTopic string
	// DeadLetterPublisher publishes to DeadLetterTopic.
	DeadLetterPublisher Publisher
	// DeadLetterRecorder persists the dead-lettered record. It is optional.
	DeadLetterRecorder DeadLetterRecorder
	// MaxHandlerAttempts bounds the attempts for one record before it is
	// treated as poison and dead-lettered.
	MaxHandlerAttempts int
	// RetryBackoff is the delay before the next handler attempt.
	RetryBackoff time.Duration
	// RetryBackoffMax caps the exponential handler retry delay.
	RetryBackoffMax time.Duration
	// FetchBackoff is the delay before the next fetch after a broker error.
	FetchBackoff time.Duration
	// FetchBackoffMax caps the exponential fetch backoff.
	FetchBackoffMax time.Duration
	// Logger receives the supervision diagnostics.
	Logger *slog.Logger
}

func (o *ConsumerOptions) applyDefaults() {
	if o.MaxHandlerAttempts <= 0 {
		o.MaxHandlerAttempts = defaultMaxHandlerAttempts
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = defaultRetryBackoff
	}
	if o.RetryBackoffMax < o.RetryBackoff {
		o.RetryBackoffMax = defaultRetryBackoff
	}
	if o.FetchBackoff <= 0 {
		o.FetchBackoff = defaultFetchBackoff
	}
	if o.FetchBackoffMax < o.FetchBackoff {
		o.FetchBackoffMax = defaultFetchBackoff
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Consumer reads one topic under one group and commits manually after the
// handler succeeds.
//
// Unlike the Phase 5 consumer, a broker outage or a failing handler never
// terminates the loop: fetch errors are retried with a backoff and a record
// that cannot be processed is moved to the dead-letter topic so its partition
// keeps moving.
type Consumer struct {
	reader *kafka.Reader
	opts   ConsumerOptions
}

// NewConsumer returns a consumer that never auto-commits and supervises its
// own fetch and handler failures.
func NewConsumer(brokers []string, group, topic string) (*Consumer, error) {
	return NewConsumerWithOptions(brokers, group, topic, ConsumerOptions{})
}

// NewConsumerWithOptions returns a consumer with explicit supervision options.
func NewConsumerWithOptions(brokers []string, group, topic string, opts ConsumerOptions) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, ErrNoBrokers
	}
	if group == "" || topic == "" {
		return nil, errors.New("broker: group and topic must not be empty")
	}
	opts.applyDefaults()
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:                brokers,
			GroupID:                group,
			Topic:                  topic,
			MinBytes:               1,
			MaxBytes:               1 << 20,
			MaxWait:                500 * time.Millisecond,
			CommitInterval:         0,
			PartitionWatchInterval: 500 * time.Millisecond,
			WatchPartitionChanges:  true,
			Dialer:                 &kafka.Dialer{Timeout: defaultDialTimeout},
			Logger:                 kafkaLogger{},
		}),
		opts: opts,
	}, nil
}

// Run consumes until ctx is cancelled. It returns nil on a clean shutdown and
// only fails when the consumer itself is unusable; recoverable broker and
// handler errors are supervised internally.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	if c == nil || c.reader == nil {
		return errors.New("broker: consumer closed")
	}
	if handler == nil {
		return errors.New("broker: handler must not be nil")
	}
	fetchDelay := c.opts.FetchBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}
		raw, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			// A broker outage must not terminate the consumer: the record stays
			// uncommitted and is redelivered once the broker is reachable again.
			c.opts.Logger.Warn("kafka fetch failed, retrying",
				"topic", c.reader.Config().Topic, "error", err, "retry_in", fetchDelay.String())
			if !sleep(ctx, fetchDelay) {
				return nil
			}
			fetchDelay = nextDelay(fetchDelay, c.opts.FetchBackoffMax)
			continue
		}
		fetchDelay = c.opts.FetchBackoff

		msg := Message{
			Topic:     raw.Topic,
			Partition: raw.Partition,
			Offset:    raw.Offset,
			Key:       raw.Key,
			Value:     raw.Value,
		}
		if err := c.process(ctx, raw, msg, handler); err != nil {
			c.opts.Logger.Error("kafka record not committed",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		}
	}
}

// process runs the handler for one record and commits the offset. A record
// that cannot be processed is dead-lettered and its offset is committed so the
// partition keeps moving.
func (c *Consumer) process(ctx context.Context, raw kafka.Message, msg Message, handler Handler) error {
	var lastErr error
	delay := c.opts.RetryBackoff
	for attempt := 1; attempt <= c.opts.MaxHandlerAttempts; attempt++ {
		err := c.safeHandle(ctx, msg, handler)
		if err == nil {
			return c.commit(ctx, raw)
		}
		lastErr = err
		if IsPermanent(err) {
			break
		}
		if attempt < c.opts.MaxHandlerAttempts {
			c.opts.Logger.Warn("kafka handler failed, retrying",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset,
				"attempt", attempt, "error", err)
			if !sleep(ctx, delay) {
				return ctx.Err()
			}
			delay = nextDelay(delay, c.opts.RetryBackoffMax)
		}
	}

	kind := "permanent"
	if !IsPermanent(lastErr) {
		kind = "retries-exhausted"
	}
	if err := c.deadLetter(ctx, msg, lastErr, kind); err != nil {
		return err
	}
	return c.commit(ctx, raw)
}

// safeHandle runs the handler and converts a panic into an error so a single
// bad record cannot take the consumer down.
func (c *Consumer) safeHandle(ctx context.Context, msg Message, handler Handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("broker: handler panic: %v", r)
		}
	}()
	return handler(ctx, msg)
}

// deadLetter publishes the record to the dead-letter topic and records it.
func (c *Consumer) deadLetter(ctx context.Context, msg Message, cause error, kind string) error {
	if cause == nil {
		cause = errors.New("broker: unknown processing failure")
	}
	if c.opts.DeadLetterTopic == "" || c.opts.DeadLetterPublisher == nil {
		c.opts.Logger.Error("dropping unprocessable record without a dead-letter topic",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", cause)
		return nil
	}

	dl := DeadLetter{
		SourceTopic: msg.Topic,
		Partition:   msg.Partition,
		Offset:      msg.Offset,
		Key:         msg.Key,
		Payload:     msg.Value,
		Error:       cause.Error(),
		ErrorKind:   kind,
		CreatedAt:   time.Now().UTC(),
	}
	body, err := EncodeDeadLetter(DeadLetterEvent{
		SourceTopic: dl.SourceTopic,
		Partition:   dl.Partition,
		Offset:      dl.Offset,
		Key:         dl.Key,
		Payload:     dl.Payload,
		Error:       dl.Error,
		ErrorKind:   dl.ErrorKind,
		CreatedAt:   dl.CreatedAt,
	})
	if err != nil {
		return err
	}
	if err := c.opts.DeadLetterPublisher.Publish(ctx, c.opts.DeadLetterTopic, string(msg.Key), body); err != nil {
		return fmt.Errorf("broker: publish dead-letter record: %w", err)
	}
	if c.opts.DeadLetterRecorder != nil {
		if err := c.opts.DeadLetterRecorder.RecordDeadLetter(ctx, dl); err != nil {
			c.opts.Logger.Warn("record dead letter failed", "error", err)
		}
	}
	c.opts.Logger.Error("record moved to the dead-letter topic",
		"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset,
		"error_kind", kind, "error", cause)
	return nil
}

// commit writes the offset. A failing commit is retried until it succeeds or
// the context is cancelled: skipping the record would break the ordering
// guarantee, while a redelivered record is harmless because every handler is
// idempotent.
func (c *Consumer) commit(ctx context.Context, raw kafka.Message) error {
	delay := c.opts.RetryBackoff
	for {
		err := c.reader.CommitMessages(ctx, raw)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.opts.Logger.Warn("kafka commit failed, retrying",
			"topic", raw.Topic, "partition", raw.Partition, "offset", raw.Offset,
			"error", err, "retry_in", delay.String())
		if !sleep(ctx, delay) {
			return ctx.Err()
		}
		delay = nextDelay(delay, c.opts.RetryBackoffMax)
	}
}

// Close releases the consumer connection. A pending uncommitted offset is
// dropped, so the record is redelivered on the next start.
func (c *Consumer) Close() error {
	if c == nil || c.reader == nil {
		return nil
	}
	return c.reader.Close()
}

// sleep waits for d and reports whether the wait completed instead of the
// context being cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// nextDelay doubles the current delay, caps it at max and adds jitter so a
// fleet of workers does not retry in lockstep. The jitter never pushes the
// result above the cap.
func nextDelay(current, max time.Duration) time.Duration {
	if current <= 0 {
		current = time.Millisecond
	}
	next := current * 2
	if next > max {
		next = max
	}
	out := jitter(next)
	if out > max {
		out = max
	}
	return out
}

// jitter spreads a delay by +/-20 percent.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	span := int64(d) / 5
	if span <= 0 {
		return d
	}
	offset := rand.Int63n(2*span+1) - span
	out := int64(d) + offset
	if out < 0 {
		out = 0
	}
	return time.Duration(out)
}
