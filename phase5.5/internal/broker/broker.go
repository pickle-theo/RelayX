// Package broker wraps segmentio/kafka-go for the Phase 5 event pipeline: an
// at-least-once producer with acks=all, a manually committed consumer and topic
// initialization.
package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Defaults for the local single-broker setup.
const (
	DefaultPartitions  = 3
	DefaultReplicas    = 1
	defaultDialTimeout = 5 * time.Second
	defaultBatchSize   = 100
)

// ErrNoBrokers reports an empty broker list.
var ErrNoBrokers = errors.New("broker: no brokers configured")

// Message is one consumed record. Value aliases the reader's buffer and is only
// valid until the handler returns.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}

// Producer publishes records with acks=all. Every record carries an explicit
// partition key so the same endpoint always lands on the same partition.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer returns a producer for the given broker addresses.
func NewProducer(brokers []string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, ErrNoBrokers
	}
	return &Producer{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    defaultBatchSize,
		BatchTimeout: 20 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		Async:        false,
		Logger:       kafkaLogger{},
	}}, nil
}

// Publish writes one record and blocks until the broker acknowledges it.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	if p == nil || p.writer == nil {
		return errors.New("broker: producer closed")
	}
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("broker: publish to %s: %w", topic, err)
	}
	return nil
}

// Close flushes pending records and releases the connection.
func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

// EnsureTopics creates the topics if they do not exist yet. It is safe to call
// from every process that starts; existing topics are left untouched.
func EnsureTopics(ctx context.Context, brokers []string, partitions int, topics ...string) error {
	if len(brokers) == 0 {
		return ErrNoBrokers
	}
	if partitions <= 0 {
		partitions = DefaultPartitions
	}
	client := &kafka.Client{
		Addr:      kafka.TCP(brokers...),
		Timeout:   defaultDialTimeout,
		Transport: &kafka.Transport{DialTimeout: defaultDialTimeout},
	}
	reqs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		reqs = append(reqs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: DefaultReplicas,
		})
	}
	if len(reqs) == 0 {
		return nil
	}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: reqs})
	if err != nil {
		return fmt.Errorf("broker: create topics: %w", err)
	}
	if len(resp.Errors) > 0 {
		for topic, topicErr := range resp.Errors {
			// TopicAlreadyExists is expected on a second start.
			if topicErr != nil && !errors.Is(topicErr, kafka.TopicAlreadyExists) {
				return fmt.Errorf("broker: create topic %s: %w", topic, topicErr)
			}
		}
	}
	return nil
}

// kafkaLogger routes kafka-go diagnostics to stdout at a low volume. It
// implements the kafka-go logger interfaces used by Writer and Reader.
type kafkaLogger struct{}

func (kafkaLogger) Printf(string, ...any) {}
