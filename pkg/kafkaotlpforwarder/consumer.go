package kafkaotlpforwarder

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerConfig holds Kafka consumer configuration.
type ConsumerConfig struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
	FromBeginning bool
}

// Validate checks if the consumer configuration is valid.
func (c *ConsumerConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers cannot be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.ConsumerGroup == "" {
		return fmt.Errorf("consumer group cannot be empty")
	}
	return nil
}

// SetBrokersFromString parses a comma-separated list of brokers.
func (c *ConsumerConfig) SetBrokersFromString(brokers string) {
	parts := strings.Split(brokers, ",")
	c.Brokers = make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			c.Brokers = append(c.Brokers, trimmed)
		}
	}
}

// Consumer wraps a franz-go Kafka consumer.
type Consumer struct {
	client *kgo.Client
	config *ConsumerConfig
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(), // Manual offset commits for reliability
	}

	if cfg.FromBeginning {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &Consumer{
		client: client,
		config: cfg,
	}, nil
}

// Close closes the Kafka consumer.
func (c *Consumer) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// Poll fetches records from Kafka.
func (c *Consumer) Poll(ctx context.Context) kgo.Fetches {
	return c.client.PollFetches(ctx)
}

// CommitOffsets commits the current offsets.
func (c *Consumer) CommitOffsets(ctx context.Context) error {
	return c.client.CommitUncommittedOffsets(ctx)
}
