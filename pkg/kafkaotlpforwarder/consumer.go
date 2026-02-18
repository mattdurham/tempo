package kafkaotlpforwarder

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// ConsumerConfig holds Kafka consumer configuration.
type ConsumerConfig struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
	FromBeginning bool
	SASLUsername  string
	SASLPassword  string
	SASLMechanism string
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

		// Connection and timeout settings (matching Tempo's ingest setup)
		kgo.ClientID("kafka-otlp-forwarder"),
		kgo.DialTimeout(10 * time.Second),
		kgo.RequestTimeoutOverhead(10 * time.Second),

		// Metadata refresh settings (matching Tempo's setup)
		kgo.MetadataMinAge(10 * time.Second),
		kgo.MetadataMaxAge(10 * time.Second),

		// Consumer group timeouts (matching Tempo's block-builder)
		kgo.SessionTimeout(3 * time.Minute),
		kgo.RebalanceTimeout(5 * time.Minute),

		// Fetch configuration for better performance
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(100_000_000),          // 100MB
		kgo.FetchMaxWait(5 * time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),  // 50MB per partition
		kgo.BrokerMaxReadBytes(200_000_000),     // 2x fetch max
	}

	if cfg.FromBeginning {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	// Add SASL authentication if credentials provided
	if cfg.SASLUsername != "" && cfg.SASLPassword != "" {
		saslOpt, err := parseSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return nil, fmt.Errorf("invalid SASL mechanism: %w", err)
		}
		opts = append(opts, saslOpt)
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

func parseSASLMechanism(mechanism, username, password string) (kgo.Opt, error) {
	switch mechanism {
	case "PLAIN":
		return kgo.SASL(plain.Auth{
			User: username,
			Pass: password,
		}.AsMechanism()), nil
	case "SCRAM-SHA-256":
		return kgo.SASL(scram.Auth{
			User: username,
			Pass: password,
		}.AsSha256Mechanism()), nil
	case "SCRAM-SHA-512":
		return kgo.SASL(scram.Auth{
			User: username,
			Pass: password,
		}.AsSha512Mechanism()), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", mechanism)
	}
}

// TestConnectivity verifies Kafka connectivity by forcing a metadata refresh.
func (c *Consumer) TestConnectivity(ctx context.Context) error {
	// Force metadata refresh to test connectivity
	// This will return an error if auth fails or broker is unreachable
	c.client.ForceMetadataRefresh()

	// Do a quick poll to trigger actual connection
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	fetches := c.client.PollFetches(pollCtx)

	// Check for client errors
	if err := fetches.Err(); err != nil {
		return fmt.Errorf("connectivity test failed: %w", err)
	}

	return nil
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
