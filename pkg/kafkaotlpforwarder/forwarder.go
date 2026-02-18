package kafkaotlpforwarder

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/tempo/pkg/tempopb"
)

// ForwarderConfig holds configuration for the forwarder.
type ForwarderConfig struct {
	ConsumerConfig   *ConsumerConfig
	Endpoints        []string
	ErrorMode        ErrorMode
	BatchWaitTimeout time.Duration
}

// Validate checks if the forwarder configuration is valid.
func (f *ForwarderConfig) Validate() error {
	if f.ConsumerConfig == nil {
		return fmt.Errorf("consumer config cannot be nil")
	}
	if err := f.ConsumerConfig.Validate(); err != nil {
		return fmt.Errorf("invalid consumer config: %w", err)
	}
	if len(f.Endpoints) == 0 {
		return fmt.Errorf("endpoints cannot be empty")
	}
	if _, valid := ParseErrorMode(string(f.ErrorMode)); !valid {
		return fmt.Errorf("invalid error mode: %s", f.ErrorMode)
	}
	if f.BatchWaitTimeout <= 0 {
		return fmt.Errorf("batch wait timeout must be positive")
	}
	return nil
}

// ParseErrorMode parses an error mode string.
func ParseErrorMode(s string) (ErrorMode, bool) {
	switch ErrorMode(s) {
	case ErrorModeFailFast:
		return ErrorModeFailFast, true
	case ErrorModeBestEffort:
		return ErrorModeBestEffort, true
	case ErrorModeAllOrNothing:
		return ErrorModeAllOrNothing, true
	default:
		return "", false
	}
}

// Forwarder coordinates Kafka consumption and OTLP forwarding.
type Forwarder struct {
	consumer *Consumer
	sender   *Sender
	metrics  *Metrics
	logger   log.Logger
}

// NewForwarder creates a new Forwarder instance.
func NewForwarder(cfg *ForwarderConfig, metrics *Metrics, logger log.Logger) (*Forwarder, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	consumer, err := NewConsumer(cfg.ConsumerConfig)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	// Create mock OTLP clients for now (will be replaced with real implementation)
	clients := make([]OTLPClient, len(cfg.Endpoints))
	// TODO: Initialize real OTLP clients

	sender, err := NewSender(clients, cfg.ErrorMode, metrics, cfg.BatchWaitTimeout)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("create sender: %w", err)
	}

	return &Forwarder{
		consumer: consumer,
		sender:   sender,
		metrics:  metrics,
		logger:   logger,
	}, nil
}

// Run starts the forwarder processing loop.
func (f *Forwarder) Run(ctx context.Context) error {
	level.Info(f.logger).Log("msg", "starting forwarder")

	for {
		select {
		case <-ctx.Done():
			level.Info(f.logger).Log("msg", "shutting down forwarder")
			return ctx.Err()
		default:
		}

		// Poll for records
		fetches := f.consumer.Poll(ctx)
		if fetches.IsClientClosed() {
			return fmt.Errorf("kafka client closed")
		}

		if err := f.processFetches(ctx, fetches); err != nil {
			return fmt.Errorf("process fetches: %w", err)
		}
	}
}

func (f *Forwarder) processFetches(ctx context.Context, fetches kgo.Fetches) error {
	if fetches.Empty() {
		return nil
	}

	// Process all records in batch
	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()

		// Update metrics
		f.metrics.BytesRead.Add(float64(len(rec.Value)))
		f.metrics.KafkaOffset.WithLabelValues(fmt.Sprintf("%d", rec.Partition)).Set(float64(rec.Offset))

		// Decode tempopb.PushBytesRequest
		var req tempopb.PushBytesRequest
		if err := req.Unmarshal(rec.Value); err != nil {
			level.Error(f.logger).Log("msg", "failed to unmarshal record", "err", err)
			continue
		}

		// Convert to OTLP
		tracesData, err := ConvertToOTLP(&req)
		if err != nil {
			level.Error(f.logger).Log("msg", "failed to convert to OTLP", "err", err)
			continue
		}

		// Send to all endpoints
		if err := f.sender.SendBatch(ctx, tracesData); err != nil {
			return fmt.Errorf("send batch: %w", err)
		}
	}

	// Commit offsets after successful send
	if err := f.consumer.CommitOffsets(ctx); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	return nil
}

// Close closes the forwarder and releases resources.
func (f *Forwarder) Close() {
	if f.consumer != nil {
		f.consumer.Close()
	}
}
