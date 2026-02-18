package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/grafana/tempo/pkg/kafkaotlpforwarder"
)

type arrayFlags []string

func (a *arrayFlags) String() string {
	return fmt.Sprintf("%v", *a)
}

func (a *arrayFlags) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func runTestMode(cfg *kafkaotlpforwarder.ConsumerConfig, logger log.Logger) {
	level.Info(logger).Log("msg", "running in test mode", "brokers", fmt.Sprintf("%v", cfg.Brokers), "topic", cfg.Topic)

	// Create consumer
	consumer, err := kafkaotlpforwarder.NewConsumer(cfg)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create consumer", "err", err)
		os.Exit(1)
	}
	defer consumer.Close()

	level.Info(logger).Log("msg", "connected to Kafka, polling for one record...")

	// Poll for records with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fetches := consumer.Poll(ctx)
	if fetches.IsClientClosed() {
		level.Error(logger).Log("msg", "kafka client closed")
		os.Exit(1)
	}

	if err := fetches.Err(); err != nil {
		level.Error(logger).Log("msg", "fetch error", "err", err)
		os.Exit(1)
	}

	if fetches.Empty() {
		level.Info(logger).Log("msg", "no records available (topic may be empty or at end)")
		level.Info(logger).Log("msg", "test successful: connected to Kafka successfully")
		return
	}

	// Process first record only
	recordCount := 0
	var firstRecord []byte
	var firstPartition int32
	var firstOffset int64

	for iter := fetches.RecordIter(); !iter.Done() && recordCount < 1; {
		rec := iter.Next()
		firstRecord = rec.Value
		firstPartition = rec.Partition
		firstOffset = rec.Offset
		recordCount++
	}

	if recordCount == 0 {
		level.Info(logger).Log("msg", "no records in fetch")
		level.Info(logger).Log("msg", "test successful: connected to Kafka successfully")
		return
	}

	level.Info(logger).Log(
		"msg", "successfully read record",
		"partition", firstPartition,
		"offset", firstOffset,
		"size_bytes", len(firstRecord),
	)

	// Try to decode as tempopb.PushBytesRequest
	tracesData, err := kafkaotlpforwarder.DecodePushBytesRequest(firstRecord)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to decode as PushBytesRequest", "err", err)
		level.Info(logger).Log("msg", "test successful: connected and read data, but format validation failed")
		os.Exit(1)
	}

	level.Info(logger).Log(
		"msg", "successfully decoded and converted to OTLP",
		"resource_spans_count", len(tracesData.ResourceSpans),
	)

	// Count total spans
	totalSpans := 0
	for _, rs := range tracesData.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			totalSpans += len(ss.Spans)
		}
	}

	level.Info(logger).Log("msg", "record contains traces", "total_spans", totalSpans)
	level.Info(logger).Log("msg", "✓ test successful: Kafka connectivity and data format validated")
}

func main() {
	var (
		kafkaBrokers       string
		kafkaTopic         string
		consumerGroup      string
		fromBeginning      bool
		endpoints          arrayFlags
		errorMode          string
		batchWaitTimeout   time.Duration
		metricsPort        int
		testMode           bool
		kafkaUsername      string
		kafkaPassword      string
		kafkaSASLMechanism string
	)

	flag.StringVar(&kafkaBrokers, "kafka-brokers", "localhost:9092", "Comma-separated Kafka broker addresses")
	flag.StringVar(&kafkaTopic, "kafka-topic", "tempo-ingest", "Kafka topic to consume from")
	flag.StringVar(&consumerGroup, "consumer-group", "kafka-otlp-forwarder", "Consumer group ID for offset tracking")
	flag.BoolVar(&fromBeginning, "from-beginning", false, "Start from earliest offset (ignores committed offset)")
	flag.Var(&endpoints, "endpoint", "OTLP gRPC endpoint (repeatable for multiple targets)")
	flag.StringVar(&errorMode, "error-mode", "fail-fast", "Error handling: fail-fast, best-effort, all-or-nothing")
	flag.DurationVar(&batchWaitTimeout, "batch-wait-timeout", 30*time.Second, "Max time to wait for batch completion")
	flag.IntVar(&metricsPort, "metrics-port", 10001, "Port to expose Prometheus metrics")
	flag.BoolVar(&testMode, "test", false, "Test mode: connect to Kafka, read one record, and exit without forwarding")
	flag.StringVar(&kafkaUsername, "kafka-username", "", "Kafka SASL username (optional)")
	flag.StringVar(&kafkaPassword, "kafka-password", "", "Kafka SASL password (optional)")
	flag.StringVar(&kafkaSASLMechanism, "kafka-sasl-mechanism", "PLAIN", "Kafka SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")

	flag.Parse()

	// Validate required flags (skip endpoint validation in test mode)
	if !testMode && len(endpoints) == 0 {
		fmt.Fprintf(os.Stderr, "Error: at least one --endpoint must be specified\n")
		flag.Usage()
		os.Exit(1)
	}

	// Setup logger
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "caller", log.DefaultCaller)

	// Setup metrics
	reg := prometheus.NewRegistry()
	metrics := kafkaotlpforwarder.NewMetrics(reg)

	// Start metrics server
	metricsAddr := fmt.Sprintf(":%d", metricsPort)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	metricsServer := &http.Server{
		Addr:    metricsAddr,
		Handler: mux,
	}

	go func() {
		level.Info(logger).Log("msg", "starting metrics server", "addr", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			level.Error(logger).Log("msg", "metrics server failed", "err", err)
			os.Exit(1)
		}
	}()

	// Parse error mode
	mode, valid := kafkaotlpforwarder.ParseErrorMode(errorMode)
	if !valid {
		level.Error(logger).Log("msg", "invalid error mode", "mode", errorMode)
		os.Exit(1)
	}

	// Create consumer config
	consumerCfg := &kafkaotlpforwarder.ConsumerConfig{
		Topic:          kafkaTopic,
		ConsumerGroup:  consumerGroup,
		FromBeginning:  fromBeginning,
		SASLUsername:   kafkaUsername,
		SASLPassword:   kafkaPassword,
		SASLMechanism:  kafkaSASLMechanism,
	}
	consumerCfg.SetBrokersFromString(kafkaBrokers)

	// Test mode: connect, read one record, and exit
	if testMode {
		runTestMode(consumerCfg, logger)
		return
	}

	// Create forwarder config
	cfg := &kafkaotlpforwarder.ForwarderConfig{
		ConsumerConfig:   consumerCfg,
		Endpoints:        endpoints,
		ErrorMode:        mode,
		BatchWaitTimeout: batchWaitTimeout,
	}

	// Create forwarder
	forwarder, err := kafkaotlpforwarder.NewForwarder(cfg, metrics, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create forwarder", "err", err)
		os.Exit(1)
	}
	defer forwarder.Close()

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		level.Info(logger).Log("msg", "received shutdown signal")
		cancel()
	}()

	// Run forwarder
	level.Info(logger).Log(
		"msg", "starting kafka-otlp-forwarder",
		"brokers", kafkaBrokers,
		"topic", kafkaTopic,
		"consumer_group", consumerGroup,
		"endpoints", endpoints,
		"error_mode", errorMode,
	)

	if err := forwarder.Run(ctx); err != nil && err != context.Canceled {
		level.Error(logger).Log("msg", "forwarder failed", "err", err)
		os.Exit(1)
	}

	// Shutdown metrics server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		level.Error(logger).Log("msg", "metrics server shutdown failed", "err", err)
	}

	level.Info(logger).Log("msg", "shutdown complete")
}
