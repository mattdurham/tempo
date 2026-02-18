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

func main() {
	var (
		kafkaBrokers     string
		kafkaTopic       string
		consumerGroup    string
		fromBeginning    bool
		endpoints        arrayFlags
		errorMode        string
		batchWaitTimeout time.Duration
		metricsPort      int
	)

	flag.StringVar(&kafkaBrokers, "kafka-brokers", "localhost:9092", "Comma-separated Kafka broker addresses")
	flag.StringVar(&kafkaTopic, "kafka-topic", "tempo-ingest", "Kafka topic to consume from")
	flag.StringVar(&consumerGroup, "consumer-group", "kafka-otlp-forwarder", "Consumer group ID for offset tracking")
	flag.BoolVar(&fromBeginning, "from-beginning", false, "Start from earliest offset (ignores committed offset)")
	flag.Var(&endpoints, "endpoint", "OTLP gRPC endpoint (repeatable for multiple targets)")
	flag.StringVar(&errorMode, "error-mode", "fail-fast", "Error handling: fail-fast, best-effort, all-or-nothing")
	flag.DurationVar(&batchWaitTimeout, "batch-wait-timeout", 30*time.Second, "Max time to wait for batch completion")
	flag.IntVar(&metricsPort, "metrics-port", 9090, "Port to expose Prometheus metrics")

	flag.Parse()

	// Validate required flags
	if len(endpoints) == 0 {
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
		Topic:         kafkaTopic,
		ConsumerGroup: consumerGroup,
		FromBeginning: fromBeginning,
	}
	consumerCfg.SetBrokersFromString(kafkaBrokers)

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
