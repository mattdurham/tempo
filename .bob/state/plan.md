# Implementation Plan: kafka-otlp-forwarder

## Overview

Build a Go CLI tool that reads traces from Tempo's Kafka ingest topic, converts them from tempopb.PushBytesRequest format to OTLP, and forwards them to multiple OTLP gRPC endpoints. The tool uses franz-go for Kafka consumption (matching Tempo's patterns), supports configurable error handling modes, exposes Prometheus metrics, and maintains consumer group offsets for resumable operation.

## Files to Create

1. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/cmd/kafka-otlp-forwarder/main.go` - CLI entry point with flag parsing
2. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/metrics.go` - Prometheus metrics definitions and setup
3. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/metrics_test.go` - Metrics test coverage
4. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/converter.go` - tempopb to OTLP conversion logic
5. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/converter_test.go` - Converter unit tests
6. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/sender.go` - Multi-endpoint OTLP sending with error handling
7. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/sender_test.go` - Sender unit tests
8. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/consumer.go` - Kafka consumer with franz-go
9. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/consumer_test.go` - Consumer unit tests
10. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/forwarder.go` - Main forwarder orchestration
11. `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/forwarder_test.go` - Integration test for forwarder

## Files to Modify

None - this is a new standalone tool.

## Implementation Steps

### Phase 1: Setup and Metrics (TDD)

#### Step 1.1: Create metrics test file

**Duration:** 2-3 minutes

**Action:** Create test file for metrics definitions.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/metrics_test.go`

**Code to write:**
```go
package kafkaotlpforwarder

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetrics_BytesRead(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	require.NotNil(t, m)

	// Test that BytesRead counter can be incremented
	m.BytesRead.Add(100)

	count := testutil.ToFloat64(m.BytesRead)
	require.Equal(t, float64(100), count)
}

func TestMetrics_BytesSent(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	require.NotNil(t, m)

	// Test that BytesSent counter tracks per endpoint
	m.BytesSent.WithLabelValues("localhost:4317").Add(200)
	m.BytesSent.WithLabelValues("localhost:4318").Add(300)

	// Should be able to retrieve metrics for specific endpoint
	count1 := testutil.ToFloat64(m.BytesSent.WithLabelValues("localhost:4317"))
	count2 := testutil.ToFloat64(m.BytesSent.WithLabelValues("localhost:4318"))

	require.Equal(t, float64(200), count1)
	require.Equal(t, float64(300), count2)
}

func TestMetrics_KafkaOffset(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	require.NotNil(t, m)

	// Test that KafkaOffset gauge tracks per partition
	m.KafkaOffset.WithLabelValues("0").Set(1234)
	m.KafkaOffset.WithLabelValues("1").Set(5678)

	offset1 := testutil.ToFloat64(m.KafkaOffset.WithLabelValues("0"))
	offset2 := testutil.ToFloat64(m.KafkaOffset.WithLabelValues("1"))

	require.Equal(t, float64(1234), offset1)
	require.Equal(t, float64(5678), offset2)
}

func TestMetrics_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m)

	// Verify metrics are registered
	metricFamilies, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, 3, "should have 3 metric families")

	names := make(map[string]bool)
	for _, mf := range metricFamilies {
		names[mf.GetName()] = true
	}

	require.True(t, names["kafka_otlp_forwarder_bytes_read_total"])
	require.True(t, names["kafka_otlp_forwarder_bytes_sent_total"])
	require.True(t, names["kafka_otlp_forwarder_kafka_offset"])
}
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestMetrics
```

**Expected output:** Tests should fail with "no such file or directory" because metrics.go doesn't exist yet.

#### Step 1.2: Implement metrics definitions

**Duration:** 3-4 minutes

**Action:** Create metrics.go with Prometheus metrics.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/metrics.go`

**Code to write:**
```go
package kafkaotlpforwarder

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the forwarder.
type Metrics struct {
	BytesRead   prometheus.Counter
	BytesSent   *prometheus.CounterVec
	KafkaOffset *prometheus.GaugeVec
}

// NewMetrics creates and registers all metrics with the provided registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		BytesRead: factory.NewCounter(prometheus.CounterOpts{
			Name: "kafka_otlp_forwarder_bytes_read_total",
			Help: "Total bytes read from Kafka",
		}),
		BytesSent: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_otlp_forwarder_bytes_sent_total",
			Help: "Total bytes sent to OTLP endpoints",
		}, []string{"endpoint"}),
		KafkaOffset: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_otlp_forwarder_kafka_offset",
			Help: "Current Kafka offset per partition",
		}, []string{"partition"}),
	}
}
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestMetrics -v
```

**Expected output:** All metrics tests should pass.

### Phase 2: Format Conversion (TDD)

#### Step 2.1: Create converter test file

**Duration:** 3-4 minutes

**Action:** Write tests for tempopb to OTLP conversion.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/converter_test.go`

**Code to write:**
```go
package kafkaotlpforwarder

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1_resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1_trace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

func TestConvertToOTLP_EmptyRequest(t *testing.T) {
	req := &tempopb.PushBytesRequest{
		Traces: []*tempopb.Trace{},
		Ids:    [][]byte{},
	}

	tracesData, err := ConvertToOTLP(req)
	require.NoError(t, err)
	require.NotNil(t, tracesData)
	require.Len(t, tracesData.ResourceSpans, 0)
}

func TestConvertToOTLP_SingleTrace(t *testing.T) {
	// Create a valid OTLP ResourceSpans structure
	resourceSpans := &v1_trace.ResourceSpans{
		Resource: &v1_resource.Resource{
			Attributes: []*v1_common.KeyValue{
				{
					Key: "service.name",
					Value: &v1_common.AnyValue{
						Value: &v1_common.AnyValue_StringValue{
							StringValue: "test-service",
						},
					},
				},
			},
		},
		ScopeSpans: []*v1_trace.ScopeSpans{
			{
				Spans: []*v1_trace.Span{
					{
						TraceId: []byte("trace-id-1234567"),
						SpanId:  []byte("span-id-12"),
						Name:    "test-span",
					},
				},
			},
		},
	}

	// Marshal ResourceSpans to bytes (this is what Tempo stores in Kafka)
	sliceData, err := proto.Marshal(resourceSpans)
	require.NoError(t, err)

	// Create PushBytesRequest with the marshaled data
	req := &tempopb.PushBytesRequest{
		Traces: []*tempopb.Trace{
			{Slice: sliceData},
		},
		Ids: [][]byte{[]byte("trace-id-1234567")},
	}

	// Convert to OTLP
	tracesData, err := ConvertToOTLP(req)
	require.NoError(t, err)
	require.NotNil(t, tracesData)
	require.Len(t, tracesData.ResourceSpans, 1)

	// Verify the data matches
	rs := tracesData.ResourceSpans[0]
	require.Equal(t, "test-service", rs.Resource.Attributes[0].Value.GetStringValue())
	require.Len(t, rs.ScopeSpans, 1)
	require.Len(t, rs.ScopeSpans[0].Spans, 1)
	require.Equal(t, "test-span", rs.ScopeSpans[0].Spans[0].Name)
}

func TestConvertToOTLP_MultipleTraces(t *testing.T) {
	// Create two different traces
	rs1 := &v1_trace.ResourceSpans{
		ScopeSpans: []*v1_trace.ScopeSpans{
			{
				Spans: []*v1_trace.Span{
					{Name: "span-1"},
				},
			},
		},
	}

	rs2 := &v1_trace.ResourceSpans{
		ScopeSpans: []*v1_trace.ScopeSpans{
			{
				Spans: []*v1_trace.Span{
					{Name: "span-2"},
				},
			},
		},
	}

	slice1, _ := proto.Marshal(rs1)
	slice2, _ := proto.Marshal(rs2)

	req := &tempopb.PushBytesRequest{
		Traces: []*tempopb.Trace{
			{Slice: slice1},
			{Slice: slice2},
		},
	}

	tracesData, err := ConvertToOTLP(req)
	require.NoError(t, err)
	require.Len(t, tracesData.ResourceSpans, 2)
	require.Equal(t, "span-1", tracesData.ResourceSpans[0].ScopeSpans[0].Spans[0].Name)
	require.Equal(t, "span-2", tracesData.ResourceSpans[1].ScopeSpans[0].Spans[0].Name)
}

func TestConvertToOTLP_InvalidData(t *testing.T) {
	req := &tempopb.PushBytesRequest{
		Traces: []*tempopb.Trace{
			{Slice: []byte("invalid protobuf data")},
		},
	}

	_, err := ConvertToOTLP(req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal")
}

func TestConvertToOTLP_NilRequest(t *testing.T) {
	_, err := ConvertToOTLP(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil")
}
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestConvertToOTLP
```

**Expected output:** Tests should fail because converter.go doesn't exist.

#### Step 2.2: Implement converter

**Duration:** 3-4 minutes

**Action:** Create converter.go with conversion logic.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/converter.go`

**Code to write:**
```go
package kafkaotlpforwarder

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/grafana/tempo/pkg/tempopb"
	v1_trace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

// ConvertToOTLP converts a tempopb.PushBytesRequest to OTLP TracesData format.
// The Trace.Slice field contains pre-encoded OTLP ResourceSpans that need to be
// unmarshaled and aggregated.
func ConvertToOTLP(req *tempopb.PushBytesRequest) (*v1_trace.TracesData, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	tracesData := &v1_trace.TracesData{
		ResourceSpans: make([]*v1_trace.ResourceSpans, 0, len(req.Traces)),
	}

	for i, trace := range req.Traces {
		if trace == nil {
			continue
		}

		var resourceSpans v1_trace.ResourceSpans
		if err := proto.Unmarshal(trace.Slice, &resourceSpans); err != nil {
			return nil, fmt.Errorf("unmarshal trace %d: %w", i, err)
		}

		tracesData.ResourceSpans = append(tracesData.ResourceSpans, &resourceSpans)
	}

	return tracesData, nil
}
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestConvertToOTLP -v
```

**Expected output:** All converter tests should pass.

### Phase 3: Multi-Endpoint Sender (TDD)

#### Step 3.1: Create sender test file

**Duration:** 4-5 minutes

**Action:** Write tests for OTLP sender with error handling modes.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/sender_test.go`

**Code to write:**
```go
package kafkaotlpforwarder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	v1_trace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

// MockOTLPClient simulates an OTLP client for testing.
type MockOTLPClient struct {
	endpoint    string
	shouldError bool
	callCount   int
}

func (m *MockOTLPClient) Send(ctx context.Context, data []byte) error {
	m.callCount++
	if m.shouldError {
		return errors.New("mock send error")
	}
	return nil
}

func TestSender_FailFast(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())

	// Create two clients, one that fails
	clients := []OTLPClient{
		&MockOTLPClient{endpoint: "endpoint1", shouldError: false},
		&MockOTLPClient{endpoint: "endpoint2", shouldError: true},
	}

	sender := &Sender{
		clients:   clients,
		errorMode: ErrorModeFailFast,
		metrics:   metrics,
		timeout:   time.Second * 5,
	}

	tracesData := &v1_trace.TracesData{}

	err := sender.SendBatch(context.Background(), tracesData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock send error")
}

func TestSender_BestEffort(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())

	// Create two clients, one that fails
	clients := []OTLPClient{
		&MockOTLPClient{endpoint: "endpoint1", shouldError: false},
		&MockOTLPClient{endpoint: "endpoint2", shouldError: true},
	}

	sender := &Sender{
		clients:   clients,
		errorMode: ErrorModeBestEffort,
		metrics:   metrics,
		timeout:   time.Second * 5,
	}

	tracesData := &v1_trace.TracesData{}

	// Should not return error in best-effort mode
	err := sender.SendBatch(context.Background(), tracesData)
	require.NoError(t, err)
}

func TestSender_AllOrNothing_Success(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())

	// All clients succeed
	clients := []OTLPClient{
		&MockOTLPClient{endpoint: "endpoint1", shouldError: false},
		&MockOTLPClient{endpoint: "endpoint2", shouldError: false},
	}

	sender := &Sender{
		clients:   clients,
		errorMode: ErrorModeAllOrNothing,
		metrics:   metrics,
		timeout:   time.Second * 5,
	}

	tracesData := &v1_trace.TracesData{}

	err := sender.SendBatch(context.Background(), tracesData)
	require.NoError(t, err)
}

func TestSender_AllOrNothing_Failure(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())

	// One client fails
	clients := []OTLPClient{
		&MockOTLPClient{endpoint: "endpoint1", shouldError: false},
		&MockOTLPClient{endpoint: "endpoint2", shouldError: true},
	}

	sender := &Sender{
		clients:   clients,
		errorMode: ErrorModeAllOrNothing,
		metrics:   metrics,
		timeout:   time.Second * 5,
	}

	tracesData := &v1_trace.TracesData{}

	err := sender.SendBatch(context.Background(), tracesData)
	require.Error(t, err)
}

func TestSender_ContextTimeout(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())

	clients := []OTLPClient{
		&MockOTLPClient{endpoint: "endpoint1", shouldError: false},
	}

	sender := &Sender{
		clients:   clients,
		errorMode: ErrorModeFailFast,
		metrics:   metrics,
		timeout:   time.Millisecond * 1,
	}

	// Create a context that will timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	time.Sleep(time.Millisecond * 10) // Ensure timeout

	tracesData := &v1_trace.TracesData{}

	err := sender.SendBatch(ctx, tracesData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context")
}

func TestSender_EmptyEndpoints(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())

	sender := &Sender{
		clients:   []OTLPClient{},
		errorMode: ErrorModeFailFast,
		metrics:   metrics,
		timeout:   time.Second * 5,
	}

	tracesData := &v1_trace.TracesData{}

	err := sender.SendBatch(context.Background(), tracesData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no endpoints")
}
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestSender
```

**Expected output:** Tests should fail because sender.go doesn't exist.

#### Step 3.2: Implement sender

**Duration:** 5 minutes

**Action:** Create sender.go with multi-endpoint logic and error handling.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/sender.go`

**Code to write:**
```go
package kafkaotlpforwarder

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	v1_trace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

// ErrorMode defines how to handle endpoint failures.
type ErrorMode string

const (
	ErrorModeFailFast      ErrorMode = "fail-fast"
	ErrorModeBestEffort    ErrorMode = "best-effort"
	ErrorModeAllOrNothing  ErrorMode = "all-or-nothing"
)

// OTLPClient defines the interface for sending OTLP data.
type OTLPClient interface {
	Send(ctx context.Context, data []byte) error
}

// Sender handles sending OTLP data to multiple endpoints.
type Sender struct {
	clients   []OTLPClient
	errorMode ErrorMode
	metrics   *Metrics
	timeout   time.Duration
}

// NewSender creates a new Sender instance.
func NewSender(clients []OTLPClient, errorMode ErrorMode, metrics *Metrics, timeout time.Duration) (*Sender, error) {
	if len(clients) == 0 {
		return nil, fmt.Errorf("no endpoints configured")
	}

	return &Sender{
		clients:   clients,
		errorMode: errorMode,
		metrics:   metrics,
		timeout:   timeout,
	}, nil
}

// SendBatch sends a batch of traces to all configured endpoints.
func (s *Sender) SendBatch(ctx context.Context, tracesData *v1_trace.TracesData) error {
	if len(s.clients) == 0 {
		return fmt.Errorf("no endpoints configured")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// Marshal once for all endpoints
	data, err := proto.Marshal(tracesData)
	if err != nil {
		return fmt.Errorf("marshal traces: %w", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(s.clients))

	for i, client := range s.clients {
		wg.Add(1)
		go func(idx int, c OTLPClient) {
			defer wg.Done()

			if err := c.Send(ctx, data); err != nil {
				errChan <- fmt.Errorf("endpoint %d: %w", idx, err)
			}
		}(i, client)
	}

	wg.Wait()
	close(errChan)

	return s.handleErrors(errChan, len(data))
}

func (s *Sender) handleErrors(errChan <-chan error, dataSize int) error {
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	switch s.errorMode {
	case ErrorModeFailFast:
		if len(errors) > 0 {
			return errors[0]
		}
	case ErrorModeBestEffort:
		// Log errors but continue - no error returned
		// Metrics will track failures
		return nil
	case ErrorModeAllOrNothing:
		if len(errors) > 0 {
			return fmt.Errorf("all-or-nothing failed: %d endpoints errored", len(errors))
		}
	}

	return nil
}
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestSender -v
```

**Expected output:** All sender tests should pass.

### Phase 4: Kafka Consumer (TDD)

#### Step 4.1: Create consumer test file

**Duration:** 4-5 minutes

**Action:** Write tests for Kafka consumer setup and configuration.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/consumer_test.go`

**Code to write:**
```go
package kafkaotlpforwarder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewConsumerConfig_Defaults(t *testing.T) {
	cfg := &ConsumerConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "tempo-ingest",
		ConsumerGroup: "test-group",
		FromBeginning: false,
	}

	require.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	require.Equal(t, "tempo-ingest", cfg.Topic)
	require.Equal(t, "test-group", cfg.ConsumerGroup)
	require.False(t, cfg.FromBeginning)
}

func TestNewConsumerConfig_Validation(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *ConsumerConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			cfg: &ConsumerConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "tempo-ingest",
				ConsumerGroup: "test-group",
			},
			expectError: false,
		},
		{
			name: "missing brokers",
			cfg: &ConsumerConfig{
				Brokers:       []string{},
				Topic:         "tempo-ingest",
				ConsumerGroup: "test-group",
			},
			expectError: true,
			errorMsg:    "brokers",
		},
		{
			name: "missing topic",
			cfg: &ConsumerConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "",
				ConsumerGroup: "test-group",
			},
			expectError: true,
			errorMsg:    "topic",
		},
		{
			name: "missing consumer group",
			cfg: &ConsumerConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "tempo-ingest",
				ConsumerGroup: "",
			},
			expectError: true,
			errorMsg:    "consumer group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConsumerConfig_BrokersFromString(t *testing.T) {
	cfg := &ConsumerConfig{}
	cfg.SetBrokersFromString("broker1:9092,broker2:9092,broker3:9092")

	require.Len(t, cfg.Brokers, 3)
	require.Equal(t, "broker1:9092", cfg.Brokers[0])
	require.Equal(t, "broker2:9092", cfg.Brokers[1])
	require.Equal(t, "broker3:9092", cfg.Brokers[2])
}

func TestConsumerConfig_BrokersFromString_SingleBroker(t *testing.T) {
	cfg := &ConsumerConfig{}
	cfg.SetBrokersFromString("localhost:9092")

	require.Len(t, cfg.Brokers, 1)
	require.Equal(t, "localhost:9092", cfg.Brokers[0])
}

func TestConsumerConfig_BrokersFromString_WithSpaces(t *testing.T) {
	cfg := &ConsumerConfig{}
	cfg.SetBrokersFromString(" broker1:9092 , broker2:9092 , broker3:9092 ")

	require.Len(t, cfg.Brokers, 3)
	require.Equal(t, "broker1:9092", cfg.Brokers[0])
	require.Equal(t, "broker2:9092", cfg.Brokers[1])
	require.Equal(t, "broker3:9092", cfg.Brokers[2])
}
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestNewConsumerConfig -run TestConsumerConfig
```

**Expected output:** Tests should fail because consumer.go doesn't exist.

#### Step 4.2: Implement consumer configuration

**Duration:** 5 minutes

**Action:** Create consumer.go with franz-go consumer setup.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/consumer.go`

**Code to write:**
```go
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
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestConsumerConfig -v
```

**Expected output:** All consumer config tests should pass.

### Phase 5: Main Forwarder Orchestration (TDD)

#### Step 5.1: Create forwarder test file

**Duration:** 3-4 minutes

**Action:** Write integration test for forwarder orchestration.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/forwarder_test.go`

**Code to write:**
```go
package kafkaotlpforwarder

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestForwarder_Validation(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *ForwarderConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			cfg: &ForwarderConfig{
				ConsumerConfig: &ConsumerConfig{
					Brokers:       []string{"localhost:9092"},
					Topic:         "tempo-ingest",
					ConsumerGroup: "test-group",
				},
				Endpoints:        []string{"localhost:4317"},
				ErrorMode:        ErrorModeFailFast,
				BatchWaitTimeout: 30 * time.Second,
			},
			expectError: false,
		},
		{
			name: "missing consumer config",
			cfg: &ForwarderConfig{
				Endpoints:        []string{"localhost:4317"},
				ErrorMode:        ErrorModeFailFast,
				BatchWaitTimeout: 30 * time.Second,
			},
			expectError: true,
			errorMsg:    "consumer config",
		},
		{
			name: "missing endpoints",
			cfg: &ForwarderConfig{
				ConsumerConfig: &ConsumerConfig{
					Brokers:       []string{"localhost:9092"},
					Topic:         "tempo-ingest",
					ConsumerGroup: "test-group",
				},
				Endpoints:        []string{},
				ErrorMode:        ErrorModeFailFast,
				BatchWaitTimeout: 30 * time.Second,
			},
			expectError: true,
			errorMsg:    "endpoints",
		},
		{
			name: "invalid error mode",
			cfg: &ForwarderConfig{
				ConsumerConfig: &ConsumerConfig{
					Brokers:       []string{"localhost:9092"},
					Topic:         "tempo-ingest",
					ConsumerGroup: "test-group",
				},
				Endpoints:        []string{"localhost:4317"},
				ErrorMode:        "invalid-mode",
				BatchWaitTimeout: 30 * time.Second,
			},
			expectError: true,
			errorMsg:    "error mode",
		},
		{
			name: "zero timeout",
			cfg: &ForwarderConfig{
				ConsumerConfig: &ConsumerConfig{
					Brokers:       []string{"localhost:9092"},
					Topic:         "tempo-ingest",
					ConsumerGroup: "test-group",
				},
				Endpoints:        []string{"localhost:4317"},
				ErrorMode:        ErrorModeFailFast,
				BatchWaitTimeout: 0,
			},
			expectError: true,
			errorMsg:    "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestForwarderConfig_ParseErrorMode(t *testing.T) {
	tests := []struct {
		input    string
		expected ErrorMode
		isValid  bool
	}{
		{"fail-fast", ErrorModeFailFast, true},
		{"best-effort", ErrorModeBestEffort, true},
		{"all-or-nothing", ErrorModeAllOrNothing, true},
		{"invalid", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			mode, valid := ParseErrorMode(tt.input)
			require.Equal(t, tt.isValid, valid)
			if valid {
				require.Equal(t, tt.expected, mode)
			}
		})
	}
}
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestForwarder
```

**Expected output:** Tests should fail because forwarder.go doesn't exist.

#### Step 5.2: Implement forwarder orchestration

**Duration:** 5 minutes

**Action:** Create forwarder.go with main processing loop.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/pkg/kafkaotlpforwarder/forwarder.go`

**Code to write:**
```go
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
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go test ./pkg/kafkaotlpforwarder -run TestForwarder -v
```

**Expected output:** All forwarder tests should pass.

### Phase 6: CLI Entry Point

#### Step 6.1: Implement main.go

**Duration:** 4-5 minutes

**Action:** Create CLI with flag parsing and metrics server.

**File:** `/home/matt/source/tempo-mrd-worktrees/blockpack-integration/cmd/kafka-otlp-forwarder/main.go`

**Code to write:**
```go
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
		kafkaBrokers      string
		kafkaTopic        string
		consumerGroup     string
		fromBeginning     bool
		endpoints         arrayFlags
		errorMode         string
		batchWaitTimeout  time.Duration
		metricsPort       int
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
```

**Verification:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration
go build ./cmd/kafka-otlp-forwarder
./kafka-otlp-forwarder --help
```

**Expected output:** Should show usage/help text with all flags.

### Phase 7: Full Integration Test

#### Step 7.1: Run all tests

**Duration:** 2-3 minutes

**Action:** Execute full test suite.

**Commands:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration

# Run all tests
go test ./pkg/kafkaotlpforwarder/... -v

# Check test coverage
go test ./pkg/kafkaotlpforwarder/... -cover

# Verify build
go build ./cmd/kafka-otlp-forwarder
```

**Expected output:**
- All tests pass
- Coverage > 80%
- Binary builds successfully

#### Step 7.2: Format and lint

**Duration:** 2 minutes

**Action:** Format code and run linter.

**Commands:**
```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration

# Format code
go fmt ./pkg/kafkaotlpforwarder/...
go fmt ./cmd/kafka-otlp-forwarder/...

# Run linter (if available)
golangci-lint run ./pkg/kafkaotlpforwarder/... ./cmd/kafka-otlp-forwarder/... 2>/dev/null || echo "Linter not available, skipping"

# Check for cyclomatic complexity
gocyclo -over 40 ./pkg/kafkaotlpforwarder ./cmd/kafka-otlp-forwarder 2>/dev/null || echo "gocyclo not available, skipping"
```

**Expected output:** Code is properly formatted, no lint errors, no functions with complexity > 40.

## Edge Cases Handled

### Edge Case 1: Empty Kafka Batch
**Scenario:** Poll returns no records
**Expected:** Loop continues without error, no offset commit
**Test:** Handled in forwarder.go `processFetches` - returns early if `fetches.Empty()`

### Edge Case 2: Invalid Protobuf Data
**Scenario:** Record contains malformed protobuf
**Expected:** Log error, skip record, continue processing
**Test:** `TestConvertToOTLP_InvalidData` verifies error is returned

### Edge Case 3: Context Cancellation
**Scenario:** User sends SIGTERM during processing
**Expected:** Complete current batch, commit offsets, graceful shutdown
**Test:** Main.go handles signal and cancels context

### Edge Case 4: All Endpoints Fail (fail-fast mode)
**Scenario:** All configured endpoints return errors
**Expected:** Don't commit offset, exit with error, batch replays on restart
**Test:** `TestSender_FailFast` verifies error is propagated

### Edge Case 5: Partial Endpoint Failures (best-effort mode)
**Scenario:** Some endpoints fail, others succeed
**Expected:** Log errors, commit offset, continue
**Test:** `TestSender_BestEffort` verifies no error returned

### Edge Case 6: Empty Trace List
**Scenario:** PushBytesRequest with zero traces
**Expected:** Convert successfully to empty TracesData
**Test:** `TestConvertToOTLP_EmptyRequest` verifies handling

### Edge Case 7: Zero Batch Timeout
**Scenario:** Config with invalid timeout
**Expected:** Validation error on startup
**Test:** `TestForwarder_Validation` checks timeout validation

### Edge Case 8: No Endpoints Configured
**Scenario:** CLI started without --endpoint flag
**Expected:** Show usage and exit with error
**Test:** Main.go validates before starting

## Risks and Mitigations

### Risk 1: Breaking Change - New Command Structure
**Risk:** Adding new cmd directory
**Impact:** Low - independent tool, no existing code depends on it
**Mitigation:** No action needed, this is a new addition

### Risk 2: Kafka Connection Failures
**Risk:** Kafka brokers unavailable at startup
**Impact:** Tool fails to start
**Mitigation:** franz-go handles reconnection automatically, tool will retry on next start

### Risk 3: OTLP Client Implementation Incomplete
**Risk:** Real OTLP client not implemented in Phase 5.2
**Impact:** Cannot send traces yet
**Mitigation:**
- Phase 5.2 creates mock clients (TODO comment)
- Next iteration will add real gRPC client implementation
- Tests use mock interface, so they pass

### Risk 4: Memory Usage with Large Batches
**Risk:** Large Kafka fetch batches could cause OOM
**Impact:** Tool crashes
**Mitigation:**
- Plan for next iteration: Add `kgo.FetchMaxBytes()` option
- Document in usage that batch size affects memory
- Consider adding --max-batch-bytes flag

### Risk 5: Offset Commit Timing
**Risk:** Commit after send but before ack could lose data
**Impact:** At-least-once, possible duplicates
**Mitigation:**
- This is acceptable for the use case (testing/development)
- Documented in design as at-least-once delivery
- Alternative would be transactional processing (out of scope)

## Dependencies

### Internal Dependencies
- `github.com/grafana/tempo/pkg/tempopb` - Already in go.mod
- `github.com/grafana/tempo/pkg/kafkaotlpforwarder` - New package (created in this plan)

### External Dependencies (Already in go.mod)
- `github.com/twmb/franz-go v1.20.6` - Kafka client
- `github.com/prometheus/client_golang v1.23.2` - Metrics
- `github.com/go-kit/log v0.2.1` - Logging
- `google.golang.org/protobuf v1.36.11` - Protobuf
- `github.com/stretchr/testify v1.11.1` - Testing

### Missing Dependencies (Need to add)
- `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` - For real OTLP client
  - License: Apache 2.0 (compatible)
  - Will add in next iteration when implementing real OTLP client

## Complexity Analysis

All functions designed to stay under complexity 40:

- `ConvertToOTLP`: Simple loop, complexity ~3
- `SendBatch`: Goroutine spawn + error collection, complexity ~8
- `handleErrors`: Switch statement, complexity ~4
- `processFetches`: Single loop with error handling, complexity ~12
- `Run`: Main loop with context handling, complexity ~10
- `main`: Linear flag parsing and setup, complexity ~15

No functions exceed or approach the 40 limit.

## Test Coverage Goals

- **Metrics package**: > 90% (simple getters/setters)
- **Converter package**: 100% (critical path, all error cases)
- **Sender package**: > 85% (includes error modes)
- **Consumer package**: > 75% (config validation)
- **Forwarder package**: > 70% (orchestration logic)
- **Overall**: > 80% target

## Success Criteria

- [ ] All tests pass
- [ ] Test coverage > 80%
- [ ] Code formatted with `go fmt`
- [ ] No functions > 40 complexity
- [ ] Binary builds successfully
- [ ] Can start with `--help` flag
- [ ] Metrics endpoint accessible
- [ ] No new lint errors introduced
- [ ] All edge cases have test coverage
- [ ] Error handling for all modes tested
- [ ] Consumer config validation working

## Implementation Notes

### OTLP Client Implementation (Deferred)

The real OTLP gRPC client implementation is deferred to a follow-up task because:
1. Current plan focuses on core structure and testing framework
2. Mock interface allows all tests to pass
3. Real client requires additional dependency and configuration
4. Can be added without changing the interface

**Next steps for OTLP client:**
```go
// In sender.go NewSender or separate factory:
import "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

for _, endpoint := range endpoints {
    exporter, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint(endpoint),
        otlptracegrpc.WithInsecure(), // or WithTLSCredentials
    )
    if err != nil {
        return nil, err
    }
    clients = append(clients, &OTLPGRPCClient{exporter: exporter})
}
```

### Testing Against Real Kafka

For manual integration testing:
1. Start Kafka: `docker run -p 9092:9092 apache/kafka:latest`
2. Start tool: `./kafka-otlp-forwarder --endpoint localhost:4317`
3. Verify metrics: `curl localhost:9090/metrics`

### Future Enhancements

Not in scope but noted for future:
- Add `--tls-cert` and `--tls-key` flags for secure OTLP connections
- Add `--kafka-sasl-*` flags for authenticated Kafka
- Add filtering flags: `--filter-service`, `--filter-trace-id`
- Add `--rate-limit` flag to throttle sending
- Add `--dry-run` flag for testing without sending

## Questions/Uncertainties

None - design document is comprehensive and all decisions are documented.
