# kafka-otlp-forwarder Design

**Date:** 2026-02-18
**Purpose:** Tool to read traces from Tempo's Kafka queue and forward them to OTLP endpoints

## Overview

A Go CLI tool that consumes traces from Tempo's Kafka ingest topic, converts them from Tempo's internal format to OTLP, and forwards them to one or more OTLP endpoints. Primary use case is testing and development.

## Requirements

- Read from all Kafka partitions with configurable consumer group
- Support consumer group continuation (resumable)
- Start from beginning or continue from last committed offset
- Convert tempopb.PushBytesRequest format to OTLP
- Forward to multiple OTLP endpoints in parallel
- Wait for full batch completion before moving to next batch
- Configurable error handling (fail-fast, best-effort, all-or-nothing)
- Expose Prometheus metrics for bytes read, bytes sent, and offset position
- No filtering or sampling - simple forwarding only

## CLI Interface

```bash
kafka-otlp-forwarder \
  --kafka-brokers=localhost:9092 \
  --kafka-topic=tempo-ingest \
  --consumer-group=replay-testing \
  --from-beginning \
  --endpoint=localhost:4317 \
  --endpoint=localhost:4318 \
  --error-mode=fail-fast \
  --batch-wait-timeout=30s \
  --metrics-port=9090
```

### Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--kafka-brokers` | Comma-separated Kafka broker addresses | `localhost:9092` |
| `--kafka-topic` | Kafka topic to consume from | `tempo-ingest` |
| `--consumer-group` | Consumer group ID for offset tracking | `kafka-otlp-forwarder` |
| `--from-beginning` | Start from earliest offset (ignores committed offset) | `false` |
| `--endpoint` | OTLP gRPC endpoint (repeatable for multiple targets) | Required |
| `--error-mode` | Error handling: `fail-fast`, `best-effort`, `all-or-nothing` | `fail-fast` |
| `--batch-wait-timeout` | Max time to wait for batch completion | `30s` |
| `--metrics-port` | Port to expose Prometheus metrics | `9090` |

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    kafka-otlp-forwarder                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────┐ │
│  │ Kafka        │─────▶│ Format       │─────▶│ OTLP     │ │
│  │ Consumer     │      │ Converter    │      │ Sender   │ │
│  │ (franz-go)   │      │ (tempopb→    │      │ (multi)  │ │
│  └──────────────┘      │  OTLP)       │      └──────────┘ │
│         │              └──────────────┘            │       │
│         │                                           │       │
│         ▼                                           ▼       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │           Prometheus Metrics                         │  │
│  │  - kafka_otlp_forwarder_bytes_read_total            │  │
│  │  - kafka_otlp_forwarder_bytes_sent_total{endpoint}  │  │
│  │  - kafka_otlp_forwarder_kafka_offset{partition}     │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Detailed Design

### 1. Kafka Consumer Setup

Uses **franz-go (kgo)** library, matching Tempo's block-builder implementation.

**Initialization:**
```go
opts := []kgo.Opt{
    kgo.SeedBrokers(brokers...),
    kgo.ConsumerGroup(consumerGroup),
    kgo.ConsumeTopics(topic),
    kgo.DisableAutoCommit(), // Manual offset commits
}

if fromBeginning {
    opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
}

client, err := kgo.NewClient(opts...)
```

**Partition Discovery:**
- Automatically discovers all partitions via `ConsumeTopics()`
- Consumer group handles partition assignment
- Multiple instances can run in same consumer group for scale

**Processing Loop:**
```go
for {
    fetches := client.PollFetches(ctx)

    for iter := fetches.RecordIter(); !iter.Done(); {
        rec := iter.Next()

        // Decode tempopb.PushBytesRequest
        req, err := decoder.Decode(rec.Value)

        // Track metrics
        metrics.BytesRead.Add(len(rec.Value))
        metrics.KafkaOffset.WithLabelValues(
            fmt.Sprintf("%d", rec.Partition),
        ).Set(float64(rec.Offset))

        batch = append(batch, req)
    }

    // Send batch when complete
    sendToEndpoints(batch)

    // Commit offsets after successful send
    client.CommitUncommittedOffsets(ctx)
}
```

### 2. Format Conversion (tempopb → OTLP)

**Message formats:**

Tempo's Kafka format (`tempopb.PushBytesRequest`):
```protobuf
message PushBytesRequest {
    repeated Trace traces = 1;
    repeated bytes ids = 2;
}

message Trace {
    bytes slice = 1;  // Contains encoded OTLP data
}
```

OTLP format: `otlptracev1.TracesData` with ResourceSpans hierarchy.

**Conversion strategy:**

The `Trace.slice` field contains **pre-encoded OTLP data**. Tempo's distributor wraps received OTLP into this format for Kafka. Conversion is primarily unwrapping:

```go
func convertToOTLP(req *tempopb.PushBytesRequest) (*otlptracev1.TracesData, error) {
    tracesData := &otlptracev1.TracesData{}

    for _, trace := range req.Traces {
        // Decode the inner OTLP data
        var resourceSpans otlptracev1.ResourceSpans
        if err := proto.Unmarshal(trace.Slice, &resourceSpans); err != nil {
            return nil, fmt.Errorf("unmarshal trace: %w", err)
        }

        tracesData.ResourceSpans = append(tracesData.ResourceSpans, &resourceSpans)
    }

    return tracesData, nil
}
```

**Dependencies:**
- `github.com/grafana/tempo/pkg/tempopb` — PushBytesRequest types
- `github.com/grafana/tempo/pkg/ingest` — Decoder implementation
- `go.opentelemetry.io/proto/otlp/trace/v1` — OTLP types
- `google.golang.org/protobuf/proto` — Marshaling

### 3. Multi-Endpoint Sending

**Parallel sending:**
```go
func sendBatch(ctx context.Context, tracesData *otlptracev1.TracesData,
               endpoints []string, errorMode string) error {
    data, _ := proto.Marshal(tracesData)

    var wg sync.WaitGroup
    errors := make(chan error, len(endpoints))

    for _, endpoint := range endpoints {
        wg.Add(1)
        go func(ep string) {
            defer wg.Done()

            err := sendToEndpoint(ctx, ep, data)
            if err != nil {
                errors <- fmt.Errorf("%s: %w", ep, err)
            } else {
                metrics.BytesSent.WithLabelValues(ep).Add(float64(len(data)))
            }
        }(endpoint)
    }

    wg.Wait()
    close(errors)

    return handleErrors(errors, errorMode)
}
```

**Error handling modes:**

1. **fail-fast** (default):
   - Stop on first endpoint failure
   - Don't commit offset
   - Exit process with error
   - Batch replays on restart

2. **best-effort**:
   - Log errors but continue
   - Commit offset even if endpoints fail
   - Good for non-critical forwarding
   - Data may be lost on failure

3. **all-or-nothing**:
   - Only commit if ALL endpoints succeed
   - Any failure prevents commit
   - Batch replays on next run
   - Guarantees delivery to all or none

**OTLP Client:**
Uses `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` for:
- gRPC connection management
- Built-in retries
- Proper error handling
- Connection pooling

### 4. Offset Management & Batching

**Batch processing:**
- One batch = all records from single `PollFetches()` call
- Entire batch converted and sent together
- Offsets committed only after successful send
- `--batch-wait-timeout` applies to endpoint sending

**Offset commit logic:**
```go
// Only commit after successful batch send
if err := sendBatch(ctx, tracesData, endpoints, errorMode); err != nil {
    if errorMode == "fail-fast" || errorMode == "all-or-nothing" {
        return err  // Don't commit, will replay
    }
    // best-effort continues and commits below
}

client.CommitUncommittedOffsets(ctx)
```

**Guarantees:**
- At-least-once delivery (with fail-fast or all-or-nothing)
- Consumer group enables resumption across restarts
- Multiple instances share work via partition assignment
- Graceful shutdown completes current batch

### 5. Prometheus Metrics

Exposed on `--metrics-port` at `/metrics` endpoint.

**Metrics:**

```go
// Counter: Total bytes read from Kafka
kafka_otlp_forwarder_bytes_read_total

// Counter: Total bytes sent to each endpoint
kafka_otlp_forwarder_bytes_sent_total{endpoint="localhost:4317"}

// Gauge: Current Kafka offset per partition
kafka_otlp_forwarder_kafka_offset{partition="0"}
```

**Example usage:**
```promql
# Throughput rate
rate(kafka_otlp_forwarder_bytes_read_total[5m])

# Per-endpoint send rate
rate(kafka_otlp_forwarder_bytes_sent_total[5m])

# Lag (requires kafka_exporter for high water mark)
kafka_topic_partition_current_offset - kafka_otlp_forwarder_kafka_offset
```

## Implementation Notes

### Project Structure

```
cmd/kafka-otlp-forwarder/
  main.go                 # CLI and flag parsing
pkg/forwarder/
  consumer.go             # Kafka consumer setup
  converter.go            # tempopb → OTLP conversion
  sender.go               # Multi-endpoint OTLP sending
  metrics.go              # Prometheus metrics
```

### Dependencies

```go
require (
    github.com/grafana/tempo/pkg/tempopb
    github.com/grafana/tempo/pkg/ingest
    github.com/twmb/franz-go/pkg/kgo
    go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc
    go.opentelemetry.io/proto/otlp/trace/v1
    github.com/prometheus/client_golang/prometheus
    google.golang.org/protobuf/proto
)
```

### Testing Strategy

1. **Unit tests:**
   - Format conversion correctness
   - Error handling for each mode
   - Metrics updates

2. **Integration tests:**
   - End-to-end with test Kafka + mock OTLP endpoints
   - Offset commit behavior
   - Consumer group continuation

3. **Manual testing:**
   - Against live Tempo Kafka queue
   - Multiple endpoint scenarios
   - Failure injection

## Trade-offs

**Pros:**
- Simple, focused tool for single purpose
- Reuses Tempo's proven Kafka consumption patterns
- Flexible error handling for different use cases
- Observable via Prometheus metrics
- Consumer group enables scale-out

**Cons:**
- No filtering/sampling capabilities
- No built-in rate limiting
- Requires understanding of consumer groups for operations
- Duplicate sends possible with best-effort mode

## Future Enhancements (Out of Scope)

- Filtering by service name, trace ID, span attributes
- Sampling percentage configuration
- Rate limiting to protect endpoints
- Dead letter queue for failed batches
- HTTP endpoint support (in addition to gRPC)
- Multiple topic consumption
