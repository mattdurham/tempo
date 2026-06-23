# Docker Compose: Tempo Parquet vs Blockpack Comparison

**Status:** TODO
**Created:** 2026-02-15
**Priority:** MEDIUM
**Type:** Infrastructure/Testing

## Overview

Create a Docker Compose environment for side-by-side comparison of Tempo storage backends (Parquet vs Blockpack) under realistic production conditions. The setup enables performance testing, resource monitoring, and validation of Blockpack integration with the Tempo ecosystem.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Docker Compose Stack                    │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐                                            │
│  │    Kafka     │ (Message Queue)                            │
│  │   (Traces)   │                                            │
│  └──────┬───────┘                                            │
│         │                                                     │
│         ├────────────────┬───────────────────────────────┐   │
│         │                │                               │   │
│         ▼                ▼                               ▼   │
│  ┌─────────────┐  ┌─────────────┐             ┌──────────┐  │
│  │   Tempo-1   │  │   Tempo-2   │             │  Alloy   │  │
│  │  (Parquet)  │  │ (Blockpack) │             │(Monitor) │  │
│  │             │  │             │             │          │  │
│  │ Single Proc │  │ Single Proc │             │          │  │
│  └──────┬──────┘  └──────┬──────┘             └────┬─────┘  │
│         │                │                         │         │
│         ▼                ▼                         │         │
│  ┌─────────────┐  ┌─────────────┐                 │         │
│  │  S3/MinIO   │  │  S3/MinIO   │                 │         │
│  │  (Parquet)  │  │ (Blockpack) │                 │         │
│  └─────────────┘  └─────────────┘                 │         │
│                                                    │         │
│                                                    ▼         │
│                                          ┌──────────────┐    │
│                                          │   Grafana    │    │
│                                          │    Cloud     │    │
│                                          └──────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Components

### 2. Tempo Instance 1 (Parquet Backend)
- **Config**: Single-process mode, Parquet storage
- **Storage**: MinIO bucket `tempo-parquet`
- **Ingestion**: Kafka consumer reading from configurable name but using consumer group mattd-parquet
- **Ports**: 3200 (HTTP), 4317 (OTLP gRPC)
- **Image**: `grafana/tempo:latest`

### 3. Tempo Instance 2 (Blockpack Backend)
- **Config**: Single-process mode, Blockpack storage
- **Storage**: MinIO bucket `tempo-blockpack`
- **Ingestion**: Kafka consumer reading from configurable name but using consumer group mattd-blockpack
- **Ports**: 3201 (HTTP), 4318 (OTLP gRPC)
- **Image**: Custom build with blockpack integration

### 4. MinIO (S3-Compatible Storage)
- **Purpose**: Object storage for both Tempo instances
- **Buckets**: `tempo-parquet`, `tempo-blockpack`
- **Ports**: 9000 (API), 9001 (Console)
- **Image**: `minio/minio:latest`

### 5. Grafana Alloy
- **Purpose**: Metrics collection and forwarding to Grafana Cloud
- **Monitors**:
  - Tempo metrics (query latency, ingestion rate, storage size)
  - Container metrics (CPU, memory, disk I/O)
  - Kafka metrics (lag, throughput)
- **Config**: Forward to Grafana Cloud (requires API key)
- **Image**: `grafana/alloy:latest`

### 6. Project Rhythm Integration
- **Source**: `livestores/project-rhythm` (trace generator or real traffic)
- **Integration**: Produce traces to Kafka topic
- **Options**:
  - Use rhythm as trace generator (synthetic load)
  - OR replay production traces from rhythm
  - OR use rhythm's Kafka producer directly

## Configuration Files

### Directory Structure
```
.docker/
├── docker-compose.yml           # Main compose file
├── tempo-parquet.yaml           # Tempo config for Parquet
├── tempo-blockpack.yaml         # Tempo config for Blockpack
├── alloy-config.yaml            # Alloy monitoring config
├── minio-init.sh                # MinIO bucket creation script
```

### docker-compose.yml

**Services**:
- `minio` - S3-compatible storage
- `tempo-parquet` - Tempo with Parquet backend
- `tempo-blockpack` - Tempo with Blockpack backend
- `alloy` - Metrics collector

**Networks**:
- `tempo-comparison` - Internal network for all services

**Volumes**:
- `minio-data` - MinIO persistence
- `tempo-parquet-data` - Tempo Parquet local storage
- `tempo-blockpack-data` - Tempo Blockpack local storage

### tempo-parquet.yaml

```yaml
stream_over_http_enabled: true
server:
  http_listen_port: 3200
  grpc_listen_port: 9095

distributor:
  receivers:
    kafka:
      brokers:
        - kafka:9092
      topic: tempo.traces
      group_id: tempo-parquet-consumer
      protocol_version: 2.8.0

ingester:
  max_block_duration: 5m

storage:
  trace:
    backend: s3
    s3:
      bucket: tempo-parquet
      endpoint: minio:9000
      access_key: minioadmin
      secret_key: minioadmin
      insecure: true
    block:
      version: vParquet4  # Latest Parquet format
      encoding: zstd

compactor:
  compaction:
    block_retention: 24h
```

### tempo-blockpack.yaml

```yaml
stream_over_http_enabled: true
server:
  http_listen_port: 3201
  grpc_listen_port: 9096

distributor:
  receivers:
    kafka:
      brokers:
        - kafka:9092
      topic: tempo.traces
      group_id: tempo-blockpack-consumer
      protocol_version: 2.8.0

ingester:
  max_block_duration: 5m

storage:
  trace:
    backend: s3
    s3:
      bucket: tempo-blockpack
      endpoint: minio:9000
      access_key: minioadmin
      secret_key: minioadmin
      insecure: true
    block:
      version: vBlockpack1  # Custom blockpack format
      encoding: zstd

compactor:
  compaction:
    block_retention: 24h
```

### alloy-config.yaml

```yaml
# Prometheus metrics collection
prometheus.scrape "tempo_parquet" {
  targets = [
    {"__address__" = "tempo-parquet:3200"},
  ]
  forward_to = [prometheus.remote_write.grafana_cloud.receiver]
  job_name = "tempo-parquet"
}

prometheus.scrape "tempo_blockpack" {
  targets = [
    {"__address__" = "tempo-blockpack:3201"},
  ]
  forward_to = [prometheus.remote_write.grafana_cloud.receiver]
  job_name = "tempo-blockpack"
}

prometheus.scrape "kafka" {
  targets = [
    {"__address__" = "kafka:9092"},
  ]
  forward_to = [prometheus.remote_write.grafana_cloud.receiver]
  job_name = "kafka"
}

# Docker container metrics
prometheus.exporter.cadvisor "containers" {
  docker_only = true
}

prometheus.scrape "cadvisor" {
  targets = prometheus.exporter.cadvisor.containers.targets
  forward_to = [prometheus.remote_write.grafana_cloud.receiver]
}

# Remote write to Grafana Cloud
prometheus.remote_write "grafana_cloud" {
  endpoint {
    url = env("GRAFANA_CLOUD_PROMETHEUS_URL")
    basic_auth {
      username = env("GRAFANA_CLOUD_PROMETHEUS_USER")
      password = env("GRAFANA_CLOUD_API_KEY")
    }
  }
}
```

## Implementation Tasks

### Phase 1: Basic Infrastructure (3-4 hours)

- [ ] Create `.docker/` directory structure
- [ ] Write `docker-compose.yml` with all services
- [ ] Configure Kafka + Zookeeper
- [ ] Configure MinIO with bucket initialization
- [ ] Write init scripts (`kafka-init.sh`, `minio-init.sh`)
- [ ] Test basic stack startup (`docker-compose up`)

### Phase 2: Tempo Configuration (4-5 hours)

- [ ] Write `tempo-parquet.yaml` (Kafka → Parquet → S3)
- [ ] Write `tempo-blockpack.yaml` (Kafka → Blockpack → S3)
- [ ] Configure Kafka consumers for both instances
- [ ] Verify S3 bucket permissions and access
- [ ] Test ingestion path (Kafka → Tempo → S3)
- [ ] Validate block format in S3 buckets

### Phase 3: Project Rhythm Integration (3-4 hours)

- [ ] Clone/integrate `livestores/project-rhythm`
- [ ] Create `rhythm/Dockerfile` for containerization
- [ ] Write `rhythm/config.yaml` (Kafka producer config)
- [ ] Configure trace generation rate and cardinality
- [ ] Test end-to-end flow (Rhythm → Kafka → Tempo → S3)
- [ ] Verify trace data quality (sampling, completeness)

### Phase 4: Monitoring with Alloy (2-3 hours)

- [ ] Write `alloy-config.yaml` (Prometheus scraping)
- [ ] Configure Grafana Cloud credentials (env vars)
- [ ] Add cAdvisor for container metrics
- [ ] Test metric collection and forwarding
- [ ] Verify data appears in Grafana Cloud
- [ ] Create basic dashboard (optional)

### Phase 5: Documentation & Validation (2-3 hours)

- [ ] Write README.md for `.docker/` directory
- [ ] Document startup/shutdown procedures
- [ ] Add troubleshooting section
- [ ] Create comparison queries (Parquet vs Blockpack)
- [ ] Document expected metrics and benchmarks
- [ ] Add cleanup/reset scripts

## Deliverables

1. **Docker Compose Stack**:
   - `docker-compose.yml` with all 7 services
   - Configuration files for each service
   - Initialization scripts

2. **Documentation**:
   - `.docker/README.md` (setup and usage)
   - Architecture diagram
   - Troubleshooting guide
   - Comparison testing guide

3. **Monitoring Dashboards**:
   - Grafana Cloud dashboard JSON (optional)
   - Key metrics to compare (latency, IOPS, storage size)

4. **Validation Tests**:
   - Script to generate synthetic load
   - Comparison queries (same query, both backends)
   - Performance benchmarking harness

## Key Metrics to Compare

### Storage Efficiency
- Block size distribution
- Compression ratio
- S3 object count
- Total storage size (bytes)

### Query Performance
- TraceQL query latency (p50, p95, p99)
- Simple queries (trace by ID)
- Complex queries (TraceQL with aggregations)
- Metric stream queries (Blockpack only)

### Ingestion Performance
- Ingestion rate (spans/second)
- Kafka consumer lag
- Memory usage during ingestion
- CPU usage during ingestion

### Operational Metrics
- Compaction duration
- Compaction frequency
- Block retention effectiveness
- GC pressure and memory allocations

## Success Criteria

- [ ] All services start successfully via `docker-compose up`
- [ ] Traces flow: Rhythm → Kafka → Both Tempo instances → S3
- [ ] Both Tempo instances queryable via API
- [ ] Metrics visible in Grafana Cloud
- [ ] Side-by-side query comparison works
- [ ] Storage comparison data available (Parquet vs Blockpack)
- [ ] Documentation complete and tested

## Environment Variables Required

```bash
# Grafana Cloud
GRAFANA_CLOUD_PROMETHEUS_URL=https://prometheus-...grafana.net/api/prom/push
GRAFANA_CLOUD_PROMETHEUS_USER=<user-id>
GRAFANA_CLOUD_API_KEY=<api-key>

# MinIO (optional override)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Project Rhythm
RHYTHM_KAFKA_BROKER=kafka:9092
RHYTHM_TRACE_RATE=1000  # traces/second
```

## Related Files

- `internal/tempoapi/` - Tempo API integration code
- `.planning/phases/01-core-tempo-api-integration/` - Tempo integration docs
- `benchmark/` - Existing benchmark harnesses
- `doc/BLOCKPACK_FORMAT.md` - Blockpack format specification

## Future Enhancements

### Phase 6 (Optional)
- [ ] Add Grafana instance (local visualization)
- [ ] Implement automated comparison testing
- [ ] Add Prometheus Pushgateway for custom metrics
- [ ] Chaos testing (kill Kafka, MinIO, Tempo instances)
- [ ] Multi-tenancy testing (multiple Kafka topics/tenants)

### Phase 7 (Advanced)
- [ ] Load testing harness (ramp up to 10K spans/sec)
- [ ] Cost analysis (S3 API calls, storage costs)
- [ ] Distributed tracing across Tempo instances
- [ ] Automated performance regression detection

## Estimated Effort

**Total**: 14-19 hours

- Phase 1: 3-4 hours (infrastructure)
- Phase 2: 4-5 hours (Tempo config)
- Phase 3: 3-4 hours (Rhythm integration)
- Phase 4: 2-3 hours (monitoring)
- Phase 5: 2-3 hours (documentation)

## Next Steps

1. Review Project Rhythm documentation (`livestores/project-rhythm`)
2. Verify Tempo Kafka consumer support (check Tempo docs)
3. Confirm Blockpack format version in Tempo integration
4. Set up Grafana Cloud account (if not exists)
5. Create `.docker/` directory and start Phase 1

---

**Note**: This setup enables realistic production-like testing of Blockpack vs Parquet under identical conditions, making performance and storage comparisons meaningful and reproducible.
