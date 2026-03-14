# Discovery: Tempo Repository - Docker, Grafana, Metrics & Dashboards

## File Structure

### Docker Configuration
- **Primary docker-compose**: `/home/mdurham/source/tempo-mrd/.docker/docker-compose.yml`
- **Tempo configurations**:
  - `/home/mdurham/source/tempo-mrd/.docker/tempo-blockpack.yaml` (vblockpack backend, HTTP port 3201)
  - `/home/mdurham/source/tempo-mrd/.docker/tempo-parquet.yaml` (vParquet5 backend, HTTP port 3200)
  - `/home/mdurham/source/tempo-mrd/.docker/tempo-simple.yaml` (alternative simple config)
- **Grafana provisioning**:
  - `/home/mdurham/source/tempo-mrd/.docker/grafana-datasources.yaml` (main datasource config)
  - `/home/mdurham/source/tempo-mrd/.docker/grafana-datasources-parquet.yaml`
  - `/home/mdurham/source/tempo-mrd/.docker/grafana-datasources-simple.yaml`
- **Prometheus configuration**: `/home/mdurham/source/tempo-mrd/.docker/prometheus.yaml`
- **Network proxies**:
  - `/home/mdurham/source/tempo-mrd/.docker/toxiproxy-blockpack.json` (simulates 20ms S3 latency)
  - `/home/mdurham/source/tempo-mrd/.docker/toxiproxy-parquet.json`

### Metrics & Dashboards
- **Dashboard files**: `/home/mdurham/source/tempo-mrd/operations/tempo-mixin-compiled/dashboards/`
- **Source metrics files**: `/home/mdurham/source/tempo-mrd/tempodb/` (compactor.go, tempodb.go, blocklist/poller.go)
- **Backend instrumentation**: `/home/mdurham/source/tempo-mrd/tempodb/backend/instrumentation/`

## Key Components

### 1. Docker Compose Services
**File**: `/home/mdurham/source/tempo-mrd/.docker/docker-compose.yml`

#### Core Services:
- **s3proxy** (port 8080): S3 API backed by filesystem for local storage
- **toxiproxy** (ports 8474 API, 9000 proxy): Simulates 20ms S3 latency to test performance
- **kafka** (port 9092): Message broker for Tempo ingest path
- **tempo-parquet** (ports 3200 HTTP, 4317 OTLP gRPC, 4318 OTLP HTTP):
  - Backend: vParquet5 block version
  - Storage: S3 bucket `tempo-parquet`
- **tempo-blockpack** (ports 3201 HTTP, 4319 OTLP gRPC, 4320 OTLP HTTP):
  - Backend: vblockpack block version
  - Storage: S3 bucket `tempo-blockpack`
  - Hostname: `block-builder-0`
- **otel-loadgen-parquet** & **otel-loadgen-blockpack**: Load generators (profile: loadgen)
- **prometheus** (port 9090): Metrics collection
- **grafana** (port 3000): Visualization with anonymous access enabled

#### Network:
- Custom bridge network: `tempo-network`

### 2. Grafana Datasources Configuration
**File**: `/home/mdurham/source/tempo-mrd/.docker/grafana-datasources.yaml`

```yaml
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090

  - name: Tempo Parquet
    type: tempo
    url: http://tempo-parquet:3200
    isDefault: true

  - name: Tempo Blockpack
    type: tempo
    url: http://tempo-blockpack:3201
```

**Provisioning Location**: `/etc/grafana/provisioning/datasources/datasources.yaml` (mounted in container)

### 3. Prometheus Configuration
**File**: `/home/mdurham/source/tempo-mrd/.docker/prometheus.yaml`

- **Scrape interval**: 15 seconds
- **Evaluation interval**: 15 seconds
- **Scrape targets**:
  - `tempo-parquet:3200` (job: `tempo-parquet`)
  - `tempo-blockpack:3201` (job: `tempo-blockpack`)
- **Remote write**: Optional remote Grafana Cloud integration via environment variables:
  - `${GRAFANA_PROM_URL}`
  - `${GRAFANA_PROM_USER}`
  - `${GRAFANA_PROM_TOKEN}`

## Prometheus Metrics Exposed by Tempo

### Compaction Metrics
**Source**: `/home/mdurham/source/tempo-mrd/tempodb/compactor.go` (lines 36-70)

| Metric Name | Type | Labels | Help |
|------------|------|--------|------|
| `tempodb_compaction_blocks_total` | Counter | `level` | Total number of blocks compacted |
| `tempodb_compaction_objects_written_total` | Counter | `level` | Total number of objects written to backend during compaction |
| `tempodb_compaction_bytes_written_total` | Counter | `level` | Total bytes written to backend during compaction |
| `tempodb_compaction_errors_total` | Counter | none | Total number of errors during compaction |
| `tempodb_compaction_objects_combined_total` | Counter | `level` | Total objects combined during compaction |
| `tempodb_compaction_outstanding_blocks` | Gauge | `tenant` | Number of blocks remaining to be compacted before next maintenance cycle |
| `tempodb_compaction_spans_combined_total` | Gauge | `replication_factor` | Number of spans deduped per replication factor |

### Retention Metrics
**Source**: `/home/mdurham/source/tempo-mrd/tempodb/tempodb.go` (lines 50-73)

| Metric Name | Type | Labels | Help |
|------------|------|--------|------|
| `tempodb_retention_duration_seconds` | Histogram | none | Time to perform retention tasks |
| `tempodb_retention_errors_total` | Counter | none | Total errors while performing retention tasks |
| `tempodb_retention_marked_for_deletion_total` | Counter | none | Total blocks marked for deletion |
| `tempodb_retention_deleted_total` | Counter | none | Total blocks deleted |

### Blocklist Metrics
**Source**: `/home/mdurham/source/tempo-mrd/tempodb/blocklist/poller.go` (lines 36-80)

| Metric Name | Type | Labels | Help |
|------------|------|--------|------|
| `tempodb_backend_objects_total` | Gauge | `tenant`, `status` | Total objects (traces) in backend |
| `tempodb_backend_bytes_total` | Gauge | `tenant`, `status` | Total bytes in backend |
| `tempodb_blocklist_poll_errors_total` | Counter | `tenant` | Errors while polling blocklist |
| `tempodb_blocklist_poll_duration_seconds` | Histogram | none | Time to poll and update blocklist |
| `tempodb_blocklist_length` | Gauge | `tenant` | Total blocks per tenant |
| `tempodb_blocklist_tenant_index_errors_total` | Counter | `tenant` | Errors building tenant index |
| `tempodb_blocklist_tenant_index_builder` | Gauge | `tenant` | 1 if building tenant index (0 otherwise) |
| `tempodb_blocklist_tenant_index_age_seconds` | Gauge | `tenant` | Age of last pulled tenant index |

### Backend Request Metrics
**Source**: `/home/mdurham/source/tempo-mrd/tempodb/backend/instrumentation/backend_transports.go` (lines 12-20)

| Metric Name | Type | Labels | Help |
|------------|------|--------|------|
| `tempodb_backend_request_duration_seconds` | Histogram | `operation`, `status_code` | Time spent doing backend storage requests |

## Grafana Dashboards

### Dashboard Structure
**Location**: `/home/mdurham/source/tempo-mrd/operations/tempo-mixin-compiled/dashboards/`

Available dashboards:
1. `tempo-operational.json` (id: 122) - Operational metrics with SLO tracking
2. `tempo-backendwork.json` - Backend work and compaction metrics
3. `tempo-block-builder.json` - Block builder performance
4. `tempo-reads.json` - Read path metrics
5. `tempo-writes.json` - Write path metrics
6. `tempo-resources.json` - Resource utilization
7. `tempo-rollout-progress.json` - Rollout tracking
8. `tempo-tenants.json` - Per-tenant metrics

### Example Dashboard Features
The dashboards use:
- **Datasource variable**: `${metrics}` for Prometheus datasource
- **Template variables**: `$cluster`, `$namespace`, `$component`, `$latency_metrics`
- **Common metric queries**:
  - Compaction errors: `tempodb_compaction_errors_total`
  - Blocklist poll duration (histogram): `tempodb_blocklist_poll_duration_seconds_bucket` with `histogram_quantile(.99/.9/.5)`
  - Retention duration (histogram): `tempodb_retention_duration_seconds_bucket`
  - Query latency percentiles from request metrics

## Tempo Configuration Details

### Blockpack Backend Config
**File**: `/home/mdurham/source/tempo-mrd/.docker/tempo-blockpack.yaml`

```yaml
server:
  http_listen_port: 3201
  grpc_server_max_recv_msg_size: 104857600
  grpc_server_max_send_msg_size: 104857600

distributor:
  kafka_write_path_enabled: true
  receivers:
    otlp:
      protocols:
        grpc: 0.0.0.0:4319
        http: 0.0.0.0:4320

block_builder:
  consume_cycle_duration: 10s
  block:
    version: vblockpack

storage:
  trace:
    backend: s3
    block:
      version: vblockpack
    s3:
      bucket: tempo-blockpack
      endpoint: toxiproxy:9000
```

### Parquet Backend Config
**File**: `/home/mdurham/source/tempo-mrd/.docker/tempo-parquet.yaml`

```yaml
server:
  http_listen_port: 3200
  grpc_server_max_recv_msg_size: 104857600
  grpc_server_max_send_msg_size: 104857600

block_builder:
  consume_cycle_duration: 10s
  block:
    version: vParquet5

storage:
  trace:
    backend: s3
    block:
      version: vParquet5
    s3:
      bucket: tempo-parquet
      endpoint: toxiproxy:9000
```

## Compaction Configuration

Both configurations use backend scheduler and worker for compaction:

```yaml
backend_scheduler:
  provider:
    compaction:
      measure_interval: 1m
      min_cycle_interval: 30s
      max_jobs_per_tenant: 1000
      min_input_blocks: 2
      max_input_blocks: 4
      max_compaction_level: 2
      compaction:
        block_retention: 48h
        compacted_block_retention: 1h
        max_compaction_objects: 6000000
        max_block_bytes: 107374182400
        compaction_window: 1h
        retention_concurrency: 10
        max_time_per_tenant: 5m
        compaction_cycle: 30s
```

## Key Metrics Relationships

### Compaction Pipeline
1. **Outstanding blocks** (`tempodb_compaction_outstanding_blocks`) - number waiting to compact
2. **Compaction activity** - errors, objects combined, bytes written (per level)
3. **Span deduplication** (`tempodb_compaction_spans_combined_total`) - spans merged per replication factor

### Storage Health
1. **Blocklist state** - length per tenant, bytes/objects total
2. **Blocklist polling** - duration histogram, error counters
3. **Tenant index health** - age, errors, builder status

### Backend Operations
1. **Request timing** (`tempodb_backend_request_duration_seconds`) - S3 operation latencies by operation and status code
2. **Retention cycles** (`tempodb_retention_duration_seconds`) - histogram of retention task duration

## Blockpack Integration Notes

### Blockpack Files
- **Main implementation**: `/home/mdurham/source/tempo-mrd/tempodb/encoding/vblockpack/`
  - `compactor.go` - Compaction logic using blockpack.CompactBlocks
  - `create.go` - Block creation from trace iterator
  - `backend_block.go` - Backend block interface implementation
  - `wal_block.go` - WAL block interface implementation

### Blockpack as Vendor
- Location: `/home/mdurham/source/tempo-mrd/vendor/github.com/grafana/blockpack/`
- Used as columnar format library without custom metrics registration
- Delegates span-level merging to blockpack.CompactBlocks
- No direct Prometheus instrumentation in blockpack package (integration at tempo layer)

### Storage Details
- S3 storage via toxiproxy with 20ms simulated latency
- Buckets: `tempo-blockpack` and `tempo-parquet`
- Local filesystem backed S3 via s3proxy service
- Credentials: minioadmin/minioadmin

## Tempo Metrics Endpoint

Both Tempo instances expose metrics on their HTTP ports:
- **Tempo Parquet**: `http://tempo-parquet:3200/metrics`
- **Tempo Blockpack**: `http://tempo-blockpack:3201/metrics`

These are scraped by Prometheus every 15 seconds per the configuration.

## Summary

The Tempo repository has a well-structured Docker environment with:
1. **Dual backends** for comparison: vblockpack and vParquet5
2. **Complete observability stack**: Prometheus scrapes metrics from both Tempo instances every 15 seconds, Grafana visualizes via 8+ dashboards
3. **Comprehensive metrics** covering: compaction (blocks, objects, bytes, errors), blocklist maintenance (polling duration, state, tenant indexes), retention (duration, errors, deletion), and backend requests
4. **Network simulation**: Toxiproxy adds 20ms latency to S3 operations for realistic performance testing
5. **Load generation**: Optional otel-loadgen services for both backends (profile: loadgen)
6. **Grafana provisioning**: Datasources and dashboards configured via YAML provisioning (auto-discovered from operations/tempo-mixin-compiled/)
