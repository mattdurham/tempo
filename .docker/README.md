# Tempo Parquet vs Blockpack Comparison Stack

A Docker Compose environment for comparing Grafana Tempo's Parquet and Blockpack storage backends using k6 as a load generator.

## Overview

This stack includes:
- **Kafka**: Message queue (ready for future integration)
- **MinIO**: S3-compatible object storage for Tempo backends
- **Tempo (Parquet)**: Tempo instance with Parquet storage backend (port 3200, OTLP 4317)
- **Tempo (Blockpack)**: Tempo instance with Blockpack storage backend (port 3201, OTLP 4318)
- **Grafana Alloy**: Metrics collection and forwarding to Grafana Cloud
- **k6 with xk6-otel**: Load generator sending traces via OTLP to both instances

Both Tempo instances are built from the blockpack source code (at `/home/matt/source/blockpack`) with the blockpack integration included.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- ~2GB available disk space
- ~2GB available RAM

### Setup Grafana Cloud Credentials

Create a `.env` file with your Grafana Cloud API key:

```bash
cp .env.example .env
# Edit .env and add your GRAFANA_API_KEY
```

Or pass as environment variable:

```bash
export GRAFANA_API_KEY="glc_eyJ..."
```

### Launch the Stack

```bash
cd /home/matt/source/tempo-mrd-worktrees/blockpack-integration/.docker
docker-compose up --build
```

This will:
1. Build Tempo instances from blockpack source code with blockpack integration
2. Start Kafka, Zookeeper, and MinIO
3. Create S3 buckets in MinIO (tempo-parquet, tempo-blockpack)
4. Start both Tempo instances configured to use the respective backends
5. Start Grafana Alloy to scrape and forward metrics to Grafana Cloud
6. Run k6 load generator sending traces to both instances for 60 seconds

## Monitor the Services

**Grafana Cloud** (metrics and dashboards):
- View metrics: https://grafana.net
- Metrics forwarded from:
  - `tempo-parquet` instance metrics (CPU, memory, query latency, ingestion rate)
  - `tempo-blockpack` instance metrics (CPU, memory, query latency, ingestion rate)
- Scrape interval: 15 seconds
- Key metrics: `tempo_*`, `process_*`, `go_*`

**MinIO Console** (browse stored blocks):
- URL: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`

**Tempo Parquet** (query API):
- HTTP: http://localhost:3200
- OTLP gRPC: localhost:4317
- Metrics: http://localhost:3200/metrics

**Tempo Blockpack** (query API):
- HTTP: http://localhost:3201
- OTLP gRPC: localhost:4318
- Metrics: http://localhost:3201/metrics

## Query Traces

After the load generator finishes, query both instances:

```bash
# Query Parquet backend
curl http://localhost:3200/api/search

# Query Blockpack backend
curl http://localhost:3201/api/search
```

## Configuration

### Environment Variables

**Grafana Cloud Metrics**:
- `GRAFANA_API_KEY`: Your Grafana Cloud API token (required for metrics forwarding)

**k6 Load Generation**:
```bash
docker-compose up \
  -e TRACE_RATE=20 \
  -e VUS=5 \
  -e DURATION=120s
```

- `TRACE_RATE`: Number of traces to send per iteration (default: 10)
- `VUS`: Virtual users / concurrent k6 workers (default: 2)
- `DURATION`: How long to run (default: 60s)
- `TEMPO_PARQUET_ENDPOINT`: Parquet instance endpoint (default: tempo-parquet:4317)
- `TEMPO_BLOCKPACK_ENDPOINT`: Blockpack instance endpoint (default: tempo-blockpack:4318)

### Tempo Configuration

Edit `tempo-parquet.yaml` and `tempo-blockpack.yaml` to customize:
- Ingestion settings (max_block_duration, etc.)
- Storage configuration (S3 buckets, compression, etc.)
- Compaction settings (retention, frequency, etc.)

### Grafana Alloy Configuration

Edit `alloy-config.yaml` to customize:
- Metrics scrape interval (default: 15s)
- Metrics filters (currently: tempo_*, process_*, go_*)
- Grafana Cloud endpoint URL
- Remote write queue settings

Alloy is configured to:
1. Scrape metrics from both Tempo instances on port 3200 and 3201
2. Filter metrics to relevant ones (tempo_*, process_*, go_*)
3. Relabel instance names for clarity (tempo-parquet, tempo-blockpack)
4. Forward to Grafana Cloud with basic auth

### k6 Script

Edit `k6-script.js` to:
- Adjust trace structure and attributes
- Add custom metrics and checks
- Modify load test stages

## Troubleshooting

### Containers won't start
```bash
# Check logs
docker-compose logs -f

# Check disk space
df -h
```

### MinIO initialization fails
```bash
# Manually create buckets
docker-compose exec minio mc alias set local http://minio:9000 minioadmin minioadmin
docker-compose exec minio mc mb local/tempo-parquet
docker-compose exec minio mc mb local/tempo-blockpack
```

### k6 script errors
```bash
# Check script syntax
docker-compose exec k6-generator k6 validate /scripts/script.js

# Run with verbose logging
docker-compose up k6-generator --no-color | grep -i error
```

### Tempo build fails
```bash
# Check if blockpack source exists
ls -la ../../blockpack

# Clean up Docker buildkit cache
docker builder prune

# Rebuild with no cache
docker-compose build --no-cache
```

### View MinIO storage after load test

```bash
docker-compose exec minio mc ls local/tempo-parquet
docker-compose exec minio mc ls local/tempo-blockpack
```

## Performance Comparison Workflow

1. **Start the stack**: `docker-compose up --build`
2. **Let k6 run**: Traces are sent to both instances (60s default)
3. **Wait for blocks to flush**: Ingester flushes blocks every 5 minutes
4. **Compare storage**:
   ```bash
   docker-compose exec minio mc du local/tempo-parquet
   docker-compose exec minio mc du local/tempo-blockpack
   ```
5. **Query both endpoints**: Use Grafana or curl to query and compare latency
6. **Inspect blocks**: Check MinIO console for object sizes and counts

## Next Steps

### Phase 2: Extended Testing
- [ ] Run longer load tests (hours/days)
- [ ] Implement Grafana dashboards for metrics
- [ ] Add query latency benchmarks
- [ ] Measure compression ratios

### Phase 3: Production Scenarios
- [ ] Replay production traces from Project Rhythm
- [ ] Test with realistic cardinality (attributes, labels)
- [ ] Add chaos testing (kill containers, network latency)
- [ ] Monitor memory and CPU utilization

### Phase 4: Automation
- [ ] Create comparison report generation script
- [ ] Automate performance regression detection
- [ ] Add CI/CD integration for benchmarking
- [ ] Store historical comparison data

## Cleanup

```bash
# Stop services
docker-compose down

# Remove volumes (data)
docker-compose down -v

# Clean up everything including images
docker-compose down -v --rmi all
```

## Files

- `docker-compose.yml` - Main compose configuration
- `tempo-parquet.yaml` - Parquet backend configuration
- `tempo-blockpack.yaml` - Blockpack backend configuration
- `alloy-config.yaml` - Grafana Alloy metrics collection and forwarding
- `.env.example` - Example environment variables
- `k6-script.js` - Load generation script
- `k6/Dockerfile` - k6 image with xk6-otel extension

## References

- [Grafana Tempo](https://grafana.com/docs/tempo/)
- [k6 Load Testing](https://k6.io/docs/)
- [xk6-otel Extension](https://github.com/grafana/xk6-otel)
- [MinIO Documentation](https://docs.min.io/)
