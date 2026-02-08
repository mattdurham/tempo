# Distributed Tempo with Blockpack Format

This is a variant of the distributed Tempo example that uses the **blockpack** storage format instead of the default parquet format.

## What is Blockpack?

Blockpack is an alternative columnar storage format for Tempo traces that offers:
- **Better compression** with configurable codecs (zstd, snappy, lz4)
- **MinHash indexing** for fast similarity search
- **Dictionary encoding** for string attributes
- **Bit packing** for efficient integer storage
- **Flexible configuration** for different workload profiles

## Differences from Standard Distributed Setup

The only difference is the storage configuration in `tempo.yaml`:

```yaml
storage:
  trace:
    block:
      version: vblockpack  # Use blockpack instead of vparquet5
      blockpack:
        compression_codec: zstd
        compression_level: 3
        column_block_size: 65536
        write_buffer_size: 1048576
        enable_dictionary: true
        dictionary_max_size: 1048576
        enable_minhash: true
        minhash_permutations: 128
        enable_bit_packing: true
```

## Architecture

This example runs a full distributed Tempo deployment:

```
┌─────────────┐
│ Distributor │ ──► Kafka (Redpanda)
└─────────────┘         │
                        │ ingest topic
                        ▼
              ┌──────────────────┐
              │  Block Builders  │ ──► MinIO (S3)
              │  (2 partitions)  │     (blockpack blocks)
              └──────────────────┘
                        │
                        ▼
                ┌───────────────┐
                │  Live Stores  │ ──► Kafka
                │  (4 replicas) │     (blockpack WAL)
                └───────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │ Backend Workers  │ ──► MinIO
              │   (Compaction)   │     (compacted blocks)
              └──────────────────┘
```

All blocks are written in blockpack format, providing efficient storage and query performance.

## Usage

Start the stack:

```bash
docker-compose up -d
```

Send traces:

```bash
# Generate sample traces using Grafana Alloy
docker-compose logs -f alloy
```

Query traces via Grafana:
- Open http://localhost:3000
- Navigate to Explore → Tempo
- Run TraceQL queries

## Configuration Tuning

### High Throughput Profile
For maximum ingest performance:

```yaml
blockpack:
  compression_codec: lz4      # Faster compression
  compression_level: 1
  column_block_size: 131072   # Larger blocks (128KB)
  write_buffer_size: 2097152  # Larger buffer (2MB)
  enable_dictionary: true
  enable_bit_packing: true
```

### Maximum Compression Profile
For storage cost optimization:

```yaml
blockpack:
  compression_codec: zstd
  compression_level: 9        # Maximum compression
  column_block_size: 32768    # Smaller blocks (32KB)
  enable_dictionary: true
  dictionary_max_size: 2097152 # Larger dict (2MB)
  enable_minhash: true
  enable_bit_packing: true
```

### Low Memory Profile
For resource-constrained environments:

```yaml
blockpack:
  compression_codec: snappy
  compression_level: 1
  column_block_size: 32768    # Smaller blocks (32KB)
  write_buffer_size: 524288   # Smaller buffer (512KB)
  enable_dictionary: true
  dictionary_max_size: 262144 # Smaller dict (256KB)
  enable_minhash: false       # Disable MinHash
  enable_bit_packing: true
```

## Monitoring

Access the monitoring stack:
- **Grafana**: http://localhost:3000
- **Prometheus**: http://localhost:9090
- **Redpanda Console**: http://localhost:8080

## Cleanup

```bash
docker-compose down -v
```

## Comparison with Parquet

To compare blockpack vs parquet performance, run both examples side-by-side:

```bash
# Run standard distributed example (parquet)
cd ../distributed
docker-compose up -d

# Run blockpack example
cd ../distributed-blockpack
docker-compose -p tempo-blockpack up -d
```

Compare:
- Storage size (MinIO bucket size)
- Query latency (Grafana query performance)
- Ingestion throughput (Kafka consumer lag)
- Memory usage (Prometheus metrics)

## See Also

- [Blockpack Configuration Documentation](../../../docs/configuration/blockpack.md)
- [Standard Distributed Example](../distributed/)
- [Tempo Documentation](https://grafana.com/docs/tempo/latest/)
