# Blockpack Configuration

This document describes the configuration options available for the blockpack storage format in Tempo.

## Overview

Blockpack is a columnar storage format optimized for tracing data. It provides efficient compression, indexing, and query performance for trace data.

## Configuration Structure

Blockpack configuration is part of the block configuration in Tempo. It can be configured in the following locations:

- `tempodb.block.blockpack` - Global blockpack settings
- `modules.livestore.block_config.blockpack` - Live store blockpack settings
- `modules.blockbuilder.block.blockpack` - Block builder blockpack settings

## Configuration Options

### compression_codec

Specifies the compression codec to use for blockpack data.

**Type:** string
**Default:** `zstd`
**Valid values:** `zstd`, `snappy`, `lz4`, `none`

**Description:** The compression codec used for compressing column blocks. ZSTD provides the best compression ratio but may be slower. Snappy and LZ4 are faster but provide less compression. Use `none` to disable compression (not recommended for production).

**Example:**
```yaml
storage:
  trace:
    block:
      blockpack:
        compression_codec: zstd
```

### compression_level

Specifies the compression level for the chosen codec.

**Type:** integer
**Default:** `3`
**Valid values:** `0` to `22` (for zstd), `0` (for snappy/lz4/none)

**Description:** Higher compression levels provide better compression ratios but take more CPU time. For ZSTD, values range from 1 (fastest) to 22 (best compression). For other codecs, this value is typically ignored.

**Example:**
```yaml
storage:
  trace:
    block:
      blockpack:
        compression_codec: zstd
        compression_level: 5
```

### column_block_size

Specifies the target size for individual column blocks.

**Type:** integer (bytes)
**Default:** `65536` (64KB)
**Valid values:** Any positive integer

**Description:** The target size for each column block before compression. Larger values can improve compression ratios but may increase memory usage during queries. Smaller values provide more granular access but may reduce compression efficiency.

**Example:**
```yaml
storage:
  trace:
    block:
      blockpack:
        column_block_size: 131072  # 128KB
```

### write_buffer_size

Specifies the buffer size for writing blocks.

**Type:** integer (bytes)
**Default:** `1048576` (1MB)
**Valid values:** Any positive integer

**Description:** The size of the buffer used when writing blockpack data. Larger buffers can improve write throughput but use more memory. This is particularly important during block creation and compaction.

**Example:**
```yaml
storage:
  trace:
    block:
      blockpack:
        write_buffer_size: 2097152  # 2MB
```

### enable_dictionary

Enables dictionary encoding for string columns.

**Type:** boolean
**Default:** `true`

**Description:** When enabled, frequently occurring strings are stored in a dictionary and referenced by index. This can significantly reduce storage size for columns with repeated values (e.g., service names, span names). Disable if your data has high cardinality with few repeated values.

**Example:**
```yaml
storage:
  trace:
    block:
      blockpack:
        enable_dictionary: true
```

### dictionary_max_size

Specifies the maximum size of the dictionary before falling back to plain encoding.

**Type:** integer (bytes)
**Default:** `1048576` (1MB)
**Valid values:** Any positive integer

**Description:** If the dictionary grows beyond this size, the encoder will fall back to plain encoding for the remainder of the column. This prevents unbounded memory growth for high-cardinality columns.

**Example:**
```yaml
storage:
  trace:
    block:
      blockpack:
        dictionary_max_size: 2097152  # 2MB
```

### enable_minhash

Enables MinHash indexing for similarity search.

**Type:** boolean
**Default:** `true`

**Description:** MinHash indexing allows for efficient similarity searches across traces. When enabled, blockpack computes MinHash signatures for trace data, enabling fast approximate matching queries. Disable to reduce index size and write overhead if similarity searches are not needed.

**Example:**
```yaml
storage:
  trace:
    block:
      blockpack:
        enable_minhash: true
```

### minhash_permutations

Specifies the number of permutations for MinHash.

**Type:** integer
**Default:** `128`
**Valid values:** Any positive integer

**Description:** More permutations provide better accuracy for similarity searches but increase index size and computation time. Typical values range from 64 to 256.

**Example:**
```yaml
storage:
  trace:
    block:
      blockpack:
        minhash_permutations: 256
```

### enable_bit_packing

Enables bit-packing for integer columns.

**Type:** boolean
**Default:** `true`

**Description:** When enabled, integer values are packed using only the minimum number of bits required to represent the range of values in the column. This can significantly reduce storage for integer columns with small ranges (e.g., status codes, small counters).

**Example:**
```yaml
storage:
  trace:
    block:
      blockpack:
        enable_bit_packing: true
```

## Complete Example

Here's a complete example configuration showing all blockpack options:

```yaml
storage:
  trace:
    backend: local
    local:
      path: /var/tempo/blocks
    block:
      version: vBlockpack1
      blockpack:
        # Compression settings
        compression_codec: zstd
        compression_level: 5

        # Block and buffer sizes
        column_block_size: 65536      # 64KB
        write_buffer_size: 1048576    # 1MB

        # Dictionary encoding
        enable_dictionary: true
        dictionary_max_size: 1048576  # 1MB

        # MinHash indexing
        enable_minhash: true
        minhash_permutations: 128

        # Bit packing
        enable_bit_packing: true
```

## Performance Tuning

### High Throughput Ingestion

For systems with high write throughput, consider:

```yaml
blockpack:
  compression_codec: lz4          # Faster compression
  compression_level: 0
  write_buffer_size: 4194304      # 4MB
  column_block_size: 131072       # 128KB
  enable_dictionary: true
  enable_minhash: true
```

### Maximum Compression

For systems optimizing for storage size:

```yaml
blockpack:
  compression_codec: zstd
  compression_level: 9            # Higher compression
  write_buffer_size: 1048576
  column_block_size: 262144       # 256KB (larger blocks compress better)
  enable_dictionary: true
  dictionary_max_size: 2097152    # 2MB
  enable_minhash: true
```

### Low Memory Usage

For memory-constrained environments:

```yaml
blockpack:
  compression_codec: snappy       # Lower memory footprint
  compression_level: 0
  write_buffer_size: 524288       # 512KB
  column_block_size: 32768        # 32KB
  dictionary_max_size: 524288     # 512KB
  enable_minhash: false           # Disable to save memory
```

## Migration from Parquet

If migrating from vParquet to vBlockpack, consider:

1. Start with default blockpack settings
2. Monitor performance metrics (write throughput, query latency, storage size)
3. Adjust compression settings based on your workload
4. Enable/disable MinHash based on whether you use similarity searches
5. Tune buffer and block sizes based on available memory

## Validation

Blockpack configuration is validated on startup. Common validation errors:

- **Invalid compression codec**: Must be one of `zstd`, `snappy`, `lz4`, or `none`
- **Negative compression level**: Must be non-negative
- **Zero or negative sizes**: All size parameters must be positive integers

Check the Tempo logs for detailed validation error messages.
