# blockio/writer — Test Plan

This file is a stub — entries to be added as the test plan is documented.

---

## Overview

The `blockio/writer` package tests should cover:
- Block encoding (all column types and encoding kinds)
- Flush and multi-block write correctness
- Range index construction and serialization
- Bloom filter write (FileBloom section)
- Auto-embedding (TextEmbedder integration)
- Log body auto-parse (JSON/logfmt → log.* columns)
- Compaction round-trip (write → compact → read)
- Format version output (V12, V13, V14)

When test entries are written, add WRITER-TEST-* entries here following the format
established in `internal/modules/executor/TESTS.md`.
