# vectormath — Test Plan

This file is a stub — entries to be added as the test plan is documented.

---

## Overview

The `vectormath` package tests should cover:
- Cosine similarity computation correctness
- Edge cases: zero vectors, unit vectors, identical vectors
- SIMD-accelerated vs. scalar path equivalence
- Overflow and precision for high-dimension vectors

When test entries are written, add VMATH-TEST-* entries here following the format
established in `internal/modules/executor/TESTS.md`.
