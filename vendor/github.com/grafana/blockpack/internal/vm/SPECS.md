# vm — Specifications

This document defines the public contracts and invariants for the `vm` package.

This file is a stub — entries to be added as the VM's public contracts are documented.

---

## Overview

The `vm` package provides a stack-based bytecode virtual machine for evaluating TraceQL
and LogQL filter predicates against blockpack columnar data. It compiles query ASTs to
`Program` values that are then executed by the `executor` package against block data.

Key types:
- `Program` — compiled query program (predicates, column filters, vector scorer)
- `TextEmbedder` — minimal interface for VECTOR_AI() query support (Embed only); intentionally narrower than shared.TextEmbedder
- `CompileOptions` — options for compilation (Embedder, filter programs)
- `QuerySpec` — intermediate representation for metrics queries

See `internal/vm/NOTES.md` for design decisions.
See `internal/vm/TESTS.md` for test plan.
