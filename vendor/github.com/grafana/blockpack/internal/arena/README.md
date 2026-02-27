# Arena

Low-level arena memory allocator for pointer-free data with GC-aware traceability.

## Purpose

This package provides a bump-pointer arena allocator that amortizes Go heap allocations for high-throughput data processing. It is used extensively by the blockpack reader and writer to allocate columnar data (spans, column values, query results) without per-object GC pressure. The arena is designed so that holding any pointer into arena memory keeps the entire arena alive via GC tracing, even though the allocated data itself is pointer-free. This is achieved by embedding a back-pointer to the `Arena` struct in every allocated chunk.

The `arena/slice` subpackage provides arena-backed slices with append, grow, and realloc semantics that avoid the write barriers of normal Go slices.

## Key Types

- `Arena` -- The allocator. Zero value is ready to use. Fields `Next`, `End`, `Cap` are exported for open-coded fast-path allocation in hot loops.
- `slice.Slice[T]` -- Arena-backed slice with `ptr *T`, `len uint32`, `cap uint32`. Does not contain GC-visible pointers.
- `slice.Addr[T]` -- Address-based slice (no write barriers on load/store). Used for performance-critical paths.
- `slice.Untyped` -- Type-erased address slice. Used for heterogeneous storage.

## Key Functions

### Arena (arena.go, alloc.go)

- `New[T](a *Arena, value T) *T` -- Allocate and initialize a value on the arena.
- `(*Arena).Alloc(size int) *byte` -- Low-level bump allocation (pointer-aligned). Nosplit for speed.
- `(*Arena).Reserve(size int)` -- Pre-grow to avoid future `Grow` calls.
- `(*Arena).Free()` -- Reset to empty, retaining the largest block for reuse. All prior pointers become invalid.
- `(*Arena).Grow(size int)` -- Allocate a new chunk of at least `size` bytes.
- `(*Arena).KeepAlive(v any)` -- Tie external memory lifetime to the arena. Slow; avoid in hot paths.
- `AllocTraceable(size int, ptr unsafe.Pointer) *byte` -- Allocate a GC-traced chunk with a back-pointer.
- `SuggestSize(bytes int) int` -- Round up to next power of 2.

### Slice (slice/slice.go)

- `slice.Make[T](a *Arena, n int) Slice[T]` -- Allocate a slice of length `n`.
- `slice.Of[T](a *Arena, values ...T) Slice[T]` -- Allocate and copy values.
- `(Slice[T]).Append(a *Arena, elems ...T) Slice[T]` -- Append with arena realloc.
- `(Slice[T]).AppendOne(a *Arena, elem T) Slice[T]` -- Optimized single-element append.
- `(Slice[T]).Grow(a *Arena, n int) Slice[T]` -- Extend capacity, inlined realloc with fast-path.
- `(Slice[T]).Load(n int) T` / `Store(n int, v T)` -- Indexed access (unsafe in release, bounds-checked in debug).
- `(Slice[T]).Raw() []T` -- Convert to Go slice. Return value must not escape the module.
- `slice.OffArena[T](ptr *T, length int) Untyped` -- Wrap non-arena memory as Untyped (sign-bit tagged).

## Data Flow

1. **Writer path**: `internal/blockio/writer` creates an `Arena`, allocates buffered spans and column data via `slice.Make`/`Append`, then calls `arena.Free()` after flushing each block.
2. **Reader path**: `internal/blockio/reader` allocates query result structures and column data on an arena, returning slices that live until the query completes.
3. **Executor path**: `internal/executor` uses arenas for aggregation buffers and block selection results.

## Design Decisions

1. **Back-pointer tracing** -- Each chunk is `struct { Data [N]byte; Arena *Arena }`. Holding any pointer into `Data` keeps the `*Arena` field alive via GC, which in turn keeps all chunks alive. This replaces runtime finalizers for lifetime management.
2. **Power-of-2 chunk sizes** -- Chunks double in size. After `Free()`, only the largest block is retained, so steady-state usage converges to a single allocation.
3. **Pre-generated shapes** -- `shapes.go` (code-generated) provides `reflect.Type` for every power-of-2 size up to `1<<49`, avoiding dynamic reflection overhead in `AllocTraceable`.
4. **Exported Next/End/Cap** -- These fields are public so hot callsites can open-code the fast allocation path without a function call, since Go does not inline `Alloc` due to the `nosplit` directive.
5. **No write barriers in Addr/Untyped** -- `slice.Addr[T]` replaces pointers with `xunsafe.Addr` to eliminate GC write barriers on slice operations.
6. **Sign-bit tagging** -- `OffArena` sets the sign bit to distinguish non-arena memory from arena memory in `Untyped` slices.

## Invariants

- **Never store Go pointers in arena memory.** The arena allocates pointer-free shapes. Storing a `*T` in arena memory will not be traced by the GC and will cause use-after-free.
- **Never use arena pointers after `Free()`.** Free resets the bump pointer; old allocations will be overwritten.
- **`Slice.Raw()` must not escape.** The returned Go slice bypasses GC tracking. Storing it in a heap-reachable location can cause dangling references.
- **Alignment limit is pointer size (8 bytes).** `New` panics on over-aligned types. Do not allocate types requiring >8-byte alignment.
- **`shapes.go` is generated.** Do not edit manually. Regenerate with `go generate ./internal/arena/`.
- **32-bit targets are unsupported.** The shapes array uses sizes up to `1<<49`.

## Extending

- **Increase max chunk size**: Edit `make_shapes.sh` argument and run `go generate`.
- **Add new Slice operations**: Add methods to `Slice[T]` in `slice/slice.go`. Ensure they use `xunsafe` for the release path and bounds-checked `Raw()` for the debug path.

## Testing

```bash
go test ./internal/arena/...
```

## Package Dependencies

- **Imports**: `internal/debug`, `internal/xunsafe`, `internal/xunsafe/layout`, `reflect`, `math/bits`, `unsafe`, `runtime`, `fmt`
- **Used by**: `internal/blockio/reader`, `internal/blockio/writer`, `internal/executor`, and `arena/slice`
