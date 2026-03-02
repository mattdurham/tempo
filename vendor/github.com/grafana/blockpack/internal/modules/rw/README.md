# rw

Storage abstraction layer for blockpack readers.

## Responsibility

rw **provides the byte-moving interface and standard provider compositions for blockpack storage backends**. It does NOT parse or encode blockpack file formats, access span fields, or perform any I/O itself — it wraps caller-supplied storage backends.

## Key Types

| Type | Role |
|------|------|
| `ReaderProvider` | Storage backend interface (`Size`, `ReadAt`) |
| `DataType` | Read-type hint passed to `ReadAt` (footer, block, index, …) |
| `TrackingReaderProvider` | Wraps a `ReaderProvider`; counts `IOOps` and `BytesRead` |
| `RangeCachingProvider` | Wraps a `ReaderProvider`; serves repeated sub-ranges from memory |
| `DefaultProvider` | Standard composition: `RangeCachingProvider(TrackingReaderProvider(storage))` |

## Data Flow

```
Caller
  └── DefaultProvider.ReadAt
        └── RangeCachingProvider.ReadAt   ← cache hit: return from memory, no I/O counted
              └── TrackingReaderProvider.ReadAt  ← counts IOOps + BytesRead
                    └── user storage (e.g. *os.File, object-storage client)
```

## Invariants (do NOT break)

- Cache wraps tracker, not the reverse — `IOOps()` must reflect real backend I/O only.
- `RangeCachingProvider` has no eviction — providers must not be reused across different files.
- All exported types are safe for concurrent use.

## Extending

See `SPECS.md` for full contracts and `NOTES.md` for design rationale before adding new provider types or modifying the composition order.
