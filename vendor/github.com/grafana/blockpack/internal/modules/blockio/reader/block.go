// Package reader implements the blockpack file reader.
package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"sync"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Column holds a decoded column ready for query evaluation.

// per-column intern map for lazy decode

// Dictionary fields — MUST be heap-allocated (never arena) per NOTES §10.

// Inline bytes (kinds 3/4) — no dictionary.

// Presence bitset — MAY be arena-allocated.

// NOTE-001: Lazy decode fields — rawEncoding holds decompressed column bytes.
// For eagerly-decoded columns: rawEncoding is nil (already decoded into Dict/Idx slices).
// For V14 lazily-registered columns: compressedEncoding holds the pending bytes until
// ensureDecompressed() runs on first access, populating rawEncoding; then decodeNow()
// consumes rawEncoding and clears it. rawEncoding is valid only inside decodeOnce.Do.

// compressedEncoding holds the snappy-compressed column blob for V14 lazy columns.
// It is a zero-copy sub-slice of the block's rawBytes and is nil after decompression.
// SPEC-V14-002: decompression is deferred to first column access (ensureDecompressed).

// sparseDictIdx holds the raw sparse dict indexes before the dense Idx slice is built.
// Non-nil means expandDenseIdx() has not been called yet (lazy dense expansion).
// Set by decodeDictKind2Sparse / decodeRLEIndexes; cleared after first value access.
// NOTE-PERF-1: sparse dict columns (kind 2 / kind 7 RLE) defer the O(spanCount)
// expandSparseIndexes allocation until the column is first accessed, avoiding
// allocation for columns that are decoded but never read (e.g. early block exit).

// Total span count this column covers (including nulls).

// ensures decodeNow runs at most once, safe for concurrent callers
// ensures expandDenseIdx runs at most once
// ensures ensureDecompressed runs at most once
// true after decodeNow completes; the ONLY cross-goroutine signal
// NOTE-CONC-001: rawEncoding and compressedEncoding must ONLY be read/written inside their
// respective Once closures (decodeOnce and decompressOnce). The outer fast-path check uses
// decoded.Load() — an atomic read — to avoid races between concurrent accessors.
// IsPresent calls decodeNow (via decodeOnce) rather than a separate presenceOnce to
// eliminate the rawEncoding race that existed when presenceOnce and decodeOnce were independent.
// V14 lazy only: expected decompressed size for SPEC-ROOT-012 bomb guard

// IsDecoded reports whether this column's values have been fully decoded.
// NOTE-001: returns false when decodeNow has not yet completed (column is lazily registered).
// Callers use this to skip columns not in wantColumns — touching any value accessor on an
// un-decoded column triggers decodeNow (snappy + zstd decompression), which must be avoided
// for columns registered lazily but never requested.
// NOTE-CONC-001: uses the decoded atomic.Bool to avoid racing with decodeNow's Once write.
func (c *Column) IsDecoded() bool { return c.decoded.Load() }

// needsDecode reports whether decodeNow still needs to run.
// NOTE-CONC-001: reads decoded atomically — the only safe cross-goroutine check.
func (c *Column) needsDecode() bool { return !c.decoded.Load() }

// SizeBytes returns an estimate of the in-memory size of this column's decoded data
// for objectcache LRU budgeting (NOTE-200). Only the immutable decoded slices that the
// process-level parsedV8ColumnCache shares across queries are counted; the per-query
// mutable scratch (rawEncoding/compressedEncoding/intern/sync.Once) is not cached.
func (c *Column) SizeBytes() int64 {
	n := int64(len(c.StringIdx)+len(c.Int64Idx)+len(c.Uint64Idx)+
		len(c.Float64Idx)+len(c.BoolIdx)+len(c.BytesIdx)) * 4
	n += int64(len(c.sparseDictIdx)) * 4
	n += int64(len(c.Present))
	n += int64(len(c.Int64Dict)+len(c.Uint64Dict)+len(c.Float64Dict)) * 8
	n += int64(len(c.BoolDict))
	for _, s := range c.StringDict {
		n += int64(len(s)) + 16 // string header + bytes
	}
	for _, b := range c.BytesDict {
		n += int64(len(b)) + 24 // slice header + bytes
	}
	for _, b := range c.BytesInline {
		n += int64(len(b)) + 24
	}
	// NOTE-351: uniform-stride columns store inline bytes in one contiguous slab with no
	// per-row header overhead — count just the slab body.
	n += int64(len(c.uniformSlab))
	return n
}

// EnsureDecoded triggers full decode if this column was lazily registered.
// Per-row value accessors (StringValue, Int64Value, etc.) call decodeNow automatically,
// so EnsureDecoded is only needed by callers that access the underlying slices directly
// (StringDict, StringIdx, etc.), bypassing the per-row path.
// NOTE-026: required by scanStringDictFloat before iterating StringDict.
func (c *Column) EnsureDecoded() {
	if c.needsDecode() {
		c.decodeNow()
	}
	c.expandDenseIdx()
}

// expandDenseIdx builds the dense Idx slice from sparseDictIdx + Present on first access.
// Called automatically by StringValue, Int64Value, etc. when sparseDictIdx is set.
// NOTE-PERF-1: deferred from decode time to first-access time.
func (c *Column) expandDenseIdx() {
	c.denseOnce.Do(func() {
		if c.sparseDictIdx == nil {
			return
		}
		dense := expandSparseIndexes(c.sparseDictIdx, c.Present, c.SpanCount)
		c.sparseDictIdx = nil // release sparse slice — no longer needed
		assignDictIdx(c, dense)
	})
}

// IsPresent reports whether span at idx has a value.
// NOTE-CONC-001: always goes through needsDecode() (atomic load) before reading c.Present.
// This ensures the happens-before chain from decodeOnce.Do is established before we read
// c.Present. Reading c.Present without this guard races with a concurrent decodeNow write.
// After decodeNow returns, c.Present is nil for all-present columns (no bitmap = every span
// present) or a non-nil bitset for partial-presence columns.
func (c *Column) IsPresent(idx int) bool {
	if c.needsDecode() {
		c.decodeNow()
	}
	p := c.Present
	if p == nil {
		return true // no presence bitmap = all spans present
	}
	return shared.IsPresent(p, idx)
}

// PresenceView ensures the column is decoded and returns its raw presence bitmap.
// A nil result means every span is present (no presence bitmap was stored). The
// returned slice is the column's immutable decoded Present bitmap and must not be
// mutated by callers — test bits with shared.IsPresent.
//
// NOTE-222: hoist the per-row IsPresent atomic out of tight scan loops. IsPresent(idx)
// performs an atomic decoded.Load() (via needsDecode) on EVERY call so that a concurrent
// decodeNow write is observed with the correct happens-before ordering. In a scan over
// SpanCount rows the column is decoded exactly once — on the first IsPresent — yet the
// atomic load was paid on every subsequent row. A querier CPU profile (2026-06-12) showed
// Column.IsPresent at ~7% of blockpack self-time, dominated by these per-row scans in
// column_provider.go. PresenceView establishes the decode happens-before chain ONCE
// (it goes through needsDecode/decodeNow just like IsPresent) and hands the caller the
// stable Present bitmap, so the loop can bit-test inline with no further atomics. After
// decodeNow returns, Present is immutable for the lifetime of the column (it is part of
// the shared decoded snapshot), so reading it repeatedly without re-loading the atomic is
// race-free for the duration of a single scan.
func (c *Column) PresenceView() []byte {
	if c.needsDecode() {
		c.decodeNow()
	}
	return c.Present
}

// StringValue returns the string value at idx and whether it is present.
// For ColumnTypeUUID columns, the 16-byte binary value is formatted as a UUID string
// (e.g. "213085fc-b15b-45fc-8fa0-d448d4a246be"), preserving the original string representation.
// NOTE-001: triggers lazy decode on first call if column is not yet decoded.
func (c *Column) StringValue(idx int) (string, bool) {
	if c.needsDecode() {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return "", false
	}

	if c.Type == shared.ColumnTypeUUID {
		return c.uuidStringValue(idx)
	}

	if len(c.StringIdx) > idx {
		di := int(c.StringIdx[idx])
		if di < len(c.StringDict) {
			return c.StringDict[di], true
		}
	}

	return "", false
}

// uuidStringValue formats a 16-byte UUID from BytesDict/BytesInline as an RFC 4122 UUID string.
func (c *Column) uuidStringValue(idx int) (string, bool) {
	var b []byte

	if c.hasInlineBytes() {
		b = c.bytesInlineAt(idx)
	} else if len(c.BytesIdx) > idx {
		di := int(c.BytesIdx[idx])
		if di < len(c.BytesDict) {
			b = c.BytesDict[di]
		}
	}

	if len(b) != 16 {
		return "", false
	}

	// Format as RFC 4122 UUID without heap allocation: encode into a fixed
	// 36-byte buffer (8-4-4-4-12 hex groups separated by hyphens).
	var buf [36]byte
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf[:]), true
}

// StringValues returns all string values as a flat slice of length SpanCount,
// building it in one pass over StringIdx/StringDict. Absent rows are represented
// as "". Used by the regex scan path to avoid per-row dictionary indirection.
// NOTE-015: batch string extraction — see executor/NOTES.md NOTE-015.
func (c *Column) StringValues() []string {
	out := make([]string, c.SpanCount)
	for i := range c.SpanCount {
		if v, ok := c.StringValue(i); ok {
			out[i] = v
		}
	}
	return out
}

// Int64Value returns the int64 value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if column is not yet decoded.
func (c *Column) Int64Value(idx int) (int64, bool) {
	if c.needsDecode() {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return 0, false
	}

	if len(c.Int64Idx) > idx {
		di := int(c.Int64Idx[idx])
		if di < len(c.Int64Dict) {
			return c.Int64Dict[di], true
		}
	}

	return 0, false
}

// Uint64Value returns the uint64 value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if column is not yet decoded.
func (c *Column) Uint64Value(idx int) (uint64, bool) {
	if c.needsDecode() {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return 0, false
	}

	if len(c.Uint64Idx) > idx {
		di := int(c.Uint64Idx[idx])
		if di < len(c.Uint64Dict) {
			return c.Uint64Dict[di], true
		}
	}

	return 0, false
}

// Float64Value returns the float64 value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if column is not yet decoded.
func (c *Column) Float64Value(idx int) (float64, bool) {
	if c.needsDecode() {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return 0, false
	}

	if len(c.Float64Idx) > idx {
		di := int(c.Float64Idx[idx])
		if di < len(c.Float64Dict) {
			return c.Float64Dict[di], true
		}
	}

	return 0, false
}

// BoolValue returns the bool value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if column is not yet decoded.
func (c *Column) BoolValue(idx int) (bool, bool) {
	if c.needsDecode() {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return false, false
	}

	if len(c.BoolIdx) > idx {
		di := int(c.BoolIdx[idx])
		if di < len(c.BoolDict) {
			return c.BoolDict[di] != 0, true
		}
	}

	return false, false
}

// BytesValue returns the bytes value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if column is not yet decoded.
func (c *Column) BytesValue(idx int) ([]byte, bool) {
	if c.needsDecode() {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return nil, false
	}

	if c.hasInlineBytes() {
		if b := c.bytesInlineAt(idx); b != nil {
			return b, true
		}

		return nil, false
	}

	if len(c.BytesIdx) > idx {
		di := int(c.BytesIdx[idx])
		if di < len(c.BytesDict) {
			return c.BytesDict[di], true
		}
	}

	return nil, false
}

// VectorF32Value returns the []float32 embedding vector at idx for ColumnTypeVectorF32 columns.
// Returns (nil, false) when the row is not present or the column is not ColumnTypeVectorF32.
// The raw bytes stored by decodeVectorF32 are dim*4 bytes in LE byte order.
func (c *Column) VectorF32Value(idx int) ([]float32, bool) {
	if c.needsDecode() {
		c.decodeNow()
	}
	if c.Type != shared.ColumnTypeVectorF32 {
		return nil, false
	}
	if !c.IsPresent(idx) {
		return nil, false
	}
	raw := c.bytesInlineAt(idx)
	if raw == nil {
		return nil, false
	}
	if len(raw)%4 != 0 {
		return nil, false
	}
	dim := len(raw) / 4
	vec := make([]float32, dim)
	for i := range dim {
		bits := binary.LittleEndian.Uint32(raw[i*4 : i*4+4])
		vec[i] = math.Float32frombits(bits)
	}
	return vec, true
}

// ColIterEntry is a single entry in the pre-computed deduplicated column iteration list.
// Built lazily on first IterFields() call (NOTE-243) from the registered column set.
// NOTE-049: Pre-computed column iteration order eliminates the per-span seen-map alloc.

// Block holds decoded columns for a single block.
type Block struct {
	columns map[shared.ColumnKey]*Column
	// nameIndex maps column name → best-type Column for O(1) GetColumn lookups.
	// Built by buildNameIndex after all columns are registered. When multiple typed
	// variants exist for the same name, the lowest ColumnType value wins (stable tie-breaker).
	nameIndex map[string]*Column
	// NOTE-002: lazyColumnStore is the arena-like backing store for lazily-registered
	// Column structs. One slice allocation replaces N individual *Column allocations.
	// Pointers into this slice (stored in columns map) are stable because the slice
	// is sized to exact capacity before any appends — no reallocation ever occurs.
	lazyColumnStore []Column
	// NOTE-153: lazyStorePtr is the pool handle for lazyColumnStore's backing array. When
	// parseBlockColumnsReuse sourced lazyColumnStore from lazyColumnStorePool, this holds the
	// *[]Column so ReleaseLazyColumnStore can return it. nil when the arena was not pooled
	// (e.g. WantAll path, where no lazy columns are registered).
	lazyStorePtr *[]Column
	// iterFields is the pre-computed deduplicated column iteration list, built by
	// BuildIterFields. When non-nil, IterateFields uses this slice directly — zero allocs.
	// NOTE-049: see blockio/NOTES.md §49.
	// NOTE-243: built lazily on first IterFields() call (guarded by iterFieldsOnce) so
	// metrics queries — which only access named columns and never enumerate attributes —
	// pay no O(colCount) build/alloc per block. Search/filter queries trigger the build
	// on their first per-row IterateFields call.
	iterFields     []ColIterEntry
	iterFieldsOnce sync.Once
	meta           shared.BlockMeta
	spanCount      int
}

// newBlockForParsing creates a Block with an empty columns map, for use with AddColumnsToBlock.
// Call buildNameIndex after all columns have been added.
func newBlockForParsing(meta shared.BlockMeta) *Block {
	return &Block{
		columns:   make(map[shared.ColumnKey]*Column),
		meta:      meta,
		spanCount: int(meta.SpanCount),
	}
}

// buildNameIndex builds the nameIndex from the current columns map.
// Must be called after all columns have been registered (post-parse or post-AddColumnsToBlock).
func (b *Block) buildNameIndex() {
	b.nameIndex = make(map[string]*Column, len(b.columns))
	for k, col := range b.columns {
		if prev, ok := b.nameIndex[k.Name]; !ok || k.Type < prev.Type {
			b.nameIndex[k.Name] = col
		}
	}
}

// buildIterFields pre-computes a deduplicated column iteration slice for use by
// modulesSpanFieldsAdapter.IterateFields. After this call, IterateFields on any adapter
// for this block is allocation-free.
// NOTE-049: Eliminates the per-span make(map[string]struct{}) in IterateFields.
func (b *Block) buildIterFields() {
	seen := make(map[string]struct{}, len(b.columns))
	entries := make([]ColIterEntry, 0, len(b.columns))
	for key := range b.columns {
		// NOTE-ITER-1: skip body-parsed auto-columns; they are not original attributes.
		if key.Type == shared.ColumnTypeRangeString {
			continue
		}
		if _, already := seen[key.Name]; already {
			continue
		}
		seen[key.Name] = struct{}{}
		// Use GetColumn for a stable tie-breaking column when multiple type variants
		// share the same name — matches the semantics of direct GetColumn calls.
		col := b.GetColumn(key.Name)
		if col == nil {
			continue
		}
		entries = append(entries, ColIterEntry{Col: col, Name: key.Name})
	}
	b.iterFields = entries
}

// resetIterFields discards any previously-built iterFields and resets the lazy-build
// guard so the next IterFields() rebuilds from the current column set. Used after a
// post-parse mutation of the columns map (e.g. AddColumnsToBlock's second-pass decode).
// NOTE-243: callers must ensure no concurrent IterFields() is in flight when resetting —
// in practice AddColumnsToBlock runs between scan passes, not during a per-row scan.
func (b *Block) resetIterFields() {
	b.iterFields = nil
	b.iterFieldsOnce = sync.Once{}
}

// IterFields returns the deduplicated column iteration list, building it lazily on the
// first call (NOTE-243). Metrics queries that only access named columns never call this,
// so they pay no O(colCount) build/alloc per block; search/filter queries trigger the
// build on their first per-row IterateFields call. The sync.Once makes the lazy build
// safe under concurrent per-row IterateFields calls on the same block.
func (b *Block) IterFields() []ColIterEntry {
	b.iterFieldsOnce.Do(b.buildIterFields)
	return b.iterFields
}

// SpanCount returns the number of spans in the block.
func (b *Block) SpanCount() int { return b.spanCount }

// GetColumn returns the column with the given name, using the lowest ColumnType
// value as a stable tie-breaker when multiple typed variants exist.
// When a name has only one type variant (the common case) this is equivalent to
// a typed lookup. Use GetColumnByType for precise (name, type) access.
func (b *Block) GetColumn(name string) *Column {
	return b.nameIndex[name]
}

// GetColumnByType returns the column with the exact (name, type) combination, or nil.
func (b *Block) GetColumnByType(name string, typ shared.ColumnType) *Column {
	return b.columns[shared.ColumnKey{Name: name, Type: typ}]
}

// GetAllColumns returns all columns with the given name across all types.
// Returns nil if no column with that name exists.
func (b *Block) GetAllColumns(name string) []*Column {
	var result []*Column
	for k, col := range b.columns {
		if k.Name == name {
			result = append(result, col)
		}
	}
	return result
}

// Columns returns the full column map keyed by (name, type).
func (b *Block) Columns() map[shared.ColumnKey]*Column { return b.columns }

// Meta returns the block metadata.
func (b *Block) Meta() shared.BlockMeta { return b.meta }

// BlockWithBytes bundles a decoded Block with its raw bytes for AddColumnsToBlock.
