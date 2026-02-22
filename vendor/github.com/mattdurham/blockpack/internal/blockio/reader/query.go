package reader

import (
	"encoding/binary"
	"math"
	"regexp"
	"sort"

	"github.com/mattdurham/blockpack/internal/arena"
	aslice "github.com/mattdurham/blockpack/internal/arena/slice"
	"github.com/mattdurham/blockpack/internal/blockio/shared"
	ondisk "github.com/mattdurham/blockpack/internal/types"
)

// SpanCount returns the total number of spans in the blockpack.
func (r *Reader) SpanCount() int {
	return r.totalSpans
}

// BlocksForDedicated returns block IDs that contain the dedicated column value.
func (r *Reader) BlocksForDedicated(column string, key ondisk.DedicatedValueKey, arena *arena.Arena) []int {
	// Lazy parse this column if needed
	if err := r.ensureDedicatedColumnParsed(column, arena); err != nil {
		return nil
	}

	entry, ok := r.dedicatedIndex[column]
	if !ok {
		return nil
	}

	// Handle type mismatch for range columns with low-cardinality fallback
	// If querying with base type (String/Bytes/Float64) but column is stored as range type,
	// convert the key to the range type equivalent using bucket-ID lookup when boundaries are available
	if entry.typ != key.Type() {
		// Check if we can convert the key
		switch {
		case key.Type() == ondisk.ColumnTypeString && entry.typ == ondisk.ColumnTypeRangeString:
			decoded, err := ondisk.DecodeDedicatedKey(key.Encode())
			if err != nil {
				return nil
			}
			stringVal := string(decoded.Data())
			meta, hasMeta := r.rangeBucketMeta[column]
			if hasMeta && len(meta.StringBoundaries) > 0 {
				bucketID := ondisk.GetBucketIDString(stringVal, meta.StringBoundaries)
				key = ondisk.RangeBucketValueKey(bucketID, entry.typ)
			} else {
				key = ondisk.RangeStringValueKey(stringVal, entry.typ)
			}
		case key.Type() == ondisk.ColumnTypeBytes && entry.typ == ondisk.ColumnTypeRangeBytes:
			decoded, err := ondisk.DecodeDedicatedKey(key.Encode())
			if err != nil {
				return nil
			}
			meta, hasMeta := r.rangeBucketMeta[column]
			if hasMeta && len(meta.BytesBoundaries) > 0 {
				bucketID := ondisk.GetBucketIDBytes(decoded.Data(), meta.BytesBoundaries)
				key = ondisk.RangeBucketValueKey(bucketID, entry.typ)
			} else {
				key = ondisk.RangeBytesValueKey(decoded.Data(), entry.typ)
			}
		case key.Type() == ondisk.ColumnTypeFloat64 && entry.typ == ondisk.ColumnTypeRangeFloat64:
			decoded, err := ondisk.DecodeDedicatedKey(key.Encode())
			if err != nil {
				return nil
			}
			// Decode float64 from bytes
			if len(decoded.Data()) != 8 {
				return nil
			}
			floatBits := binary.LittleEndian.Uint64(decoded.Data())
			floatVal := math.Float64frombits(floatBits)
			meta, hasMeta := r.rangeBucketMeta[column]
			if hasMeta && len(meta.Float64Boundaries) > 0 {
				bucketID := ondisk.GetBucketIDFloat64(floatVal, meta.Float64Boundaries)
				key = ondisk.RangeBucketValueKey(bucketID, entry.typ)
			} else {
				key = ondisk.RangeFloat64ValueKey(floatVal, entry.typ)
			}
		default:
			// Incompatible types
			return nil
		}
	}

	encoded := key.Encode()
	ids := entry.values[encoded]
	if arena == nil {
		out := make([]int, len(ids))
		copy(out, ids)
		return out
	}
	// Use arena allocation
	out := aslice.Make[int](arena, len(ids))
	copy(out.Raw(), ids)
	return out.Raw()
}

// HasTraceBlockIndex returns true if the blockpack file has a trace block index.
// This allows distinguishing between "no index available" vs "trace not found in index".
func (r *Reader) HasTraceBlockIndex() bool {
	return r.traceBlockIndex != nil
}

// BlocksForTraceID returns block IDs that contain spans for the given trace ID.
// Returns nil if the trace ID is not found in the index or if no index exists.
// Use HasTraceBlockIndex() to distinguish between these cases.
func (r *Reader) BlocksForTraceID(traceID [16]byte) []int {
	if r.traceBlockIndex == nil {
		return nil
	}
	entries, ok := r.traceBlockIndex[traceID]
	if !ok {
		return nil
	}
	// Return block IDs only (backward compatible)
	out := make([]int, len(entries))
	for i, e := range entries {
		out[i] = e.BlockID
	}
	return out
}

// TraceBlockEntries returns detailed block entries with span-level indices for a trace ID.
// Each entry includes the block ID and the specific span row indices within that block.
// Returns nil if the trace ID is not found or no index exists.
func (r *Reader) TraceBlockEntries(traceID [16]byte) []TraceBlockEntry {
	if r.traceBlockIndex == nil {
		return nil
	}
	entries, ok := r.traceBlockIndex[traceID]
	if !ok {
		return nil
	}
	// Return a copy to avoid external modifications
	out := make([]TraceBlockEntry, len(entries))
	for i, e := range entries {
		out[i] = TraceBlockEntry{
			BlockID:     e.BlockID,
			SpanIndices: make([]uint16, len(e.SpanIndices)),
		}
		copy(out[i].SpanIndices, e.SpanIndices)
	}
	return out
}

// DedicatedColumnNames returns the list of all dedicated column names in the file.
func (r *Reader) DedicatedColumnNames() []string {
	result := make([]string, 0, len(r.dedicatedIndexOffsets))
	for name := range r.dedicatedIndexOffsets {
		result = append(result, name)
	}
	return result
}

// DedicatedColumnTypeFromFile returns the type of a dedicated column from the file's index.
// Returns (type, true) if the column exists in the dedicated index, (0, false) otherwise.
// This is different from DedicatedColumnType which only checks the explicit hardcoded list.
func (r *Reader) DedicatedColumnTypeFromFile(column string) (ColumnType, bool) {
	meta, ok := r.dedicatedIndexOffsets[column]
	if !ok {
		return 0, false
	}
	return meta.typ, true
}

// DedicatedColumnIndexSize returns the size in bytes of the dedicated index for a column.
func (r *Reader) DedicatedColumnIndexSize(column string) int {
	meta, ok := r.dedicatedIndexOffsets[column]
	if !ok {
		return 0
	}
	return meta.length
}

// DedicatedValues returns all unique values for a dedicated column.
// This is useful for pattern matching (LIKE queries) where we need to check
// which values in the dedicated index match a pattern, then get their blocks.
func (r *Reader) DedicatedValues(column string) []DedicatedValueKey {
	// Lazy parse this column if needed
	if err := r.ensureDedicatedColumnParsed(column, nil); err != nil {
		return nil
	}

	entry, ok := r.dedicatedIndex[column]
	if !ok {
		return nil
	}

	result := make([]DedicatedValueKey, 0, len(entry.values))
	for encodedKey := range entry.values {
		key, err := DecodeDedicatedKey(encodedKey)
		if err != nil {
			continue
		}
		result = append(result, key)
	}
	return result
}

// GetRangeBucketMetadata returns bucket metadata for a range-bucketed dedicated column
func (r *Reader) GetRangeBucketMetadata(column string) (*shared.RangeBucketMetadata, error) {
	// Ensure column is parsed
	if err := r.ensureDedicatedColumnParsed(column, nil); err != nil {
		return nil, err
	}

	// Return bucket metadata if available
	meta, ok := r.rangeBucketMeta[column]
	if !ok {
		return nil, nil // Not a range-bucketed column
	}
	return meta, nil
}

// BlocksForDedicatedRegex returns block IDs that contain dedicated column values
// matching the given regex pattern.
//
// For ColumnTypeString (exact-match): matches the regex against each stored value directly.
//
// For ColumnTypeRangeString (KLL-bucketed): extracts the mandatory literal prefix from the
// regex and checks which buckets have ranges that overlap [prefix, increment(prefix)).
// Buckets outside that range are skipped. Returns nil (full scan) when no literal prefix
// can be extracted.
func (r *Reader) BlocksForDedicatedRegex(column string, pattern *regexp.Regexp, arena *arena.Arena) []int {
	if err := r.ensureDedicatedColumnParsed(column, arena); err != nil {
		return nil
	}

	entry, ok := r.dedicatedIndex[column]
	if !ok {
		return nil
	}

	blockSet := make(map[int]struct{})

	switch entry.typ {
	case ColumnTypeString:
		// Exact-match column: test the regex against every stored value.
		for encodedKey, blockIDs := range entry.values {
			key, err := DecodeDedicatedKey(encodedKey)
			if err != nil {
				continue
			}
			if pattern.MatchString(string(key.Data())) {
				for _, blockID := range blockIDs {
					blockSet[blockID] = struct{}{}
				}
			}
		}

	case ColumnTypeRangeString:
		// KLL-bucketed column: use StringBoundaries to prune buckets.
		// Bucket i covers [boundaries[i], boundaries[i+1]).
		// Extract the mandatory literal prefix of the regex; skip buckets whose
		// range cannot contain any string with that prefix.
		bucketMeta, hasMeta := r.rangeBucketMeta[column]
		if !hasMeta || len(bucketMeta.StringBoundaries) < 2 {
			return nil
		}
		prefix, _ := pattern.LiteralPrefix()
		if prefix == "" {
			return nil // no prefix — cannot prune, fall back to full scan
		}
		pEnd, hasEnd := incrementStringPrefix(prefix)
		boundaries := bucketMeta.StringBoundaries
		for i := 0; i < len(boundaries)-1; i++ {
			lo := boundaries[i]
			hi := boundaries[i+1]
			// Bucket [lo, hi) overlaps [prefix, pEnd) when lo < pEnd && hi > prefix.
			if hasEnd && lo >= pEnd {
				continue
			}
			if hi <= prefix {
				continue
			}
			bucketKey := ondisk.RangeBucketValueKey(uint16(i), ColumnTypeRangeString).Encode() //nolint:gosec
			for _, blockID := range entry.values[bucketKey] {
				blockSet[blockID] = struct{}{}
			}
		}

	default:
		return nil
	}

	// Convert to sorted slice
	if arena == nil {
		result := make([]int, 0, len(blockSet))
		for blockID := range blockSet {
			result = append(result, blockID)
		}
		sort.Ints(result)
		return result
	}
	// Use arena allocation
	result := aslice.Make[int](arena, 0)
	for blockID := range blockSet {
		result = result.AppendOne(arena, blockID)
	}
	resultRaw := result.Raw()
	sort.Ints(resultRaw)
	return resultRaw
}

// incrementStringPrefix returns the lexicographically smallest string that is strictly
// greater than every string with the given prefix. For example, "test-" → "test.".
// Returns ("", false) when the prefix consists entirely of 0xFF bytes (no upper bound exists).
func incrementStringPrefix(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}
