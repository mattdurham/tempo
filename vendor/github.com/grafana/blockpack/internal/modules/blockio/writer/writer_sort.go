package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"slices"
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// sortPending sorts the pending span buffer by (service.name ASC, MinHashSig ASC, TraceID ASC).
// This is the canonical sort order per NOTES §2.
//
// Sorts a []int index slice to avoid copying pendingSpan values during the O(n log n) comparison
// phase, then applies the final permutation in one O(n) copy pass.
func sortPending(pending []pendingSpan) {
	n := len(pending)
	if n <= 1 {
		return
	}

	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	slices.SortFunc(indices, func(ai, bi int) int {
		a, b := &pending[ai], &pending[bi]
		if a.svcName != b.svcName {
			if a.svcName < b.svcName {
				return -1
			}
			return 1
		}
		for i := range 4 {
			if a.minHashSig[i] != b.minHashSig[i] {
				if a.minHashSig[i] < b.minHashSig[i] {
					return -1
				}
				return 1
			}
		}
		return bytes.Compare(a.traceID[:], b.traceID[:])
	})

	// Apply the permutation with a single O(n) copy pass.
	sorted := make([]pendingSpan, n)
	for i, idx := range indices {
		sorted[i] = pending[idx]
	}
	copy(pending, sorted)
}

// addHashToMinHeap inserts a hash value into the 4-element sorted min-heap,
// keeping the smallest 4 hashes. Inlines FNV-1a: no heap allocation,
// no string→[]byte conversion.
func addHashToMinHeap(name string, minHashSig *[4]uint64) {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	h := offset
	for i := range len(name) {
		h ^= uint64(name[i])
		h *= prime
	}
	v := h

	// Insert into the 4-element sorted min-heap (keep smallest 4 hashes).
	insertMinHeap(v, minHashSig)
}

// addKVHashToMinHeap hashes "key=value" into the 4-element sorted min-heap.
// The combined hash is computed inline without allocating a new string: FNV-1a is
// applied over key bytes, then '=' byte, then value bytes. This produces better
// attribute-set discrimination than hashing keys alone.
func addKVHashToMinHeap(key, value string, minHashSig *[4]uint64) {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	h := offset
	for i := range len(key) {
		h ^= uint64(key[i])
		h *= prime
	}
	h ^= '='
	h *= prime
	for i := range len(value) {
		h ^= uint64(value[i])
		h *= prime
	}
	insertMinHeap(h, minHashSig)
}

// insertMinHeap inserts v into the 4-element sorted min-heap (ascending order),
// keeping the smallest 4 values. Shared by addHashToMinHeap and addKVHashToMinHeap.
func insertMinHeap(v uint64, minHashSig *[4]uint64) {
	for i := range 4 {
		if v < minHashSig[i] {
			// Shift larger values right and insert v at position i.
			for j := 3; j > i; j-- {
				minHashSig[j] = minHashSig[j-1]
			}
			minHashSig[i] = v
			break
		}
	}
}

// computeMinHashSigFromProto computes a compact MinHash signature for a pendingSpan's attribute set.
// Uses FNV-1a hashing of "key=value" pairs for string attributes so that spans sharing identical
// attribute keys but different values (e.g. resource.region="us-east-1" vs "eu-west-1") produce
// distinct signatures, enabling tighter block-level clustering. Non-string attributes fall back to
// key-only hashing. Classic MinHash: keep the 4 smallest hashes.
// Iterates directly over proto attribute slices — no AttrKV materialization required.
//
//nolint:dupl // intentional mirror of computeMinHashSigFromLog for the span path; different receiver types prevent sharing
func computeMinHashSigFromProto(ps *pendingSpan) {
	// Initialize with max uint64 values.
	ps.minHashSig = [4]uint64{
		^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0),
	}

	hashKV := func(kv *commonv1.KeyValue) {
		if sv, ok := kv.Value.GetValue().(*commonv1.AnyValue_StringValue); ok {
			addKVHashToMinHeap(kv.Key, sv.StringValue, &ps.minHashSig)
		} else {
			addHashToMinHeap(kv.Key, &ps.minHashSig)
		}
	}

	if ps.span != nil {
		for _, kv := range ps.span.Attributes {
			if kv != nil {
				hashKV(kv)
			}
		}
	}
	if ps.rs != nil && ps.rs.Resource != nil {
		for _, kv := range ps.rs.Resource.Attributes {
			if kv != nil {
				hashKV(kv)
			}
		}
	}
	if ps.ss != nil && ps.ss.Scope != nil {
		for _, kv := range ps.ss.Scope.Attributes {
			if kv != nil {
				hashKV(kv)
			}
		}
	}
}

// computeMinHashSigFromBlock computes a compact MinHash signature for a pendingSpan
// sourced from a columnar block. Uses FNV-1a hashing of "key=value" pairs for string
// columns, matching computeMinHashSigFromProto's behavior so that compacted blocks
// produce the same clustering as freshly written ones.
// Only attribute columns (span.*, resource.*, scope.*) are hashed — intrinsic columns
// (trace:id, span:id, span:start, etc.) are not included.
func computeMinHashSigFromBlock(ps *pendingSpan, block *modules_reader.Block) {
	ps.minHashSig = [4]uint64{
		^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0),
	}

	rowIdx := ps.srcRowIdx
	for key, col := range block.Columns() {
		if !col.IsPresent(rowIdx) {
			continue
		}
		var attrKey string
		switch {
		case strings.HasPrefix(key.Name, "span."):
			attrKey = key.Name[5:]
		case strings.HasPrefix(key.Name, "resource."):
			attrKey = key.Name[9:]
		case strings.HasPrefix(key.Name, "scope."):
			attrKey = key.Name[6:]
		default:
			continue
		}
		if sv, ok := col.StringValue(rowIdx); ok {
			addKVHashToMinHeap(attrKey, sv, &ps.minHashSig)
		} else {
			addHashToMinHeap(attrKey, &ps.minHashSig)
		}
	}
}
