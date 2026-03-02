package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"slices"
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
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

// computeMinHashSigFromProto computes a compact MinHash signature for a pendingSpan's attribute set.
// Uses FNV-1a hashing of attribute key names. Classic MinHash: keep the 4 smallest hashes.
// Iterates directly over proto attribute slices — no AttrKV materialization required.
func computeMinHashSigFromProto(ps *pendingSpan) {
	// Initialize with max uint64 values.
	ps.minHashSig = [4]uint64{
		^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0),
	}

	addHash := func(name string) {
		// Inline FNV-1a: no heap allocation, no string→[]byte conversion.
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
		for i := range 4 {
			if v < ps.minHashSig[i] {
				// Shift larger values right and insert v at position i.
				for j := 3; j > i; j-- {
					ps.minHashSig[j] = ps.minHashSig[j-1]
				}
				ps.minHashSig[i] = v
				break
			}
		}
	}

	if ps.span != nil {
		for _, kv := range ps.span.Attributes {
			if kv != nil {
				addHash(kv.Key)
			}
		}
	}
	if ps.rs != nil && ps.rs.Resource != nil {
		for _, kv := range ps.rs.Resource.Attributes {
			if kv != nil {
				addHash(kv.Key)
			}
		}
	}
	if ps.ss != nil && ps.ss.Scope != nil {
		for _, kv := range ps.ss.Scope.Attributes {
			if kv != nil {
				addHash(kv.Key)
			}
		}
	}
}

// computeMinHashSigFromBlock computes a compact MinHash signature for a pendingSpan
// sourced from a columnar block. Uses the same FNV-1a min-heap logic as
// computeMinHashSigFromProto, but iterates block column names instead of proto attributes.
// Only attribute columns (span.*, resource.*, scope.*) are hashed — intrinsic columns
// (trace:id, span:id, span:start, etc.) are not included, matching the proto path behavior.
func computeMinHashSigFromBlock(ps *pendingSpan, block *modules_reader.Block) {
	ps.minHashSig = [4]uint64{
		^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0),
	}

	addHash := func(name string) {
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
		for i := range 4 {
			if v < ps.minHashSig[i] {
				for j := 3; j > i; j-- {
					ps.minHashSig[j] = ps.minHashSig[j-1]
				}
				ps.minHashSig[i] = v
				break
			}
		}
	}

	rowIdx := ps.srcRowIdx
	for name, col := range block.Columns() {
		if !col.IsPresent(rowIdx) {
			continue
		}
		switch {
		case strings.HasPrefix(name, "span."):
			addHash(name[5:])
		case strings.HasPrefix(name, "resource."):
			addHash(name[9:])
		case strings.HasPrefix(name, "scope."):
			addHash(name[6:])
		}
	}
}
