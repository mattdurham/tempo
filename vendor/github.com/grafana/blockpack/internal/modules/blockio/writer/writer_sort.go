package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"slices"
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// sortPending sorts the pending span buffer by (service.name ASC, span.name ASC, MinHashSig ASC, TraceID ASC).
// NOTE-457: span.name as secondary sort key guarantees same-name spans cluster into the same blocks,
// enabling the exact-value range index fast path (min==max per block → zero false positives for
// {span.name = "X"} queries). MinHash as tertiary preserves attribute-set similarity within a name.
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
		return spanSortKeyCmp(&pending[ai], &pending[bi])
	})

	// Apply the permutation with a single O(n) copy pass.
	sorted := make([]pendingSpan, n)
	for i, idx := range indices {
		sorted[i] = pending[idx]
	}
	copy(pending, sorted)
}

// spanSortKeyCmp is the active span ordering comparator. It is a var (not a
// direct call) solely so the NOTE-466 (issue #385) investigation benchmark can
// swap in candidate orders to measure their compression / range-index trade-off
// end-to-end through the real writer. Production always uses compareSpanSortKey;
// the benchmark restores it after each run.
var spanSortKeyCmp = compareSpanSortKey

// compareSpanSortKey is the production span ordering comparator (NOTE-457):
//
//	(service.name ASC, span.name ASC, MinHashSig ASC, TraceID ASC)
//
// service.name / span.name primary keys keep blocks value-homogeneous so the
// exact-value range-index fast path (min==max per block → zero false positives)
// fires for {resource.service.name = X} and {span.name = X} equality queries.
// MinHash tertiary preserves attribute-set similarity within a (service, name)
// group; TraceID quaternary keeps a trace's same-group spans physically adjacent.
//
// NOTE-466: issue #385 evaluated promoting MinHash to the primary key
// (MinHash, TraceID) and a coarse bucket-then-sort hybrid. See NOTES.md NOTE-466
// for the measured trade-off; this ordering is retained because the range-index
// homogeneity it guarantees is worth more than the marginal cross-service
// compression gain MinHash-primary offers.
func compareSpanSortKey(a, b *pendingSpan) int {
	if a.svcName != b.svcName {
		if a.svcName < b.svcName {
			return -1
		}
		return 1
	}
	if a.spanName != b.spanName {
		if a.spanName < b.spanName {
			return -1
		}
		return 1
	}
	if c := compareMinHashSig(&a.minHashSig, &b.minHashSig); c != 0 {
		return c
	}
	return bytes.Compare(a.traceID[:], b.traceID[:])
}

// compareMinHashSig lexicographically compares two 4-word MinHash signatures.
func compareMinHashSig(a, b *[4]uint64) int {
	for i := range 4 {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// computeMinHashSigFromProto computes a compact MinHash signature for a pendingSpan's attribute set.
// Uses FNV-1a hashing of "key=value" pairs for string attributes so that spans sharing identical
// attribute keys but different values (e.g. resource.region="us-east-1" vs "eu-west-1") produce
// distinct signatures, enabling tighter block-level clustering. Non-string attributes fall back to
// key-only hashing. Classic MinHash: keep the 4 smallest hashes.
// Iterates directly over proto attribute slices — no AttrKV materialization required.
//
//nolint:dupl // intentional mirror of computeMinHashSigFromTempoProto for OTLP types; different proto types prevent sharing
func computeMinHashSigFromProto(ps *pendingSpan) {
	// Initialize with max uint64 values.
	ps.minHashSig = [4]uint64{
		^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0),
	}

	hashKV := func(kv *commonv1.KeyValue) {
		if kv.Value == nil {
			shared.AddHashToMinHeap(kv.Key, &ps.minHashSig)
			return
		}
		if sv, ok := kv.Value.GetValue().(*commonv1.AnyValue_StringValue); ok {
			shared.AddKVHashToMinHeap(kv.Key, sv.StringValue, &ps.minHashSig)
		} else {
			shared.AddHashToMinHeap(kv.Key, &ps.minHashSig)
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

// computeMinHashSigFromTempoProto computes a MinHash signature for a pendingSpan sourced from
// Tempo-native proto types (github.com/grafana/tempo/pkg/tempopb/...).
// Mirrors computeMinHashSigFromProto for tempocommon.KeyValue.
//
//nolint:dupl // intentional mirror of computeMinHashSigFromProto for Tempo types
func computeMinHashSigFromTempoProto(ps *pendingSpan) {
	ps.minHashSig = [4]uint64{
		^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0),
	}

	hashKV := func(kv *tempocommon.KeyValue) {
		if kv.Value == nil {
			shared.AddHashToMinHeap(kv.Key, &ps.minHashSig)
			return
		}
		if sv, ok := kv.Value.GetValue().(*tempocommon.AnyValue_StringValue); ok {
			shared.AddKVHashToMinHeap(kv.Key, sv.StringValue, &ps.minHashSig)
		} else {
			shared.AddHashToMinHeap(kv.Key, &ps.minHashSig)
		}
	}

	if ps.tempoSpan != nil {
		for _, kv := range ps.tempoSpan.Attributes {
			if kv != nil {
				hashKV(kv)
			}
		}
	}
	if ps.tempoRS != nil && ps.tempoRS.Resource != nil {
		for _, kv := range ps.tempoRS.Resource.Attributes {
			if kv != nil {
				hashKV(kv)
			}
		}
	}
	if ps.tempoSS != nil && ps.tempoSS.Scope != nil {
		for _, kv := range ps.tempoSS.Scope.Attributes {
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
			shared.AddKVHashToMinHeap(attrKey, sv, &ps.minHashSig)
		} else {
			shared.AddHashToMinHeap(attrKey, &ps.minHashSig)
		}
	}
}
