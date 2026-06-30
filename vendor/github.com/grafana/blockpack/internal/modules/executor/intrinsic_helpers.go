package executor

// intrinsic_helpers.go — utilities extracted from metrics_trace_intrinsic.go after #433
// (IntrinsicTOC removal). These helpers are still used by stream_structural.go,
// metrics_trace.go, and predicates.go.

import (
	"sync"
)

// compactPoolMaxPooledBytes bounds the backing-array byte size for per-block scratch slices.
const compactPoolMaxPooledBytes = 32 << 20 // 32 MiB

var compactInt32Pool sync.Pool

func acquireCompactInt32(n int) []int32 {
	if v := compactInt32Pool.Get(); v != nil {
		if s, ok := v.([]int32); ok && cap(s) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]int32, n)
}

func releaseCompactInt32(s []int32) {
	if cap(s)*4 > compactPoolMaxPooledBytes {
		return
	}
	compactInt32Pool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

// normalizeIntrinsicFieldName converts aliases like "duration" → "span:duration"
// so callers can use shorthand names for well-known fields.
func normalizeIntrinsicFieldName(field string) string {
	switch field {
	case "duration":
		return colNameSpanDuration
	case "name":
		return colNameSpanName
	case "status":
		return colNameSpanStatus
	case "kind":
		return colNameSpanKind
	case "start":
		return colNameSpanStart
	}
	return field
}

// pow2Floor returns the largest power of two ≤ v, or v itself for non-positive values.
func pow2Floor(v float64) float64 {
	if v <= 0 {
		return v
	}
	// Repeatedly halve until ≤ v; this is the branch-free version.
	p := 1.0
	for p*2 <= v {
		p *= 2
	}
	return p
}

var (
	compactBoolPool  sync.Pool
	compactUint8Pool sync.Pool
)

func acquireCompactUint8(n int) []uint8 {
	if v := compactUint8Pool.Get(); v != nil {
		if p, ok := v.(*[]uint8); ok && cap(*p) >= n {
			s := (*p)[:n]
			clear(s)
			return s
		}
	}
	return make([]uint8, n)
}

func releaseCompactUint8(s []uint8) {
	if cap(s) > compactPoolMaxPooledBytes {
		return
	}
	s = s[:cap(s)]
	compactUint8Pool.Put(&s)
}

func acquireCompactBool(n int) []bool {
	if v := compactBoolPool.Get(); v != nil {
		if s, ok := v.([]bool); ok && cap(s) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]bool, n)
}

func releaseCompactBool(s []bool) {
	if cap(s) > compactPoolMaxPooledBytes { // NOTE-355: drop oversized outlier (1 byte/elem)
		return
	}
	compactBoolPool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}
