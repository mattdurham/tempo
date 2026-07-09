package executor

// intrinsic_helpers.go — scratch-buffer pools extracted from metrics_trace_intrinsic.go after
// #433 (IntrinsicTOC removal). Still used by stream_structural.go. normalizeIntrinsicFieldName
// and pow2Floor (metrics_trace.go-only helpers) were deleted 2026-07-08 (issue #481 part 3)
// along with ExecuteTraceMetrics, their only caller.

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
