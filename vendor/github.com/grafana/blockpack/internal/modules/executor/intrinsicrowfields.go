package executor

import "sync"

type intrinsicRowFields struct {
	spanName      string
	serviceName   string
	statusMessage string
	spanStart     uint64
	spanEnd       uint64
	spanDuration  uint64
	spanKind      int64
	spanStatus    int64
	spanID        [8]byte
	parentID      [8]byte
	present       uint16
	traceID       [16]byte
}

// NOTE-349: intrinsicRowFields is ~120 bytes and a fresh []intrinsicRowFields of
// SpanCount entries is allocated once per block per query on the structural search
// path (lookupIntrinsicFieldsTypedForBlock) and the ref-filter path
// (lookupIntrinsicFieldsTyped). The slice is fully consumed by the caller's row loop
// and then discarded — a textbook sync.Pool target. The pool amortizes the allocation
// (and its GC scan cost: the struct holds three string headers + two byte arrays) across
// query churn. Correctness: scatter functions only write entries that have refs, so an
// unpopulated row keeps whatever was in the recycled backing array; getIntrinsicRowFields
// therefore zeroes the returned prefix so every row.present starts at 0 (the gate every
// reader checks) and no stale string/byte payload from a prior use is observable.
var intrinsicRowFieldsPool = sync.Pool{
	New: func() any { return new([]intrinsicRowFields) },
}

// getIntrinsicRowFields returns a zeroed []intrinsicRowFields of length n drawn from the
// pool (growing the backing array when needed). The returned slice must be handed back via
// putIntrinsicRowFields once the caller's row loop has fully consumed it; it must not
// escape or be retained past that point.
func getIntrinsicRowFields(n int) []intrinsicRowFields {
	if n <= 0 {
		return nil
	}
	p := intrinsicRowFieldsPool.Get().(*[]intrinsicRowFields)
	s := *p
	if cap(s) < n {
		s = make([]intrinsicRowFields, n)
	} else {
		s = s[:n]
		clear(s)
	}
	return s
}

// putIntrinsicRowFields returns a slice obtained from getIntrinsicRowFields to the pool.
// A nil/empty slice is ignored. The slice must not be used after this call.
func putIntrinsicRowFields(s []intrinsicRowFields) {
	if cap(s) == 0 {
		return
	}
	s = s[:0]
	intrinsicRowFieldsPool.Put(&s)
}
