package benchmark

import (
	"runtime/metrics"

	"golang.org/x/sys/unix"
)

// cpuSnapshot is a point-in-time reading of user and GC CPU seconds consumed by the process.
type cpuSnapshot struct {
	user float64
	gc   float64
}

// Preallocated sample slices reused on every readCPUSecs call to avoid
// per-call heap allocations inside hot benchmark loops.
var (
	cpuGCSample   = []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	cpuUserSample = []metrics.Sample{{Name: "/cpu/classes/user:cpu-seconds"}}
)

// readCPUSecs returns the current process CPU-seconds split into user and GC components.
//
// User CPU is measured via CLOCK_PROCESS_CPUTIME_ID, which is a kernel-maintained
// counter with nanosecond resolution updated continuously — unlike runtime/metrics
// which only updates on Go scheduler preemptions (~10 ms ticks). This ensures
// sub-millisecond queries (e.g. blockpack) report non-zero CPU time.
//
// GC CPU is still sourced from runtime/metrics, which is the only way to separate
// GC time from user time. Its coarser resolution only affects the precision of GC
// CPU accounting.
//
// Take a snapshot before and after a query; the delta is the actual CPU time used.
func readCPUSecs() cpuSnapshot {
	// GC CPU from runtime/metrics (low resolution, but only available source).
	metrics.Read(cpuGCSample)
	var gcSecs float64
	if cpuGCSample[0].Value.Kind() == metrics.KindFloat64 {
		gcSecs = cpuGCSample[0].Value.Float64()
	}

	// Total process CPU via CLOCK_PROCESS_CPUTIME_ID (nanosecond resolution).
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &ts); err == nil {
		totalSecs := float64(ts.Sec) + float64(ts.Nsec)/1e9
		userSecs := totalSecs - gcSecs
		if userSecs < 0 {
			userSecs = 0
		}
		return cpuSnapshot{user: userSecs, gc: gcSecs}
	}

	// Fallback: runtime/metrics user CPU (coarse, may read zero for fast queries).
	metrics.Read(cpuUserSample)
	if cpuUserSample[0].Value.Kind() == metrics.KindFloat64 {
		return cpuSnapshot{user: cpuUserSample[0].Value.Float64(), gc: gcSecs}
	}
	return cpuSnapshot{gc: gcSecs}
}
