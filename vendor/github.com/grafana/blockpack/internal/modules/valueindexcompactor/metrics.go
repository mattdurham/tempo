package valueindexcompactor

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// NOTE-VI-023: see internal/modules/valueindexcompactor/NOTES.md.
// Any changes to this file must be reflected there.

// NOTE-VI-023: Prometheus metric/label name constants for the compactor.
// Repeated label keys/values are pulled out as constants so golangci-lint's
// goconst does not flag them and the spelling is enforced in one place.
const (
	compactorLabelStatus = "status"
	compactorLabelOp     = "op"

	compactorStatusSuccess = "success"
	compactorStatusError   = "error"

	compactorOpList   = "list"
	compactorOpGet    = "get"
	compactorOpPut    = "put"
	compactorOpDelete = "delete"
)

// compactorMetrics holds the Prometheus collectors for the value-index
// compactor service. A nil *compactorMetrics is safe: every method becomes a
// no-op, so a disabled or nil-Registerer config costs nothing (NOTE-VI-023).
//
// The run-duration and merge-duration histograms are pre-resolved at
// construction so the hot path is a single Observe with no label allocation,
// matching the cache layer pattern (memcache durGetHit etc.).
type compactorMetrics struct {
	runs            *prometheus.CounterVec // status
	errors          *prometheus.CounterVec // op
	filesRead       prometheus.Counter
	filesWritten    prometheus.Counter
	filesDeleted    prometheus.Counter
	entriesRetained prometheus.Counter
	entriesDropped  prometheus.Counter
	runDur          prometheus.Observer
	mergeDur        prometheus.Observer
}

// newCompactorMetrics builds and registers the compactor collectors against
// reg. When reg is nil it returns nil so the service runs with all-no-op
// metrics. Registration tolerates AlreadyRegisteredError so multiple compactor
// instances (or a co-located consumer) can share one global registry without
// panicking.
func newCompactorMetrics(reg prometheus.Registerer) *compactorMetrics {
	if reg == nil {
		return nil
	}
	m := &compactorMetrics{}
	m.runs = compactorRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_runs_total",
		Help: "Completed compaction passes, by status (success, error).",
	}, []string{compactorLabelStatus}))
	m.errors = compactorRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_errors_total",
		Help: "Errors by operation (list, get, put, delete).",
	}, []string{compactorLabelOp}))

	m.filesRead = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_files_read_total",
		Help: "Input files read across all compaction jobs.",
	}))
	m.filesWritten = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_files_written_total",
		Help: "Output files written.",
	}))
	m.filesDeleted = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_files_deleted_total",
		Help: "Input files deleted after successful merge.",
	}))
	m.entriesRetained = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_entries_retained_total",
		Help: "Entries written to output (source still exists).",
	}))
	m.entriesDropped = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_entries_dropped_total",
		Help: "Entries dropped due to retention (source deleted).",
	}))

	runH := compactorRegisterHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:                            "blockpack_value_index_compactor_run_duration_seconds",
		Help:                            "Duration of one full compaction pass.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 15 * time.Minute,
	}))
	mergeH := compactorRegisterHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:                            "blockpack_value_index_compactor_merge_duration_seconds",
		Help:                            "Time to merge one level's files for one column.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 15 * time.Minute,
	}))
	m.runDur = runH
	m.mergeDur = mergeH
	return m
}

// incRun records one completed compaction pass by status.
func (m *compactorMetrics) incRun(status string) {
	if m == nil {
		return
	}
	m.runs.WithLabelValues(status).Inc()
}

// incError records one error for the given operation.
func (m *compactorMetrics) incError(op string) {
	if m == nil {
		return
	}
	m.errors.WithLabelValues(op).Inc()
}

// observeRun records the duration of one full compaction pass.
func (m *compactorMetrics) observeRun(d time.Duration) {
	if m == nil {
		return
	}
	m.runDur.Observe(d.Seconds())
}

// observeMerge records the duration of one level's merge for one column.
func (m *compactorMetrics) observeMerge(d time.Duration) {
	if m == nil {
		return
	}
	m.mergeDur.Observe(d.Seconds())
}

// addMergeCounts records the file- and entry-level outcome of one merge.
func (m *compactorMetrics) addMergeCounts(read, written, deleted, retained, dropped int) {
	if m == nil {
		return
	}
	if read > 0 {
		m.filesRead.Add(float64(read))
	}
	if written > 0 {
		m.filesWritten.Add(float64(written))
	}
	if deleted > 0 {
		m.filesDeleted.Add(float64(deleted))
	}
	if retained > 0 {
		m.entriesRetained.Add(float64(retained))
	}
	if dropped > 0 {
		m.entriesDropped.Add(float64(dropped))
	}
}

// compactorRegisterCounterVec registers cv, returning the existing collector on
// AlreadyRegisteredError instead of panicking.
func compactorRegisterCounterVec(reg prometheus.Registerer, cv *prometheus.CounterVec) *prometheus.CounterVec {
	if err := reg.Register(cv); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				return existing
			}
		}
	}
	return cv
}

// compactorRegisterCounter registers c, returning the existing collector on
// AlreadyRegisteredError instead of panicking.
func compactorRegisterCounter(reg prometheus.Registerer, c prometheus.Counter) prometheus.Counter {
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(prometheus.Counter); ok {
				return existing
			}
		}
	}
	return c
}

// compactorRegisterHistogram registers h, returning the existing collector on
// AlreadyRegisteredError instead of panicking.
func compactorRegisterHistogram(reg prometheus.Registerer, h prometheus.Histogram) prometheus.Histogram {
	if err := reg.Register(h); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(prometheus.Histogram); ok {
				return existing
			}
		}
	}
	return h
}
