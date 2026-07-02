package valueindexcompactor

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// NOTE-VI-023: see internal/modules/valueindexcompactor/NOTES.md.
// Any changes to this file must be reflected there.

// NOTE-VI-023: Prometheus metric/label name constants for the compactor.
const (
	compactorLabelStatus = "status"
	compactorLabelOp     = "op"
	compactorLabelLevel  = "level"

	compactorStatusSuccess = "success"
	compactorStatusError   = "error"

	compactorOpList   = "list"
	compactorOpGet    = "get"
	compactorOpPut    = "put"
	compactorOpDelete = "delete"
)

// runDurationBuckets covers the expected range for a full pass over 1000+
// columns: sub-second (warm cache) through 30 minutes (first cold pass).
var runDurationBuckets = []float64{
	1, 5, 15, 30, 60, 120, 300, 600, 900, 1800,
}

// mergeDurationBuckets covers per-column merge time: milliseconds through
// several minutes for very large L0 sets.
var mergeDurationBuckets = []float64{
	0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300,
}

// compactorMetrics holds the Prometheus collectors for the value-index
// compactor service. A nil *compactorMetrics is safe: every method is a no-op.
type compactorMetrics struct {
	// Pass-level counters / histograms.
	runs    *prometheus.CounterVec // {status}
	errors  *prometheus.CounterVec // {op}
	runDur  prometheus.Observer
	lastRun prometheus.Gauge // unix timestamp of last completed pass

	// Per-merge counters / histograms.
	mergeDur        prometheus.Observer
	filesRead       prometheus.Counter
	filesWritten    prometheus.Counter
	filesDeleted    prometheus.Counter
	filesSkipped    prometheus.Counter // unparseable / wrong-magic files
	entriesRetained prometheus.Counter
	entriesDropped  prometheus.Counter

	// Columns that had at least one level compacted in the pass.
	columnsCompacted *prometheus.CounterVec // {level}

	// Backlog: L0 files observed at scan time (gauge, set each pass).
	backlogL0Files *prometheus.GaugeVec // {tenant}
}

func newCompactorMetrics(reg prometheus.Registerer) *compactorMetrics {
	if reg == nil {
		return nil
	}
	m := &compactorMetrics{}

	// ── pass-level ───────────────────────────────────────────────────────────
	m.runs = compactorRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_runs_total",
		Help: "Completed compaction passes by status (success|error).",
	}, []string{compactorLabelStatus}))

	m.errors = compactorRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_errors_total",
		Help: "Errors by operation (list, get, put, delete).",
	}, []string{compactorLabelOp}))

	m.lastRun = compactorRegisterGauge(reg, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "blockpack_value_index_compactor_last_run_timestamp_seconds",
		Help: "Unix timestamp of the last completed compaction pass (success or error). " +
			"Alert if this is stale.",
	}))

	runH := compactorRegisterHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "blockpack_value_index_compactor_run_duration_seconds",
		Help:    "Wall time for one full compaction pass over all tenants and columns.",
		Buckets: runDurationBuckets,
	}))
	m.runDur = runH

	// ── per-merge ─────────────────────────────────────────────────────────────
	mergeH := compactorRegisterHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "blockpack_value_index_compactor_merge_duration_seconds",
		Help:    "Wall time to merge one level's files for one (tenant, column) pair.",
		Buckets: mergeDurationBuckets,
	}))
	m.mergeDur = mergeH

	m.filesRead = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_files_read_total",
		Help: "Input files read across all compaction jobs.",
	}))
	m.filesWritten = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_files_written_total",
		Help: "Output files written (L1+).",
	}))
	m.filesDeleted = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_files_deleted_total",
		Help: "Input files deleted after a successful merge.",
	}))
	m.filesSkipped = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_files_skipped_total",
		Help: "Files skipped because their name could not be parsed (wrong magic / old format).",
	}))
	m.entriesRetained = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_entries_retained_total",
		Help: "Posting-list entries propagated to output (source block still exists).",
	}))
	m.entriesDropped = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_entries_dropped_total",
		Help: "Posting-list entries dropped because their source block was deleted by retention.",
	}))

	m.columnsCompacted = compactorRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_columns_compacted_total",
		Help: "Column directories where at least one level was merged, by input level.",
	}, []string{compactorLabelLevel}))

	// ── backlog ───────────────────────────────────────────────────────────────
	m.backlogL0Files = compactorRegisterGaugeVec(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "blockpack_value_index_compactor_backlog_l0_files",
		Help: "Number of L0 index files still awaiting compaction, observed at the start of each pass. " +
			"A rising value means the compactor is falling behind ingestion.",
	}, []string{"tenant"}))

	return m
}

func (m *compactorMetrics) incRun(status string) {
	if m == nil {
		return
	}
	m.runs.WithLabelValues(status).Inc()
}

func (m *compactorMetrics) incError(op string) {
	if m == nil {
		return
	}
	m.errors.WithLabelValues(op).Inc()
}

func (m *compactorMetrics) observeRun(d time.Duration) {
	if m == nil {
		return
	}
	m.runDur.Observe(d.Seconds())
	m.lastRun.SetToCurrentTime()
}

func (m *compactorMetrics) observeMerge(d time.Duration) {
	if m == nil {
		return
	}
	m.mergeDur.Observe(d.Seconds())
}

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

func (m *compactorMetrics) incSkipped(n int) {
	if m == nil || n == 0 {
		return
	}
	m.filesSkipped.Add(float64(n))
}

func (m *compactorMetrics) incColumnsCompacted(level int) {
	if m == nil {
		return
	}
	m.columnsCompacted.WithLabelValues(fmt.Sprintf("%d", level)).Inc()
}

func (m *compactorMetrics) setBacklogL0(tenant string, n int) {
	if m == nil {
		return
	}
	m.backlogL0Files.WithLabelValues(tenant).Set(float64(n))
}

// ── registration helpers ─────────────────────────────────────────────────────

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

func compactorRegisterGauge(reg prometheus.Registerer, g prometheus.Gauge) prometheus.Gauge {
	if err := reg.Register(g); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(prometheus.Gauge); ok {
				return existing
			}
		}
	}
	return g
}

func compactorRegisterGaugeVec(reg prometheus.Registerer, gv *prometheus.GaugeVec) *prometheus.GaugeVec {
	if err := reg.Register(gv); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.GaugeVec); ok {
				return existing
			}
		}
	}
	return gv
}
