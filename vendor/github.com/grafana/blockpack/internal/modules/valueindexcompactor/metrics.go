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
	compactorOpSweep  = "sweep"
	compactorOpPanic  = "panic"
	// compactorOpDecode covers a trace-index input file that fails
	// valueindex.DecodeTraceGroups (Stage 3, traceindex.go wiring). Distinct
	// from compactorOpGet since the object was fetched successfully; only its
	// payload is unreadable.
	compactorOpDecode = "decode"
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
	entriesCorrupt  prometheus.Counter // refs with an unresolvable SourceID -- data corruption, not routine retention

	// oversizedLevelsSkipped counts levels skipped because even the minimum
	// forced-progress batch (2 files) would vastly exceed effectiveBatchBytes --
	// individual files at that level have already grown far larger than the
	// configured cap (typically after many rounds of merging compounded sizes at a
	// high level), so merging them further risks OOM regardless of concurrency.
	// Non-zero values mean some column's file sizes have outgrown what this
	// compactor can safely merge further -- worth investigating, not silently fine.
	oversizedLevelsSkipped prometheus.Counter

	// Columns that had at least one level compacted in the pass.
	columnsCompacted *prometheus.CounterVec // {level}

	// Backlog: L0 files observed at scan time (gauge, set each pass).
	backlogL0Files *prometheus.GaugeVec // {tenant}

	// mergesInFlight tracks the number of concurrently in-flight column merges
	// dispatched by Run() (NOTE-VI: Run() concurrency restructuring).
	mergesInFlight prometheus.Gauge

	// configuredConcurrency reports the effective (post-defaulting) CompactConcurrency
	// value this Service was constructed with, for dashboard correlation during a
	// gradual per-shard rollout.
	configuredConcurrency prometheus.Gauge
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
		Help: "Errors by operation (list, get, put, delete, panic).",
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
	m.oversizedLevelsSkipped = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_oversized_levels_skipped_total",
		Help: "Levels skipped because even the minimum forced-progress batch (2 files) would " +
			"vastly exceed the configured batch byte cap -- individual files have already grown " +
			"too large to merge further safely. Non-zero values are worth investigating.",
	}))
	m.entriesRetained = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_entries_retained_total",
		Help: "Posting-list entries propagated to output (source block still exists).",
	}))
	m.entriesDropped = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_entries_dropped_total",
		Help: "Posting-list entries dropped because their source block was deleted by retention.",
	}))
	m.entriesCorrupt = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_entries_corrupt_total",
		Help: "Posting-list entries dropped because their SourceID could not be resolved against " +
			"the input file's own StringTable -- data corruption, not routine retention pruning. " +
			"Any non-zero rate should be investigated (tempo-dev-test-03 incident, 2026-07-15).",
	}))

	m.columnsCompacted = compactorRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_index_compactor_columns_compacted_total",
		Help: "Column directories where at least one level was merged, by input level.",
	}, []string{compactorLabelLevel}))

	// backlogL0Files is set from len(byLevel[0]) each compactColumn call, labeled
	// by tenant -- data already fetched during the level-grouping pass, so this
	// costs zero extra I/O (unlike a dedicated directory walk, which previously
	// doubled pass latency and caused OOMKills at scale).
	m.backlogL0Files = compactorRegisterGaugeVec(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "blockpack_value_index_compactor_backlog_l0_files",
		Help: "L0 files observed awaiting compaction in the most recent compactColumn call, by tenant.",
	}, []string{"tenant"}))

	m.mergesInFlight = compactorRegisterGauge(reg, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "blockpack_value_index_compactor_merges_in_flight",
		Help: "Number of column merges Run() currently has dispatched concurrently.",
	}))

	m.configuredConcurrency = compactorRegisterGauge(reg, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "blockpack_value_index_compactor_configured_concurrency",
		Help: "Effective (post-defaulting) CompactConcurrency this Service was constructed with.",
	}))

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
}

func (m *compactorMetrics) setLastRunNow() {
	if m == nil {
		return
	}
	m.lastRun.SetToCurrentTime()
}

func (m *compactorMetrics) incInFlight() {
	if m == nil {
		return
	}
	m.mergesInFlight.Inc()
}

func (m *compactorMetrics) decInFlight() {
	if m == nil {
		return
	}
	m.mergesInFlight.Dec()
}

func (m *compactorMetrics) setConfiguredConcurrency(n int) {
	if m == nil {
		return
	}
	m.configuredConcurrency.Set(float64(n))
}

func (m *compactorMetrics) setBacklogL0(tenant string, n int) {
	if m == nil {
		return
	}
	m.backlogL0Files.WithLabelValues(tenant).Set(float64(n))
}

func (m *compactorMetrics) observeMerge(d time.Duration) {
	if m == nil {
		return
	}
	m.mergeDur.Observe(d.Seconds())
}

func (m *compactorMetrics) addMergeCounts(read, written, deleted, retained, dropped, corrupt int) {
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
	if corrupt > 0 {
		m.entriesCorrupt.Add(float64(corrupt))
	}
}

func (m *compactorMetrics) incSkipped(n int) {
	if m == nil || n == 0 {
		return
	}
	m.filesSkipped.Add(float64(n))
}

func (m *compactorMetrics) incOversizedLevelsSkipped() {
	if m == nil {
		return
	}
	m.oversizedLevelsSkipped.Inc()
}

func (m *compactorMetrics) incColumnsCompacted(level int) {
	if m == nil {
		return
	}
	m.columnsCompacted.WithLabelValues(fmt.Sprintf("%d", level)).Inc()
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
