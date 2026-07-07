package valuecountscompactor

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// NOTE: see internal/modules/valuecountscompactor/NOTES.md.
// Any changes to this file must be reflected there.

// Prometheus metric/label name constants for the compactor.
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
	// compactorOpDecode has no VI equivalent: valuecounts.DecodeVCNTObject
	// failures in mergeLevel bump this op label (VI's
	// DecodeFilteredBucketFile failures don't bump a metric today).
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

// compactorMetrics holds the Prometheus collectors for the value-counts
// compactor service. A nil *compactorMetrics is safe: every method is a no-op.
type compactorMetrics struct {
	// Pass-level counters / histograms.
	runs    *prometheus.CounterVec // {status}
	errors  *prometheus.CounterVec // {op}
	runDur  prometheus.Observer
	lastRun prometheus.Gauge // unix timestamp of last completed pass

	// Per-merge counters / histograms.
	mergeDur     prometheus.Observer
	filesRead    prometheus.Counter
	filesWritten prometheus.Counter
	filesDeleted prometheus.Counter
	filesSkipped prometheus.Counter // unparseable filenames

	// filesQuarantined counts input files deleted after a permanent (non-transient)
	// decode failure -- valuecounts.DecodeVCNTObject only understands the
	// self-describing format (#490 A-3), so any error it returns means the file's
	// data is unrecoverable with this codebase, not a transient read glitch.
	// Retrying changes nothing for a deterministic decode failure, so mergeLevel
	// quarantines (deletes) the file immediately rather than leaving it to block
	// every future compaction attempt for its column forever.
	// Each increment is a deliberate, logged data-loss event -- alert if non-zero.
	filesQuarantined prometheus.Counter

	// recordsRead/recordsWritten count total decoded input records vs. merged
	// output records — not a retained/dropped split like VI's, since
	// valuecounts.Compact doesn't currently return per-group drop stats
	// (documented gap, NOTE-VC-007).
	recordsRead    prometheus.Counter
	recordsWritten prometheus.Counter

	// mergeDeferredFiles counts files left unprocessed in a merge because
	// MaxRecordsPerMerge was hit before they were reached; distinct from
	// filesSkipped, which is for unparseable filenames.
	mergeDeferredFiles prometheus.Counter

	// mergeDeleteFailedAfterRetry counts input deletes that still failed after
	// exhausting deleteWithRetry's retry attempts within one mergeLevel call.
	// NOTE-VC-009: unlike valueindexcompactor, valuecounts.Compact sums Count
	// rather than deduping by identity, so a surviving un-deleted input can be
	// double-summed by a future merge — a non-zero value here means that risk
	// has materialized and needs manual operator reconciliation.
	mergeDeleteFailedAfterRetry prometheus.Counter

	// Columns that had at least one level compacted in the pass.
	columnsCompacted *prometheus.CounterVec // {level}
}

func newCompactorMetrics(reg prometheus.Registerer) *compactorMetrics {
	if reg == nil {
		return nil
	}
	m := &compactorMetrics{}

	// ── pass-level ───────────────────────────────────────────────────────────
	m.runs = compactorRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_runs_total",
		Help: "Completed compaction passes by status (success|error).",
	}, []string{compactorLabelStatus}))

	m.errors = compactorRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_errors_total",
		Help: "Errors by operation (list, get, put, delete, decode).",
	}, []string{compactorLabelOp}))

	m.lastRun = compactorRegisterGauge(reg, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "blockpack_value_count_compactor_last_run_timestamp_seconds",
		Help: "Unix timestamp of the last completed compaction pass (success or error). " +
			"Alert if this is stale.",
	}))

	runH := compactorRegisterHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "blockpack_value_count_compactor_run_duration_seconds",
		Help:    "Wall time for one full compaction pass over all tenants and columns.",
		Buckets: runDurationBuckets,
	}))
	m.runDur = runH

	// ── per-merge ─────────────────────────────────────────────────────────────
	mergeH := compactorRegisterHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "blockpack_value_count_compactor_merge_duration_seconds",
		Help:    "Wall time to merge one level's files for one (tenant, column) pair.",
		Buckets: mergeDurationBuckets,
	}))
	m.mergeDur = mergeH

	m.filesRead = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_files_read_total",
		Help: "Input files read across all compaction jobs.",
	}))
	m.filesWritten = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_files_written_total",
		Help: "Output files written (L1+).",
	}))
	m.filesDeleted = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_files_deleted_total",
		Help: "Input files deleted after a successful merge.",
	}))
	m.filesSkipped = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_files_skipped_total",
		Help: "Files skipped because their name could not be parsed.",
	}))
	m.filesQuarantined = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_files_quarantined_total",
		Help: "Input files deleted after a permanent decode failure (both self-describing and " +
			"legacy formats rejected the data) -- each increment is unrecoverable data loss for " +
			"that one file, not a transient error. Alert if non-zero.",
	}))
	m.recordsRead = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_records_read_total",
		Help: "Decoded input records read across all merges.",
	}))
	m.recordsWritten = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_records_written_total",
		Help: "Merged output records written across all merges.",
	}))
	m.mergeDeferredFiles = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_merge_deferred_files_total",
		Help: "Files left unprocessed in a merge because MaxRecordsPerMerge was reached; " +
			"deferred to the next pass.",
	}))
	m.mergeDeleteFailedAfterRetry = compactorRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_merge_delete_failed_after_retry_total",
		Help: "Input file deletes that still failed after exhausting retry attempts within one " +
			"merge. Non-zero values mean a surviving input may be double-counted by a future " +
			"merge (valuecounts.Compact sums Count rather than deduping by identity, NOTE-VC-009)" +
			" — alert on this and reconcile manually.",
	}))

	m.columnsCompacted = compactorRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_count_compactor_columns_compacted_total",
		Help: "Column directories where at least one level was merged, by input level.",
	}, []string{compactorLabelLevel}))

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

func (m *compactorMetrics) observeMerge(d time.Duration) {
	if m == nil {
		return
	}
	m.mergeDur.Observe(d.Seconds())
}

func (m *compactorMetrics) addMergeCounts(read, written, deleted, recordsRead, recordsWritten int) {
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
	if recordsRead > 0 {
		m.recordsRead.Add(float64(recordsRead))
	}
	if recordsWritten > 0 {
		m.recordsWritten.Add(float64(recordsWritten))
	}
}

func (m *compactorMetrics) incSkipped(n int) {
	if m == nil || n == 0 {
		return
	}
	m.filesSkipped.Add(float64(n))
}

func (m *compactorMetrics) incQuarantined(n int) {
	if m == nil || n == 0 {
		return
	}
	m.filesQuarantined.Add(float64(n))
}

func (m *compactorMetrics) incDeferred(n int) {
	if m == nil || n == 0 {
		return
	}
	m.mergeDeferredFiles.Add(float64(n))
}

func (m *compactorMetrics) incDeleteFailedAfterRetry() {
	if m == nil {
		return
	}
	m.mergeDeleteFailedAfterRetry.Inc()
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
