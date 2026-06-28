package valueindexconsumer

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// NOTE-VI-023: Prometheus metric/label name constants for the consumer. Repeated
// label keys/values are pulled out as constants so golangci-lint's goconst does
// not flag them and so the spelling is enforced in one place.
const (
	consumerLabelStatus = "status"
	consumerLabelColumn = "column"
	consumerLabelOp     = "op"

	consumerStatusSuccess = "success"
	consumerStatusError   = "error"

	consumerOpExtract = "extract"
	consumerOpFlush   = "flush"
	consumerOpClaim   = "claim"
	consumerOpAck     = "ack"
)

// consumerMetrics holds the Prometheus collectors for the value-index consumer
// service. A nil *consumerMetrics is safe: every method becomes a no-op, so the
// hot path costs nothing when Config.Registerer is nil (NOTE-VI-023).
//
// Hot-path observers (extract/flush durations and flush bytes) are pre-resolved
// at construction time so observation is a single Observe call with no per-call
// label allocation, matching the cache layer pattern (memcache durGetHit etc.).
type consumerMetrics struct {
	files         *prometheus.CounterVec // status
	entries       *prometheus.CounterVec // column
	errors        *prometheus.CounterVec // op
	extractDur    prometheus.Observer
	flushDur      prometheus.Observer
	flushBytes    prometheus.Observer
	staleReclaims prometheus.Counter
	pendingJobs   prometheus.Gauge
}

// newConsumerMetrics builds and registers the consumer collectors against reg.
// When reg is nil it returns nil so the service runs with all-no-op metrics.
// Registration tolerates AlreadyRegisteredError so multiple consumer instances
// (or the consumer and a co-located compactor) can share one global registry
// without panicking.
func newConsumerMetrics(reg prometheus.Registerer) *consumerMetrics {
	if reg == nil {
		return nil
	}
	m := &consumerMetrics{}
	m.files = consumerRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_index_consumer_files_total",
		Help: "Files fully processed and flushed, by status (success, error).",
	}, []string{consumerLabelStatus}))
	m.entries = consumerRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_index_consumer_entries_total",
		Help: "Entries extracted and written across all columns, by column.",
	}, []string{consumerLabelColumn}))
	m.errors = consumerRegisterCounterVec(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "blockpack_value_index_consumer_errors_total",
		Help: "Errors by operation (extract, flush, claim, ack).",
	}, []string{consumerLabelOp}))

	extractH := consumerRegisterHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:                            "blockpack_value_index_consumer_extract_duration_seconds",
		Help:                            "Time to extract all column entries from one blockpack file.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 15 * time.Minute,
	}))
	flushH := consumerRegisterHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:                            "blockpack_value_index_consumer_flush_duration_seconds",
		Help:                            "Time to build and PUT one L0 value index file to object storage.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 15 * time.Minute,
	}))
	flushB := consumerRegisterHistogram(reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:                            "blockpack_value_index_consumer_flush_bytes",
		Help:                            "Size in bytes of each L0 value index file written.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 15 * time.Minute,
	}))
	m.extractDur = extractH
	m.flushDur = flushH
	m.flushBytes = flushB

	m.staleReclaims = consumerRegisterCounter(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockpack_value_index_consumer_stale_reclaims_total",
		Help: "Items reclaimed after stale claim timeout (proxy for worker crashes).",
	}))
	m.pendingJobs = consumerRegisterGauge(reg, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "blockpack_value_index_consumer_pending_jobs",
		Help: "Current count of pending jobs in the job table.",
	}))
	return m
}

// observeExtract records the time to extract one blockpack file.
func (m *consumerMetrics) observeExtract(d time.Duration) {
	if m == nil {
		return
	}
	m.extractDur.Observe(d.Seconds())
}

// observeFlush records the time and size of one L0 file flush.
func (m *consumerMetrics) observeFlush(d time.Duration, nbytes int) {
	if m == nil {
		return
	}
	m.flushDur.Observe(d.Seconds())
	m.flushBytes.Observe(float64(nbytes))
}

// incFile records one fully-processed file by status.
func (m *consumerMetrics) incFile(status string) {
	if m == nil {
		return
	}
	m.files.WithLabelValues(status).Inc()
}

// addEntries records n entries written for one column.
func (m *consumerMetrics) addEntries(column string, n int) {
	if m == nil || n == 0 {
		return
	}
	m.entries.WithLabelValues(column).Add(float64(n))
}

// incError records one error for the given operation.
func (m *consumerMetrics) incError(op string) {
	if m == nil {
		return
	}
	m.errors.WithLabelValues(op).Inc()
}

// addStaleReclaims records n items reclaimed after a stale claim timeout.
func (m *consumerMetrics) addStaleReclaims(n int) {
	if m == nil || n == 0 {
		return
	}
	m.staleReclaims.Add(float64(n))
}

// setPendingJobs records the current pending-job count.
func (m *consumerMetrics) setPendingJobs(n int) {
	if m == nil {
		return
	}
	m.pendingJobs.Set(float64(n))
}

// consumerRegisterCounterVec registers cv, returning the existing collector on
// AlreadyRegisteredError instead of panicking.
func consumerRegisterCounterVec(reg prometheus.Registerer, cv *prometheus.CounterVec) *prometheus.CounterVec {
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

// consumerRegisterCounter registers c, returning the existing collector on
// AlreadyRegisteredError instead of panicking.
func consumerRegisterCounter(reg prometheus.Registerer, c prometheus.Counter) prometheus.Counter {
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

// consumerRegisterGauge registers g, returning the existing collector on
// AlreadyRegisteredError instead of panicking.
func consumerRegisterGauge(reg prometheus.Registerer, g prometheus.Gauge) prometheus.Gauge {
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

// consumerRegisterHistogram registers h, returning the existing collector on
// AlreadyRegisteredError instead of panicking.
func consumerRegisterHistogram(reg prometheus.Registerer, h prometheus.Histogram) prometheus.Histogram {
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
