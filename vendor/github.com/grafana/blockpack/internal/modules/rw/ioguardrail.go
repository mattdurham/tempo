package rw

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// NOTE-403: I/O guardrails. The read path (chunked trace index #341, page-pruned
// intrinsic decode #347) deliberately trades whole-section fetches for a small number
// of bounded range reads. That is a bytes win but multiplies request count, and on
// high-latency object storage each request pays a fixed first-byte cost (50-100ms).
// The win only holds if io_ops stays low and bytes/io stays large. These guardrails
// turn the previously markdown-only bands (rw/BENCHMARKS.md) into a programmatic
// classification CI / dashboards can assert against, so a regression where pruning
// multiplies small requests is caught instead of silently eroding latency.

// IOBand classifies an I/O metric against the documented efficiency thresholds.
type IOBand uint8

// IOBand values, ordered worst-to-best is intentionally avoided; instead they are
// ordered Good < Warning < Critical so a caller can take the max severity across
// metrics and gate on a single threshold (e.g. fail CI if any band >= BandCritical).
const (
	// BandGood means the metric is within the healthy target range.
	BandGood IOBand = iota
	// BandWarning means the metric has drifted out of target but is not yet critical.
	BandWarning
	// BandCritical means the metric is in the range that materially erodes latency.
	BandCritical
)

// String returns the lowercase band name ("good", "warning", "critical").
func (b IOBand) String() string {
	switch b {
	case BandGood:
		return "good"
	case BandWarning:
		return "warning"
	case BandCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// I/O guardrail thresholds. Single source of truth for the bands documented in
// rw/BENCHMARKS.md; the rationale (object-storage first-byte latency dominates cost)
// is recorded there and in blockio/NOTES.md §1. Keep this in sync with that table.
const (
	// ioOpsWarnThreshold: io_ops at or above this is at least a Warning.
	ioOpsWarnThreshold int64 = 500
	// ioOpsCriticalThreshold: io_ops above this is Critical.
	ioOpsCriticalThreshold int64 = 1000

	// bytesPerIOWarnThreshold: bytes/io at or below this is at least a Warning.
	bytesPerIOWarnThreshold int64 = 100 * 1024 // 100 KB
	// bytesPerIOCriticalThreshold: bytes/io below this is Critical.
	bytesPerIOCriticalThreshold int64 = 10 * 1024 // 10 KB
)

// IOHealth is a snapshot of read-path I/O efficiency derived from the raw counters.
// It is the programmatic form of the bands in rw/BENCHMARKS.md.
type IOHealth struct {
	// IOOps is the number of real storage I/O operations (cache hits excluded).
	IOOps int64
	// BytesRead is the total bytes read from storage (cache hits excluded).
	BytesRead int64
	// BytesPerIO is BytesRead/IOOps, or 0 when IOOps == 0.
	BytesPerIO int64
	// IOOpsBand classifies IOOps against the io_ops thresholds.
	IOOpsBand IOBand
	// BytesPerIOBand classifies BytesPerIO against the bytes/io thresholds.
	BytesPerIOBand IOBand
}

// Band returns the worst (highest-severity) of the two component bands.
// A caller gates on a single value: e.g. fail CI if Band() == BandCritical.
func (h IOHealth) Band() IOBand {
	if h.IOOpsBand > h.BytesPerIOBand {
		return h.IOOpsBand
	}
	return h.BytesPerIOBand
}

// classifyIOOps maps an io_ops count to its band (higher count is worse).
func classifyIOOps(ioOps int64) IOBand {
	switch {
	case ioOps > ioOpsCriticalThreshold:
		return BandCritical
	case ioOps >= ioOpsWarnThreshold:
		return BandWarning
	default:
		return BandGood
	}
}

// classifyBytesPerIO maps a bytes/io value to its band (smaller reads are worse).
// IOOps == 0 (no real I/O, e.g. a fully cached query) is treated as Good: there is
// no small-read problem when there were no reads.
func classifyBytesPerIO(ioOps, bytesPerIO int64) IOBand {
	if ioOps == 0 {
		return BandGood
	}
	switch {
	case bytesPerIO < bytesPerIOCriticalThreshold:
		return BandCritical
	case bytesPerIO <= bytesPerIOWarnThreshold:
		return BandWarning
	default:
		return BandGood
	}
}

// EvaluateIOHealth classifies raw io_ops / bytes_read counters against the
// documented bands. It is pure (no provider state) so callers can evaluate
// counters captured at any point, including per-query-phase deltas.
func EvaluateIOHealth(ioOps, bytesRead int64) IOHealth {
	var bytesPerIO int64
	if ioOps > 0 {
		bytesPerIO = bytesRead / ioOps
	}
	return IOHealth{
		IOOps:          ioOps,
		BytesRead:      bytesRead,
		BytesPerIO:     bytesPerIO,
		IOOpsBand:      classifyIOOps(ioOps),
		BytesPerIOBand: classifyBytesPerIO(ioOps, bytesPerIO),
	}
}

// IOHealth returns the guardrail classification of this tracker's current counters.
func (t *TrackingReaderProvider) IOHealth() IOHealth {
	return EvaluateIOHealth(t.IOOps(), t.BytesRead())
}

// IOHealth returns the guardrail classification of this provider's current counters.
func (d *DefaultProvider) IOHealth() IOHealth {
	return EvaluateIOHealth(d.IOOps(), d.BytesRead())
}
