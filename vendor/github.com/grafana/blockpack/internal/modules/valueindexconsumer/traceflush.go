package valueindexconsumer

// traceflush.go — trace-by-ID index flush path (Stage 2, traceindex.go wiring
// plan). This is a PARALLEL path alongside the per-column buffer/flush path in
// service.go, not a replacement: every span row is additionally buffered into
// a per-tenant TraceGroup spill (keyed off the span:id sentinel column,
// independent of the configured column allowlist) and flushed as its own
// valueindex.EncodeTraceGroups payload under the trace:id column directory.
// Trace-by-id coverage must not depend on which attribute columns an operator
// chose to index.
//
// Dedup scope (deliberate, see TestFlushTraceGroups_DedupWithinOneFlushWindow):
// the flush path groups spilled rows by TraceID only. It does NOT dedup by
// (TraceID, SpanID) within one flush window; that contract belongs to
// valueindex.MergeTraceGroups at compaction time, keeping L0 file production
// simple and avoiding two divergent copies of the same dedup logic.

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"

	"go.opentelemetry.io/otel/attribute"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// traceGroupBuffer spills (TraceID, SpanEntry, TimeSec) rows for one tenant to
// a local temp file between flushes. There is exactly one buffer per tenant
// (not one per column, unlike columnBuffer) since a tenant has exactly one
// trace-index colDir.
type traceGroupBuffer struct {
	file       *os.File
	bw         *bufio.Writer
	pendingIDs map[string]struct{}
	tenant     string
	hasData    bool
}

// traceRow is one spilled record: a span observation plus the TraceID and
// TimeSec needed to group it into a TraceGroup at flush time (SpanEntry
// itself carries neither).
//
// Binary layout (little-endian):
//
//	[16] trace_id
//	[8]  span_id
//	[8]  parent_span_id
//	[5]  block_ref: page[3]+len_pages[2]
//	[2]  row_idx
//	[8]  time_sec
//	[4]  source_ref_len
type traceRow struct {
	SourceRef    string
	TraceID      [16]byte
	SpanID       [8]byte
	ParentSpanID [8]byte
	BlockRef     valueindex.BlockRef
	TimeSec      uint64
	RowIdx       uint16
}

const (
	traceRowFixedSize   = 16 + 8 + 8 + 5 + 2 + 8 + 4 // 51 bytes
	traceRowBlockRefOff = 16 + 8 + 8                 // offset of block_ref in fixed header
)

// bufferTraceRow appends one span row to the tenant's trace-group spill,
// lazily creating the buffer, and marks msgID pending on first touch this
// ingest call (mirrors ingest's per-column touched-buffer bookkeeping).
func (s *Service) bufferTraceRow(e ColumnEntry, msgID string, touchedTrace map[string]struct{}) error {
	tenant := tenantFromPath(e.SourceRef)
	buf, err := s.traceBufferFor(tenant)
	if err != nil {
		return err
	}
	if werr := writeTraceRow(buf.bw, e); werr != nil {
		return fmt.Errorf("valueindexconsumer: write trace row: %w", werr)
	}
	buf.hasData = true
	if _, seen := touchedTrace[tenant]; !seen {
		touchedTrace[tenant] = struct{}{}
		buf.pendingIDs[msgID] = struct{}{}
	}
	return nil
}

// traceBufferFor returns or lazily creates the disk-backed trace-group buffer
// for tenant.
func (s *Service) traceBufferFor(tenant string) (*traceGroupBuffer, error) {
	if buf, ok := s.traceBuffers[tenant]; ok {
		return buf, nil
	}
	f, err := os.CreateTemp("", "vic-trace-*")
	if err != nil {
		return nil, fmt.Errorf("valueindexconsumer: create trace spill file for tenant %q: %w", tenant, err)
	}
	buf := &traceGroupBuffer{
		file:       f,
		bw:         bufio.NewWriterSize(f, spillWriteBufSize),
		tenant:     tenant,
		pendingIDs: make(map[string]struct{}),
	}
	s.traceBuffers[tenant] = buf
	return buf, nil
}

// flushTraceGroups reads the tenant's spilled rows, groups them by TraceID
// into []valueindex.TraceGroup, PUTs the encoded payload under the trace:id
// column directory, acks the relevant messages, and resets the spill file for
// reuse. Mirrors flushColumn's crash-safety and ack-bookkeeping shape.
func (s *Service) flushTraceGroups(ctx context.Context, buf *traceGroupBuffer) error {
	ctx, span := tracer.Start(ctx, "value_index.flush.tracegroups")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("tenant", buf.tenant))
	}

	if err := buf.bw.Flush(); err != nil {
		s.metrics.incError(consumerOpFlush)
		return fmt.Errorf("valueindexconsumer: flush trace spill %q: %w", buf.tenant, err)
	}
	if _, err := buf.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("valueindexconsumer: seek trace spill %q: %w", buf.tenant, err)
	}

	flushStart := s.now()
	groups, rowCount, err := readTraceGroups(buf.file)
	if err != nil {
		s.metrics.incError(consumerOpFlush)
		return fmt.Errorf("valueindexconsumer: read trace spill %q: %w", buf.tenant, err)
	}

	if len(groups) == 0 {
		// No trace rows this flush window (defensive; mirrors flushColumn's
		// empty-payload guard): nothing to PUT.
		return nil
	}

	data, err := valueindex.EncodeTraceGroups(groups)
	if err != nil {
		// String-table overflow within one flush window is an accepted,
		// documented low-probability edge case for v1 (flush windows are
		// time-bounded, far smaller than a full compaction batch) -- surfaced
		// as a hard error rather than silently dropping data or attempting
		// batch-splitting here (deferred to a follow-up if telemetry shows
		// it's needed; see plan.md Stage 2 edge cases).
		s.metrics.incError(consumerOpFlush)
		return fmt.Errorf("valueindexconsumer: encode trace groups %q: %w", buf.tenant, err)
	}

	wallMinSec, wallMaxSec := traceGroupTimeRange(groups)
	key := path.Join(
		buf.tenant,
		s.cfg.IndexPrefix,
		valueindex.ColHash(shared.TraceIDColumnName),
		valueindex.ColTypeName(shared.ColumnTypeUUID),
		valueindex.FormatFilenameV2(0, wallMinSec, wallMaxSec, valueindex.NewID()),
	)
	if err := s.store.Put(key, data); err != nil {
		s.metrics.incError(consumerOpFlush)
		return fmt.Errorf("valueindexconsumer: put %q: %w", key, err)
	}
	colElapsed := s.now().Sub(flushStart)
	s.metrics.observeFlush(colElapsed, len(data))

	if span.IsRecording() {
		span.SetAttributes(attribute.Int("groups", len(groups)), attribute.Int("rows", rowCount))
	}
	s.logger.Debug("value-index consumer trace groups flushed",
		"tenant", buf.tenant,
		"groups", len(groups),
		"rows", rowCount,
		"size_bytes", len(data),
		"s3_key", key,
		"elapsed", colElapsed)

	ackIDs := s.resolvePendingAcks(buf.pendingIDs)
	buf.pendingIDs = make(map[string]struct{})
	buf.hasData = false

	if err := buf.file.Truncate(0); err != nil {
		return fmt.Errorf("valueindexconsumer: truncate trace spill %q: %w", buf.tenant, err)
	}
	if _, err := buf.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("valueindexconsumer: reset trace spill %q: %w", buf.tenant, err)
	}
	buf.bw.Reset(buf.file)

	if len(ackIDs) > 0 {
		if err := s.consumer.Ack(ctx, ackIDs...); err != nil {
			s.metrics.incError(consumerOpAck)
			s.logger.Error("value-index consumer trace ack failed", "acked", len(ackIDs), "err", err)
			return fmt.Errorf("valueindexconsumer: ack: %w", err)
		}
		for range ackIDs {
			s.metrics.incFile(consumerStatusSuccess)
		}
		s.logger.Debug("value-index consumer trace messages acked", "acked", len(ackIDs), "ok", true)
	}
	return nil
}

// readTraceGroups reads back every spilled traceRow from r and groups them by
// TraceID. A group's TimeSec is the minimum TimeSec across its constituent
// rows (the earliest bucket the trace was seen in this window), matching
// MergeTraceGroups's own "earliest bucket" semantics so a freshly-flushed L0
// file is already consistent with what a later merge would produce.
func readTraceGroups(r io.Reader) ([]valueindex.TraceGroup, int, error) {
	type accum struct {
		spans   []valueindex.SpanEntry
		timeSec uint64
		seen    bool
	}
	byTrace := make(map[[16]byte]*accum)
	br := bufio.NewReader(r)
	rowCount := 0
	for {
		row, err := readTraceRow(br)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, 0, err
		}
		a := byTrace[row.TraceID]
		if a == nil {
			a = &accum{}
			byTrace[row.TraceID] = a
		}
		if !a.seen || row.TimeSec < a.timeSec {
			a.timeSec = row.TimeSec
		}
		a.seen = true
		a.spans = append(a.spans, valueindex.SpanEntry{
			SourceRef:    row.SourceRef,
			SpanID:       row.SpanID,
			ParentSpanID: row.ParentSpanID,
			BlockRef:     row.BlockRef,
			RowIdx:       row.RowIdx,
		})
		rowCount++
	}

	groups := make([]valueindex.TraceGroup, 0, len(byTrace))
	for tid, a := range byTrace {
		groups = append(groups, valueindex.TraceGroup{
			TraceID: tid,
			TimeSec: a.timeSec,
			Spans:   a.spans,
		})
	}
	return groups, rowCount, nil
}

// traceGroupTimeRange returns the min/max TimeSec across groups, used as the
// wall-clock range embedded in the flushed file's name (NOTE-VI-030 analog).
// There is no BucketGroup-style footer for TraceGroup files (Finding 2), so
// the range is computed directly from the in-memory groups instead of decoded
// back out of the encoded payload.
func traceGroupTimeRange(groups []valueindex.TraceGroup) (minSec, maxSec uint64) {
	minSec = math.MaxUint64
	for i := range groups {
		t := groups[i].TimeSec
		if t < minSec {
			minSec = t
		}
		if t > maxSec {
			maxSec = t
		}
	}
	if minSec == math.MaxUint64 {
		minSec = 0
	}
	return minSec, maxSec
}

// ── binary trace-row encoding ───────────────────────────────────────────────

// writeTraceRow appends one traceRow to the spill file in little-endian
// binary format, mirroring writeEntry's shape.
func writeTraceRow(w io.Writer, e ColumnEntry) error {
	if len(e.SourceRef) > math.MaxUint32 {
		return fmt.Errorf("valueindexconsumer: trace row source ref too large")
	}

	var buf [traceRowFixedSize]byte
	copy(buf[0:16], e.TraceID[:])
	copy(buf[16:24], e.SpanID[:])
	copy(buf[24:32], e.ParentSpanID[:])
	// BlockRef fields (block_page[3]+block_len_pages[2]) at offset 32. Each
	// byte() is an intentional little-endian mask (the encoding itself), not a
	// lossy overflow.
	buf[32] = byte(e.BlockRef.PageNum)       //nolint:gosec // LE byte 0
	buf[33] = byte(e.BlockRef.PageNum >> 8)  //nolint:gosec // LE byte 1
	buf[34] = byte(e.BlockRef.PageNum >> 16) //nolint:gosec // LE byte 2 (uint24)
	buf[35] = byte(e.BlockRef.LenPages)      //nolint:gosec // LE byte 0
	buf[36] = byte(e.BlockRef.LenPages >> 8) //nolint:gosec // LE byte 1
	binary.LittleEndian.PutUint16(buf[37:39], e.RowIdx)
	binary.LittleEndian.PutUint64(buf[39:47], e.TimeSec)
	binary.LittleEndian.PutUint32(buf[47:51], uint32(len(e.SourceRef))) //nolint:gosec

	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	_, err := io.WriteString(w, e.SourceRef)
	return err
}

// readTraceRow reads one traceRow from the spill file. Returns io.EOF at end.
func readTraceRow(r io.Reader) (traceRow, error) {
	var hdr [traceRowFixedSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return traceRow{}, io.EOF
		}
		return traceRow{}, err
	}

	var row traceRow
	copy(row.TraceID[:], hdr[0:16])
	copy(row.SpanID[:], hdr[16:24])
	copy(row.ParentSpanID[:], hdr[24:32])
	row.BlockRef = valueindex.BlockRef{
		PageNum:  uint32(hdr[32]) | uint32(hdr[33])<<8 | uint32(hdr[34])<<16,
		LenPages: uint16(hdr[35]) | uint16(hdr[36])<<8,
	}
	row.RowIdx = binary.LittleEndian.Uint16(hdr[37:39])
	row.TimeSec = binary.LittleEndian.Uint64(hdr[39:47])
	srcLen := binary.LittleEndian.Uint32(hdr[47:51])

	src := make([]byte, srcLen)
	if _, err := io.ReadFull(r, src); err != nil {
		return traceRow{}, err
	}
	row.SourceRef = string(src)
	return row, nil
}
