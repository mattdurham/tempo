package valueindexconsumer

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// ObjectPutter is the minimal object-storage surface the service needs: write a
// fully-formed value index file to a key. blockpack.WritableStorage satisfies
// it; tests substitute a fake. It is exported so external callers (tempo) can
// supply their object store via the public valueindexconsumer subpackage.
type ObjectPutter interface {
	Put(path string, data []byte) error
}

// spillWriteBufSize is the size of the bufio.Writer wrapping each spill file
// (NOTE-VI-025, issue #412). Entry writes are batched into this buffer and
// flushed to the underlying os.File only when it fills (or at explicit Flush
// points), collapsing one-pwrite-per-entry into one-pwrite-per-256KB.
const spillWriteBufSize = 256 * 1024

// columnBuffer spills entries for one column to a local temp file between flushes.
// All entries are appended as binary records; on flush the file is read back,
// fed to a fresh valueindex.Writer, and the resulting index blob is PUT to S3.
// No in-memory accumulation means memory usage is bounded by one block's decoded
// columns rather than all accumulated entries since the last flush.
//
// NOTE-VI-025 (issue #412): entry writes go through a bufio.Writer (bw) wrapping
// file, not file directly. Writing each entry's 37-byte header, source ref, and
// value straight to *os.File was three pwrite syscalls per entry — ~500M syscalls
// for a large L1 block and 60% of consumer CPU. bw batches those into 256KB
// pwrites. bw MUST be flushed before the file is read back (flushColumn) and
// before the spill is reused after truncate, so no buffered bytes are lost.
//
// NOTE-LINT-407: fields ordered largest-to-smallest so the uint8 colType and the
// bool hasData pack into a single word at the tail.
type columnBuffer struct {
	file       *os.File
	bw         *bufio.Writer
	pendingIDs map[string]struct{}
	colName    string
	colHash    string
	tenant     string
	colType    shared.ColumnType
	hasData    bool // true once at least one entry has been written
}

// bufferKey identifies a spill buffer by both column name AND column type
// (NOTE-VI-022). A single column name can surface with more than one column type
// across blocks — e.g. an attribute named "span.start" stored as a float64 in one
// block and a string in another due to mixed-type spans. valueindex.Writer is
// single-typed (its NewWriter colType must match every value passed to AddEntry),
// so observations of differing types for the same name MUST land in separate
// buffers, each producing a type-consistent L0 index file. Keying buffers by name
// alone locked the writer to the first-seen type and crashed AddEntry on the first
// differently-typed value (issue #408).
type bufferKey struct {
	name    string
	colType shared.ColumnType
}

// entry binary layout (little-endian):
//
//	[1]  col_type
//	[16] trace_id
//	[4]  block_id (0 for v2 entries)
//	[5]  block_ref: block_page[3]+block_len_pages[2] (all zero for v1 entries)
//	[8]  time_sec
//	[4]  source_ref_len
//	[4]  value_len
const (
	entryFixedSize   = 1 + 16 + 4 + 5 + 8 + 4 + 4 // 42 bytes
	entryBlockRefOff = 1 + 16 + 4                 // offset of block_ref in fixed header
)

// Service is the value-index consumer orchestrator. It is single-goroutine: Run
// owns all mutable state, so no locking is required.
type Service struct {
	consumer  Consumer
	extractor Extractor
	store     ObjectPutter

	buffers    map[bufferKey]*columnBuffer // keyed by (column name, column type)
	pendingCol map[string]int              // message ID → count of buffers still holding its entries
	columns    map[string]struct{}

	metrics *consumerMetrics // nil when Config.Registerer is nil (no-op)
	logger  *slog.Logger     // never nil — defaults to slog.Default() (NOTE-VI-028)
	now     func() time.Time
	cfg     Config
}

// NewService builds a consumer service. The consumer, extractor and store are
// injected so the orchestration is testable without real Redis, reader, or
// object store.
func NewService(cfg Config, consumer Consumer, extractor Extractor, store ObjectPutter) (*Service, error) {
	cfg = cfg.withDefaults()
	if consumer == nil || extractor == nil || store == nil {
		return nil, errors.New("valueindexconsumer: consumer, extractor and store are required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		consumer:   consumer,
		extractor:  extractor,
		store:      store,
		buffers:    make(map[bufferKey]*columnBuffer),
		pendingCol: make(map[string]int),
		cfg:        cfg,
		columns:    cfg.columnSet(),
		metrics:    newConsumerMetrics(cfg.Registerer),
		logger:     logger,
		now:        time.Now,
	}, nil
}

// Run drives the consume → accumulate → flush loop until ctx is canceled.
//
// Flush cadence is driven by an elapsed-time check evaluated after each message
// (NOTE-VI-020), NOT by a select on a ticker channel. Because ingest()→Extract()
// is synchronous and can take minutes for a large L1 block, a ticker case in the
// poll loop's select would only be reachable *between* messages — if a single
// message takes longer than FlushInterval the ticker case would never run while
// it was blocked, so the timer effectively never fired. Checking elapsed wall
// time after each ingest guarantees a flush happens between messages once the
// interval has passed, regardless of how long any one message took. FlushInterval
// must therefore exceed a single message's processing time, which the 15m default
// (DefaultFlushInterval) is sized to do for L1 blocks.
func (s *Service) Run(ctx context.Context) error {
	defer s.closeAllBuffers()

	lastFlush := s.now()
	for {
		if ctx.Err() != nil {
			_ = s.flushAll(context.Background())
			return ctx.Err()
		}

		msgs, err := s.consumer.Poll(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			s.metrics.incError(consumerOpClaim)
			return fmt.Errorf("valueindexconsumer: poll: %w", err)
		}
		for _, msg := range msgs {
			if err := s.ingest(ctx, msg); err != nil {
				return err
			}
		}

		s.observeQueueState(ctx)

		// Elapsed-time flush check between messages: fires even when an
		// individual ingest exceeded FlushInterval, which a ticker-in-select
		// could not (it is only reachable between Poll calls, never mid-ingest).
		if s.now().Sub(lastFlush) >= s.cfg.FlushInterval {
			if err := s.flushAll(ctx); err != nil {
				return err
			}
			lastFlush = s.now()
		}
	}
}

// observeQueueState updates the pending-jobs gauge and stale-reclaims counter
// from the consumer's optional reporting interfaces. A consumer that implements
// neither is a no-op. A PendingCount error is silently ignored — a missing
// gauge sample is preferable to crashing the consume loop on a transient backend
// hiccup. When metrics are disabled (nil), the reporting calls are skipped
// entirely so a disabled config costs nothing.
func (s *Service) observeQueueState(ctx context.Context) {
	if s.metrics == nil {
		return
	}
	if r, ok := s.consumer.(StaleReclaimReporter); ok {
		if reclaimed := r.StaleReclaimsSince(); reclaimed > 0 {
			s.metrics.addStaleReclaims(reclaimed)
			s.logger.Info("value-index consumer reclaimed stale claims", "reclaimed", reclaimed)
		}
	}
	if r, ok := s.consumer.(PendingReporter); ok {
		if n, err := r.PendingCount(ctx); err == nil {
			s.metrics.setPendingJobs(n)
		}
	}
}

// ingest extracts entries from one message and appends them to per-column spill files.
//
// NOTE-VI-028 (issue #410): wraps the Extract call in a value_index.extract span
// and logs the job boundary (claimed → extraction complete) so the otherwise
// silent pod is observable while burning CPU on a large block.
func (s *Service) ingest(ctx context.Context, msg Message) error {
	touched := make(map[bufferKey]struct{})
	perColumn := make(map[string]int)

	s.logger.Info("value-index consumer job claimed", "file", msg.Event.Path, "msg_id", msg.ID)

	ctx, span := tracer.Start(ctx, "value_index.extract")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("file", msg.Event.Path))
	}

	var totalEntries int
	extractStart := s.now()
	err := s.extractor.Extract(ctx, msg.Event, func(e ColumnEntry) error {
		totalEntries++
		if len(s.columns) > 0 {
			if _, ok := s.columns[e.ColName]; !ok {
				return nil
			}
		}
		key := bufferKey{name: e.ColName, colType: e.ColType}
		buf, berr := s.bufferFor(key)
		if berr != nil {
			return berr
		}
		if buf.tenant == "" {
			buf.tenant = tenantFromPath(e.SourceRef)
		}
		if werr := writeEntry(buf.bw, e); werr != nil {
			return fmt.Errorf("valueindexconsumer: write entry: %w", werr)
		}
		buf.hasData = true
		perColumn[e.ColName]++
		if _, seen := touched[key]; !seen {
			touched[key] = struct{}{}
			buf.pendingIDs[msg.ID] = struct{}{}
		}
		return nil
	})
	extractElapsed := s.now().Sub(extractStart)
	s.metrics.observeExtract(extractElapsed)
	if err != nil {
		s.metrics.incError(consumerOpExtract)
		s.metrics.incFile(consumerStatusError)
		s.logger.Error("value-index consumer extraction failed",
			"file", msg.Event.Path, "elapsed", extractElapsed, "err", err)
		return fmt.Errorf("valueindexconsumer: extract %q: %w", msg.Event.Path, err)
	}
	for col, n := range perColumn {
		s.metrics.addEntries(col, n)
	}

	s.logger.Info("value-index consumer extraction complete",
		"file", msg.Event.Path,
		"columns", len(touched),
		"entries", totalEntries,
		"elapsed", extractElapsed)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.Int("columns", len(touched)),
			attribute.Int("entries", totalEntries),
		)
	}

	// Flush each touched buffer's write buffer to its spill file at the end of
	// this job's extraction pass (NOTE-VI-025, issue #412). Doing it here — not
	// only at flushColumn — bounds the amount of unwritten data to one job's
	// worth and keeps the spill file consistent on disk between jobs.
	for key := range touched {
		buf := s.buffers[key]
		if ferr := buf.bw.Flush(); ferr != nil {
			s.metrics.incError(consumerOpExtract)
			return fmt.Errorf("valueindexconsumer: flush spill buffer %q: %w", buf.colName, ferr)
		}
	}

	if len(touched) == 0 {
		// No configured column touched: the file is fully handled by this ack.
		if ackErr := s.consumer.Ack(ctx, msg.ID); ackErr != nil {
			s.metrics.incError(consumerOpAck)
			s.logger.Error("value-index consumer ack failed", "file", msg.Event.Path, "msg_id", msg.ID, "err", ackErr)
			return ackErr
		}
		s.metrics.incFile(consumerStatusSuccess)
		s.logger.Info("value-index consumer job acked", "file", msg.Event.Path, "msg_id", msg.ID, "ok", true)
		return nil
	}
	s.pendingCol[msg.ID] = len(touched)
	return nil
}

// bufferFor returns or lazily creates a disk-backed buffer for the (column name,
// column type) pair. Distinct types for the same name get distinct buffers so each
// writer stays type-consistent (NOTE-VI-022, issue #408).
func (s *Service) bufferFor(key bufferKey) (*columnBuffer, error) {
	if buf, ok := s.buffers[key]; ok {
		return buf, nil
	}
	f, err := os.CreateTemp("", "vic-*")
	if err != nil {
		return nil, fmt.Errorf("valueindexconsumer: create spill file for %q: %w", key.name, err)
	}
	buf := &columnBuffer{
		file:       f,
		bw:         bufio.NewWriterSize(f, spillWriteBufSize),
		colName:    key.name,
		colType:    key.colType,
		colHash:    valueindex.ColHash(key.name),
		pendingIDs: make(map[string]struct{}),
	}
	s.buffers[key] = buf
	return buf, nil
}

// flushAll flushes every column buffer that has data.
//
// NOTE-VI-028 (issue #410): wraps the full flush pass in a value_index.flush
// span and logs the boundary so a timer/shutdown flush is visible. The
// buffer_count attribute and log field count only buffers that actually had
// data to write.
func (s *Service) flushAll(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "value_index.flush")
	defer span.End()

	flushStart := s.now()
	flushed := 0
	for _, buf := range s.buffers {
		if !buf.hasData {
			continue
		}
		if err := s.flushColumn(ctx, buf); err != nil {
			return err
		}
		flushed++
	}

	elapsed := s.now().Sub(flushStart)
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("buffer_count", flushed))
	}
	if flushed > 0 {
		s.logger.Info("value-index consumer flush complete", "buffers", flushed, "elapsed", elapsed)
	}
	return nil
}

// flushColumn reads the spill file, builds an L0 value index, PUTs it to S3,
// acks the relevant messages, and resets the spill file for reuse.
//
// NOTE-VI-028 (issue #410): wraps the single-column S3 put in a
// value_index.flush.column span and logs per-column flush detail at debug.
func (s *Service) flushColumn(ctx context.Context, buf *columnBuffer) error {
	ctx, span := tracer.Start(ctx, "value_index.flush.column")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("col", buf.colName),
			attribute.String("type", valueindex.ColTypeName(buf.colType)),
		)
	}

	// Drain any buffered entry bytes to the file before reading it back
	// (NOTE-VI-025, issue #412). ingest flushes after each pass, but flushing
	// here too keeps flushColumn correct independent of caller ordering.
	if err := buf.bw.Flush(); err != nil {
		s.metrics.incError(consumerOpFlush)
		return fmt.Errorf("valueindexconsumer: flush spill buffer %q: %w", buf.colName, err)
	}

	// Seek to beginning for reading.
	if _, err := buf.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("valueindexconsumer: seek spill %q: %w", buf.colName, err)
	}

	flushStart := s.now()
	entryCount := 0
	w := valueindex.NewWriter(buf.colName, buf.colType)
	br := bufio.NewReader(buf.file)
	for {
		e, err := readEntry(br)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.metrics.incError(consumerOpFlush)
			return fmt.Errorf("valueindexconsumer: read spill %q: %w", buf.colName, err)
		}
		var addErr error
		if e.BlockRef.PageNum > 0 || e.BlockRef.LenPages > 0 {
			if e.SpanID != ([8]byte{}) {
				addErr = w.AddEntryV4(e.Value, e.TraceID, e.SourceRef, e.BlockRef, e.TimeSec, e.SpanID, e.RowIdx)
			} else {
				addErr = w.AddEntryV2(e.Value, e.TraceID, e.SourceRef, e.BlockRef, e.TimeSec)
			}
		} else {
			addErr = w.AddEntry(e.Value, e.TraceID, e.SourceRef, e.BlockID, e.TimeSec)
		}
		if err := addErr; err != nil {
			s.metrics.incError(consumerOpFlush)
			return fmt.Errorf("valueindexconsumer: add entry %q: %w", buf.colName, err)
		}
		entryCount++
	}

	data, err := w.Flush(ctx, 0)
	if err != nil {
		s.metrics.incError(consumerOpFlush)
		return fmt.Errorf("valueindexconsumer: flush writer %q: %w", buf.colName, err)
	}

	// NOTE-VI-030 (#431): embed wall time range in filename for O(1) file discovery.
	var wallMin, wallMax uint64
	if r, readErr := valueindex.OpenReader(data); readErr == nil {
		m := r.Meta()
		wallMin, wallMax = m.WallMinTS, m.WallMaxTS
	}
	// WallMinTS/WallMaxTS are already in seconds (= TimeSec values from entries).
	wallMinSec := wallMin
	wallMaxSec := wallMax
	key := s.indexKeyV2(buf.tenant, buf.colHash, buf.colType, wallMinSec, wallMaxSec)
	if err := s.store.Put(key, data); err != nil {
		s.metrics.incError(consumerOpFlush)
		return fmt.Errorf("valueindexconsumer: put %q: %w", key, err)
	}
	colElapsed := s.now().Sub(flushStart)
	s.metrics.observeFlush(colElapsed, len(data))

	if span.IsRecording() {
		span.SetAttributes(attribute.Int("entries", entryCount))
	}
	s.logger.Debug("value-index consumer column flushed",
		"col", buf.colName,
		"type", valueindex.ColTypeName(buf.colType),
		"entries", entryCount,
		"size_bytes", len(data),
		"s3_key", key,
		"elapsed", colElapsed)

	// Ack messages whose entries are now all on S3.
	var ackIDs []string
	for id := range buf.pendingIDs {
		s.pendingCol[id]--
		if s.pendingCol[id] <= 0 {
			delete(s.pendingCol, id)
			ackIDs = append(ackIDs, id)
		}
	}
	buf.pendingIDs = make(map[string]struct{})
	buf.hasData = false

	// Truncate and reset the spill file for reuse.
	if err := buf.file.Truncate(0); err != nil {
		return fmt.Errorf("valueindexconsumer: truncate spill %q: %w", buf.colName, err)
	}
	if _, err := buf.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("valueindexconsumer: reset spill %q: %w", buf.colName, err)
	}
	// Re-point the write buffer at the rewound file for reuse (NOTE-VI-025).
	// Reset discards any residual buffered bytes (there are none — we flushed
	// above) and clears any sticky write error so the next pass starts clean.
	buf.bw.Reset(buf.file)

	if len(ackIDs) > 0 {
		if err := s.consumer.Ack(ctx, ackIDs...); err != nil {
			s.metrics.incError(consumerOpAck)
			s.logger.Error("value-index consumer ack failed", "acked", len(ackIDs), "err", err)
			return fmt.Errorf("valueindexconsumer: ack: %w", err)
		}
		// Each acked message corresponds to one fully-processed-and-flushed file.
		for range ackIDs {
			s.metrics.incFile(consumerStatusSuccess)
		}
		s.logger.Debug("value-index consumer messages acked", "acked", len(ackIDs), "ok", true)
	}
	return nil
}

// closeAllBuffers removes all temp spill files on shutdown.
func (s *Service) closeAllBuffers() {
	for _, buf := range s.buffers {
		_ = buf.file.Close()
		_ = os.Remove(buf.file.Name()) //nolint:gosec // G703: name comes from os.CreateTemp, not user input
	}
}

// indexKeyV2 builds the object key with embedded time range (NOTE-VI-030, #431).
func (s *Service) indexKeyV2(tenant, colHash string, colType shared.ColumnType, wallMinSec, wallMaxSec uint64) string {
	return path.Join(
		tenant,
		s.cfg.IndexPrefix,
		colHash,
		valueindex.ColTypeName(colType),
		valueindex.FormatFilenameV2(0, wallMinSec, wallMaxSec, valueindex.NewID()),
	)
}

// indexKey builds the object key for a flushed L0 file (legacy, without time range):
//
//	<tenant>/<index_prefix>/<col_hash>/<type>/L0-<xid>.blockpack
//
// The <type> segment (NOTE-VI-024, issue #409) keeps columns that share a name
// but differ in type under distinct prefixes: their index files have different
// value encodings and are not interchangeable, so they must never collide.
// Range* types map to their scalar bucket via valueindex.ColTypeName.
func (s *Service) indexKey(tenant, colHash string, colType shared.ColumnType) string {
	// Mirrors blockpack file layout: <tenant>/<block-id>/data.blockpack
	return path.Join(
		tenant,
		s.cfg.IndexPrefix,
		colHash,
		valueindex.ColTypeName(colType),
		valueindex.FormatFilename(0, valueindex.NewID()),
	)
}

// tenantFromPath extracts the tenant from "tenant/block-id/data.blockpack".
func tenantFromPath(p string) string {
	for k, c := range p {
		if c == '/' {
			return p[:k]
		}
	}
	return ""
}

// ── binary entry encoding ──────────────────────────────────────────────────────

// writeEntry appends one entry to the spill file in little-endian binary format.
func writeEntry(w io.Writer, e ColumnEntry) error {
	val, err := valueindex.CanonicalValue(e.ColType, e.Value)
	if err != nil {
		return fmt.Errorf("valueindexconsumer: encode entry value: %w", err)
	}

	// Validate field lengths fit in uint32.
	if len(e.SourceRef) > math.MaxUint32 || len(val) > math.MaxUint32 {
		return fmt.Errorf("entry field too large")
	}

	var buf [entryFixedSize]byte
	buf[0] = byte(e.ColType)
	copy(buf[1:17], e.TraceID[:])
	binary.LittleEndian.PutUint32(buf[17:21], e.BlockID)
	// BlockRef fields (block_page[3]+block_len_pages[2]) at offset 21. Each byte() is an
	// intentional little-endian mask (the encoding itself), not a lossy overflow.
	buf[entryBlockRefOff+0] = byte(e.BlockRef.PageNum)       //nolint:gosec // LE byte 0
	buf[entryBlockRefOff+1] = byte(e.BlockRef.PageNum >> 8)  //nolint:gosec // LE byte 1
	buf[entryBlockRefOff+2] = byte(e.BlockRef.PageNum >> 16) //nolint:gosec // LE byte 2 (uint24)
	buf[entryBlockRefOff+3] = byte(e.BlockRef.LenPages)      //nolint:gosec // LE byte 0
	buf[entryBlockRefOff+4] = byte(e.BlockRef.LenPages >> 8) //nolint:gosec // LE byte 1
	binary.LittleEndian.PutUint64(buf[26:34], e.TimeSec)
	binary.LittleEndian.PutUint32(buf[34:38], uint32(len(e.SourceRef))) //nolint:gosec
	binary.LittleEndian.PutUint32(buf[38:42], uint32(len(val)))         //nolint:gosec

	if _, werr := w.Write(buf[:]); werr != nil {
		return werr
	}
	if _, werr := io.WriteString(w, e.SourceRef); werr != nil {
		return werr
	}
	_, err = w.Write(val)
	return err
}

// readEntry reads one entry from the spill file. Returns io.EOF at end.
func readEntry(r io.Reader) (ColumnEntry, error) {
	var hdr [entryFixedSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return ColumnEntry{}, io.EOF
		}
		return ColumnEntry{}, err
	}

	colType := shared.ColumnType(hdr[0])
	var traceID [16]byte
	copy(traceID[:], hdr[1:17])
	blockID := binary.LittleEndian.Uint32(hdr[17:21])
	blockRef := valueindex.BlockRef{
		PageNum: uint32(
			hdr[entryBlockRefOff+0],
		) | uint32(
			hdr[entryBlockRefOff+1],
		)<<8 | uint32(
			hdr[entryBlockRefOff+2],
		)<<16,
		LenPages: uint16(hdr[entryBlockRefOff+3]) | uint16(hdr[entryBlockRefOff+4])<<8,
	}
	timeSec := binary.LittleEndian.Uint64(hdr[26:34])
	srcLen := binary.LittleEndian.Uint32(hdr[34:38])
	valLen := binary.LittleEndian.Uint32(hdr[38:42])

	src := make([]byte, srcLen)
	if _, err := io.ReadFull(r, src); err != nil {
		return ColumnEntry{}, err
	}
	val := make([]byte, valLen)
	if _, err := io.ReadFull(r, val); err != nil {
		return ColumnEntry{}, err
	}

	decoded, derr := decodeCanonicalValue(val, colType)
	if derr != nil {
		return ColumnEntry{}, fmt.Errorf("decode canonical value: %w", derr)
	}
	return ColumnEntry{
		ColType:   colType,
		TraceID:   traceID,
		BlockID:   blockID,
		BlockRef:  blockRef,
		TimeSec:   timeSec,
		SourceRef: string(src),
		Value:     decoded,
	}, nil
}

// decodeCanonicalValue reverses CanonicalValue encoding to recover the typed value.
func decodeCanonicalValue(b []byte, colType shared.ColumnType) (any, error) {
	switch colType {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		return string(b), nil
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		if len(b) != 8 {
			return nil, fmt.Errorf("int64: want 8 bytes, got %d", len(b))
		}
		return int64(binary.LittleEndian.Uint64(b)), nil //nolint:gosec
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		if len(b) != 8 {
			return nil, fmt.Errorf("uint64: want 8 bytes, got %d", len(b))
		}
		return binary.LittleEndian.Uint64(b), nil
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		if len(b) != 8 {
			return nil, fmt.Errorf("float64: want 8 bytes, got %d", len(b))
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
	case shared.ColumnTypeBool:
		if len(b) != 1 {
			return nil, fmt.Errorf("bool: want 1 byte, got %d", len(b))
		}
		return b[0] != 0, nil
	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		return append([]byte(nil), b...), nil
	case shared.ColumnTypeUUID:
		if len(b) != 16 {
			return nil, fmt.Errorf("uuid: want 16 bytes, got %d", len(b))
		}
		var uid [16]byte
		copy(uid[:], b)
		return uid, nil
	default:
		return nil, fmt.Errorf("unsupported colType %d", colType)
	}
}
