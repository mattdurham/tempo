package valueindexconsumer

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
	"time"

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
//	[4]  block_id
//	[8]  time_sec
//	[4]  source_ref_len
//	[N]  source_ref bytes
//	[4]  value_len
//	[M]  value bytes (canonical encoding from valueindex)
//
// entryFixedSize is the fixed header of a spilled ColumnEntry (NOTE-V2-002):
// col_type[1] + trace_id[16] + block_ref[5] + time_sec[8] + src_len[4] + val_len[4].
const entryFixedSize = 1 + 16 + shared.BlockFileRefWireSize + 8 + 4 + 4 // 38 bytes

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
	return &Service{
		consumer:   consumer,
		extractor:  extractor,
		store:      store,
		buffers:    make(map[bufferKey]*columnBuffer),
		pendingCol: make(map[string]int),
		cfg:        cfg,
		columns:    cfg.columnSet(),
		metrics:    newConsumerMetrics(cfg.Registerer),
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
		s.metrics.addStaleReclaims(r.StaleReclaimsSince())
	}
	if r, ok := s.consumer.(PendingReporter); ok {
		if n, err := r.PendingCount(ctx); err == nil {
			s.metrics.setPendingJobs(n)
		}
	}
}

// ingest extracts entries from one message and appends them to per-column spill files.
func (s *Service) ingest(ctx context.Context, msg Message) error {
	touched := make(map[bufferKey]struct{})
	perColumn := make(map[string]int)

	extractStart := s.now()
	err := s.extractor.Extract(ctx, msg.Event, func(e ColumnEntry) error {
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
	s.metrics.observeExtract(s.now().Sub(extractStart))
	if err != nil {
		s.metrics.incError(consumerOpExtract)
		s.metrics.incFile(consumerStatusError)
		return fmt.Errorf("valueindexconsumer: extract %q: %w", msg.Event.Path, err)
	}
	for col, n := range perColumn {
		s.metrics.addEntries(col, n)
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
			return ackErr
		}
		s.metrics.incFile(consumerStatusSuccess)
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
func (s *Service) flushAll(ctx context.Context) error {
	for _, buf := range s.buffers {
		if !buf.hasData {
			continue
		}
		if err := s.flushColumn(ctx, buf); err != nil {
			return err
		}
	}
	return nil
}

// flushColumn reads the spill file, builds an L0 value index, PUTs it to S3,
// acks the relevant messages, and resets the spill file for reuse.
func (s *Service) flushColumn(ctx context.Context, buf *columnBuffer) error {
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
		if err := w.AddEntry(e.Value, e.TraceID, e.SourceRef, e.BlockRef, e.TimeSec); err != nil {
			s.metrics.incError(consumerOpFlush)
			return fmt.Errorf("valueindexconsumer: add entry %q: %w", buf.colName, err)
		}
	}

	data, err := w.Flush(ctx, 0)
	if err != nil {
		s.metrics.incError(consumerOpFlush)
		return fmt.Errorf("valueindexconsumer: flush writer %q: %w", buf.colName, err)
	}

	key := s.indexKey(buf.tenant, buf.colHash, buf.colType)
	if err := s.store.Put(key, data); err != nil {
		s.metrics.incError(consumerOpFlush)
		return fmt.Errorf("valueindexconsumer: put %q: %w", key, err)
	}
	s.metrics.observeFlush(s.now().Sub(flushStart), len(data))

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
			return fmt.Errorf("valueindexconsumer: ack: %w", err)
		}
		// Each acked message corresponds to one fully-processed-and-flushed file.
		for range ackIDs {
			s.metrics.incFile(consumerStatusSuccess)
		}
	}
	return nil
}

// closeAllBuffers removes all temp spill files on shutdown.
func (s *Service) closeAllBuffers() {
	for _, buf := range s.buffers {
		_ = buf.file.Close()
		_ = os.Remove(buf.file.Name())
	}
}

// indexKey builds the object key for a flushed L0 file:
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
	if encErr := shared.EncodeBlockFileRef(buf[17:22], e.BlockRef); encErr != nil {
		return fmt.Errorf("valueindexconsumer: encode block ref: %w", encErr)
	}
	binary.LittleEndian.PutUint64(buf[22:30], e.TimeSec)
	binary.LittleEndian.PutUint32(buf[30:34], uint32(len(e.SourceRef))) //nolint:gosec
	binary.LittleEndian.PutUint32(buf[34:38], uint32(len(val)))         //nolint:gosec

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
	blockRef, brErr := shared.DecodeBlockFileRef(hdr[17:22])
	if brErr != nil {
		return ColumnEntry{}, fmt.Errorf("valueindexconsumer: decode block ref: %w", brErr)
	}
	timeSec := binary.LittleEndian.Uint64(hdr[22:30])
	srcLen := binary.LittleEndian.Uint32(hdr[30:34])
	valLen := binary.LittleEndian.Uint32(hdr[34:38])

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
