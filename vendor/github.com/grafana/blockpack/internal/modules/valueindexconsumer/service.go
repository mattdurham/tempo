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

// columnBuffer spills entries for one column to a local temp file between flushes.
// All entries are appended as binary records; on flush the file is read back,
// fed to a fresh valueindex.Writer, and the resulting index blob is PUT to S3.
// No in-memory accumulation means memory usage is bounded by one block's decoded
// columns rather than all accumulated entries since the last flush.
type columnBuffer struct {
	file       *os.File
	colName    string
	colType    shared.ColumnType
	colHash    string
	tenant     string
	pendingIDs map[string]struct{}
	hasData    bool // true once at least one entry has been written
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
const entryFixedSize = 1 + 16 + 4 + 8 + 4 + 4 // 37 bytes fixed header

// Service is the value-index consumer orchestrator. It is single-goroutine: Run
// owns all mutable state, so no locking is required.
type Service struct {
	consumer  Consumer
	extractor Extractor
	store     ObjectPutter

	buffers    map[string]*columnBuffer // keyed by column name
	pendingCol map[string]int           // message ID → count of columns still holding its entries
	columns    map[string]struct{}

	now func() time.Time
	cfg Config
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
		buffers:    make(map[string]*columnBuffer),
		pendingCol: make(map[string]int),
		cfg:        cfg,
		columns:    cfg.columnSet(),
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
			return fmt.Errorf("valueindexconsumer: poll: %w", err)
		}
		for _, msg := range msgs {
			if err := s.ingest(ctx, msg); err != nil {
				return err
			}
		}

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

// ingest extracts entries from one message and appends them to per-column spill files.
func (s *Service) ingest(ctx context.Context, msg Message) error {
	touched := make(map[string]struct{})

	err := s.extractor.Extract(ctx, msg.Event, func(e ColumnEntry) error {
		if len(s.columns) > 0 {
			if _, ok := s.columns[e.ColName]; !ok {
				return nil
			}
		}
		buf, berr := s.bufferFor(e.ColName, e.ColType)
		if berr != nil {
			return berr
		}
		if buf.tenant == "" {
			buf.tenant = tenantFromPath(e.SourceRef)
		}
		if werr := writeEntry(buf.file, e); werr != nil {
			return fmt.Errorf("valueindexconsumer: write entry: %w", werr)
		}
		buf.hasData = true
		if _, seen := touched[e.ColName]; !seen {
			touched[e.ColName] = struct{}{}
			buf.pendingIDs[msg.ID] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("valueindexconsumer: extract %q: %w", msg.Event.Path, err)
	}

	if len(touched) == 0 {
		return s.consumer.Ack(ctx, msg.ID)
	}
	s.pendingCol[msg.ID] = len(touched)
	return nil
}

// bufferFor returns or lazily creates a disk-backed buffer for the column.
func (s *Service) bufferFor(col string, colType shared.ColumnType) (*columnBuffer, error) {
	if buf, ok := s.buffers[col]; ok {
		return buf, nil
	}
	f, err := os.CreateTemp("", "vic-*")
	if err != nil {
		return nil, fmt.Errorf("valueindexconsumer: create spill file for %q: %w", col, err)
	}
	buf := &columnBuffer{
		file:       f,
		colName:    col,
		colType:    colType,
		colHash:    valueindex.ColHash(col),
		pendingIDs: make(map[string]struct{}),
	}
	s.buffers[col] = buf
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
	// Seek to beginning for reading.
	if _, err := buf.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("valueindexconsumer: seek spill %q: %w", buf.colName, err)
	}

	w := valueindex.NewWriter(buf.colName, buf.colType)
	br := bufio.NewReader(buf.file)
	for {
		e, err := readEntry(br)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("valueindexconsumer: read spill %q: %w", buf.colName, err)
		}
		if err := w.AddEntry(e.Value, e.TraceID, e.SourceRef, e.BlockID, e.TimeSec); err != nil {
			return fmt.Errorf("valueindexconsumer: add entry %q: %w", buf.colName, err)
		}
	}

	data, err := w.Flush(ctx, 0)
	if err != nil {
		return fmt.Errorf("valueindexconsumer: flush writer %q: %w", buf.colName, err)
	}

	key := s.indexKey(buf.tenant, buf.colHash)
	if err := s.store.Put(key, data); err != nil {
		return fmt.Errorf("valueindexconsumer: put %q: %w", key, err)
	}

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

	if len(ackIDs) > 0 {
		if err := s.consumer.Ack(ctx, ackIDs...); err != nil {
			return fmt.Errorf("valueindexconsumer: ack: %w", err)
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
//	<index_prefix>/<tenant>/<col_hash>/L0-<xid>.valueindex
func (s *Service) indexKey(tenant, colHash string) string {
	// Path: <tenant>/indexes/<index_prefix>/<col_hash>/L0-<xid>.valueindex
	// Mirrors blockpack file layout: <tenant>/<block-id>/data.blockpack
	return path.Join(tenant, s.cfg.IndexPrefix, colHash, valueindex.FormatFilename(0, valueindex.NewID()))
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
	binary.LittleEndian.PutUint64(buf[21:29], e.TimeSec)
	binary.LittleEndian.PutUint32(buf[29:33], uint32(len(e.SourceRef))) //nolint:gosec
	binary.LittleEndian.PutUint32(buf[33:37], uint32(len(val)))         //nolint:gosec

	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := io.WriteString(w, e.SourceRef); err != nil {
		return err
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
	timeSec := binary.LittleEndian.Uint64(hdr[21:29])
	srcLen := binary.LittleEndian.Uint32(hdr[29:33])
	valLen := binary.LittleEndian.Uint32(hdr[33:37])

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
	default:
		return nil, fmt.Errorf("unsupported colType %d", colType)
	}
}
