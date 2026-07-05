package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// ErrUnsupportedFormatVersion is returned by the reader when a file carries a valid
// blockpack magic number but a footer format version this build does not understand.
// NOTE-V2-004 (issue #425 migration): v2 is a hard cutover — the writer emits only
// FooterV9 and the v1 (FooterV8) read path was deleted with its sections (#433/#434/
// #435/#439). During a mixed-cluster rollout a querier may still be handed a stale v1
// object that predates the cutover. Distinguishing "wrong format version" from a
// generic parse/I/O error via errors.Is lets a caller route around a single stale
// block (skip it and let compaction rewrite it) instead of failing the whole query
// or misclassifying it as corruption. Use errors.As with *UnsupportedFormatVersionError
// to recover the offending version byte.
var ErrUnsupportedFormatVersion = errors.New("blockpack: unsupported file format version")

// UnsupportedFormatVersionError carries the offending footer version byte alongside
// ErrUnsupportedFormatVersion (which it wraps). NOTE-V2-004.
type UnsupportedFormatVersionError struct {
	// Version is the footer format version read from the file.
	Version uint16
}

func (e *UnsupportedFormatVersionError) Error() string {
	return fmt.Sprintf(
		"blockpack: unsupported file format version %d (this build reads only FooterV9=%d; re-compact any pre-v2 blocks)",
		e.Version,
		shared.FooterV9Version,
	)
}

// Unwrap lets errors.Is(err, ErrUnsupportedFormatVersion) match.
func (e *UnsupportedFormatVersionError) Unwrap() error { return ErrUnsupportedFormatVersion }

// rangeIndexMeta records the byte range within metadataBytes for a
// range column index entry (lazy parsing).

// readFooter reads and validates the file footer from the end of the file.
// The 18-byte magic footer must carry FooterV9 (the v2 lean format, the only
// supported format). A valid magic with any other version yields
// ErrUnsupportedFormatVersion (NOTE-V2-004); an absent/mismatched magic yields a
// corruption error. Legacy formats (V3–V8) were removed with the v2 lean-format
// cutover — re-compact any pre-v2 block before reading it.
func (r *Reader) readFooter() error {
	if r.fileSize < int64(shared.FooterV8Size) {
		return fmt.Errorf("file too small for footer: %d bytes", r.fileSize)
	}
	ok, err := r.tryReadFooterMagic18()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf(
			"blockpack: unsupported or corrupt file — expected a FooterV9 (v2 lean format) file (legacy V3–V8 formats were removed at the v2 cutover; re-compact any legacy blocks before reading)",
		)
	}
	return nil
}

// tryReadFooterMagic18 reads the last 18 bytes and checks for a valid footer magic.
// If magic matches but version != FooterV9, ErrUnsupportedFormatVersion is returned so
// callers can route around a stale pre-v2 block (NOTE-V2-004).
// Returns (false, nil) when magic is absent (corrupt/foreign file).
func (r *Reader) tryReadFooterMagic18() (bool, error) {
	off := r.fileSize - int64(shared.FooterV8Size) // 18-byte footer
	buf, err := r.cache.GetOrFetchFooter(r.fileID, "/v78", func() ([]byte, error) {
		b := make([]byte, shared.FooterV8Size)
		n, readErr := r.provider.ReadAt(b, off, rw.DataTypeFooter)
		if readErr != nil {
			return nil, fmt.Errorf("readFooter: %w", readErr)
		}
		if n != int(shared.FooterV8Size) {
			return nil, fmt.Errorf("readFooter: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return false, fmt.Errorf("readFooter: %w", err)
	}
	magic := binary.LittleEndian.Uint32(buf[0:])
	if magic != shared.MagicNumber {
		return false, nil
	}
	ver := binary.LittleEndian.Uint16(buf[footerV7OffVersion:])
	// V2 lean format unconditional: only FooterV9 is supported (2026-06-29). A valid
	// magic with any other version is a stale pre-v2 file — surface a typed sentinel
	// (NOTE-V2-004) so a caller can route around it during a mixed-cluster rollout.
	if ver != shared.FooterV9Version {
		return false, &UnsupportedFormatVersionError{Version: ver}
	}
	r.footerVersion = ver
	r.v8ToCOffset = binary.LittleEndian.Uint64(buf[footerV7OffDirOff:])
	r.v8ToCLen = binary.LittleEndian.Uint32(buf[footerV7OffDirLen:])
	return true, nil
}

// 18-byte footer field offsets. The V7/V8/V9 footers share this identical wire layout;
// only the version field distinguishes them. FooterV9 (v2 lean format) is the sole
// version this build accepts.
// Wire format: magic[4] · version[2] · dir_offset[8] · dir_len[4] = 18 bytes.
const (
	footerV7OffVersion = 4  // uint16 version field within the 18-byte footer
	footerV7OffDirOff  = 6  // uint64 dir_offset field
	footerV7OffDirLen  = 14 // uint32 dir_len field
)

// decodeBoundedSnappy snappy-decodes compressed, rejecting inputs whose
// decoded size would exceed MaxMetadataSize (decompression-bomb guard).
// NOTE-366: pooled scratch for the read-compressed-then-snappy-decode-then-discard pattern.
//
// readV14Section / parseV8ToCBlob / fetchToCSection / ensureV8TraceSection / chunkBytes /
// ensureTraceIndexRaw all do the same thing: r.readRange(...) allocates a fresh
// make([]byte, length) (NOTE: readRange), the bytes are fed straight to decodeBoundedSnappy,
// and the compressed buffer is then unreferenced. The decoded output escapes (to r.cache or
// a parsed struct), but the *compressed* buffer is pure transient scratch. Under heavy
// concurrent metadata loading these transient buffers piled up as live bytes attributed to
// readRange (the #3 inuse_space self-frame at ~188 MB warm). Routing them through a pool
// keyed only by the 8 MiB cap (mirroring decompBufPool / NOTE-346) keeps the steady-state
// scratch footprint bounded instead of one-shot-allocating each compressed blob.
var rangeScratchPool = sync.Pool{New: func() any { b := make([]byte, 0, 64<<10); return &b }}

// rangeScratchMaxPooledCap bounds the backing capacity any buffer may carry back into
// rangeScratchPool, so a rare large compressed section (e.g. a tens-of-MB legacy V8 trace
// index) does not pin a giant array in the pool for the process lifetime. Mirrors
// decompBufMaxPooledCap (NOTE-346). Metadata/section compressed blobs are typically KB to a
// few MB, well under this cap, so the common case recycles its buffer with no churn.
const rangeScratchMaxPooledCap = 8 << 20 // 8 MiB

// putRangeScratch returns a read-scratch buffer to rangeScratchPool, dropping any buffer
// grown beyond rangeScratchMaxPooledCap. ptr must be non-nil.
func putRangeScratch(ptr *[]byte) {
	if cap(*ptr) > rangeScratchMaxPooledCap {
		return // drop oversized buffer: let it GC rather than pin RSS in the pool
	}
	rangeScratchPool.Put(ptr)
}

// readRangeDecodeSnappy reads [offset, offset+length) into a pooled scratch buffer, snappy-
// decodes it, and returns the freshly-allocated decoded bytes. The pooled scratch (the
// compressed source) is returned to rangeScratchPool before return; the decoded output is
// independently allocated by decodeBoundedSnappy and does NOT alias the scratch, so the
// buffer is safe to recycle (NOTE-366). Returns (nil, nil) for length == 0.
func (r *Reader) readRangeDecodeSnappy(offset, length uint64, dt rw.DataType) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	scratchPtr := rangeScratchPool.Get().(*[]byte)
	scratch := *scratchPtr
	if uint64(cap(scratch)) < length { //nolint:gosec // length is a section size, bounded by MaxMetadataSize
		scratch = make([]byte, length)
	} else {
		scratch = scratch[:length]
	}
	defer func() {
		*scratchPtr = scratch[:0]
		putRangeScratch(scratchPtr)
	}()
	off := int64(offset) //nolint:gosec // safe: offset is a file offset, fits in int64
	n, err := r.provider.ReadAt(scratch, off, dt)
	if err != nil {
		return nil, fmt.Errorf("readRangeDecodeSnappy offset=%d length=%d: %w", offset, length, err)
	}
	if uint64(n) != length { //nolint:gosec // safe: n is bytes read, always non-negative
		return nil, fmt.Errorf("readRangeDecodeSnappy offset=%d: short read %d/%d", offset, n, length)
	}
	return decodeBoundedSnappy(scratch)
}

func decodeBoundedSnappy(compressed []byte) ([]byte, error) {
	decodedLen, lenErr := snappy.DecodedLen(compressed)
	if lenErr != nil {
		return nil, fmt.Errorf("snappy decoded length: %w", lenErr)
	}
	if uint64(decodedLen) > shared.MaxMetadataSize { //nolint:gosec // safe: decodedLen is non-negative
		return nil, fmt.Errorf("snappy decoded size %d exceeds MaxMetadataSize %d", decodedLen, shared.MaxMetadataSize)
	}
	// NOTE-259: pass a pre-sized, unzeroed dst of exactly decodedLen. snappy.Decode reslices
	// dst to [:decodedLen] (since decodedLen <= len(dst)) and overwrites every byte, so the
	// memclr that snappy.Decode(nil, …)'s internal make([]byte, dLen) emits is pure waste.
	// The decoded bytes escape to r.cache so the buffer can't be pooled, but []byte is
	// pointer-free, so the unzeroed backing array is GC-safe before the full overwrite.
	return snappy.Decode(shared.MakeNoZeroBytes(decodedLen), compressed)
}

// parseV8ToCBlob reads, decompresses, and parses the V8 unified ToC blob.
// Returns a map of ToCKey → ToCEntry and the file's signal type.
func (r *Reader) parseV8ToCBlob() (map[shared.ToCKey]shared.ToCEntry, uint8, error) {
	if r.v8ToCLen == 0 {
		return make(map[shared.ToCKey]shared.ToCEntry), shared.SignalTypeTrace, nil
	}
	raw, err := r.cache.GetOrFetchV8TOC(r.fileID, func() ([]byte, error) {
		// NOTE-366: read into pooled scratch, decode, recycle the compressed buffer.
		return r.readRangeDecodeSnappy(r.v8ToCOffset, uint64(r.v8ToCLen), rw.DataTypeMetadata) //nolint:gosec
	})
	if err != nil {
		return nil, 0, fmt.Errorf("parseV8ToCBlob: %w", err)
	}

	if len(raw) < shared.ToCBlobHeaderSize {
		return nil, 0, fmt.Errorf("parseV8ToCBlob: blob too short: %d bytes", len(raw))
	}
	entryCount := binary.LittleEndian.Uint32(raw[0:])
	signalType := raw[4] // signal_type[1]; reserved[3] at raw[5:8]
	if signalType == 0 {
		signalType = shared.SignalTypeTrace
	}
	pos := shared.ToCBlobHeaderSize

	tocMap := make(map[shared.ToCKey]shared.ToCEntry, int(entryCount)) //nolint:gosec
	for i := range entryCount {
		e, n, parseErr := shared.UnmarshalToCEntry(raw[pos:])
		if parseErr != nil {
			return nil, 0, fmt.Errorf("parseV8ToCBlob: entry[%d]: %w", i, parseErr)
		}
		pos += n
		tocMap[e.Key] = e
	}
	return tocMap, signalType, nil
}

// fetchToCSection reads and snappy-decodes the section identified by key from the V8 ToC.
// Returns (nil, nil) when key is absent from r.tocMap (graceful degradation).
// Results are cached via r.cache.
func (r *Reader) fetchToCSection(key shared.ToCKey) ([]byte, error) {
	e, ok := r.tocMap[key]
	if !ok {
		return nil, nil
	}
	raw, err := r.cache.GetOrFetchV8Section(r.fileID, key.Type, key.SubType, key.Name, func() ([]byte, error) {
		// NOTE-366: read into pooled scratch, decode, recycle the compressed buffer.
		dec, decErr := r.readRangeDecodeSnappy(e.Offset, uint64(e.Length), rw.DataTypeMetadata) //nolint:gosec
		if decErr != nil {
			return nil, fmt.Errorf("fetchToCSection(%v): read/snappy: %w", key, decErr)
		}
		return dec, nil
	})
	return raw, err
}

// parseSectionsV8 initializes the V8 reader by:
// 1. Parsing the ToC blob to build r.tocMap.
// 2. Eagerly loading the block index (required by all block-access methods).
// 3. (NOTE-436) no intrinsic index — all columns are inner-block columns.
func (r *Reader) parseSectionsV8() error {
	tocMap, signalType, err := r.parseV8ToCBlob()
	if err != nil {
		return fmt.Errorf("parseSectionsV8: ToC: %w", err)
	}
	r.tocMap = tocMap
	r.signalType = signalType
	r.fileVersion = shared.VersionBlockV14 // block format is still V14; reuse block parser

	// Block index: eager (required by all block-access methods).
	blockIdxRaw, err := r.fetchToCSection(shared.ToCKey{
		Type:    shared.ToCTypeIndex,
		SubType: shared.ToCSubTypeBlockIndex,
	})
	if err != nil {
		return fmt.Errorf("parseSectionsV8: block_index: %w", err)
	}
	if len(blockIdxRaw) >= 4 {
		blockCount := int(binary.LittleEndian.Uint32(blockIdxRaw[0:]))
		metas, _, parseErr := parseBlockIndex(blockIdxRaw[4:], blockCount)
		if parseErr != nil {
			return fmt.Errorf("parseSectionsV8: block_index parse: %w", parseErr)
		}
		// V2 lean format unconditional: derive PageNum from byte offset (always page-aligned).
		for i := range metas {
			metas[i].PageNum = uint32(metas[i].Offset / 4096) //nolint:gosec // offset fits uint32
		}
		r.blockMetas = metas
	}

	// NOTE-436: v2 files have no IntrinsicTOC section — all columns are inner-block
	// columns. No intrinsic index is built.

	return nil
}

// ensureV8TSSection lazily loads the V8 timestamp index on first call.
func (r *Reader) ensureV8TSSection() error {
	r.v8TSOnce.Do(func() {
		raw, err := r.fetchToCSection(shared.ToCKey{Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeTS})
		if err != nil {
			r.v8TSErr = fmt.Errorf("ensureV8TSSection: %w", err)
			return
		}
		if len(raw) == 0 {
			return
		}
		rawEntries, tsCount, _, tsErr := parseTSIndex(raw)
		if tsErr != nil {
			r.v8TSErr = fmt.Errorf("ensureV8TSSection: parse: %w", tsErr)
			return
		}
		r.tsRaw = rawEntries
		r.tsCount = tsCount
	})
	return r.v8TSErr
}

// NOTE: ensureV8ColStatsSection removed (2026-06-29, in-file block pruning removal).
// Value index is now the authoritative source for pruning.

// NOTE: ColStats() and HasColStats() removed (2026-06-29, in-file block pruning removal).
// Value index is now the authoritative source for pruning.

// ensureV14TSSection lazily loads the V14 timestamp index section on first call.
// Populates r.tsRaw and r.tsCount so BlocksInTimeRange works.
func (r *Reader) ensureV14TSSection() error {
	return r.ensureV8TSSection()
}

// parseV5MetadataLazy reads the metadata section and eagerly parses:
//   - block index entries → r.blockMetas
//   - range column index byte ranges → r.rangeOffsets (lazy)
//   - trace block index → r.traceIndex
//
// parseV5MetadataLazy was the V3/V4/V5/V6 metadata parser.
// Removed 2026-06-12 when legacy footer support was dropped.
// The function body is gone; this stub preserves the call site in reader.go during transition.

// parseBlockIndexEntry parses a single block_index ToC entry from data starting at pos,
// returning the decoded BlockMeta and the new read position.
func parseBlockIndexEntry(data []byte, pos int) (shared.BlockMeta, int, error) {
	var meta shared.BlockMeta

	// offset[8] + length[8]
	if pos+16 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for offset/length")
	}

	meta.Offset = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.Length = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// kind[1]
	if pos+1 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for kind")
	}

	meta.Kind = shared.BlockKind(data[pos])
	pos++

	// span_count[4] + min_start[8] + max_start[8]
	if pos+20 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for span_count/timestamps")
	}

	meta.SpanCount = binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	meta.MinStart = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.MaxStart = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// V13: MinTraceID/MaxTraceID omitted from block index entries.

	return meta, pos, nil
}

// parseBlockIndex parses block_count block index entries from data.
func parseBlockIndex(data []byte, blockCount int) ([]shared.BlockMeta, int, error) {
	metas := make([]shared.BlockMeta, 0, blockCount)
	pos := 0

	for i := range blockCount {
		meta, newPos, err := parseBlockIndexEntry(data, pos)
		if err != nil {
			return nil, pos, fmt.Errorf("block[%d]: %w", i, err)
		}

		metas = append(metas, meta)
		pos = newPos
	}

	return metas, pos, nil
}

// parseTraceBlockIndex parses the trace block index section.
// (trace block index parsing removed with the TraceID/DFS index in #438.)
func (r *Reader) readRange(offset, length uint64, dt rw.DataType) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}

	buf := make([]byte, length)
	n, err := r.provider.ReadAt(buf, int64(offset), dt) //nolint:gosec // safe: offset is a file offset, fits in int64
	if err != nil {
		return nil, fmt.Errorf("readRange offset=%d length=%d: %w", offset, length, err)
	}

	if uint64(n) != length { //nolint:gosec // safe: n is bytes read, always non-negative
		return nil, fmt.Errorf("readRange offset=%d: short read %d/%d", offset, n, length)
	}

	return buf, nil
}

// ColMeta holds per-column metadata from an inner block without decoding column data.
type ColMeta struct {
	Name            string
	ColType         shared.ColumnType
	Kind            uint8 // encoding kind byte (byte 1 of decompressed blob: enc_version[0]+kind[1])
	CompressedBytes uint32
	UncompressedLen uint32
}

// ParseColMetas extracts column metadata from block bytes without decoding any column data.
func ParseColMetas(blockBytes []byte, _ shared.BlockMeta) ([]ColMeta, error) {
	if len(blockBytes) < int(shared.BlockHeaderV14Size) {
		return nil, fmt.Errorf("ParseColMetas: block too short (%d bytes)", len(blockBytes))
	}
	hdr, err := parseBlockHeader(blockBytes)
	if err != nil {
		return nil, fmt.Errorf("ParseColMetas: %w", err)
	}
	entries, _, err := parseColumnMetadataArray(
		blockBytes,
		int(shared.BlockHeaderV14Size),
		int(hdr.columnCount),
		hdr.version,
	)
	if err != nil {
		return nil, fmt.Errorf("ParseColMetas: metadata: %w", err)
	}
	out := make([]ColMeta, 0, len(entries))
	for _, e := range entries {
		cm := ColMeta{
			Name:            e.name,
			ColType:         e.colType,
			CompressedBytes: e.compressedLen,
			UncompressedLen: e.uncompressedLen,
		}
		// Peek kind byte: decompressed blob = enc_version[1] + kind[1] + ...
		if len(e.inlineData) >= 2 {
			cm.Kind = e.inlineData[1]
		} else if e.compressedLen >= 2 {
			blobEnd := int(e.dataOffset) + int(e.compressedLen)
			if blobEnd <= len(blockBytes) {
				if dec, derr := snappy.Decode(nil, blockBytes[e.dataOffset:blobEnd]); derr == nil && len(dec) >= 2 {
					cm.Kind = dec[1]
				}
			}
		}
		out = append(out, cm)
	}
	return out, nil
}
