package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/objectcache"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// parsedSketchCache caches fully parsed sketchIndex objects by fileID+"/sketch".
// Strong references: entries persist until Clear is called.
// SPEC-OC-003, NOTE-003 (reader NOTES.md)
var parsedSketchCache objectcache.Cache[sketchIndex]

// parsedSketchSummaryCache caches the fully built FileSketchSummary by fileID+"/sketch-summary".
// FileSketchSummary is expensive to build (TopK aggregation across all blocks) and
// was previously rebuilt on every query because it was only cached per-Reader (short-lived).
// Strong references: entries persist until Clear is called.
// SPEC-OC-003, NOTE-003 (reader NOTES.md)
var parsedSketchSummaryCache objectcache.Cache[FileSketchSummary]

// parsedIntrinsicCache caches fully decoded IntrinsicColumn objects by
// fileID+"/intrinsic/"+colName. Strong references: entries persist until Clear is called.
// SPEC-OC-003, NOTE-003 (reader NOTES.md)
var parsedIntrinsicCache objectcache.Cache[shared.IntrinsicColumn]

// parsedMetadataCache caches the fully parsed metadata result by fileID.
// Strong references: entries persist until Clear is called.
// SPEC-OC-003, NOTE-003 (reader NOTES.md)
var parsedMetadataCache objectcache.Cache[parsedMetadata]

// parsedIntrinsicTOCCache caches the parsed intrinsic TOC map by fileID+"/intrinsic/toc".
// Previously the TOC was re-decoded from bbolt on every NewReaderFromProvider call.
// SPEC-OC-003, NOTE-003 (reader NOTES.md)
var parsedIntrinsicTOCCache objectcache.Cache[intrinsicTOC]

// intrinsicTOC wraps the intrinsic column TOC map to give it stable pointer identity
// for objectcache.Cache. A named struct is more ergonomic than *map[K]V from the cache.
type intrinsicTOC struct {
	entries map[string]shared.IntrinsicColMeta
}

// parsedMetadata holds all parsed results from parseV5MetadataLazy.
type parsedMetadata struct {
	sketchIdx     *sketchIndex
	metadataBytes []byte
	blockMetas    []shared.BlockMeta
	rangeOffsets  map[string]rangeIndexMeta
	traceIndexRaw []byte
	tsRaw         []byte // zero-copy sub-slice of metadataBytes; see NOTE-PERF-TS
	fileBloomRaw  []byte // raw FileBloom section bytes; nil for old files
	tsCount       int
}

// ClearCaches resets all process-level caches. Intended for testing.
func ClearCaches() {
	parsedSketchCache.Clear()
	parsedSketchSummaryCache.Clear()
	parsedIntrinsicCache.Clear()
	parsedMetadataCache.Clear()
	parsedIntrinsicTOCCache.Clear()
}

// rangeIndexMeta records the byte range within metadataBytes for a
// range column index entry (lazy parsing).
type rangeIndexMeta struct {
	typ    shared.ColumnType
	offset int
	length int
}

// readFooter reads the footer from the end of the file.
// Supports v3 (22 bytes), v4 (34 bytes), v5 (46 bytes), v6 (58 bytes), and v7 (18 bytes, V14 section-directory format) footer formats.
// v4 adds intrinsicIndexOffset[8] + intrinsicIndexLen[4] after the v3 fields.
// v5 adds vectorIndexOffset[8] + vectorIndexLen[4] after the v4 fields.
// v6 adds compactTracesOffset[8] + compactTracesLen[4] after the v5 fields (split compact format).
// v7 is the V14 section-directory footer: magic[4]+version[2]+dir_offset[8]+dir_len[4] = 18 bytes.
//
// Detection strategy: try v7 first (magic check), then v6, then v5, then v4, then v3.
func (r *Reader) readFooter() error {
	if r.fileSize < int64(shared.FooterV7Size) {
		return fmt.Errorf("file too small for footer: %d bytes", r.fileSize)
	}

	cacheKey := r.fileID + "/footer"

	// Try V7 first (18 bytes): magic[4]+version[2]+dir_offset[8]+dir_len[4].
	// V14 files always end with this footer. The magic+version check makes detection reliable.
	isV7, err := r.tryReadFooterV7(cacheKey)
	if err != nil {
		return err
	}
	if isV7 {
		return nil
	}

	if r.fileSize < int64(shared.FooterV3Size) {
		return fmt.Errorf("file too small for v3/v4/v5/v6 footer: %d bytes", r.fileSize)
	}

	return r.readFooterLegacy(cacheKey)
}

// tryReadFooterV7 attempts to read a V7 footer from the last 18 bytes of the file.
// Returns true and populates r.footerVersion/r.v7DirOffset/r.v7DirLen on success.
func (r *Reader) tryReadFooterV7(cacheKey string) (bool, error) {
	off := r.fileSize - int64(shared.FooterV7Size)
	buf, err := r.cache.GetOrFetch(cacheKey+"/v7", func() ([]byte, error) {
		b := make([]byte, shared.FooterV7Size)
		n, readErr := r.provider.ReadAt(b, off, rw.DataTypeFooter)
		if readErr != nil {
			return nil, fmt.Errorf("readFooter: %w", readErr)
		}
		if n != int(shared.FooterV7Size) {
			return nil, fmt.Errorf("readFooter: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return false, fmt.Errorf("readFooter: %w", err)
	}
	magic := binary.LittleEndian.Uint32(buf[0:])
	ver := binary.LittleEndian.Uint16(buf[4:])
	if magic != shared.MagicNumber || ver != shared.FooterV7Version {
		return false, nil
	}
	r.footerVersion = shared.FooterV7Version
	r.v7DirOffset = binary.LittleEndian.Uint64(buf[6:])
	r.v7DirLen = binary.LittleEndian.Uint32(buf[14:])
	return true, nil
}

// readFooterLegacy handles V3/V4/V5/V6 footer detection for non-V7 files.
func (r *Reader) readFooterLegacy(cacheKey string) error {
	// Try v6 first: only possible if file is large enough.
	// V6 contains V5 at offset (FooterV6Size - FooterV5Size) = 12.
	if r.fileSize >= int64(shared.FooterV6Size) {
		if ok, err := r.tryReadFooterV6(cacheKey); err != nil {
			return err
		} else if ok {
			return nil
		}
	}

	// Try v5: only possible if file is large enough (and too small for v6).
	// V5 contains V4 at offset (FooterV5Size - FooterV4Size) = 12.
	if r.fileSize >= int64(shared.FooterV5Size) {
		if ok, err := r.tryReadFooterV5(cacheKey); err != nil {
			return err
		} else if ok {
			return nil
		}
	}

	// Try v4: only possible if file is large enough (and file is too small for v5).
	if r.fileSize >= int64(shared.FooterV4Size) {
		if ok, err := r.tryReadFooterV4(cacheKey); err != nil {
			return err
		} else if ok {
			return nil
		}
	}

	// Fall back to V3.
	return r.readFooterV3(cacheKey)
}

// applyLegacyFooterCommon sets the common Reader fields from a parsed legacy footer buffer.
// buf must start at the version[2] field (offset 0 = version, 2 = headerOffset, ...).
func (r *Reader) applyLegacyFooterCommon(buf []byte) error {
	r.footerFields.headerOffset = binary.LittleEndian.Uint64(buf[2:])
	r.footerFields.compactOffset = binary.LittleEndian.Uint64(buf[10:])
	r.footerFields.compactLen = binary.LittleEndian.Uint32(buf[18:])
	if r.footerFields.headerOffset == 0 {
		return fmt.Errorf("readFooter: header_offset is zero")
	}
	r.headerOffset = r.footerFields.headerOffset
	r.compactOffset = r.footerFields.compactOffset
	r.compactLen = r.footerFields.compactLen
	return nil
}

// tryReadFooterV6 attempts to detect and parse a V6 (or embedded V5) footer.
func (r *Reader) tryReadFooterV6(cacheKey string) (bool, error) {
	const v5InV6Off = int(shared.FooterV6Size - shared.FooterV5Size) // = 12
	off := r.fileSize - int64(shared.FooterV6Size)
	buf, err := r.cache.GetOrFetch(cacheKey+"/v6", func() ([]byte, error) {
		b := make([]byte, shared.FooterV6Size)
		n, readErr := r.provider.ReadAt(b, off, rw.DataTypeFooter)
		if readErr != nil {
			return nil, fmt.Errorf("readFooter: %w", readErr)
		}
		if n != int(shared.FooterV6Size) {
			return nil, fmt.Errorf("readFooter: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return false, fmt.Errorf("readFooter: %w", err)
	}
	v5buf := buf[v5InV6Off:]
	switch {
	case binary.LittleEndian.Uint16(buf[0:]) == shared.FooterV6Version:
		r.footerVersion = shared.FooterV6Version
		r.intrinsicIndexOffset = binary.LittleEndian.Uint64(buf[22:])
		r.intrinsicIndexLen = binary.LittleEndian.Uint32(buf[30:])
		r.vectorIndexOffset = binary.LittleEndian.Uint64(buf[34:])
		r.vectorIndexLen = binary.LittleEndian.Uint32(buf[42:])
		r.compactTracesOffset = binary.LittleEndian.Uint64(buf[46:])
		r.compactTracesLen = binary.LittleEndian.Uint32(buf[54:])
		return true, r.applyLegacyFooterCommon(buf)
	case binary.LittleEndian.Uint16(v5buf[0:]) == shared.FooterV5Version:
		// V5 footer is embedded at offset v5InV6Off in the V6 read buffer.
		r.footerVersion = shared.FooterV5Version
		r.intrinsicIndexOffset = binary.LittleEndian.Uint64(v5buf[22:])
		r.intrinsicIndexLen = binary.LittleEndian.Uint32(v5buf[30:])
		r.vectorIndexOffset = binary.LittleEndian.Uint64(v5buf[34:])
		r.vectorIndexLen = binary.LittleEndian.Uint32(v5buf[42:])
		return true, r.applyLegacyFooterCommon(v5buf)
	}
	return false, nil
}

// tryReadFooterV5 attempts to detect and parse a V5 (or embedded V4) footer.
func (r *Reader) tryReadFooterV5(cacheKey string) (bool, error) {
	const v4InV5Off = int(shared.FooterV5Size - shared.FooterV4Size) // = 12
	off := r.fileSize - int64(shared.FooterV5Size)
	buf, err := r.cache.GetOrFetch(cacheKey+"/v5", func() ([]byte, error) {
		b := make([]byte, shared.FooterV5Size)
		n, readErr := r.provider.ReadAt(b, off, rw.DataTypeFooter)
		if readErr != nil {
			return nil, fmt.Errorf("readFooter: %w", readErr)
		}
		if n != int(shared.FooterV5Size) {
			return nil, fmt.Errorf("readFooter: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return false, fmt.Errorf("readFooter: %w", err)
	}
	v4buf := buf[v4InV5Off:]
	switch {
	case binary.LittleEndian.Uint16(buf[0:]) == shared.FooterV5Version:
		r.footerVersion = shared.FooterV5Version
		r.intrinsicIndexOffset = binary.LittleEndian.Uint64(buf[22:])
		r.intrinsicIndexLen = binary.LittleEndian.Uint32(buf[30:])
		r.vectorIndexOffset = binary.LittleEndian.Uint64(buf[34:])
		r.vectorIndexLen = binary.LittleEndian.Uint32(buf[42:])
		return true, r.applyLegacyFooterCommon(buf)
	case binary.LittleEndian.Uint16(v4buf[0:]) == shared.FooterV4Version:
		// V4 footer is embedded at offset v4InV5Off in the V5 read buffer.
		r.footerVersion = shared.FooterV4Version
		r.intrinsicIndexOffset = binary.LittleEndian.Uint64(v4buf[22:])
		r.intrinsicIndexLen = binary.LittleEndian.Uint32(v4buf[30:])
		return true, r.applyLegacyFooterCommon(v4buf)
	}
	return false, nil
}

// tryReadFooterV4 attempts to detect and parse a standalone V4 footer.
func (r *Reader) tryReadFooterV4(cacheKey string) (bool, error) {
	off := r.fileSize - int64(shared.FooterV4Size)
	buf, err := r.cache.GetOrFetch(cacheKey+"/v4", func() ([]byte, error) {
		b := make([]byte, shared.FooterV4Size)
		n, readErr := r.provider.ReadAt(b, off, rw.DataTypeFooter)
		if readErr != nil {
			return nil, fmt.Errorf("readFooter: %w", readErr)
		}
		if n != int(shared.FooterV4Size) {
			return nil, fmt.Errorf("readFooter: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return false, fmt.Errorf("readFooter: %w", err)
	}
	if binary.LittleEndian.Uint16(buf[0:]) != shared.FooterV4Version {
		return false, nil
	}
	r.footerVersion = shared.FooterV4Version
	r.intrinsicIndexOffset = binary.LittleEndian.Uint64(buf[22:])
	r.intrinsicIndexLen = binary.LittleEndian.Uint32(buf[30:])
	return true, r.applyLegacyFooterCommon(buf)
}

// readFooterV3 reads and parses a V3 footer (the minimum supported format).
func (r *Reader) readFooterV3(cacheKey string) error {
	off := r.fileSize - int64(shared.FooterV3Size)
	buf, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		b := make([]byte, shared.FooterV3Size)
		n, readErr := r.provider.ReadAt(b, off, rw.DataTypeFooter)
		if readErr != nil {
			return nil, fmt.Errorf("readFooter: %w", readErr)
		}
		if n != int(shared.FooterV3Size) {
			return nil, fmt.Errorf("readFooter: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return fmt.Errorf("readFooter: %w", err)
	}
	ver := binary.LittleEndian.Uint16(buf[0:])
	if ver != shared.FooterV3Version {
		return fmt.Errorf("readFooter: unsupported footer version %d", ver)
	}
	r.footerVersion = ver
	return r.applyLegacyFooterCommon(buf)
}

// readSectionDirectory reads and decodes the V14 section directory.
// Called after readFooter() when footerVersion == FooterV7Version.
// Returns a SectionDirectory with TypeEntries and NameEntries populated.
func (r *Reader) readSectionDirectory() (shared.SectionDirectory, error) {
	if r.v7DirLen == 0 {
		return shared.SectionDirectory{
			TypeEntries: make(map[uint8]shared.DirEntryType),
			NameEntries: make(map[string]shared.DirEntryName),
		}, nil
	}

	compressed, err := r.readRange(r.v7DirOffset, uint64(r.v7DirLen), rw.DataTypeMetadata) //nolint:gosec // safe: v7DirLen is a file offset length, fits in uint64
	if err != nil {
		return shared.SectionDirectory{}, fmt.Errorf("readSectionDirectory: read: %w", err)
	}

	raw, err := decodeBoundedSnappy(compressed)
	if err != nil {
		return shared.SectionDirectory{}, fmt.Errorf("readSectionDirectory: snappy decode: %w", err)
	}

	if len(raw) < 4 {
		return shared.SectionDirectory{}, fmt.Errorf("readSectionDirectory: too short for entry_count: %d bytes", len(raw))
	}

	entryCount := binary.LittleEndian.Uint32(raw[0:])
	pos := 4

	dir := shared.SectionDirectory{
		TypeEntries: make(map[uint8]shared.DirEntryType, int(entryCount)), //nolint:gosec // safe: entryCount is a small count
		NameEntries: make(map[string]shared.DirEntryName),
	}

	for i := range entryCount {
		if pos >= len(raw) {
			return shared.SectionDirectory{}, fmt.Errorf("readSectionDirectory: entry[%d]: truncated", i)
		}
		kind := raw[pos]
		pos++
		switch kind {
		case shared.DirEntryKindType:
			e, parseErr := shared.UnmarshalDirEntryType(raw[pos:])
			if parseErr != nil {
				return shared.SectionDirectory{}, fmt.Errorf("readSectionDirectory: entry[%d] type-keyed: %w", i, parseErr)
			}
			pos += shared.DirEntryTypeWireSize - 1 // -1 for the kind byte already consumed
			dir.TypeEntries[e.SectionType] = e
		case shared.DirEntryKindName:
			e, n, parseErr := shared.UnmarshalDirEntryName(raw[pos:])
			if parseErr != nil {
				return shared.SectionDirectory{}, fmt.Errorf("readSectionDirectory: entry[%d] name-keyed: %w", i, parseErr)
			}
			pos += n
			dir.NameEntries[e.Name] = e
		case shared.DirEntryKindSignal:
			// DirEntryKindSignal: signal_type[1] — 1 byte payload after the kind byte.
			if pos >= len(raw) {
				return shared.SectionDirectory{}, fmt.Errorf("readSectionDirectory: entry[%d] signal-kind: truncated", i)
			}
			dir.SignalType = raw[pos]
			pos++
		default:
			return shared.SectionDirectory{}, fmt.Errorf("readSectionDirectory: entry[%d]: unknown kind 0x%02x", i, kind)
		}
	}

	return dir, nil
}

// decodeBoundedSnappy snappy-decodes compressed, rejecting inputs whose
// decoded size would exceed MaxMetadataSize (decompression-bomb guard).
func decodeBoundedSnappy(compressed []byte) ([]byte, error) {
	decodedLen, lenErr := snappy.DecodedLen(compressed)
	if lenErr != nil {
		return nil, fmt.Errorf("snappy decoded length: %w", lenErr)
	}
	if uint64(decodedLen) > shared.MaxMetadataSize { //nolint:gosec // safe: decodedLen is non-negative
		return nil, fmt.Errorf("snappy decoded size %d exceeds MaxMetadataSize %d", decodedLen, shared.MaxMetadataSize)
	}
	return snappy.Decode(nil, compressed)
}

// parseSectionsLazyV14 initializes the V14 reader by reading only the block index section
// at open time. All other sections (range index, trace index, TS index, sketch index,
// file bloom) are deferred: each is read and snappy-decoded on first access via the
// corresponding ensureV14*Section method.
//
// NOTE: Only SectionBlockIndex is mandatory at open time — every block-access method
// (ReadBlockRaw, GetBlockWithBytes, ReadBlocks, …) requires r.blockMetas to be populated.
// The remaining sections are consulted only by specific query paths, so deferring their
// I/O avoids reading megabytes of metadata that a given query may never use.
//
// NOTE: Was previously named "parseSectionsLazyV14" but did not actually defer per-section I/O.
func (r *Reader) parseSectionsLazyV14() error {
	// Signal type is stored in the section directory as a DirEntryKindSignal entry.
	// Default to trace if not present (for backward compatibility with old V14 files).
	if r.sectionDir.SignalType != 0 {
		r.signalType = r.sectionDir.SignalType
	} else {
		r.signalType = shared.SignalTypeTrace
	}
	r.fileVersion = shared.VersionBlockV14

	// SectionBlockIndex (0x01): block_count[4] + entries.
	// Read eagerly — blockMetas is required by all block-access methods.
	blockIdxRaw, err := r.readV14Section(shared.SectionBlockIndex)
	if err != nil {
		return fmt.Errorf("parseSectionsLazyV14: block_index: %w", err)
	}
	if len(blockIdxRaw) >= 4 {
		blockCount := int(binary.LittleEndian.Uint32(blockIdxRaw[0:]))
		metas, _, parseErr := parseBlockIndex(blockIdxRaw[4:], blockCount)
		if parseErr != nil {
			return fmt.Errorf("parseSectionsLazyV14: block_index parse: %w", parseErr)
		}
		r.blockMetas = metas
	}

	// SectionRangeIndex (0x02), SectionTraceIndex (0x03), SectionTSIndex (0x04),
	// SectionSketchIndex (0x05), SectionFileBloom (0x06): deferred to first access.
	// See ensureV14RangeSection, ensureV14TraceSection, ensureV14TSSection,
	// ensureV14SketchSection, ensureV14BloomSection.

	// Name-keyed entries: one per intrinsic column blob.
	// Each blob on disk is snappy-compressed; the reader snappy-decodes it in GetIntrinsicColumn.
	// We peek format and count from each blob here so the executor fast path can dispatch on
	// meta.Format without a full decode (SPEC-V14-001: Format/Count must be populated at open time).
	if len(r.sectionDir.NameEntries) > 0 {
		r.intrinsicIndex = make(map[string]shared.IntrinsicColMeta, len(r.sectionDir.NameEntries))
		for name, e := range r.sectionDir.NameEntries {
			meta := shared.IntrinsicColMeta{
				Name:   name,
				Offset: e.Offset,
				Length: e.CompressedLen,
			}
			// Read the compressed blob to peek format/type/count; errors are non-fatal (fields stay 0).
			// PeekIntrinsicBlobHeader reads the first byte to distinguish paged blobs (0x02 sentinel,
			// raw pages) from non-paged blobs (snappy-compressed).
			if blob, readErr := r.readRange(e.Offset, uint64(e.CompressedLen), rw.DataTypeMetadata); readErr == nil { //nolint:gosec
				if f, ct, cnt, peekErr := shared.PeekIntrinsicBlobHeader(blob); peekErr == nil && f != 0 {
					meta.Format = f
					meta.Type = ct
					meta.Count = cnt
				}
			}
			r.intrinsicIndex[name] = meta
		}
	}

	return nil
}

// readV14Section reads and snappy-decodes one type-keyed section from the section directory.
// Returns (nil, nil) if the section is not present in the directory.
func (r *Reader) readV14Section(sectionType uint8) ([]byte, error) {
	e, ok := r.sectionDir.TypeEntries[sectionType]
	if !ok {
		return nil, nil
	}
	compressed, err := r.readRange(e.Offset, uint64(e.CompressedLen), rw.DataTypeMetadata) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("section 0x%02X read: %w", sectionType, err)
	}
	raw, err := decodeBoundedSnappy(compressed)
	if err != nil {
		return nil, fmt.Errorf("section 0x%02X snappy: %w", sectionType, err)
	}
	return raw, nil
}

// ensureV14RangeSection lazily loads the V14 range index section on first call.
// Populates r.rangeOffsets and r.metadataBytes (which ensureRangeColumnParsed indexes into).
// No-op for non-V14 files (rangeOffsets is already populated by parseV5MetadataLazy).
func (r *Reader) ensureV14RangeSection() error {
	if r.fileVersion != shared.VersionBlockV14 {
		return nil
	}
	r.v14RangeOnce.Do(func() {
		rangeIdxRaw, err := r.readV14Section(shared.SectionRangeIndex)
		if err != nil {
			r.v14RangeErr = fmt.Errorf("ensureV14RangeSection: %w", err)
			return
		}
		if len(rangeIdxRaw) < 4 {
			return
		}
		colCount := int(binary.LittleEndian.Uint32(rangeIdxRaw[0:]))
		offsets, _, parseErr := scanRangeIndexOffsets(rangeIdxRaw[4:], colCount)
		if parseErr != nil {
			r.v14RangeErr = fmt.Errorf("ensureV14RangeSection: parse: %w", parseErr)
			return
		}
		// Adjust offsets to be absolute within rangeIdxRaw.
		for k, v := range offsets {
			v.offset += 4
			offsets[k] = v
		}
		r.rangeOffsets = offsets
		// Keep a reference so lazy range parsing (ensureRangeColumnParsed) can index into it.
		r.metadataBytes = rangeIdxRaw
	})
	return r.v14RangeErr
}

// ensureV14TraceSection lazily loads the V14 trace index section on first call.
// Populates r.compactParsed so TraceEntries, BlocksForTraceID, and TraceCount work.
// No-op for non-V14 files (compactParsed is populated by ensureCompactIndexParsed).
func (r *Reader) ensureV14TraceSection() error {
	if r.fileVersion != shared.VersionBlockV14 {
		return nil
	}
	r.v14TraceOnce.Do(func() {
		traceIdxRaw, err := r.readV14Section(shared.SectionTraceIndex)
		if err != nil {
			r.v14TraceErr = fmt.Errorf("ensureV14TraceSection: %w", err)
			return
		}
		if len(traceIdxRaw) == 0 {
			return
		}
		if parseErr := r.parseCompactIndexBytesV14(traceIdxRaw); parseErr != nil {
			r.v14TraceErr = fmt.Errorf("ensureV14TraceSection: parse: %w", parseErr)
		}
	})
	return r.v14TraceErr
}

// ensureV14TSSection lazily loads the V14 timestamp index section on first call.
// Populates r.tsRaw and r.tsCount so BlocksInTimeRange works.
// No-op for non-V14 files (tsRaw/tsCount are populated by parseV5MetadataLazy).
func (r *Reader) ensureV14TSSection() error {
	if r.fileVersion != shared.VersionBlockV14 {
		return nil
	}
	r.v14TSOnce.Do(func() {
		tsRaw, err := r.readV14Section(shared.SectionTSIndex)
		if err != nil {
			r.v14TSErr = fmt.Errorf("ensureV14TSSection: %w", err)
			return
		}
		if len(tsRaw) == 0 {
			return
		}
		rawEntries, tsCount, _, tsErr := parseTSIndex(tsRaw)
		if tsErr != nil {
			r.v14TSErr = fmt.Errorf("ensureV14TSSection: parse: %w", tsErr)
			return
		}
		r.tsRaw = rawEntries
		r.tsCount = tsCount
	})
	return r.v14TSErr
}

// ensureV14SketchSection lazily loads the V14 sketch index section on first call.
// Populates r.sketchIdx so ColumnSketch and FileSketchSummary work.
// No-op for non-V14 files (sketchIdx is populated by parseV5MetadataLazy).
func (r *Reader) ensureV14SketchSection() error {
	if r.fileVersion != shared.VersionBlockV14 {
		return nil
	}
	r.v14SketchOnce.Do(func() {
		sketchRaw, err := r.readV14Section(shared.SectionSketchIndex)
		if err != nil {
			r.v14SketchErr = fmt.Errorf("ensureV14SketchSection: %w", err)
			return
		}
		if len(sketchRaw) == 0 {
			return
		}
		if _, skErr := r.parseAndCacheSketchSection(sketchRaw); skErr != nil {
			r.v14SketchErr = fmt.Errorf("ensureV14SketchSection: parse: %w", skErr)
		}
	})
	return r.v14SketchErr
}

// ensureV14BloomSection lazily loads the V14 file bloom section on first call.
// Populates r.fileBloomRaw and r.fileBloomParsed so FileBloom and FileBloomRaw work.
// No-op for non-V14 files (fileBloomRaw/fileBloomParsed are populated by parseV5MetadataLazy).
func (r *Reader) ensureV14BloomSection() error {
	if r.fileVersion != shared.VersionBlockV14 {
		return nil
	}
	r.v14BloomOnce.Do(func() {
		bloomRaw, err := r.readV14Section(shared.SectionFileBloom)
		if err != nil {
			r.v14BloomErr = fmt.Errorf("ensureV14BloomSection: %w", err)
			return
		}
		if len(bloomRaw) == 0 {
			return
		}
		fb, _, fbErr := parseFileBloomSection(bloomRaw)
		if fbErr != nil {
			r.v14BloomErr = fmt.Errorf("ensureV14BloomSection: parse: %w", fbErr)
			return
		}
		if fb != nil {
			r.fileBloomRaw = bloomRaw
			r.fileBloomParsed = fb
		}
	})
	return r.v14BloomErr
}

// readHeader reads the 22-byte file header at footer.headerOffset.
// Layout: magic[4] + version[1] + metadataOffset[8] + metadataLen[8] + signalType[1].
func (r *Reader) readHeader() error {
	const headerSize = 22
	cacheKey := r.fileID + "/header"

	buf, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		b := make([]byte, headerSize)
		n, readErr := r.provider.ReadAt(
			b,
			int64(r.headerOffset), //nolint:gosec // safe: headerOffset is a file offset, fits in int64
			rw.DataTypeHeader,
		)
		if readErr != nil {
			return nil, fmt.Errorf("readHeader: %w", readErr)
		}
		if n != headerSize {
			return nil, fmt.Errorf("readHeader: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return fmt.Errorf("readHeader: %w", err)
	}

	magic := binary.LittleEndian.Uint32(buf[0:])
	if magic != shared.MagicNumber {
		return fmt.Errorf("readHeader: bad magic 0x%08X", magic)
	}

	version := buf[4] //nolint:gosec // safe: buf is headerSize (22) bytes, validated by short-read check above
	if version != shared.VersionV13 {
		return fmt.Errorf("readHeader: unsupported version %d (V13 required)", version)
	}

	r.fileVersion = version
	r.metadataOffset = binary.LittleEndian.Uint64(buf[5:])
	r.metadataLen = binary.LittleEndian.Uint64(buf[13:])
	r.signalType = buf[21]

	return nil
}

// parseV5MetadataLazy reads the metadata section and eagerly parses:
//   - block index entries → r.blockMetas
//   - range column index byte ranges → r.rangeOffsets (lazy)
//   - trace block index → r.traceIndex
func (r *Reader) parseV5MetadataLazy() error {
	if r.metadataLen == 0 {
		return fmt.Errorf("parseMetadata: metadata_len is zero")
	}

	if r.metadataLen > shared.MaxMetadataSize {
		return fmt.Errorf("parseMetadata: metadata too large: %d bytes", r.metadataLen)
	}

	// Check process-level cache first — avoids re-reading ~45 MB from bbolt
	// and re-parsing metadata on every Reader creation for the same file.
	if r.fileID != "" {
		if pm := parsedMetadataCache.Get(r.fileID); pm != nil {
			r.metaPin = pm // keep weak cache entry alive for lifetime of this Reader
			r.metadataBytes = pm.metadataBytes
			r.blockMetas = pm.blockMetas
			r.rangeOffsets = pm.rangeOffsets
			r.traceIndexRaw = pm.traceIndexRaw
			r.tsRaw = pm.tsRaw
			r.tsCount = pm.tsCount
			r.sketchIdx = pm.sketchIdx
			r.fileBloomRaw = pm.fileBloomRaw
			return nil
		}
	}

	// NOTE-PERF: We cache the *decompressed* metadata bytes under a distinct key
	// ("/metadata/dec") so snappy.Decode runs only on a cache miss, not on every
	// Reader creation.
	cacheKey := r.fileID + "/metadata/dec"
	data, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		compressed, readErr := r.readRange(r.metadataOffset, r.metadataLen, rw.DataTypeMetadata)
		if readErr != nil {
			return nil, readErr
		}
		// Guard against decompression bombs before allocating the decode buffer.
		decodedLen, lenErr := snappy.DecodedLen(compressed)
		if lenErr != nil {
			return nil, fmt.Errorf("snappy decoded length: %w", lenErr)
		}
		if uint64(decodedLen) > shared.MaxMetadataSize { //nolint:gosec // safe: decodedLen is non-negative
			return nil, fmt.Errorf(
				"snappy decoded size %d exceeds MaxMetadataSize %d",
				decodedLen, shared.MaxMetadataSize,
			)
		}
		return snappy.Decode(nil, compressed)
	})
	if err != nil {
		return fmt.Errorf("parseMetadata: %w", err)
	}

	r.metadataBytes = data
	pos := 0

	// Block index: block_count[4] + entries.
	if pos+4 > len(data) {
		return fmt.Errorf("parseMetadata: block_index: short for block_count")
	}

	blockCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	metas, newPos, err := parseBlockIndex(data[pos:pos+len(data)-pos], blockCount)
	if err != nil {
		return fmt.Errorf("parseMetadata: block_index: %w", err)
	}

	actualConsumed := newPos
	pos += actualConsumed
	r.blockMetas = metas

	// Range index: range_count[4] + range_count entries.
	if pos+4 > len(data) {
		return fmt.Errorf("parseMetadata: range_index: short for range_count")
	}

	dedCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	rangeStart := pos
	offsets, newPos, err := scanRangeIndexOffsets(data[pos:], dedCount)
	if err != nil {
		return fmt.Errorf("parseMetadata: range_index: %w", err)
	}

	// Adjust offsets to be absolute within data.
	for k, v := range offsets {
		v.offset += rangeStart
		offsets[k] = v
	}

	pos += newPos
	r.rangeOffsets = offsets

	// Column index: block_count × col_count (always 0 in new files; skip without allocating).
	newPos, err = skipColumnIndex(data[pos:], blockCount)
	if err != nil {
		return fmt.Errorf("parseMetadata: column_index: %w", err)
	}

	pos += newPos

	// Trace block index — parsed lazily on first access (search queries never use it).
	// Store raw bytes and skip over the section to reach TS and sketch sections.
	consumed, err := skipTraceBlockIndex(data[pos:])
	if err != nil {
		return fmt.Errorf("parseMetadata: trace_index: %w", err)
	}
	if consumed > 0 {
		r.traceIndexRaw = data[pos : pos+consumed]
	}
	pos += consumed

	// TS index section — optional, present in files written after 2026-03-02.
	// parseTSIndex returns (nil, 0, 0, nil) for old files, enabling graceful degradation.
	if pos < len(data) {
		tsRaw, tsCount, tsConsumed, tsErr := parseTSIndex(data[pos:])
		if tsErr != nil {
			return fmt.Errorf("parseMetadata: ts_index: %w", tsErr)
		}
		r.tsRaw = tsRaw
		r.tsCount = tsCount
		pos += tsConsumed
	}

	sketchConsumed, skErr := r.parseAndCacheSketchSection(data[pos:])
	if skErr != nil {
		return skErr
	}
	pos += sketchConsumed

	// FileBloom section — optional, present after sketch index (always the last section).
	// NOTE-45: graceful degradation — old files without this section parse cleanly.
	if pos < len(data) {
		fb, fbConsumed, fbErr := parseFileBloomSection(data[pos:])
		if fbErr != nil {
			return fmt.Errorf("parseMetadata: file_bloom: %w", fbErr)
		}
		if fb != nil {
			clone := make([]byte, fbConsumed)
			copy(clone, data[pos:pos+fbConsumed])
			r.fileBloomRaw = clone
			r.fileBloomParsed = fb
		}
	}

	// Store in process-level cache for subsequent Reader creations on the same file.
	if r.fileID != "" {
		pm := &parsedMetadata{
			metadataBytes: r.metadataBytes,
			blockMetas:    r.blockMetas,
			rangeOffsets:  r.rangeOffsets,
			traceIndexRaw: r.traceIndexRaw,
			tsRaw:         r.tsRaw,
			tsCount:       r.tsCount,
			sketchIdx:     r.sketchIdx,
			fileBloomRaw:  r.fileBloomRaw,
		}
		if err := parsedMetadataCache.Put(r.fileID, pm); err != nil {
			return fmt.Errorf("parseV5MetadataLazy: cache: %w", err)
		}
		r.metaPin = pm // keep weak cache entry alive for lifetime of this Reader
	}
	return nil
}

// parseAndCacheSketchSection parses the sketch index section from data, caches the result,
// and returns the number of bytes consumed. Returns (0, nil) when no sketch section is present.
func (r *Reader) parseAndCacheSketchSection(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if r.fileID != "" {
		skCacheKey := r.fileID + "/sketch"
		if cached := parsedSketchCache.Get(skCacheKey); cached != nil {
			r.sketchIdx = cached
			// Sketch is cached but we don't know how many bytes it consumed.
			// Re-scan to advance pos past the sketch section so we can find FileBloom.
			_, consumed, skErr := parseSketchIndexSection(data)
			if skErr != nil {
				return 0, fmt.Errorf("parseMetadata: sketch_index (cache hit rescan): %w", skErr)
			}
			return consumed, nil
		}
	}
	sketches, consumed, skErr := parseSketchIndexSection(data)
	if skErr != nil {
		return 0, fmt.Errorf("parseMetadata: sketch_index: %w", skErr)
	}
	r.sketchIdx = sketches
	if sketches != nil && r.fileID != "" {
		if err := parsedSketchCache.Put(r.fileID+"/sketch", sketches); err != nil {
			return 0, fmt.Errorf("parseAndCacheSketchSection: cache: %w", err)
		}
	}
	return consumed, nil
}

// parseBlockIndexEntry parses one V13 block index entry.
// Returns the entry and new position.
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

// skipColumnIndex advances pos past the column index section without allocating.
// For new files, each block has col_count=0 (4 bytes per block).
// For old files, it correctly skips any existing entries.
func skipColumnIndex(data []byte, blockCount int) (int, error) {
	pos := 0

	for b := range blockCount {
		if pos+4 > len(data) {
			return pos, fmt.Errorf("column_index block[%d]: short for col_count", b)
		}

		colCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		for c := range colCount {
			if pos+2 > len(data) {
				return pos, fmt.Errorf("column_index block[%d] col[%d]: short for name_len", b, c)
			}

			nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
			advance := 2 + nameLen + 8 // name_len[2] + name + offset[4] + length[4]
			if advance < 0 || pos+advance > len(data) {
				return pos, fmt.Errorf("column_index block[%d] col[%d]: short for name/offset/length", b, c)
			}

			pos += advance
		}
	}

	return pos, nil
}

// parseTraceBlockIndex parses the trace block index section.
// Supports fmt_version 0x01 (v1: with per-block span indices, discarded on read)
// and fmt_version 0x02 (v2: block IDs only).
// Returns the parsed map and bytes consumed.
func parseTraceBlockIndex(data []byte) (map[[16]byte][]uint16, int, error) {
	if len(data) < 5 {
		return nil, 0, nil
	}

	fmtVersion := data[0]
	if fmtVersion != shared.TraceIndexFmtVersion && fmtVersion != shared.TraceIndexFmtVersion2 {
		return nil, 0, fmt.Errorf("trace_index: unsupported fmt_version %d", fmtVersion)
	}

	traceCount := int(binary.LittleEndian.Uint32(data[1:]))
	pos := 5

	result := make(map[[16]byte][]uint16, traceCount)

	for t := range traceCount {
		if pos+18 > len(data) {
			return nil, pos, fmt.Errorf("trace_index: trace[%d]: short for trace_id+block_count", t)
		}

		var tid [16]byte
		copy(tid[:], data[pos:pos+16])
		pos += 16

		blockRefCount := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		blockIDs := make([]uint16, 0, blockRefCount)

		if fmtVersion == shared.TraceIndexFmtVersion {
			// v1: block_id[2] + span_count[2] + span_indices[N×2] — discard span indices.
			for b := range blockRefCount {
				if pos+4 > len(data) {
					return nil, pos, fmt.Errorf("trace_index: trace[%d] block[%d]: short for block_id+span_count", t, b)
				}
				blockID := binary.LittleEndian.Uint16(data[pos:])
				pos += 2
				spanCount := int(binary.LittleEndian.Uint16(data[pos:]))
				pos += 2
				if pos+spanCount*2 > len(data) {
					return nil, pos, fmt.Errorf(
						"trace_index: trace[%d] block[%d]: short for span_indices (%d × 2 bytes)",
						t, b, spanCount,
					)
				}
				pos += spanCount * 2
				blockIDs = append(blockIDs, blockID)
			}
		} else {
			// v2: block_id[2] only.
			for b := range blockRefCount {
				if pos+2 > len(data) {
					return nil, pos, fmt.Errorf("trace_index: trace[%d] block[%d]: short for block_id", t, b)
				}
				blockIDs = append(blockIDs, binary.LittleEndian.Uint16(data[pos:]))
				pos += 2
			}
		}

		result[tid] = blockIDs
	}

	return result, pos, nil
}

// skipTraceBlockIndex advances past a trace block index section without building
// the map. This avoids O(N) allocations for search queries that never use the index.
// Returns (consumed, nil) on success; (0, nil) for empty/missing sections.
func skipTraceBlockIndex(data []byte) (int, error) {
	if len(data) < 5 {
		// Treat short sections as empty/missing rather than corrupt. The caller
		// (parseV5MetadataLazy) handles 0-consumed gracefully by skipping the section.
		// A truly corrupt trace index would be caught by fmtVersion validation below.
		return 0, nil
	}
	fmtVersion := data[0]
	if fmtVersion != shared.TraceIndexFmtVersion && fmtVersion != shared.TraceIndexFmtVersion2 {
		return 0, fmt.Errorf("trace_index: unsupported fmt_version %d", fmtVersion)
	}
	traceCount := int(binary.LittleEndian.Uint32(data[1:]))
	pos := 5
	for t := range traceCount {
		if pos+18 > len(data) {
			return pos, fmt.Errorf("trace_index: trace[%d]: short for trace_id+block_count", t)
		}
		pos += 16 // trace_id
		blockRefCount := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if fmtVersion == shared.TraceIndexFmtVersion {
			// v1: block_id[2] + span_count[2] + span_indices[N×2]
			for b := range blockRefCount {
				if pos+4 > len(data) {
					return pos, fmt.Errorf("trace_index: trace[%d] block[%d]: short", t, b)
				}
				spanCount := int(binary.LittleEndian.Uint16(data[pos+2:]))
				pos += 4 + spanCount*2
			}
		} else {
			// v2: block_id[2] only
			pos += blockRefCount * 2
		}
	}
	return pos, nil
}

// scanRangeIndexOffsets scans the range index data recording byte offset+length
// for each column entry WITHOUT parsing values.
// data is the slice starting immediately after range_count.
// Returns the offset map and bytes consumed.
func scanRangeIndexOffsets(data []byte, dedCount int) (map[string]rangeIndexMeta, int, error) {
	offsets := make(map[string]rangeIndexMeta, dedCount)
	pos := 0

	for i := range dedCount {
		entryStart := pos

		// name_len[2] + name
		if pos+2 > len(data) {
			return nil, pos, fmt.Errorf("range[%d]: short for name_len", i)
		}

		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+nameLen > len(data) {
			return nil, pos, fmt.Errorf("range[%d]: short for name", i)
		}

		name := string(data[pos : pos+nameLen])
		pos += nameLen

		// column_type[1]
		if pos+1 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for type", i, name)
		}

		colType := shared.ColumnType(data[pos])
		pos++

		// Bucket metadata always present: bucket_min[8] + bucket_max[8] + boundary_count[4]
		if pos+20 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for bucket header", i, name)
		}

		pos += 16 // min + max
		boundaryCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		// boundaries[boundary_count × 8]
		if pos+boundaryCount*8 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for boundaries", i, name)
		}

		pos += boundaryCount * 8

		// typed_count[4] + typed boundaries
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for typed_count", i, name)
		}

		typedCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		newPos, err := skipTypedBoundaries(data, pos, colType, typedCount)
		if err != nil {
			return nil, pos, fmt.Errorf("range[%d] %q: typed boundaries: %w", i, name, err)
		}

		pos = newPos

		// value_count[4]
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for value_count", i, name)
		}

		valueCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		// Skip value entries to find end.
		for v := range valueCount {
			newPos, err := skipRangeValueEntry(data, pos, colType)
			if err != nil {
				return nil, pos, fmt.Errorf("range[%d] %q value[%d]: %w", i, name, v, err)
			}

			pos = newPos
		}

		offsets[name] = rangeIndexMeta{
			typ:    colType,
			offset: entryStart,
			length: pos - entryStart,
		}
	}

	return offsets, pos, nil
}

// skipTypedBoundaries skips typed boundary data based on column type.
func skipTypedBoundaries(data []byte, pos int, colType shared.ColumnType, count int) (int, error) {
	switch colType {
	case shared.ColumnTypeRangeFloat64:
		// count × float64_bits(8)
		need := count * 8
		if pos+need > len(data) {
			return pos, fmt.Errorf("typed boundaries(float64): need %d bytes at pos %d", need, pos)
		}

		return pos + need, nil

	case shared.ColumnTypeRangeString:
		for i := range count {
			if pos+4 > len(data) {
				return pos, fmt.Errorf("typed boundaries(string)[%d]: short for len", i)
			}

			sLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+sLen > len(data) {
				return pos, fmt.Errorf("typed boundaries(string)[%d]: short for data", i)
			}

			pos += sLen
		}

		return pos, nil

	case shared.ColumnTypeRangeBytes:
		for i := range count {
			if pos+4 > len(data) {
				return pos, fmt.Errorf("typed boundaries(bytes)[%d]: short for len", i)
			}

			bLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+bLen > len(data) {
				return pos, fmt.Errorf("typed boundaries(bytes)[%d]: short for data", i)
			}

			pos += bLen
		}

		return pos, nil

	default:
		// RangeInt64/RangeUint64/RangeDuration: typed_count must be 0.
		if count != 0 {
			return pos, fmt.Errorf("typed boundaries: non-zero count %d for type %d", count, colType)
		}

		return pos, nil
	}
}

// skipRangeValueEntry skips one RangeValueEntry for the given column type.
func skipRangeValueEntry(data []byte, pos int, colType shared.ColumnType) (int, error) {
	// value_key depends on type.
	switch colType {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		if pos+4 > len(data) {
			return pos, fmt.Errorf("value_key(string): short for len")
		}

		kLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+kLen > len(data) {
			return pos, fmt.Errorf("value_key(string): short for data")
		}

		pos += kLen

	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		if pos+4 > len(data) {
			return pos, fmt.Errorf("value_key(bytes): short for len")
		}

		kLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+kLen > len(data) {
			return pos, fmt.Errorf("value_key(bytes): short for data")
		}

		pos += kLen

	case shared.ColumnTypeInt64, shared.ColumnTypeUint64, shared.ColumnTypeFloat64:
		if pos+8 > len(data) {
			return pos, fmt.Errorf("value_key(numeric): short")
		}

		pos += 8

	case shared.ColumnTypeBool:
		if pos+1 > len(data) {
			return pos, fmt.Errorf("value_key(bool): short")
		}

		pos++

	case shared.ColumnTypeRangeInt64,
		shared.ColumnTypeRangeUint64,
		shared.ColumnTypeRangeDuration,
		shared.ColumnTypeRangeFloat64:
		// length_prefix(1 uint8) + key_data where length_prefix is 2 (bucket ID) or 8 (raw value)
		if pos+1 > len(data) {
			return pos, fmt.Errorf("value_key(range numeric): short for length_prefix")
		}

		kLen := int(data[pos])
		pos++
		if pos+kLen > len(data) {
			return pos, fmt.Errorf("value_key(range numeric): short for key_data (len=%d)", kLen)
		}

		pos += kLen

	default:
		return pos, fmt.Errorf("skipRangeValueEntry: unknown column type %d", colType)
	}

	// block_id_count[4] + block_ids[N×4]
	if pos+4 > len(data) {
		return pos, fmt.Errorf("value_entry: short for block_id_count")
	}

	bidCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	need := bidCount * 4
	if pos+need > len(data) {
		return pos, fmt.Errorf("value_entry: short for block_ids (count=%d)", bidCount)
	}

	pos += need
	return pos, nil
}

// readRange reads exactly length bytes at absolute byte offset.
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
