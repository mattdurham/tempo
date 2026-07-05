package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// bucketfile.go — internal binary format for a value-index file (NOTE-VI-043, issue #427).
//
// This is the v2 BucketGroup format: time-bucketed, value-grouped posting lists. It
// replaces the flat per-span Entry rows (entries.go) with a structure that groups every
// span sharing a (time_sec, canonical_value) under one BucketGroup, nesting the spans
// beneath the data blocks that contain them. This deduplicates the (value, time_sec) key
// and the per-block SourceRef/BlockRef across the many spans that share them.
//
// # File layout
//
//	[ File Header  ]   magic + version + string-table
//	[ Block 0      ]   one block per time/value bucket group set
//	[ Block 1      ]
//	...
//	[ Block N      ]
//	[ Block Index  ]   one BlockDirEntry per block (offset/len + min/max time + min/max value)
//	[ Footer       ]   fixed-size: magic + block-index offset/len + file min/max time + string-table offset/len
//
// Min/max time live in the footer so the querier can prune whole files by time range
// without opening any block.
//
// # Block layout
//
//	MinTimeSec[8] MaxTimeSec[8]
//	min_value_len[2] min_value[N]  max_value_len[2] max_value[N]
//	bloom_len[4] bloom[N]                  -- bloom over canonical values in this block
//	group_count[4]
//	  for each BucketGroup (sorted by time_sec ASC, value ASC):
//	    time_sec[8]
//	    value_len[2] value[N]
//	    ref_count[2]
//	      for each BlockRef:
//	        source_id[2]        -- string-table index of the data blockpack S3 key
//	        page_num[3] len_pages[2]
//	        span_count[2]
//	          for each SpanRef:
//	            trace_id[16]
//	            idx_count[2] idx[2]*idx_count   -- row indexes within the block
//
// Block payloads are snappy-compressed; the directory stores the compressed offset/len.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/golang/snappy"
)

// BucketFileMagic identifies a v2 BucketGroup value-index file. ASCII "VBG2".
const BucketFileMagic uint32 = 0x56424732

// BucketFileVersion is the current BucketGroup file format version.
const BucketFileVersion uint8 = 0x01

// bucketFooterSize is the fixed byte size of the file footer.
//
//	magic[4] + block_index_off[8] + block_index_len[8] +
//	str_table_off[8] + str_table_len[8] +
//	min_time_sec[8] + max_time_sec[8] + version[1]
const bucketFooterSize = 4 + 8 + 8 + 8 + 8 + 8 + 8 + 1

// SpanRef identifies the spans within one data block that carry a given (time, value).
type SpanRef struct {
	SpanIndexes []uint16 // row indexes within the data block
	TraceID     [16]byte
}

// BucketBlockRef is one data-block reference within a BucketGroup. It points at a block
// in a blockpack data file (by string-table SourceRef id + page address) and lists the
// spans inside that block carrying this group's value at this time.
//
// Named BucketBlockRef (not BlockRef) to avoid colliding with the existing page-addressed
// BlockRef type in blockref.go, which it embeds.
type BucketBlockRef struct {
	Spans    []SpanRef
	Ref      BlockRef
	SourceID uint16 // string-table index of the data blockpack S3 key
}

// BucketGroup is all spans sharing one (TimeSec, CanonicalValue) across data blocks.
type BucketGroup struct {
	CanonicalValue []byte
	Refs           []BucketBlockRef
	TimeSec        uint64
}

// BucketBlock is a set of BucketGroups sorted by (TimeSec ASC, CanonicalValue ASC) plus
// per-block metadata (min/max time, min/max value, value bloom).
type BucketBlock struct {
	MinValue   []byte
	MaxValue   []byte
	Bloom      []byte
	Groups     []BucketGroup
	MinTimeSec uint64
	MaxTimeSec uint64
}

// BucketFile is the in-memory representation of a value-index file in the v2 format.
type BucketFile struct {
	StringTable *StringTable
	Blocks      []BucketBlock
	MinTimeSec  uint64
	MaxTimeSec  uint64
}

// BlockDirEntry is one record in the block index (TOC). It allows the querier to skip
// whole blocks by time range or value range without decompressing them.
type BlockDirEntry struct {
	MinValue   []byte
	MaxValue   []byte
	CompOff    uint64
	CompLen    uint64
	MinTimeSec uint64
	MaxTimeSec uint64
}

// ComputeBlockMeta fills MinTimeSec/MaxTimeSec/MinValue/MaxValue/Bloom from b.Groups.
// Groups must already be sorted by (TimeSec ASC, CanonicalValue ASC).
func (b *BucketBlock) ComputeBlockMeta() {
	if len(b.Groups) == 0 {
		b.MinTimeSec, b.MaxTimeSec = 0, 0
		b.MinValue, b.MaxValue, b.Bloom = nil, nil, nil
		return
	}
	minT, maxT := b.Groups[0].TimeSec, b.Groups[0].TimeSec
	// Track lexicographic min/max value and count distinct values for bloom sizing.
	var minV, maxV []byte
	distinct := make(map[string]struct{}, len(b.Groups))
	for i := range b.Groups {
		g := &b.Groups[i]
		if g.TimeSec < minT {
			minT = g.TimeSec
		}
		if g.TimeSec > maxT {
			maxT = g.TimeSec
		}
		if minV == nil || compareCanonicalBytes(g.CanonicalValue, minV) < 0 {
			minV = g.CanonicalValue
		}
		if maxV == nil || compareCanonicalBytes(g.CanonicalValue, maxV) > 0 {
			maxV = g.CanonicalValue
		}
		distinct[string(g.CanonicalValue)] = struct{}{}
	}
	b.MinTimeSec, b.MaxTimeSec = minT, maxT
	b.MinValue = append([]byte(nil), minV...)
	b.MaxValue = append([]byte(nil), maxV...)
	b.Bloom = make([]byte, ValueBloomSize(len(distinct)))
	for v := range distinct {
		AddValueToBloom(b.Bloom, []byte(v))
	}
}

// compareCanonicalBytes compares two canonical values lexicographically. Min/max value
// metadata is a coarse pruning aid; lexicographic ordering is correct for the bloom-style
// "could this block contain value V" question regardless of column type, so we keep it
// simple here (the precise type-aware ordering lives in predicate.go for range matching).
func compareCanonicalBytes(a, b []byte) int {
	switch {
	case len(a) != len(b):
		// Compare common prefix first, then shorter < longer.
		n := len(a)
		if len(b) < n {
			n = len(b)
		}
		for i := range n {
			if a[i] != b[i] {
				return int(a[i]) - int(b[i])
			}
		}
		if len(a) < len(b) {
			return -1
		}
		return 1
	default:
		for i := range a {
			if a[i] != b[i] {
				return int(a[i]) - int(b[i])
			}
		}
		return 0
	}
}

// sortBucketBlock sorts a block's groups by (TimeSec ASC, CanonicalValue ASC) and each
// group's refs by SourceID then page, and each ref's spans by TraceID. This canonical
// ordering makes encode deterministic and merge-join compaction possible.
func sortBucketBlock(b *BucketBlock) {
	sort.Slice(b.Groups, func(i, j int) bool {
		if b.Groups[i].TimeSec != b.Groups[j].TimeSec {
			return b.Groups[i].TimeSec < b.Groups[j].TimeSec
		}
		return compareCanonicalBytes(b.Groups[i].CanonicalValue, b.Groups[j].CanonicalValue) < 0
	})
	for gi := range b.Groups {
		refs := b.Groups[gi].Refs
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].SourceID != refs[j].SourceID {
				return refs[i].SourceID < refs[j].SourceID
			}
			return refs[i].Ref.PageNum < refs[j].Ref.PageNum
		})
		for ri := range refs {
			spans := refs[ri].Spans
			sort.Slice(spans, func(i, j int) bool {
				return compareTraceID(spans[i].TraceID, spans[j].TraceID) < 0
			})
			for si := range spans {
				sortUint16(spans[si].SpanIndexes)
			}
		}
	}
}

func compareTraceID(a, b [16]byte) int {
	return bytes.Compare(a[:], b[:])
}

func sortUint16(s []uint16) {
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
}

// EncodeBucketFile serializes f into the v2 wire format. Blocks are encoded in order;
// each block is sorted and its metadata recomputed before encoding so the caller need not
// pre-sort. The file-level MinTimeSec/MaxTimeSec are derived from the blocks.
func EncodeBucketFile(f *BucketFile) ([]byte, error) {
	if f.StringTable == nil {
		f.StringTable = NewStringTable()
	}

	body := make([]byte, 0, len(f.Blocks)*256)
	dir := make([]BlockDirEntry, 0, len(f.Blocks))
	var fileMin, fileMax uint64
	haveTime := false

	for bi := range f.Blocks {
		b := &f.Blocks[bi]
		sortBucketBlock(b)
		b.ComputeBlockMeta()

		raw := encodeBucketBlock(b)
		compressed := snappy.Encode(nil, raw)

		dir = append(dir, BlockDirEntry{
			CompOff:    uint64(len(body)),
			CompLen:    uint64(len(compressed)),
			MinTimeSec: b.MinTimeSec,
			MaxTimeSec: b.MaxTimeSec,
			MinValue:   b.MinValue,
			MaxValue:   b.MaxValue,
		})
		body = append(body, compressed...)

		if len(b.Groups) > 0 {
			if !haveTime || b.MinTimeSec < fileMin {
				fileMin = b.MinTimeSec
			}
			if !haveTime || b.MaxTimeSec > fileMax {
				fileMax = b.MaxTimeSec
			}
			haveTime = true
		}
	}
	f.MinTimeSec, f.MaxTimeSec = fileMin, fileMax

	return assembleBucketFileBytes(body, dir, f.StringTable, fileMin, fileMax), nil
}

// assembleBucketFileBytes appends the string table, block index, and footer to a body of
// already-compressed block bytes, returning the complete file. Shared by EncodeBucketFile
// (single-shot, given a *BucketFile) and StreamCompactBucketFiles (incremental, body/dir
// built block-by-block) so both produce byte-identical framing. dir's CompOff values must
// be body-relative (0-based within body); they are shifted to file-absolute offsets here.
// fileMin/fileMax are the footer's file-level min/max time, computed by the caller from only
// its non-empty blocks (a BlockDirEntry alone can't distinguish "empty block" from
// "MinTimeSec/MaxTimeSec legitimately 0", since ComputeBlockMeta zeroes both for a block with
// no groups — recomputing unconditionally from dir here would corrupt the footer whenever any
// block in dir is empty).
func assembleBucketFileBytes(body []byte, dir []BlockDirEntry, table *StringTable, fileMin, fileMax uint64) []byte {
	if table == nil {
		table = NewStringTable()
	}

	// File header: magic[4] + version[1].
	out := make([]byte, 0, len(body)+256)
	out = binary.LittleEndian.AppendUint32(out, BucketFileMagic)
	out = append(out, BucketFileVersion)

	// Body (blocks). The directory's CompOff values are body-relative; shift them to be
	// file-absolute now that we know the header length.
	headerLen := uint64(len(out))
	for i := range dir {
		dir[i].CompOff += headerLen
	}
	out = append(out, body...)

	// String table.
	strOff := uint64(len(out))
	strBytes := EncodeStringTable(table)
	out = append(out, strBytes...)
	strLen := uint64(len(strBytes))

	// Block index (TOC).
	blockIdxOff := uint64(len(out))
	out = appendBlockIndex(out, dir)
	blockIdxLen := uint64(len(out)) - blockIdxOff

	// Footer.
	out = binary.LittleEndian.AppendUint32(out, BucketFileMagic)
	out = binary.LittleEndian.AppendUint64(out, blockIdxOff)
	out = binary.LittleEndian.AppendUint64(out, blockIdxLen)
	out = binary.LittleEndian.AppendUint64(out, strOff)
	out = binary.LittleEndian.AppendUint64(out, strLen)
	out = binary.LittleEndian.AppendUint64(out, fileMin)
	out = binary.LittleEndian.AppendUint64(out, fileMax)
	out = append(out, BucketFileVersion)

	return out
}

// writeBucketFileTail writes the string table, block index, and footer directly to bw,
// completing a file whose header and block body were already written by the caller
// (StreamCompactBucketFiles' disk-backed output path, plan.md Decision 3). Unlike
// assembleBucketFileBytes (which builds these same sections into a body-relative []byte and
// shifts dir's offsets to file-absolute afterward), dir's CompOff values here are already
// file-absolute — the caller wrote the 5-byte header first and sized every block write
// against a running byte counter, so no post-hoc shift is needed. bodyEnd is the
// file-absolute offset immediately after the last block byte (i.e. where the string table
// begins). assembleBucketFileBytes itself is not modified — this is a new sibling function,
// since assembleBucketFileBytes has an existing, unrelated caller (EncodeBucketFile) outside
// this task's scope.
func writeBucketFileTail(
	bw *bufio.Writer,
	dir []BlockDirEntry,
	table *StringTable,
	fileMin, fileMax, bodyEnd uint64,
) error {
	if table == nil {
		table = NewStringTable()
	}

	strOff := bodyEnd
	strBytes := EncodeStringTable(table)
	if _, err := bw.Write(strBytes); err != nil {
		return fmt.Errorf("valueindex: writeBucketFileTail: write string table: %w", err)
	}
	strLen := uint64(len(strBytes))

	blockIdxOff := strOff + strLen
	blockIdxBytes := appendBlockIndex(nil, dir)
	if _, err := bw.Write(blockIdxBytes); err != nil {
		return fmt.Errorf("valueindex: writeBucketFileTail: write block index: %w", err)
	}
	blockIdxLen := uint64(len(blockIdxBytes))

	footer := make([]byte, 0, bucketFooterSize)
	footer = binary.LittleEndian.AppendUint32(footer, BucketFileMagic)
	footer = binary.LittleEndian.AppendUint64(footer, blockIdxOff)
	footer = binary.LittleEndian.AppendUint64(footer, blockIdxLen)
	footer = binary.LittleEndian.AppendUint64(footer, strOff)
	footer = binary.LittleEndian.AppendUint64(footer, strLen)
	footer = binary.LittleEndian.AppendUint64(footer, fileMin)
	footer = binary.LittleEndian.AppendUint64(footer, fileMax)
	footer = append(footer, BucketFileVersion)
	if _, err := bw.Write(footer); err != nil {
		return fmt.Errorf("valueindex: writeBucketFileTail: write footer: %w", err)
	}
	return nil
}

func appendBlockIndex(out []byte, dir []BlockDirEntry) []byte {
	out = binary.LittleEndian.AppendUint32(out, uint32(len(dir))) //nolint:gosec // bounded
	for i := range dir {
		d := &dir[i]
		out = binary.LittleEndian.AppendUint64(out, d.CompOff)
		out = binary.LittleEndian.AppendUint64(out, d.CompLen)
		out = binary.LittleEndian.AppendUint64(out, d.MinTimeSec)
		out = binary.LittleEndian.AppendUint64(out, d.MaxTimeSec)
		out = binary.LittleEndian.AppendUint16(out, uint16(len(d.MinValue))) //nolint:gosec
		out = append(out, d.MinValue...)
		out = binary.LittleEndian.AppendUint16(out, uint16(len(d.MaxValue))) //nolint:gosec
		out = append(out, d.MaxValue...)
	}
	return out
}

func encodeBucketBlock(b *BucketBlock) []byte {
	buf := make([]byte, 0, 64+len(b.Groups)*48)
	buf = binary.LittleEndian.AppendUint64(buf, b.MinTimeSec)
	buf = binary.LittleEndian.AppendUint64(buf, b.MaxTimeSec)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(b.MinValue))) //nolint:gosec
	buf = append(buf, b.MinValue...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(b.MaxValue))) //nolint:gosec
	buf = append(buf, b.MaxValue...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b.Bloom))) //nolint:gosec
	buf = append(buf, b.Bloom...)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b.Groups))) //nolint:gosec
	for gi := range b.Groups {
		g := &b.Groups[gi]
		buf = binary.LittleEndian.AppendUint64(buf, g.TimeSec)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(g.CanonicalValue))) //nolint:gosec
		buf = append(buf, g.CanonicalValue...)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(g.Refs))) //nolint:gosec
		for ri := range g.Refs {
			r := &g.Refs[ri]
			buf = binary.LittleEndian.AppendUint16(buf, r.SourceID)
			buf = AppendBlockRef(buf, r.Ref)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(r.Spans))) //nolint:gosec
			for si := range r.Spans {
				s := &r.Spans[si]
				buf = append(buf, s.TraceID[:]...)
				buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s.SpanIndexes))) //nolint:gosec
				for _, idx := range s.SpanIndexes {
					buf = binary.LittleEndian.AppendUint16(buf, idx)
				}
			}
		}
	}
	return buf
}

// ErrNotBucketFile signals that data is not a v2 BucketGroup file at all — its
// header or footer magic does not match BucketFileMagic. This is distinct from a
// decode error on a genuine v2 file (corruption past the magic): a caller iterating
// a discovered file set may safely SKIP an ErrNotBucketFile (e.g. a stray
// old-format or non-value-index object sharing the prefix, which holds no v2
// postings and so cannot under-count results), but must NOT silently skip any other
// decode error — a corrupt v2 file dropped from an otherwise-authoritative index
// query silently under-counts, the exact silent-partial-result bug class the
// trace-by-id review caught in findTraceGroupInCandidates (NOTE-VI-046).
// Recognize with errors.Is(err, ErrNotBucketFile).
var ErrNotBucketFile = errors.New("valueindex: not a bucket file (bad magic)")

// DecodeBucketFile parses the full v2 wire format produced by EncodeBucketFile.
// A bad header or footer magic is reported as ErrNotBucketFile (the data is not a
// v2 file at all); every other failure is a genuine decode error on a v2 file.
func DecodeBucketFile(data []byte) (*BucketFile, error) {
	if len(data) < 5+bucketFooterSize {
		// Too short to even hold a header magic + footer: it cannot be a v2 file, so
		// classify as ErrNotBucketFile — a caller iterating a discovered file set may
		// skip it (a stray/empty object holds no v2 postings). NOTE-VI-046.
		return nil, fmt.Errorf("valueindex: bucket file too short (%d bytes): %w", len(data), ErrNotBucketFile)
	}
	if binary.LittleEndian.Uint32(data[:4]) != BucketFileMagic {
		return nil, fmt.Errorf("valueindex: bad header magic: %w", ErrNotBucketFile)
	}

	footer := data[len(data)-bucketFooterSize:]
	if binary.LittleEndian.Uint32(footer[:4]) != BucketFileMagic {
		return nil, fmt.Errorf("valueindex: bad footer magic: %w", ErrNotBucketFile)
	}
	blockIdxOff := binary.LittleEndian.Uint64(footer[4:12])
	blockIdxLen := binary.LittleEndian.Uint64(footer[12:20])
	strOff := binary.LittleEndian.Uint64(footer[20:28])
	strLen := binary.LittleEndian.Uint64(footer[28:36])
	fileMin := binary.LittleEndian.Uint64(footer[36:44])
	fileMax := binary.LittleEndian.Uint64(footer[44:52])

	// Overflow-safe bounds validation: a corrupt footer can carry huge offsets/lengths
	// whose sum wraps uint64 and slips past a naive `off+len > len(data)` check, then
	// panics the decode goroutine on the slice below. Checking each term against
	// len(data) first (offsets/lengths individually cannot legitimately exceed the file
	// size) makes the subsequent sum overflow-free (NOTE-VI-046).
	dataLen := uint64(len(data))
	if strOff > dataLen || strLen > dataLen || strOff+strLen > dataLen ||
		blockIdxOff > dataLen || blockIdxLen > dataLen || blockIdxOff+blockIdxLen > dataLen {
		return nil, fmt.Errorf("valueindex: footer offsets out of bounds")
	}

	table, _, err := DecodeStringTable(data[strOff : strOff+strLen])
	if err != nil {
		return nil, fmt.Errorf("valueindex: string table: %w", err)
	}

	dir, err := decodeBlockIndex(data[blockIdxOff : blockIdxOff+blockIdxLen])
	if err != nil {
		return nil, err
	}

	f := &BucketFile{
		StringTable: table,
		Blocks:      make([]BucketBlock, 0, len(dir)),
		MinTimeSec:  fileMin,
		MaxTimeSec:  fileMax,
	}
	for i := range dir {
		d := &dir[i]
		end := d.CompOff + d.CompLen
		if end > strOff {
			return nil, fmt.Errorf("valueindex: block %d body overruns string table", i)
		}
		raw, derr := snappy.Decode(nil, data[d.CompOff:end])
		if derr != nil {
			return nil, fmt.Errorf("valueindex: block %d snappy decode: %w", i, derr)
		}
		blk, berr := decodeBucketBlock(raw)
		if berr != nil {
			return nil, fmt.Errorf("valueindex: block %d: %w", i, berr)
		}
		f.Blocks = append(f.Blocks, *blk)
	}
	return f, nil
}

func decodeBlockIndex(data []byte) ([]BlockDirEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("valueindex: block index too short")
	}
	count := int(binary.LittleEndian.Uint32(data[:4]))
	pos := 4
	dir := make([]BlockDirEntry, 0, count)
	for i := range count {
		if pos+32 > len(data) {
			return nil, fmt.Errorf("valueindex: block index entry %d truncated", i)
		}
		var d BlockDirEntry
		d.CompOff = binary.LittleEndian.Uint64(data[pos:])
		d.CompLen = binary.LittleEndian.Uint64(data[pos+8:])
		d.MinTimeSec = binary.LittleEndian.Uint64(data[pos+16:])
		d.MaxTimeSec = binary.LittleEndian.Uint64(data[pos+24:])
		pos += 32
		if pos+2 > len(data) {
			return nil, fmt.Errorf("valueindex: block index entry %d min_value_len truncated", i)
		}
		mvl := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+mvl > len(data) {
			return nil, fmt.Errorf("valueindex: block index entry %d min_value truncated", i)
		}
		d.MinValue = append([]byte(nil), data[pos:pos+mvl]...)
		pos += mvl
		if pos+2 > len(data) {
			return nil, fmt.Errorf("valueindex: block index entry %d max_value_len truncated", i)
		}
		xvl := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+xvl > len(data) {
			return nil, fmt.Errorf("valueindex: block index entry %d max_value truncated", i)
		}
		d.MaxValue = append([]byte(nil), data[pos:pos+xvl]...)
		pos += xvl
		dir = append(dir, d)
	}
	return dir, nil
}

func decodeBucketBlock(raw []byte) (*BucketBlock, error) {
	pos := 0
	need := func(n int, what string) error {
		if pos+n > len(raw) {
			return fmt.Errorf("truncated at %s", what)
		}
		return nil
	}
	if err := need(16, "block times"); err != nil {
		return nil, err
	}
	b := &BucketBlock{}
	b.MinTimeSec = binary.LittleEndian.Uint64(raw[pos:])
	b.MaxTimeSec = binary.LittleEndian.Uint64(raw[pos+8:])
	pos += 16

	readBytes := func(what string) ([]byte, error) {
		if err := need(2, what+"_len"); err != nil {
			return nil, err
		}
		n := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if err := need(n, what); err != nil {
			return nil, err
		}
		v := append([]byte(nil), raw[pos:pos+n]...)
		pos += n
		return v, nil
	}

	minV, rbErr := readBytes("min_value")
	if rbErr != nil {
		return nil, rbErr
	}
	b.MinValue = minV
	maxV, rbErr := readBytes("max_value")
	if rbErr != nil {
		return nil, rbErr
	}
	b.MaxValue = maxV

	if e := need(4, "bloom_len"); e != nil {
		return nil, e
	}
	bloomLen := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4
	if e := need(bloomLen, "bloom"); e != nil {
		return nil, e
	}
	b.Bloom = append([]byte(nil), raw[pos:pos+bloomLen]...)
	pos += bloomLen

	if e := need(4, "group_count"); e != nil {
		return nil, e
	}
	groupCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4
	b.Groups = make([]BucketGroup, 0, groupCount)
	for gi := range groupCount {
		if e := need(8, "group_time"); e != nil {
			return nil, fmt.Errorf("group %d: %w", gi, e)
		}
		g := BucketGroup{TimeSec: binary.LittleEndian.Uint64(raw[pos:])}
		pos += 8
		gv, e := readBytes("group_value")
		if e != nil {
			return nil, fmt.Errorf("group %d: %w", gi, e)
		}
		g.CanonicalValue = gv
		if e := need(2, "ref_count"); e != nil {
			return nil, fmt.Errorf("group %d: %w", gi, e)
		}
		refCount := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		g.Refs = make([]BucketBlockRef, 0, refCount)
		for ri := range refCount {
			if err := need(2+BlockRefSize+2, "ref_header"); err != nil {
				return nil, fmt.Errorf("group %d ref %d: %w", gi, ri, err)
			}
			var r BucketBlockRef
			r.SourceID = binary.LittleEndian.Uint16(raw[pos:])
			pos += 2
			r.Ref = DecodeBlockRef(raw, pos)
			pos += BlockRefSize
			spanCount := int(binary.LittleEndian.Uint16(raw[pos:]))
			pos += 2
			r.Spans = make([]SpanRef, 0, spanCount)
			for si := range spanCount {
				if err := need(16+2, "span_header"); err != nil {
					return nil, fmt.Errorf("group %d ref %d span %d: %w", gi, ri, si, err)
				}
				var s SpanRef
				copy(s.TraceID[:], raw[pos:pos+16])
				pos += 16
				idxCount := int(binary.LittleEndian.Uint16(raw[pos:]))
				pos += 2
				if err := need(idxCount*2, "span_indexes"); err != nil {
					return nil, fmt.Errorf("group %d ref %d span %d: %w", gi, ri, si, err)
				}
				s.SpanIndexes = make([]uint16, idxCount)
				for k := range idxCount {
					s.SpanIndexes[k] = binary.LittleEndian.Uint16(raw[pos:])
					pos += 2
				}
				r.Spans = append(r.Spans, s)
			}
			g.Refs = append(g.Refs, r)
		}
		b.Groups = append(b.Groups, g)
	}
	return b, nil
}
