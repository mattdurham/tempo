package reader

import (
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// BuildSyntheticIdentityBlock constructs a *Block carrying inline-bytes identity columns
// (trace:id, span:id, span:parent_id) for the given per-row values.
//
// NOTE-469/NOTE-V2-004(writer) (corrected #490 A-11, per
// .bob/state/identity-investigation.md): despite the name and the historical framing this
// comment previously carried, this builds the MODERN block shape — the current production
// writer unconditionally stores identity columns as regular block-payload columns (confirmed
// by direct empirical test), not in a file-level intrinsic section. There is no longer any
// separate "intrinsic-section" shape to contrast this with; a genuine block missing these
// columns (the true legacy #389-#420 intrinsic-only shape) is built instead by
// BuildBlockMissingIdentityColumns below.
//
// A nil entry in a column's slice marks that row absent (the present bit is left clear).
// All three slices must have the same length, which becomes the block's span count.
//
// Exported (not _test.go) so cross-package tests (e.g. the executor package) can build a
// block with identity columns. Not anchored in cmd/deadcode/main.go — that file does not
// import this package (confirmed by direct read), so there is no deadcode exception to
// register; the exported-ness alone is what makes it visible to other packages' tests.
func BuildSyntheticIdentityBlock(traceIDs, spanIDs, parentIDs [][]byte) *Block {
	n := len(traceIDs)
	meta := shared.BlockMeta{SpanCount: uint32(n)} //nolint:gosec // test-only, bounded by caller
	b := newBlockForParsing(meta)

	addCol := func(name string, vals [][]byte) {
		present := make([]byte, (n+7)/8)
		inline := make([][]byte, n)
		for i, v := range vals {
			if v == nil {
				continue
			}
			inline[i] = v
			present[i/8] |= 1 << (uint(i) % 8)
		}
		col := &Column{
			Name:        name,
			Type:        shared.ColumnTypeBytes,
			SpanCount:   n,
			Present:     present,
			BytesInline: inline,
		}
		col.decoded.Store(true)
		b.columns[shared.ColumnKey{Name: name, Type: shared.ColumnTypeBytes}] = col
	}

	addCol("trace:id", traceIDs)
	addCol("span:id", spanIDs)
	addCol("span:parent_id", parentIDs)
	b.buildNameIndex()
	return b
}

// BuildBlockMissingIdentityColumns constructs a *Block carrying spanNames as a
// span:name string column and NO trace:id/span:id/span:parent_id columns at all —
// the genuinely legacy shape from the #389-#420 intrinsic-only window (NOTE-469),
// where identity lived exclusively in the (now-deleted) file-level IntrinsicTOC/
// SpanTree section and never as block columns. Unlike BuildSyntheticIdentityBlock
// (which always creates the three identity columns, even with all rows absent, and
// therefore represents the MODERN v2 self-contained-block shape), this builder omits
// the identity column keys entirely so `Block.GetColumn("trace:id")` returns nil —
// the exact condition writer_block.go's compaction gate checks for.
//
// Exported (not _test.go) so cross-package tests (e.g. the writer package) can build
// a genuinely-legacy-shaped block. Not anchored in cmd/deadcode/main.go — that file does
// not import this package (confirmed by direct read), so there is no deadcode exception
// to register; the exported-ness alone is what makes it visible to other packages' tests.
func BuildBlockMissingIdentityColumns(spanNames []string) *Block {
	n := len(spanNames)
	meta := shared.BlockMeta{SpanCount: uint32(n)} //nolint:gosec // test-only, bounded by caller
	b := newBlockForParsing(meta)

	present := make([]byte, (n+7)/8)
	idx := make([]uint32, n)
	var dict []string
	dictIdx := make(map[string]uint32, n)
	for i, v := range spanNames {
		if v == "" {
			continue
		}
		di, ok := dictIdx[v]
		if !ok {
			di = uint32(len(dict)) //nolint:gosec // test-only, bounded by caller
			dict = append(dict, v)
			dictIdx[v] = di
		}
		idx[i] = di
		present[i/8] |= 1 << (uint(i) % 8)
	}
	col := &Column{
		Name:       "span:name",
		Type:       shared.ColumnTypeString,
		SpanCount:  n,
		Present:    present,
		StringIdx:  idx,
		StringDict: dict,
	}
	col.decoded.Store(true)
	b.columns[shared.ColumnKey{Name: "span:name", Type: shared.ColumnTypeString}] = col
	b.buildNameIndex()
	return b
}
