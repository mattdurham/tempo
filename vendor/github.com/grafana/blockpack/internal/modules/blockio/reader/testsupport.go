package reader

import (
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// BuildSyntheticIdentityBlock constructs a *Block carrying inline-bytes identity columns
// (trace:id, span:id, span:parent_id) for the given per-row values. It is test support for
// exercising the legacy (no-intrinsic-section) decode path: since NOTE-469 the production
// writer stores these identity columns exclusively in the intrinsic section and never as
// block columns, so a genuine block-column-bearing block can no longer be produced via the
// writer. This builder synthesizes one directly.
//
// A nil entry in a column's slice marks that row absent (the present bit is left clear).
// All three slices must have the same length, which becomes the block's span count.
//
// Exported (not _test.go) so cross-package tests (e.g. the executor package) can build a
// legacy-shaped block; anchored in cmd/deadcode/main.go as intentional test-support API.
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
