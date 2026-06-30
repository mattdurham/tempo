package reader

// Range index stubs — the range index was removed in issue #439.
// All callers handle nil/false returns and fall through to scanning all blocks.

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// BlocksForRange always returns nil, nil (no pruning; column not indexed).
func (r *Reader) BlocksForRange(_ string, _ shared.RangeValueKey) ([]int, error) { return nil, nil }

// BlocksForRangeInterval always returns nil, nil (no pruning; column not indexed).
func (r *Reader) BlocksForRangeInterval(_, _, _ string) ([]int, error) { return nil, nil }

// RangeColumnType always returns (0, false); no column is range-indexed.
func (r *Reader) RangeColumnType(_ string) (shared.ColumnType, bool) { return 0, false }

// RangeColumnBoundaries always returns nil; no column is range-indexed.
func (r *Reader) RangeColumnBoundaries(_ string) *RangeBoundaries { return nil }

// ColumnNames returns an empty slice; range and sketch columns no longer exist (#439).
func (r *Reader) ColumnNames() []string { return nil }
