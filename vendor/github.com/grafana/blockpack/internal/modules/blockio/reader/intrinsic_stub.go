package reader

// Intrinsic TOC stubs — the IntrinsicTOC section was removed in issue #433.
// New files (v2 format) never write an IntrinsicTOC. Legacy v1 reader support dropped.

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// HasIntrinsicSection always returns false. The IntrinsicTOC section is no longer written.
func (r *Reader) HasIntrinsicSection() bool { return false }

// HasIntrinsicColumn always returns false. No intrinsic columns are present.
func (r *Reader) HasIntrinsicColumn(_ string) bool { return false }

// GetIntrinsicColumn always returns nil, nil. Callers fall through to block columns.
func (r *Reader) GetIntrinsicColumn(_ string) (*shared.IntrinsicColumn, error) { return nil, nil }

// GetIntrinsicColumnLazyRefs always returns nil, nil.
func (r *Reader) GetIntrinsicColumnLazyRefs(_ string) (*shared.IntrinsicColumn, error) {
	return nil, nil
}

// GetIntrinsicColumnBlob always returns nil, nil.
func (r *Reader) GetIntrinsicColumnBlob(_ string) ([]byte, error) { return nil, nil }

// PrefetchIntrinsicColumns is a no-op. Nothing to prefetch.
func (r *Reader) PrefetchIntrinsicColumns(_ []string) {}

// IntrinsicColumnNames always returns nil. No intrinsic columns exist.
func (r *Reader) IntrinsicColumnNames() []string { return nil }

// IntrinsicUint64At always returns 0, false.
func (r *Reader) IntrinsicUint64At(_ string, _, _ int) (uint64, bool) { return 0, false }

// ScanIntrinsicColumn is a no-op stub. Returns nil.
func (r *Reader) ScanIntrinsicColumn(_ string, _ func(*shared.DecodedPage) error) error {
	return nil
}

// ScanIntrinsicColumnWithStats is a no-op stub. Returns (false, nil) — not streamed.
func (r *Reader) ScanIntrinsicColumnWithStats(
	_ string,
	_ func(*shared.PageStats) bool,
	_ func(*shared.DecodedPage) error,
) (bool, error) {
	return false, nil
}

// ScanDictGroupByColumn is a no-op stub.
func (r *Reader) ScanDictGroupByColumn(_ string, _ func([]byte, []uint32) error) error {
	return nil
}

// IntrinsicColumnMeta always returns the zero value and false.
func (r *Reader) IntrinsicColumnMeta(_ string) (shared.IntrinsicColMeta, bool) {
	return shared.IntrinsicColMeta{}, false
}

// EnsureIntrinsicTOC is a no-op. No IntrinsicTOC is ever present.
func (r *Reader) EnsureIntrinsicTOC() error { return nil }

// IntrinsicDictStringSet always returns nil, nil.
func (r *Reader) IntrinsicDictStringSet(_ string) (map[string]struct{}, error) { return nil, nil }

// IntrinsicBytesAt always returns nil, false.
func (r *Reader) IntrinsicBytesAt(_ string, _, _ int) ([]byte, bool) { return nil, false }

// IntrinsicDictStringAt always returns "", false.
func (r *Reader) IntrinsicDictStringAt(_ string, _, _ int) (string, bool) { return "", false }

// IntrinsicDictInt64At always returns 0, false.
func (r *Reader) IntrinsicDictInt64At(_ string, _, _ int) (int64, bool) { return 0, false }
