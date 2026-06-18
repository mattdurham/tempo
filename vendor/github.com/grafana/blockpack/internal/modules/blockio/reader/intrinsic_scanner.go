package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// IntrinsicScanner provides always-streaming sequential-scan access to a single intrinsic
// column, making streaming the default and the eager fallback transparent to the caller
// (NOTE-443, issue #362).
//
// Background: the executor had two competing APIs for reading intrinsic columns —
// Reader.ScanIntrinsicColumn (streaming, O(one page), but returns (false, nil) for
// non-streamable Dict/legacy v1/absent blobs so the caller must hand-roll the eager
// fallback) and Reader.GetIntrinsicColumn (eager, O(rows) allocation). Sequential-scan
// callsites that wanted streaming had to duplicate the try-stream-then-fall-back-to-eager
// dance, and several un-converted sites used GetIntrinsicColumn by habit rather than design
// (issues #360, #361), paying unnecessary O(rows) materialization.
//
// IntrinsicScanner centralizes that decision exactly once. A caller writes its sequential
// forward-scan logic as a single Scan(visit) callback over *shared.DecodedPage. For a
// streamable paged Flat/XOR/Delta column the scanner streams page-by-page (zero full-column
// allocation, identical to ScanIntrinsicColumn). For a non-streamable legacy v1 / non-paged
// value-decoupled column it falls back to GetIntrinsicColumn ONCE, re-presents the eager
// column as a single synthetic DecodedPage (RowBase 0, the whole column's flat values + refs
// parallel to BlockRefs), and invokes visit once. The caller never sees the difference.
//
// Scope (DecodedPage is a flat value-per-row model): IntrinsicScanner serves value-decoupled
// columns — uint64 (Flat/XOR/Delta) and per-row bytes — whose values live in
// Uint64Values/BytesValues parallel to BlockRefs. Dict columns store values in a cross-row
// DictEntries arena that has no flat DecodedPage representation; they have their own streaming
// path (ScanDictGroupByColumn, NOTE-407) and MUST NOT be routed through this adapter. Scan
// returns an error rather than silently dropping data if the eager-fallback column is Dict-
// format. The #361 sequential-scan sites all read agg.Field = span:duration (a uint64
// value-decoupled column), so this is exactly their shape.
//
// Policy (executor/SPECS.md): sequential-scan access to intrinsic columns MUST use
// IntrinsicScanner. Direct GetIntrinsicColumn calls in the executor are reserved for
// random-access patterns (predicate evaluation, multi-pass random lookups by rank).
//
// Lifetime contract (inherited from ScanIntrinsicColumn / shared.DecodedPage): the
// *shared.DecodedPage and every slice it references are valid ONLY for the duration of a
// single visit call. The streaming path reuses the same page buffers across pages; a visitor
// that retains any value or ref beyond its call MUST copy it. (In the eager-fallback path the
// single page aliases the cached column's immutable slices, which outlive the call — but a
// visitor must not rely on that, since which path runs is intentionally opaque.)
type IntrinsicScanner struct {
	r    *Reader
	name string
}

// NewIntrinsicScanner returns a scanner for the named intrinsic column on r. It performs no
// I/O itself; the column blob is fetched (from cache or disk) on the first Scan call. r must
// be non-nil.
func NewIntrinsicScanner(r *Reader, name string) *IntrinsicScanner {
	return &IntrinsicScanner{r: r, name: name}
}

// Scan invokes visit for each decoded page of the column, in row order.
//
// For a streamable paged column visit is called once per on-disk page (O(one page) transient
// allocation, reused across pages). For a non-streamable legacy v1 / non-paged value-decoupled
// column visit is called exactly once with a single synthetic page covering the whole column.
// For an absent column (no intrinsic section or name not present) visit is never called and
// Scan returns nil — identical to streaming a column with zero pages. For a Dict-format
// eager-fallback column Scan returns an error without calling visit (see the type doc — Dict
// has no flat DecodedPage representation).
//
// A visitor error short-circuits the scan and is returned. The visitor's *shared.DecodedPage
// is valid only for the duration of each call (see the type doc).
func (s *IntrinsicScanner) Scan(visit func(*shared.DecodedPage) error) error {
	// Try the streaming (paged Flat/XOR/Delta) path first.
	streamed, err := s.r.ScanIntrinsicColumn(s.name, visit)
	if err != nil {
		return err
	}
	if streamed {
		return nil
	}

	// Non-streamable legacy v1 / non-paged / absent — fall back to a single eager decode and
	// re-present it as one synthetic page. Visitors that read p.BlockRefs require the refs
	// materialized, so use the eager-refs GetIntrinsicColumn (not the lazy-refs variant).
	col, err := s.r.GetIntrinsicColumn(s.name)
	if err != nil {
		return err
	}
	if col == nil {
		// Absent column — no pages to visit, same as streaming an empty column.
		return nil
	}
	if col.Format == shared.IntrinsicFormatDict {
		// Dict columns have no flat DecodedPage representation (values live in DictEntries,
		// not in Uint64Values/BytesValues parallel to BlockRefs). Refuse rather than silently
		// hand the visitor an empty value slice. Callers needing Dict streaming use
		// ScanDictGroupByColumn (NOTE-407).
		return fmt.Errorf(
			"IntrinsicScanner.Scan %q: Dict-format column not supported (use ScanDictGroupByColumn)",
			s.name,
		)
	}

	page := &shared.DecodedPage{
		BlockRefs:    col.BlockRefs,
		Uint64Values: col.Uint64Values,
		BytesValues:  col.BytesValues,
		Type:         col.Type,
		Format:       col.Format,
		RowBase:      0,
	}
	return visit(page)
}
