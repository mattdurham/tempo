package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// EnsureIntrinsicTOC lazily loads the intrinsic column TOC if it has not been loaded yet.
// Called by GetTraceByID when a lean reader needs intrinsic data on demand.
func (r *Reader) EnsureIntrinsicTOC() error {
	if r.intrinsicIndex != nil || r.intrinsicIndexLen == 0 {
		return nil // already loaded or no intrinsic section
	}
	return r.parseIntrinsicTOC()
}

// parseIntrinsicTOC reads and parses the intrinsic column TOC from the v4+ footer.
// Called during NewReaderFromProvider. For v3 footer files or files with no intrinsic
// section, this is a no-op.
// NOTE-003: the parsed TOC map is cached in parsedIntrinsicTOCCache (strong references,
// entries persist until Clear) to avoid re-decoding the blob on every NewReaderFromProvider call.
func (r *Reader) parseIntrinsicTOC() error {
	// No-op: V8 files use the section directory for intrinsics, not this path.
	// Legacy V4/V5/V6 formats (which used a separate intrinsic TOC blob) were
	// removed 2026-06-12. This function is retained for call-site compatibility.
	return nil
}

// HasIntrinsicSection reports whether the file has a v4 footer with a non-empty
// intrinsic column section.
func (r *Reader) HasIntrinsicSection() bool {
	return len(r.intrinsicIndex) > 0
}

// HasIntrinsicColumn reports whether the named column is present in the intrinsic section.
// Unlike IntrinsicColumnMeta, this method never triggers any I/O — it is a pure map lookup.
// Use this when only presence is needed (e.g. the metrics fast-path existence check in
// metricsColumnsAreIntrinsic) to avoid triggering a blob read just to check if a column exists.
// NOTE-016: zero-I/O existence check; see parser.go parseSectionsLazyV14 for context.
func (r *Reader) HasIntrinsicColumn(name string) bool {
	_, ok := r.intrinsicIndex[name]
	return ok
}

// IntrinsicColumnMeta returns the TOC metadata for the named intrinsic column.
// Returns (IntrinsicColMeta{}, false) if no intrinsic section is present or the
// column is not in the TOC.
//
// For V14 files, Format and Type are populated lazily on first call by peeking the
// compressed blob header (one I/O per column, cached). Subsequent calls are free.
func (r *Reader) IntrinsicColumnMeta(name string) (shared.IntrinsicColMeta, bool) {
	if r.intrinsicIndex == nil {
		return shared.IntrinsicColMeta{}, false
	}
	meta, ok := r.intrinsicIndex[name]
	if !ok {
		return shared.IntrinsicColMeta{}, false
	}
	// V14 files: Format and Type are not stored in the section directory.
	// Peek the compressed blob header on first access to populate them.
	if meta.Format == 0 {
		blob, err := r.GetIntrinsicColumnBlob(name)
		if err == nil && len(blob) > 0 {
			f, ct, cnt, peekErr := shared.PeekIntrinsicBlobHeader(blob)
			if peekErr == nil {
				meta.Format = f
				meta.Type = ct
				meta.Count = cnt
				r.intrinsicIndex[name] = meta
			}
		}
	}
	return meta, ok
}

// IntrinsicColumnNames returns the names of all intrinsic columns in the TOC,
// sorted alphabetically. Returns nil if no intrinsic section is present.
// The returned slice is owned by the Reader; callers must not modify it.
// NOT safe for concurrent use — Reader is single-goroutine (see NewReaderFromProvider doc).
func (r *Reader) IntrinsicColumnNames() []string {
	if len(r.intrinsicIndex) == 0 {
		return nil
	}
	if r.intrinsicNames != nil {
		return r.intrinsicNames
	}
	names := make([]string, 0, len(r.intrinsicIndex))
	for n := range r.intrinsicIndex {
		names = append(names, n)
	}
	slices.Sort(names)
	r.intrinsicNames = names
	return names
}

// GetIntrinsicColumnBlob returns the raw (snappy-compressed) column blob bytes
// for the named intrinsic column, fetched from cache or disk. Returns nil, nil
// if no intrinsic section or column not present. The returned bytes must not be
// modified by the caller.
func (r *Reader) GetIntrinsicColumnBlob(name string) ([]byte, error) {
	if r.intrinsicIndex == nil {
		return nil, nil
	}
	meta, ok := r.intrinsicIndex[name]
	if !ok {
		return nil, nil
	}
	blob, err := r.cache.GetOrFetchIntrinsic(r.fileID, name, func() ([]byte, error) {
		return r.readRange(meta.Offset, uint64(meta.Length), rw.DataTypeMetadata)
	})
	if err != nil {
		return nil, fmt.Errorf("GetIntrinsicColumnBlob %q: read: %w", name, err)
	}
	return blob, nil
}

// intrinsicBatchFetcher is the optional interface a section cache may implement to
// batch-fetch several intrinsic column blobs for one file in a single round-trip.
// TypedTieredCache.GetMultiIntrinsic implements it; caches that don't are simply not
// prefetched (each column resolves through its own GetOrFetchIntrinsic instead).
type intrinsicBatchFetcher interface {
	GetMultiIntrinsic(fileID string, names []string) (map[string][]byte, bool, error)
}

// PrefetchIntrinsicColumns batch-fetches the named intrinsic column blobs in ONE
// pipelined cache round-trip, decodes each hit, and populates the per-Reader and
// process-level decoded-column caches so subsequent GetIntrinsicColumn calls for
// those names return from cache with no further memcache traffic.
//
// NOTE-197: a metrics/search query reads several intrinsic columns per file (every
// predicate-leaf column + each group-by column + span:start). Resolving them one at a
// time via GetIntrinsicColumn issued one memcache round-trip per column; the querier
// CPU profile is dominated by kernel networking on those round-trips. Prefetching the
// whole per-file working set in a single GetMulti collapses N round-trips into one —
// the same lever NOTE-179/185 applied to V8 block columns. Columns already decoded
// (per-Reader or process cache) are skipped, and names that miss the batch fall through
// to the normal per-name path on their first GetIntrinsicColumn, so the result is
// identical to never prefetching. Best-effort: any decode/cache error on an individual
// column is ignored here (the per-name path will surface it on access).
//
// NOT safe for concurrent use with GetIntrinsicColumn on the same Reader.
func (r *Reader) PrefetchIntrinsicColumns(names []string) {
	if r.intrinsicIndex == nil || len(names) == 0 {
		return
	}
	bf, ok := r.cache.(intrinsicBatchFetcher)
	if !ok {
		return
	}

	// Collect names that are present in this file and not already decoded.
	//
	// NOTE-199: also consult the process-level parsedIntrinsicCache here, not just the
	// per-Reader intrinsicDecoded map. On a warm cluster a prior query's Reader on the
	// same file has already decoded the working-set columns into the strong-reference
	// process cache; without this check the prefetch re-requested them via GetMultiIntrinsic
	// (a wasted memcache round-trip) AND re-ran DecodeIntrinsicColumnBlob — the single most
	// expensive step for high-cardinality group-by dicts (e.g. the rate()-by-service-name
	// path, ~27% of querier alloc_space). Hydrating the per-Reader map from the process
	// cache hit skips both: the column drops out of `want` so it is neither fetched nor
	// re-decoded, and the subsequent GetIntrinsicColumn returns the shared decoded value.
	useProcessCache := r.fileID != ""
	want := make([]string, 0, len(names))
	r.intrinsicMu.Lock()
	for _, name := range names {
		if _, present := r.intrinsicIndex[name]; !present {
			continue
		}
		if r.intrinsicDecoded != nil {
			if _, done := r.intrinsicDecoded[name]; done {
				continue
			}
		}
		if useProcessCache {
			if col := parsedIntrinsicCache.Get(r.fileID + "/intrinsic/" + name); col != nil {
				if r.intrinsicDecoded == nil {
					r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
				}
				r.intrinsicDecoded[name] = col
				continue
			}
		}
		want = append(want, name)
	}
	r.intrinsicMu.Unlock()
	if len(want) == 0 {
		return
	}

	hits, supported, err := bf.GetMultiIntrinsic(r.fileID, want)
	if err != nil || !supported || len(hits) == 0 {
		return
	}

	for name, blob := range hits {
		if blob == nil {
			continue
		}
		col, decErr := shared.DecodeIntrinsicColumnBlob(blob)
		if decErr != nil || col == nil {
			continue // per-name path will re-fetch and surface the error on access
		}
		col.Name = name

		if useProcessCache {
			// Best-effort process-cache population; ignore Put errors (e.g. size cap).
			_ = parsedIntrinsicCache.Put(r.fileID+"/intrinsic/"+name, col)
		}

		r.intrinsicMu.Lock()
		if r.intrinsicDecoded == nil {
			r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
		}
		if _, done := r.intrinsicDecoded[name]; !done {
			r.intrinsicDecoded[name] = col
		}
		r.intrinsicMu.Unlock()
	}
}

// GetIntrinsicColumn returns the decoded intrinsic column for the given name,
// or nil if the file has no intrinsic section or the column is not present.
// The column blob is read and decoded on first call; subsequent calls return the
// cached result (lazy decode, single I/O per column).
//
// span:end is synthesized from span:start + span:duration when not stored in the
// intrinsic section (files written after the span:end elimination optimization).
//
// NOT safe for concurrent use without external synchronization.
//
// NOTE-340: BlockRefs are decoded lazily for value-decoupled paged columns. This accessor
// materializes them before returning so every existing caller observes the same behavior as
// before (refs always present). The single caller that can skip the ref decode — the span:start
// fetch on the unfiltered no-group-by rate path — uses GetIntrinsicColumnLazyRefs instead.
func (r *Reader) GetIntrinsicColumn(name string) (*shared.IntrinsicColumn, error) {
	col, err := r.GetIntrinsicColumnLazyRefs(name)
	if col != nil {
		col.EnsureBlockRefs()
	}
	return col, err
}

// GetIntrinsicColumnLazyRefs is like GetIntrinsicColumn but does NOT force the lazy BlockRefs
// decode (NOTE-340). Callers that read only Uint64Values/BytesValues + Count (the unfiltered
// no-group-by count/rate fast path) avoid the per-row ref decode entirely. Any caller that
// reads col.BlockRefs MUST call col.EnsureBlockRefs() first.
func (r *Reader) GetIntrinsicColumnLazyRefs(name string) (*shared.IntrinsicColumn, error) {
	if r.intrinsicIndex == nil {
		return nil, nil
	}
	meta, ok := r.intrinsicIndex[name]
	if !ok {
		// Synthesize span:end from span:start + span:duration.
		if name == "span:end" {
			return r.synthesizeSpanEnd()
		}
		return nil, nil
	}

	// Fast path: check per-Reader cache under read lock.
	r.intrinsicMu.RLock()
	if r.intrinsicDecoded != nil {
		if cached, ok := r.intrinsicDecoded[name]; ok {
			r.intrinsicMu.RUnlock()
			return cached, nil
		}
	}
	r.intrinsicMu.RUnlock()

	// Check process-level cache first — decoded IntrinsicColumn value fields are immutable
	// once written; refIndex is a derived, concurrency-safe cache built under sync.Once, so
	// EnsureRefIndex mutations do not break the immutability assumption for value fields.
	// Guard: only use process-level cache when fileID is non-empty to prevent cross-file collisions.
	// NOTE-003: process-level cache uses objectcache.Cache (strong references, entries
	// persist until Clear) for decoded IntrinsicColumn values.
	useProcessCache := r.fileID != ""
	if useProcessCache {
		procKey := r.fileID + "/intrinsic/" + name
		if col := parsedIntrinsicCache.Get(procKey); col != nil {
			r.intrinsicMu.Lock()
			if r.intrinsicDecoded == nil {
				r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
			}
			r.intrinsicDecoded[name] = col
			r.intrinsicMu.Unlock()
			return col, nil
		}
	}

	// I/O and decoding done without holding the lock.
	blob, err := r.cache.GetOrFetchIntrinsic(r.fileID, name, func() ([]byte, error) {
		return r.readRange(meta.Offset, uint64(meta.Length), rw.DataTypeMetadata)
	})
	if err != nil {
		return nil, fmt.Errorf("GetIntrinsicColumn %q: read: %w", name, err)
	}

	col, err := shared.DecodeIntrinsicColumnBlob(blob)
	if err != nil {
		return nil, fmt.Errorf("GetIntrinsicColumn %q: decode: %w", name, err)
	}
	col.Name = name

	if useProcessCache {
		if err := parsedIntrinsicCache.Put(r.fileID+"/intrinsic/"+name, col); err != nil {
			return nil, fmt.Errorf("GetIntrinsicColumn %q: cache: %w", name, err)
		}
	}

	// Store under write lock. Double-check: another goroutine may have decoded it
	// concurrently while we were doing I/O — prefer its result if present.
	r.intrinsicMu.Lock()
	if r.intrinsicDecoded == nil {
		r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
	} else if existing, ok := r.intrinsicDecoded[name]; ok {
		r.intrinsicMu.Unlock()
		return existing, nil
	}
	r.intrinsicDecoded[name] = col
	r.intrinsicMu.Unlock()
	return col, nil
}

// synthesizeSpanEnd builds a span:end intrinsic column from span:start + span:duration.
// The result is cached like any other intrinsic column.
func (r *Reader) synthesizeSpanEnd() (*shared.IntrinsicColumn, error) {
	r.intrinsicMu.RLock()
	if r.intrinsicDecoded != nil {
		if cached, ok := r.intrinsicDecoded["span:end"]; ok {
			r.intrinsicMu.RUnlock()
			return cached, nil
		}
	}
	r.intrinsicMu.RUnlock()

	startCol, err := r.GetIntrinsicColumn("span:start")
	if err != nil || startCol == nil {
		return nil, nil //nolint:nilerr // best-effort synthesis; missing columns are not errors
	}
	durCol, err := r.GetIntrinsicColumn("span:duration")
	if err != nil || durCol == nil {
		return nil, nil //nolint:nilerr // best-effort synthesis; missing columns are not errors
	}

	// Both must be flat uint64 columns.
	if len(startCol.Uint64Values) == 0 || len(durCol.Uint64Values) == 0 {
		return nil, nil
	}

	// Flat columns are independently sorted by value, so position i in span:start
	// and position i in span:duration refer to different spans. Join on BlockRef
	// to pair the correct start and duration for each span.
	durByRef := make(map[shared.BlockRef]uint64, len(durCol.Uint64Values))
	for i, ref := range durCol.BlockRefs {
		durByRef[ref] = durCol.Uint64Values[i]
	}

	n := len(startCol.Uint64Values)
	col := &shared.IntrinsicColumn{
		Name:         "span:end",
		Type:         shared.ColumnTypeUint64,
		Format:       shared.IntrinsicFormatFlat,
		Count:        uint32(n), //nolint:gosec
		Uint64Values: make([]uint64, 0, n),
		BlockRefs:    make([]shared.BlockRef, 0, n),
	}
	for i, ref := range startCol.BlockRefs {
		dur, ok := durByRef[ref]
		if !ok {
			continue // span has start but no duration — skip
		}
		col.Uint64Values = append(col.Uint64Values, startCol.Uint64Values[i]+dur)
		col.BlockRefs = append(col.BlockRefs, ref)
	}
	col.Count = uint32(len(col.BlockRefs)) //nolint:gosec

	r.intrinsicMu.Lock()
	if r.intrinsicDecoded == nil {
		r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
	} else if existing, ok := r.intrinsicDecoded["span:end"]; ok {
		r.intrinsicMu.Unlock()
		return existing, nil
	}
	r.intrinsicDecoded["span:end"] = col
	r.intrinsicMu.Unlock()
	return col, nil
}

// IntrinsicBytesAt returns the bytes value for an intrinsic flat column at (blockIdx, rowIdx).
// Returns (nil, false) when the column is absent or the ref is not found.
//
// WARNING: O(total entries for this column) linear scan. Do NOT call in per-row loops.
// For batch access, use buildIntrinsicBlockIndex (writer package) or lookupIntrinsicFields
// (executor package) which build a hash index once per block.
func (r *Reader) IntrinsicBytesAt(name string, blockIdx, rowIdx int) ([]byte, bool) {
	col, err := r.GetIntrinsicColumn(name)
	if err != nil || col == nil {
		return nil, false
	}
	for i, ref := range col.BlockRefs {
		if int(ref.BlockIdx) == blockIdx && int(ref.RowIdx) == rowIdx && i < len(col.BytesValues) {
			return col.BytesValues[i], true
		}
	}
	return nil, false
}

// IntrinsicUint64At returns the uint64 value for an intrinsic flat column at (blockIdx, rowIdx).
// Returns (0, false) when the column is absent or the ref is not found.
//
// WARNING: O(total entries for this column) linear scan. Do NOT call in per-row loops.
// For batch access, use buildIntrinsicBlockIndex (writer package) or lookupIntrinsicFields
// (executor package) which build a hash index once per block.
func (r *Reader) IntrinsicUint64At(name string, blockIdx, rowIdx int) (uint64, bool) {
	col, err := r.GetIntrinsicColumn(name)
	if err != nil || col == nil {
		return 0, false
	}
	for i, ref := range col.BlockRefs {
		if int(ref.BlockIdx) == blockIdx && int(ref.RowIdx) == rowIdx && i < len(col.Uint64Values) {
			return col.Uint64Values[i], true
		}
	}
	return 0, false
}

// IntrinsicDictStringAt returns the string value for an intrinsic dict column at (blockIdx, rowIdx).
// Returns ("", false) when the column is absent or the ref is not found.
//
// WARNING: O(total entries for this column) linear scan. Do NOT call in per-row loops.
// For batch access, use buildIntrinsicBlockIndex (writer package) or lookupIntrinsicFields
// (executor package) which build a hash index once per block.
func (r *Reader) IntrinsicDictStringAt(name string, blockIdx, rowIdx int) (string, bool) {
	col, err := r.GetIntrinsicColumn(name)
	if err != nil || col == nil {
		return "", false
	}
	for _, entry := range col.DictEntries {
		for _, ref := range entry.BlockRefs {
			if int(ref.BlockIdx) == blockIdx && int(ref.RowIdx) == rowIdx {
				return entry.Value, true
			}
		}
	}
	return "", false
}

// IntrinsicDictInt64At returns the int64 value for an intrinsic dict column at (blockIdx, rowIdx).
// Returns (0, false) when the column is absent or the ref is not found.
//
// WARNING: O(total entries for this column) linear scan. Do NOT call in per-row loops.
// For batch access, use buildIntrinsicBlockIndex (writer package) or lookupIntrinsicFields
// (executor package) which build a hash index once per block.
func (r *Reader) IntrinsicDictInt64At(name string, blockIdx, rowIdx int) (int64, bool) {
	col, err := r.GetIntrinsicColumn(name)
	if err != nil || col == nil {
		return 0, false
	}
	for _, entry := range col.DictEntries {
		for _, ref := range entry.BlockRefs {
			if int(ref.BlockIdx) == blockIdx && int(ref.RowIdx) == rowIdx {
				return entry.Int64Val, true
			}
		}
	}
	return 0, false
}
