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

// parseIntrinsicTOC reads and parses the intrinsic column TOC from the v4 footer.
// Called during NewReaderFromProvider. For v3 footer files or files with no intrinsic
// section, this is a no-op.
// NOTE-003: the parsed TOC map is cached in parsedIntrinsicTOCCache (GC-cooperative)
// to avoid re-decoding the blob on every NewReaderFromProvider call.
func (r *Reader) parseIntrinsicTOC() error {
	if r.footerVersion != shared.FooterV4Version || r.intrinsicIndexLen == 0 {
		return nil
	}

	// Check process-level TOC cache first.
	if r.fileID != "" {
		tocKey := r.fileID + "/intrinsic/toc"
		if cached := parsedIntrinsicTOCCache.Get(tocKey); cached != nil {
			r.tocPin = cached // keep weak cache entry alive for lifetime of this Reader
			// r.intrinsicIndex aliases cached.entries — map is immutable after construction.
			r.intrinsicIndex = cached.entries
			return nil
		}
	}

	blob, err := r.readRange(r.intrinsicIndexOffset, uint64(r.intrinsicIndexLen), rw.DataTypeMetadata)
	if err != nil {
		return fmt.Errorf("parseIntrinsicTOC: read: %w", err)
	}

	entries, err := shared.DecodeTOC(blob)
	if err != nil {
		return fmt.Errorf("parseIntrinsicTOC: decode: %w", err)
	}

	r.intrinsicIndex = make(map[string]shared.IntrinsicColMeta, len(entries))
	for _, e := range entries {
		r.intrinsicIndex[e.Name] = e
	}

	// Store parsed TOC in process-level cache.
	if r.fileID != "" {
		toc := &intrinsicTOC{entries: r.intrinsicIndex}
		if err := parsedIntrinsicTOCCache.Put(r.fileID+"/intrinsic/toc", toc); err != nil {
			return fmt.Errorf("parseIntrinsicTOC: cache: %w", err)
		}
		r.tocPin = toc // keep weak cache entry alive for lifetime of this Reader
	}

	return nil
}

// HasIntrinsicSection reports whether the file has a v4 footer with a non-empty
// intrinsic column section.
func (r *Reader) HasIntrinsicSection() bool {
	return len(r.intrinsicIndex) > 0
}

// IntrinsicColumnMeta returns the TOC metadata for the named intrinsic column.
// Returns (IntrinsicColMeta{}, false) if no intrinsic section is present or the
// column is not in the TOC. This is cheap — no I/O.
func (r *Reader) IntrinsicColumnMeta(name string) (shared.IntrinsicColMeta, bool) {
	if r.intrinsicIndex == nil {
		return shared.IntrinsicColMeta{}, false
	}
	meta, ok := r.intrinsicIndex[name]
	return meta, ok
}

// IntrinsicColumnNames returns the names of all intrinsic columns in the TOC,
// sorted alphabetically. Returns nil if no intrinsic section is present.
func (r *Reader) IntrinsicColumnNames() []string {
	if len(r.intrinsicIndex) == 0 {
		return nil
	}
	names := make([]string, 0, len(r.intrinsicIndex))
	for n := range r.intrinsicIndex {
		names = append(names, n)
	}
	slices.Sort(names)
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
	cacheKey := fmt.Sprintf("%s/intrinsic/%s", r.fileID, name)
	blob, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		return r.readRange(meta.Offset, uint64(meta.Length), rw.DataTypeMetadata)
	})
	if err != nil {
		return nil, fmt.Errorf("GetIntrinsicColumnBlob %q: read: %w", name, err)
	}
	return blob, nil
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
func (r *Reader) GetIntrinsicColumn(name string) (*shared.IntrinsicColumn, error) {
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

	if r.intrinsicDecoded != nil {
		if cached, ok := r.intrinsicDecoded[name]; ok {
			return cached, nil
		}
	}

	// Check process-level cache first — decoded IntrinsicColumn is immutable once written.
	// Guard: only use process-level cache when fileID is non-empty to prevent cross-file collisions.
	// NOTE-003: process-level cache uses objectcache.Cache (GC-cooperative weak pointers)
	// instead of sync.Map with strong refs to allow GC to reclaim decoded columns.
	useProcessCache := r.fileID != ""
	if useProcessCache {
		procKey := r.fileID + "/intrinsic/" + name
		if col := parsedIntrinsicCache.Get(procKey); col != nil {
			if r.intrinsicDecoded == nil {
				r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
			}
			r.intrinsicDecoded[name] = col
			return col, nil
		}
	}

	cacheKey := fmt.Sprintf("%s/intrinsic/%s", r.fileID, name)
	blob, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
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

	if r.intrinsicDecoded == nil {
		r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
	}
	r.intrinsicDecoded[name] = col
	return col, nil
}

// GetIntrinsicColumnForRefs returns a partial IntrinsicColumn containing only pages that
// cover at least one of the target refs. This is the page-skipping optimized path for
// reverse lookups: instead of decoding the entire column (O(N_file) rows), it decodes
// only pages whose ref-range index (MinRef/MaxRef/RefBloom) matches a target ref.
//
// refs is a slice of BlockRef identifying the target rows. Returns (nil, nil) when:
//   - refs is nil or empty (no rows needed)
//   - the file has no intrinsic section
//   - the named column is not in the TOC
//
// The returned column may contain rows from pages adjacent to the matched pages (bloom
// false positives). Callers must still verify ref membership when consuming results.
//
// Results are NOT cached (they are query-scoped partial columns). The blob cache IS used
// for the raw column data to avoid redundant I/O.
func (r *Reader) GetIntrinsicColumnForRefs(name string, refs []shared.BlockRef) (*shared.IntrinsicColumn, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if r.intrinsicIndex == nil {
		return nil, nil
	}
	if _, ok := r.intrinsicIndex[name]; !ok {
		return nil, nil
	}

	blob, err := r.GetIntrinsicColumnBlob(name)
	if err != nil {
		return nil, fmt.Errorf("GetIntrinsicColumnForRefs %q: %w", name, err)
	}
	if blob == nil {
		return nil, nil
	}

	// Build the ref filter map from the provided refs.
	refFilter := make(map[uint32]struct{}, len(refs))
	for _, ref := range refs {
		refFilter[uint32(ref.BlockIdx)<<16|uint32(ref.RowIdx)] = struct{}{}
	}

	col, err := shared.DecodePagedColumnBlobFiltered(blob, refFilter)
	if err != nil {
		return nil, fmt.Errorf("GetIntrinsicColumnForRefs %q: decode: %w", name, err)
	}
	if col != nil {
		col.Name = name
	}
	return col, nil
}

// synthesizeSpanEnd builds a span:end intrinsic column from span:start + span:duration.
// The result is cached like any other intrinsic column.
func (r *Reader) synthesizeSpanEnd() (*shared.IntrinsicColumn, error) {
	if r.intrinsicDecoded != nil {
		if cached, ok := r.intrinsicDecoded["span:end"]; ok {
			return cached, nil
		}
	}

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

	if r.intrinsicDecoded == nil {
		r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
	}
	r.intrinsicDecoded["span:end"] = col
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
