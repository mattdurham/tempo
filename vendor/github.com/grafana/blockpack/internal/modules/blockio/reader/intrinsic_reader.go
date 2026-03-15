package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// parseIntrinsicTOC reads and parses the intrinsic column TOC from the v4 footer.
// Called during NewReaderFromProvider. For v3 footer files or files with no intrinsic
// section, this is a no-op.
func (r *Reader) parseIntrinsicTOC() error {
	if r.footerVersion != shared.FooterV4Version || r.intrinsicIndexLen == 0 {
		return nil
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
	sort.Strings(names)
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
	useProcessCache := r.fileID != ""
	if useProcessCache {
		procKey := r.fileID + "/intrinsic/" + name
		if cached, ok := parsedIntrinsicCache.Load(procKey); ok {
			col := cached.(*shared.IntrinsicColumn)
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
		parsedIntrinsicCache.Store(r.fileID+"/intrinsic/"+name, col)
	}

	if r.intrinsicDecoded == nil {
		r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
	}
	r.intrinsicDecoded[name] = col
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
