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

// GetIntrinsicColumn returns the decoded intrinsic column for the given name,
// or nil if the file has no intrinsic section or the column is not present.
// The column blob is read and decoded on first call; subsequent calls return the
// cached result (lazy decode, single I/O per column).
//
// NOT safe for concurrent use without external synchronization.
func (r *Reader) GetIntrinsicColumn(name string) (*shared.IntrinsicColumn, error) {
	if r.intrinsicIndex == nil {
		return nil, nil
	}
	meta, ok := r.intrinsicIndex[name]
	if !ok {
		return nil, nil
	}

	if r.intrinsicDecoded != nil {
		if cached, ok := r.intrinsicDecoded[name]; ok {
			return cached, nil
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

	if r.intrinsicDecoded == nil {
		r.intrinsicDecoded = make(map[string]*shared.IntrinsicColumn)
	}
	r.intrinsicDecoded[name] = col
	return col, nil
}
