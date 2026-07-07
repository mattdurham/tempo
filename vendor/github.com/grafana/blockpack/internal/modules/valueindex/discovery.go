package valueindex

// discovery.go — value-index file discovery (NOTE-VI-032, issue #458).
//
// DiscoverIndexFiles turns an object-storage Lister into the set of value-index
// file keys for a single (tenant, colHash, colType) that overlap a query time
// window. It is the bridge between the on-disk layout written by the consumer
// (<tenant>/<indexPrefix>/<colHash>/<colTypeName>/L<level>-<min>-<max>-<id>.blockpack)
// and the pure in-memory QueryFiles execution path.
//
// This performs a live S3 LIST per call; an in-process listing cache (issue #462)
// is the follow-up optimisation that wraps it.

import (
	"context"
	"path"
)

// Lister lists object keys with a given prefix. blockpack.WritableStorage and the
// valueindexcompactor.IndexStore both satisfy this interface.
type Lister interface {
	// List returns the full keys of all objects whose name begins with prefix.
	List(ctx context.Context, prefix string) ([]string, error)
}

// DiscoverIndexFiles lists all value-index files for a single
// (tenant, colHash, colTypeName) that overlap the query time window
// [queryMinSec, queryMaxSec]. Keys are returned sorted by
// (Level ASC, WallMinSec ASC, WallMaxSec ASC) — lowest compaction level first —
// so callers traverse the freshest, least-merged files before compacted ones.
//
// Legacy v1 filenames (L<level>-<id>, no embedded time range) fail ParseFilenameV2
// and are skipped like any other unparseable key — the v1 fallback was removed.
//
// When no files overlap the window, returns (nil, nil).
func DiscoverIndexFiles(
	ctx context.Context,
	lister Lister,
	tenant, indexPrefix, colHash, colTypeName string,
	queryMinSec, queryMaxSec uint64,
) ([]string, error) {
	prefix := path.Join(tenant, indexPrefix, colHash, colTypeName) + "/"
	keys, err := lister.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	matches := make([]FileMeta, 0, len(keys))
	for _, key := range keys {
		meta, parseErr := ParseFilenameV2(path.Base(key))
		if parseErr != nil {
			// Skip keys that are not value-index files (e.g. unexpected objects
			// sharing the prefix). A malformed name cannot be queried anyway.
			continue
		}
		if !meta.IsInTimeRange(queryMinSec, queryMaxSec) {
			continue
		}
		// Preserve the full key so the caller can Get/Download it directly;
		// FileMeta only carries the leaf name from ParseFilenameV2.
		meta.Filename = key
		matches = append(matches, meta)
	}

	if len(matches) == 0 {
		return nil, nil
	}

	SortFileMetas(matches)

	out := make([]string, len(matches))
	for i := range matches {
		out[i] = matches[i].Filename
	}
	return out, nil
}
