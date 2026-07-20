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

// CompactedKeyChecker reports which of a candidate set of object keys are already marked
// compacted in blockpack_file_catalog (issue #522 Phase 1.5). VI's residual risk from a
// compacted-but-undeleted source is milder than VCNT's/cube's (identity-based dedup in
// StreamCompactBucketFiles already prevents double-counting) -- staleness/precedence, not
// double-counting -- so unlike buildVCNTSection's mandatory filter, this stays nil-tolerant:
// nil disables the filter entirely, preserving every existing caller's exact current behavior.
// *pgcatalog.Store satisfies this structurally; kept as a local interface (mirroring
// IndexStore/SourceExister's own shape) so this low-level, backend-agnostic package never gains a
// direct Postgres dependency.
type CompactedKeyChecker interface {
	ListCompactedKeys(ctx context.Context, keys []string) (map[string]struct{}, error)
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
	return discoverIndexFiles(
		ctx,
		lister,
		tenant,
		indexPrefix,
		colHash,
		colTypeName,
		queryMinSec,
		queryMaxSec,
		SortFileMetas,
		nil,
	)
}

// DiscoverIndexFilesFiltered mirrors DiscoverIndexFiles but additionally excludes any key
// checker reports as already compacted (issue #522 Phase 1.5) -- a new, opt-in sibling function,
// not a modification of DiscoverIndexFiles itself, mirroring this file's own established
// DiscoverIndexFilesNewestFirst precedent (every existing caller's behavior must stay byte-for-
// byte unchanged). checker may be nil, disabling the filter (identical to DiscoverIndexFiles).
func DiscoverIndexFilesFiltered(
	ctx context.Context,
	lister Lister,
	tenant, indexPrefix, colHash, colTypeName string,
	queryMinSec, queryMaxSec uint64,
	checker CompactedKeyChecker,
) ([]string, error) {
	return discoverIndexFiles(
		ctx,
		lister,
		tenant,
		indexPrefix,
		colHash,
		colTypeName,
		queryMinSec,
		queryMaxSec,
		SortFileMetas,
		checker,
	)
}

// DiscoverIndexFilesNewestFirst mirrors DiscoverIndexFiles but returns keys sorted by
// SortFileMetasNewestFirst instead of SortFileMetas -- lowest compaction level (freshest) still
// takes priority, but within a level, newest wall-clock time first. Added as a SIBLING function
// -- DiscoverIndexFiles itself is never modified, since every existing non-early-stopping caller
// must keep today's ascending order unchanged.
func DiscoverIndexFilesNewestFirst(
	ctx context.Context,
	lister Lister,
	tenant, indexPrefix, colHash, colTypeName string,
	queryMinSec, queryMaxSec uint64,
) ([]string, error) {
	return discoverIndexFiles(
		ctx,
		lister,
		tenant,
		indexPrefix,
		colHash,
		colTypeName,
		queryMinSec,
		queryMaxSec,
		SortFileMetasNewestFirst,
		nil,
	)
}

// discoverIndexFiles is the shared implementation behind DiscoverIndexFiles/
// DiscoverIndexFilesFiltered and DiscoverIndexFilesNewestFirst -- identical List/parse/filter
// logic, differing only in which sort function orders the final key list and whether checker
// (nil-tolerant, issue #522 Phase 1.5) excludes already-compacted keys before download.
func discoverIndexFiles(
	ctx context.Context,
	lister Lister,
	tenant, indexPrefix, colHash, colTypeName string,
	queryMinSec, queryMaxSec uint64,
	sortFunc func([]FileMeta),
	checker CompactedKeyChecker,
) ([]string, error) {
	prefix := path.Join(tenant, indexPrefix, colHash, colTypeName) + "/"
	keys, err := lister.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	candidates := make([]FileMeta, 0, len(keys))
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
		candidates = append(candidates, meta)
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	matches := candidates
	if checker != nil {
		candidateKeys := make([]string, len(candidates))
		for i, m := range candidates {
			candidateKeys[i] = m.Filename
		}
		compacted, checkErr := checker.ListCompactedKeys(ctx, candidateKeys)
		if checkErr != nil {
			// A catalog query failure must not silently defeat the filter by falling
			// through to an unfiltered result -- fail closed like buildVCNTSection's
			// own mandatory-filter contract, even though this filter is only "milder".
			return nil, checkErr
		}
		matches = make([]FileMeta, 0, len(candidates))
		for _, m := range candidates {
			if _, isCompacted := compacted[m.Filename]; isCompacted {
				continue
			}
			matches = append(matches, m)
		}
	}
	if len(matches) == 0 {
		return nil, nil
	}

	sortFunc(matches)

	out := make([]string, len(matches))
	for i := range matches {
		out[i] = matches[i].Filename
	}
	return out, nil
}
