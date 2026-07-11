package vblockpack

// vcnt_prune.go — shared, pure filename-decision helper for tempo-side VCNT listing-time
// pruning (issue #495, follow-up to #494's v2-ranged VCNT filenames). Called from both
// cube_backfill.go:buildVCNTSection (querier/cube-backfill path) and
// modules/frontend/vcnt_fetch.go:fetchVCNTSection (frontend plan-time path) — see each
// call site's own doc comment for why their surrounding fetch loops stay unmerged; only this
// small, store-independent predicate is shared between them.

import (
	blockpack "github.com/grafana/blockpack"
)

// VCNTFileOverlapsRange reports whether the .vcnt file named name should be fetched to answer
// a query over [minSec, maxSec] (argument order matches blockpack.VCNTFileMeta.IsInTimeRange's
// own (queryMinSec, queryMaxSec) order — do not swap).
//
// A v1-shaped or otherwise unparseable name (blockpack.VCNTParseFilenameV2 returns an error)
// ALWAYS returns true: unknown range means always fetch, never drop. This is an unconditional,
// hard-coded safety rule, not a tunable — it deliberately avoids repeating blockpack's own
// valueindex/discovery.go mistake (NOTE-VI-030) of treating "I don't know this file's range"
// as "skip it," which silently and permanently drops pre-v2-format files from ever being
// considered.
func VCNTFileOverlapsRange(name string, minSec, maxSec uint64) bool {
	meta, err := blockpack.VCNTParseFilenameV2(name)
	if err != nil {
		return true
	}
	return meta.IsInTimeRange(minSec, maxSec)
}
