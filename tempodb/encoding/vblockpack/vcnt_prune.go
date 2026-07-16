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
// #508 Decision 3: this pure, store-independent predicate moved into blockpack root
// (vcnt.go:VCNTFileOverlapsRange, used by cube_backfill_runner.go's buildVCNTSection) so the
// moved cube backfill orchestration has no tempo-side dependency. Aliased here so
// modules/frontend/vcnt_fetch.go's call site (vblockpack.VCNTFileOverlapsRange) needs zero
// changes. See blockpack's own doc comment for the "unparseable name always returns true"
// safety rule this predicate enforces.
var VCNTFileOverlapsRange = blockpack.VCNTFileOverlapsRange
