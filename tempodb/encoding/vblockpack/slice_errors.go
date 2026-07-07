package vblockpack

import "errors"

// ErrSliceIndexCoverageGap is returned by a #487 time-slice job (opts.IndexOnly / req.IndexOnly)
// when the value index would otherwise routinely decline to answer the query (no coverage for
// a leaf, an unindexable/negation predicate, a non-filter query, or the index reader being
// unconfigured). A slice job's narrowed [Start, End) window means a full block scan is not a
// safe fallback here the way it is for a normal (non-slice) query: a scan would ignore the
// slice's narrow window at the per-span level and could double-count or over-fetch across
// overlapping slice jobs dispatched for the same block. This is distinct from an index/data
// inconsistency (the index HAD coverage but named a block/page the data file cannot resolve,
// NOTE-VI-078) — that is index corruption; this is "the index architecturally cannot answer,
// and this job is not allowed to fall back."
var ErrSliceIndexCoverageGap = errors.New("vblockpack: slice-mode index coverage gap")
