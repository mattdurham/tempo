package reader

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
// NOTE-449: CacheStats accumulates per-section cache hit/miss counts for one block read.
// SPEC-OBS-004: aggregate counts avoid per-fetch span overhead (see root SPEC-OBS-004).
// SPEC-OBS-005: fetched-flag pattern in readBlockColumnarWithCache populates these counts.
// Passed as a nil-safe pointer into readBlockColumnarWithCache so the caller (executor's
// processGroup) can attach aggregate counts to the blockpack.block OTel span without
// per-fetch span overhead. nil pointer means no counting — safe to omit on any call path.
// Zero-value is the correct initial state (all zeros, no alloc needed).

const (
	// CacheStatsSectionToc is the index for block ToC section hits/misses.
	CacheStatsSectionToc = 0
	// CacheStatsSectionCol is the index for block column section hits/misses.
	CacheStatsSectionCol = 1
	// CacheStatsCount is the number of tracked sections.
	CacheStatsCount = 2
)

// CacheStats accumulates section-level cache hit/miss counts for one block read.
// Designed to be stack-allocated in the caller and passed by pointer.
type CacheStats struct {
	Hits   [CacheStatsCount]int32
	Misses [CacheStatsCount]int32
}
