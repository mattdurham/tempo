package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import "time"

// RecentFirstBudget mirrors blockpack.RecentFirstBudget (root package, queryoptions.go).
// Duplicated here — not imported — because internal/modules/executor is a lower-level
// package the root blockpack package imports, so it cannot reference the root type without
// an import cycle. api.go copies field-by-field when building CollectOptions/Options from
// the public QueryOptions.RecentFirstBudget, mirroring the existing pattern of copying
// opts.Limit/opts.StartBlock/etc. field-by-field across that same boundary. Task 3's bounded
// structural path (executor.Options) reuses THIS type — same package, no duplication.
//
// NOTE-VI-099: rationale for the truncate-before-coalesce and concurrent-overshoot-is-acceptable
// design choices below.
//
// Activates the bounded, pointed newest-first execution strategy (issue #481 part 2): blocks
// are read newest-first (Direction=Backward, WantSort=false) and reading stops at the FIRST
// cap reached — Limit matches found, MaxBlocks blocks read, MaxBytes bytes read, or
// MaxDuration elapsed. A zero field means "no cap on that dimension." MaxBlocks is enforced
// EXACTLY: scanBlocks truncates the already-reversed (newest-first) SelectedBlocks list to
// MaxBlocks entries BEFORE coalescing, so at most MaxBlocks blocks are ever read regardless of
// coalescing group boundaries (team-lead ruling R14). MaxBytes/MaxDuration are checked once per
// coalesced GROUP (not per block or per row) after that group's blocks finish processing.
// R14-AMENDED: because blockGroupPipeline dispatches up to defaultPipelineWorkers (8) groups'
// I/O CONCURRENTLY via a pre-filled semaphore (it does not wait for a per-group budget decision
// before issuing the next group's I/O), the actual overshoot bound is
// min(remaining groups, defaultPipelineWorkers) groups' worth of I/O, not a flat "one group" —
// for a file with <= 8 total groups, ALL of them may already be in flight before any check can
// act. This is acceptable: the caps exist to prevent the catastrophic unbounded-read class
// (multi-hundred-second/OOM queries), and a <=8-group file is inherently small (<= ~64MB of
// coalesced reads) — the overshoot ceiling is structurally bounded exactly where it's cheapest.
// The RESULT SET is unaffected by this either way and remains an exact, concurrency-independent
// bound: results only ever reflect groups whose processGroup call completed before a stop fired
// (processGroup runs strictly in ascending group order), so a group whose I/O was wastefully
// prefetched but never reached processGroup never contributes rows to the answer. Callers must
// report ACTUAL bytes/blocks read from QueryStats (never the configured budget values) so this
// overshoot is visible to the user, not hidden behind the cap number.
//
// SPEC-ROOT-023: executor-package-local mirror, import-cycle workaround, zero new invariant —
// see the root blockpack.RecentFirstBudget (queryoptions.go) for the authoritative spec entry.
type RecentFirstBudget struct {
	MaxBlocks   int
	MaxBytes    int64
	MaxDuration time.Duration
}
