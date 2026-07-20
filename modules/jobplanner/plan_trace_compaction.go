package jobplanner

// plan_trace_compaction.go — issue #522 Phase 4c(d)/#158: job-planner's own poll loop selects
// pairwise trace/span compaction candidates from tempo's own `file_catalog` table.
// Vblockpack-encoded tenants only, by construction: filecatalog.Lister's own reconciliation
// (modules/backendscheduler/filecatalog/lister.go) now skips any block whose Version isn't
// "vblockpack" (issue #522 #158's own fix -- file_catalog never contains a vparquet/standard
// row at all), so no extra WHERE clause is needed here to enforce plan.md Phase 4c's "vparquet/
// standard tenants' compaction is completely untouched, forever" scoping.
//
// Grouping (per brainstormer-522's confirmed guidance): group candidates by (tenant,
// time-window) ONLY, mirroring tempodb/blockselector/compaction_block_selector.go's own window
// computation -- compaction_level is deliberately NOT part of the grouping key yet, since
// nothing (not even filecatalog.Lister's reconciliation) writes it for real today; grouping by
// an always-0 column would look level-aware without actually being so (a fresh L0 block and a
// once-merged L1 output, both landing at level=0, would be indistinguishable -- worse than not
// grouping by level at all). Pairwise-merging any 2 same-tenant, same-window blocks is correct
// regardless of their prior merge depth: there is no cube-style boundary invariant here
// requiring level-homogeneous merging, only an efficiency concern (balanced merge-tree shape)
// deferred to a follow-up once #159 ships real level-tracking. The 1GiB terminal-leaf size
// cutoff is unaffected either way, since it's gated on size_bytes, not compaction_level.
//
// Additionally excludes any tenant with a live tenant_redaction_state.pending=TRUE row (#152)
// via an anti-join -- closes the redaction/compaction race: a tenant mid-redaction-batch must
// not have its blocks pairwise-merged out from under the batch's own trace-ID scan.

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

const (
	// traceCompactionWindowSeconds groups candidates by wall-clock window, mirroring
	// timeWindowBlockSelector's own windowForTime (tempodb/blockselector/
	// compaction_block_selector.go: t.Unix() / int64(MaxCompactionRange/time.Second)) -- 1 hour
	// matches CompactorConfig's own MaxCompactionRange default.
	traceCompactionWindowSeconds = int64(time.Hour / time.Second)

	// traceCompactionGlobalSizeCutoffBytes is plan.md Phase 4b/#151's 1GiB terminal-leaf
	// cutoff: once a block reaches this size, it is permanently excluded from further
	// pairwise merging (mirrors DefaultMaxBlockBytes' own value, tempodb/config.go).
	traceCompactionGlobalSizeCutoffBytes = int64(1 * 1024 * 1024 * 1024)
)

// traceCompactionCandidateQuery selects every live, under-cutoff file_catalog row for a tenant
// with no pending redaction batch, ordered so groupTraceCompactionPairs can group contiguous
// rows by (tenant, window) in one linear pass. compacted_at IS NULL is included for forward
// compatibility with #159 (nothing sets it for trace/span rows yet, so this clause is currently
// always-true, not yet load-bearing).
const traceCompactionCandidateQuery = `
	SELECT tenant, block_id, start_sec, size_bytes
	FROM file_catalog
	WHERE deleted_at IS NULL AND compacted_at IS NULL AND size_bytes < $1
	  AND tenant NOT IN (SELECT tenant FROM tenant_redaction_state WHERE pending)
	ORDER BY tenant, start_sec, block_id`

type traceCompactionCandidateRow struct {
	tenant    string
	blockID   string
	startSec  int64
	sizeBytes int64
}

// planTraceCompaction queries file_catalog for eligible candidates, groups them into
// same-tenant, same-window pairs, and inserts one trace_compaction job per pair found.
func (s *Service) planTraceCompaction(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, traceCompactionCandidateQuery, traceCompactionGlobalSizeCutoffBytes)
	if err != nil {
		return fmt.Errorf("jobplanner: query trace_compaction candidates: %w", err)
	}
	defer rows.Close()

	var candidates []traceCompactionCandidateRow
	for rows.Next() {
		var c traceCompactionCandidateRow
		if scanErr := rows.Scan(&c.tenant, &c.blockID, &c.startSec, &c.sizeBytes); scanErr != nil {
			return fmt.Errorf("jobplanner: scan trace_compaction candidate: %w", scanErr)
		}
		candidates = append(candidates, c)
	}
	if rows.Err() != nil {
		return fmt.Errorf("jobplanner: iterate trace_compaction candidates: %w", rows.Err())
	}

	var firstErr error
	for _, pair := range groupTraceCompactionPairs(candidates) {
		if insErr := s.jobStore.InsertTraceCompaction(ctx, pair.tenant, jobstore.TraceCompactionDetail{
			InputBlockIDs: []string{pair.blockIDA, pair.blockIDB},
		}); insErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("jobplanner: insert trace_compaction for tenant %q: %w", pair.tenant, insErr)
			}
			continue
		}
		metricTraceCompactionsPlanned.Inc()
	}
	return firstErr
}

type traceCompactionPair struct {
	tenant             string
	blockIDA, blockIDB string
}

// groupTraceCompactionPairs groups candidates (already ordered by tenant, start_sec, block_id)
// into (tenant, window) groups and emits one pair for the first 2 rows of any group with >= 2 --
// mirrors timeWindowBlockSelector's own stripe-growth shape, minus the level dimension (see this
// file's own doc comment for why).
func groupTraceCompactionPairs(candidates []traceCompactionCandidateRow) []traceCompactionPair {
	type groupKey struct {
		tenant string
		window int64
	}
	grouped := make(map[groupKey][]traceCompactionCandidateRow)
	var order []groupKey
	for _, c := range candidates {
		key := groupKey{tenant: c.tenant, window: c.startSec / traceCompactionWindowSeconds}
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], c)
	}

	var pairs []traceCompactionPair
	for _, key := range order {
		group := grouped[key]
		if len(group) < 2 {
			continue
		}
		pairs = append(pairs, traceCompactionPair{tenant: key.tenant, blockIDA: group[0].blockID, blockIDB: group[1].blockID})
	}
	return pairs
}
