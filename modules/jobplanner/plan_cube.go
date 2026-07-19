package jobplanner

// plan_cube.go — issue #518: polls cube_entries for cubes that have completed
// at least one L0 backfill pass, chain-enqueuing the next bounded-window
// cube_backfill job for each. cube has no triggered/done boolean columns
// (unlike viusage) -- "has an L0 watermark at all" (watermarks -> '1' IS NOT
// NULL, RollupL0 == 1) is the existing hasL0 heuristic already used at
// cubequerypath.go's OnCreateAttempt call site, reused here for consistency.

import (
	"context"
	"encoding/json"
	"fmt"

	blockpack "github.com/grafana/blockpack"

	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

// cubeBackfillInserter is the minimal seam planCubeRow needs -- lets
// TestPlanCubeRow_* exercise the query-row-to-Insert-call mapping against a
// fake, without a real Postgres connection. *jobstore.Store satisfies this in
// production.
type cubeBackfillInserter interface {
	InsertCubeBackfill(ctx context.Context, tenant string, d jobstore.CubeBackfillDetail) error
}

// cubeEntriesPlanQuery finds cubes with at least one confirmed L0 watermark
// (schema: vendor/github.com/grafana/blockpack/internal/modules/cube/schema.sql;
// '1' is blockpack.CubeRollupL0's JSON map key, matching Go's default
// map[uint32]V JSON encoding).
const cubeEntriesPlanQuery = `
	SELECT cube_id, tenant, watermarks
	FROM cube_entries
	WHERE watermarks -> '1' IS NOT NULL`

// planCubeRow enqueues the next bounded-window cube_backfill job for
// (tenant, cubeID).
func planCubeRow(ctx context.Context, inserter cubeBackfillInserter, cfg common.JobPlannerConfig, tenant, cubeID string) error {
	return inserter.InsertCubeBackfill(ctx, tenant, jobstore.CubeBackfillDetail{
		CubeID:        cubeID,
		WindowMinutes: cfg.CubeWindowMinutes,
	})
}

// planCube runs cubeEntriesPlanQuery against the real database and
// chain-enqueues a job per candidate cube whose L0 watermark hasn't reached
// minute 0. A single row's scan/decode/insert failure is recorded but does
// not stop the remaining rows in this tick from being planned.
func (s *Service) planCube(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, cubeEntriesPlanQuery)
	if err != nil {
		return fmt.Errorf("jobplanner: query cube_entries: %w", err)
	}
	defer rows.Close()

	var firstErr error
	for rows.Next() {
		var (
			cubeID, tenant string
			watermarksRaw  []byte
		)
		if scanErr := rows.Scan(&cubeID, &tenant, &watermarksRaw); scanErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("jobplanner: scan cube_entries row: %w", scanErr)
			}
			continue
		}

		watermarks := map[uint32]blockpack.CubeResolutionWatermark{}
		if len(watermarksRaw) > 0 {
			if decErr := json.Unmarshal(watermarksRaw, &watermarks); decErr != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("jobplanner: decode watermarks for cube %q: %w", cubeID, decErr)
				}
				continue
			}
		}
		wm, ok := watermarks[blockpack.CubeRollupL0]
		if !ok || wm.MinMinute == 0 {
			// Accepted limitation (plan.md Part 3.3): a cube whose L0 watermark
			// has genuinely reached minute 0 (very old, near-retention-boundary
			// data) is indistinguishable here from "still on its first pass" --
			// a rare, harmless wasted poll cycle, not a correctness bug.
			continue
		}

		if insErr := planCubeRow(ctx, s.jobStore, s.cfg, tenant, cubeID); insErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("jobplanner: insert cube_backfill for tenant %q cube %q: %w", tenant, cubeID, insErr)
			}
			continue
		}
		metricCubesPlanned.Inc()
	}
	if rows.Err() != nil {
		return fmt.Errorf("jobplanner: iterate cube_entries: %w", rows.Err())
	}
	return firstErr
}
