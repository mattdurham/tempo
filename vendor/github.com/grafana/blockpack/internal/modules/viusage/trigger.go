package viusage

// NOTE: SPEC-VIUSAGE-003 — RecordUseAndMaybeTrigger implements R4's repeated-use trigger
// (record + evaluate + lease-acquire in one conditional-PUT pass) and R8's lease
// acquire/renew/expire lifecycle (4.4). A column, once Triggered, never reverts to
// untriggered (R5: no eviction) — but an expired, unreleased lease (R8's crash-self-heal
// case) still allows a fresh call to re-acquire the lease and signal the caller to
// (re)launch a backfill job, distinct from a first-time trigger.

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// TriggerConfig parameterises the repeated-use trigger (R4). Default values (Threshold=1,
// WindowSeconds=3600, LeaseTTLSeconds=1800 — Threshold=1 per explicit team-lead ruling
// 2026-07-11: any query against a non-dedicated column is worth indexing immediately, no
// repeated-use gate; WindowSeconds/LeaseTTLSeconds remain R8's documented, unmeasured
// starting defaults, plan.md Section 4.2) are wired in by A6's config plumbing (task #109),
// which is this struct's actual construction site; no defaulting helper lives here since
// nothing in this package would call it.
type TriggerConfig struct {
	// Threshold is the number of distinct uses within WindowSeconds required to fire a
	// backfill. Default 1 (fires on first use, per explicit team-lead ruling — any query
	// against a non-dedicated column is worth indexing immediately).
	Threshold int
	// WindowSeconds is the rolling window uses are counted over. Default 3600 (1h).
	WindowSeconds uint64
	// LeaseTTLSeconds bounds how long a BackfillInProgress lease is honored before a
	// second replica may re-acquire it (R8). Default 30 minutes — long enough to cover
	// a real 48h/4-worker backfill's slowest expected single unit of work with margin,
	// short enough that a crashed job self-heals within a bounded, observable time.
	LeaseTTLSeconds uint64
}

// TriggerResult is RecordUseAndMaybeTrigger's outcome.
type TriggerResult struct {
	Entry Entry
	// ShouldBackfill is true when the caller must (re)launch a backfill job for Entry —
	// either a first-time threshold crossing, or an R8 crash-self-heal re-acquisition of
	// an expired lease for an already-Triggered, not-yet-Done column.
	ShouldBackfill bool
}

// processLeaseOwnerID identifies this process for BackfillState.LeaseOwnerID (4.1:
// observability only, not used for correctness — TTL expiry is).
var processLeaseOwnerID = func() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}()

// pruneUseTimestamps keeps only timestamps within [nowSec-windowSeconds, nowSec], then
// further bounds the result to the most recent MaxTrackedUses entries (belt-and-braces
// backstop against pathologically large windows/bursts, per 4.1's UseTimestamps doc).
// ts is reused in place (its backing array), matching Load()'s per-call fresh-copy
// semantics — safe to mutate.
func pruneUseTimestamps(ts []uint64, nowSec, windowSeconds uint64) []uint64 {
	var windowStart uint64
	if nowSec > windowSeconds {
		windowStart = nowSec - windowSeconds
	}
	out := ts[:0]
	for _, t := range ts {
		if t >= windowStart {
			out = append(out, t)
		}
	}
	if len(out) > MaxTrackedUses {
		out = out[len(out)-MaxTrackedUses:]
	}
	return out
}

// RecordUseAndMaybeTrigger appends one usage timestamp for (tenant, colName, colType)
// and, in the SAME conditional-PUT retry pass, evaluates the repeated-use threshold and
// — if crossed and no unexpired lease is already held — acquires the backfill lease and
// marks Triggered=true, returning enough information for the caller (tempo) to launch a
// backfill job. Combining record+evaluate+lease-acquire into one PUT avoids a
// record-then-separately-check race between concurrent callers and halves the number of
// registry round-trips relative to cube's separate maybeCreateCube/TryCreate calls.
//
// Returns TriggerResult{ShouldBackfill: true, Entry: ...} the FIRST time (and only the
// first time, across all callers/replicas, thanks to the lease) the threshold is crossed
// for this column. A caller whose own call did not win the lease (another replica's
// concurrent call did) gets ShouldBackfill=false with the same Entry — idempotent,
// mirroring cube's Created-vs-already-existed distinction.
//
// R8 crash self-heal: once Triggered, this function never re-evaluates the threshold
// (R5: no eviction — a triggered column stays triggered forever), but it DOES check
// whether the entry's lease is still valid. If Triggered=true, Done=false, and the lease
// has expired (BackfillInProgress=false, or LeaseExpiresAt <= now — the crashed-worker
// case), it re-acquires the lease and returns ShouldBackfill=true again so the caller
// relaunches the backfill job. If Done=true, or the lease is currently held and
// unexpired, ShouldBackfill is always false.
func RecordUseAndMaybeTrigger(
	ctx context.Context,
	registry *Registry,
	tenant, colName, colType string,
	now time.Time,
	cfg TriggerConfig,
) (TriggerResult, error) {
	colHash := valueindex.ColHash(colName)
	nowSec := uint64(now.Unix()) //nolint:gosec // unix seconds fits uint64 for any realistic timestamp
	var shouldBackfill bool

	entry, err := registry.updateEntryWithRetry(
		ctx, tenant, colHash, colType,
		func() Entry {
			return Entry{
				Tenant:       tenant,
				ColumnHash:   colHash,
				ColumnName:   colName,
				ColumnType:   colType,
				FirstSeenSec: nowSec,
				CreatedAt:    nowSec,
			}
		},
		func(e *Entry) error {
			shouldBackfill = false
			e.UseTimestamps = pruneUseTimestamps(append(e.UseTimestamps, nowSec), nowSec, cfg.WindowSeconds)

			switch {
			case e.Backfill.Done:
				// R5: fully backfilled already — never re-trigger.
			case e.Backfill.Triggered:
				if e.Backfill.BackfillInProgress && e.Backfill.LeaseExpiresAt > nowSec {
					break // another owner holds an active lease — in progress, nothing to do
				}
				// R8: Triggered but the lease is absent/expired — crash self-heal.
				acquireLease(e, nowSec, cfg.LeaseTTLSeconds)
				shouldBackfill = true
			case len(e.UseTimestamps) >= cfg.Threshold:
				e.Backfill.Triggered = true
				acquireLease(e, nowSec, cfg.LeaseTTLSeconds)
				shouldBackfill = true
			}
			return nil
		},
	)
	if err != nil {
		return TriggerResult{}, err
	}
	return TriggerResult{Entry: entry, ShouldBackfill: shouldBackfill}, nil
}

// acquireLease sets e's BackfillState lease fields (4.4 step 1/4: acquire or re-acquire).
func acquireLease(e *Entry, nowSec, leaseTTLSeconds uint64) {
	e.Backfill.BackfillInProgress = true
	e.Backfill.LeaseExpiresAt = nowSec + leaseTTLSeconds
	e.Backfill.LeaseOwnerID = processLeaseOwnerID
}
