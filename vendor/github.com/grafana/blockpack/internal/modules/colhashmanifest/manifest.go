package colhashmanifest

// NOTE-COLMANIFEST-1: this manifest is purely additive, best-effort observability -- it
// must NEVER be consulted by any VI/VCNT read/write/query code path, and a failure to
// update it must NEVER fail the real write it piggybacks on. See NOTES.md.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// manifestOpTimeout bounds every Store call this package issues (the Get inside Load and the
// Put inside RecordColumn) with its own short, independent deadline, derived from whatever
// context the caller supplies rather than inherited unbounded (SPEC-COLMANIFEST-5,
// NOTE-COLMANIFEST-2). Load and RecordColumn are invoked synchronously, inline, from
// valueindexconsumer's flushColumn and valuecountscompactor's mergeLevel -- both run on their
// service's single processing goroutine, so a Store implementation that HANGS (as opposed to
// erroring quickly) must never be able to block the real VI flush / VCNT compaction pipeline
// indefinitely. A few seconds is generous for a same-region object-storage round trip while
// still bounding the worst case to a small, fixed delay per call.
const manifestOpTimeout = 3 * time.Second

// Source identifies which pipeline first observed a colHash. SourceBoth records that both
// pipelines have independently computed the same colHash for the same column name -- a
// genuine, if rare, possibility since valueindex.ColHash and valuecounts.ColHash are the
// identical SHA-256[:16] hash, computed independently (task #216 design decision 1).
const (
	SourceVI   = "vi"
	SourceVCNT = "vcnt"
	SourceBoth = "both"
)

// indexVersion is the current version of the manifest's index.json wire format.
const indexVersion = 1

// Entry is one (tenant, colHash) audit record: the human-readable column name a VI/VCNT
// write path observed for this hash, and bookkeeping about who/when. This is the ONLY
// place blockpack ever records a colHash -> column name mapping; ColHash itself
// (valueindex.ColHash / valuecounts.ColHash) is a genuine one-way hash with no other
// mechanism to recover the source name.
type Entry struct {
	// Tenant this entry belongs to.
	Tenant string `json:"tenant"`
	// ColumnHash is valueindex.ColHash(ColumnName) == valuecounts.ColHash(ColumnName) --
	// both packages compute the identical SHA-256[:16] hash independently.
	ColumnHash string `json:"column_hash"`
	// ColumnName is the human-readable column name that produced ColumnHash.
	ColumnName string `json:"column_name"`
	// FirstSeenBy is SourceVI, SourceVCNT, or SourceBoth (once observed by both pipelines).
	FirstSeenBy string `json:"first_seen_by"`
	// FirstSeenAtSec is the unix-second timestamp of the first recorded observation. Never
	// updated after creation, even when FirstSeenBy is later upgraded to SourceBoth.
	FirstSeenAtSec uint64 `json:"first_seen_at_sec"`
	// LastSeenAtSec is the unix-second timestamp of the most recent observation that
	// actually changed this entry (a new colHash, or an upgrade to SourceBoth). Repeated
	// observations from a source already recorded in FirstSeenBy are no-ops and do NOT
	// bump this field (task #216 design decision 3: "write/update once per colHash when
	// first seen").
	LastSeenAtSec uint64 `json:"last_seen_at_sec"`
}

// manifestIndex is the JSON structure stored in the per-tenant manifest file.
type manifestIndex struct {
	Entries []Entry `json:"entries"`
	Version int     `json:"version"`
}

// Store is the minimal object-storage surface this package needs: read and write a
// whole object by key. Unlike internal/modules/cube's ObjectStore or
// internal/modules/viusage's ObjectStore, Store deliberately has NO conditional-PUT/ETag
// contract and Get does NOT need to distinguish "not found" from any other error --
// this manifest is advisory-only (NOTE-COLMANIFEST-1), so a plain read-modify-write and a
// blanket "any Get error means start empty" are acceptable simplifications callers'
// existing object-storage interfaces already satisfy without any widening (see NOTES.md).
type Store interface {
	// Get reads the entire object at key. ANY error (including "not found") is treated
	// by Load as "no manifest yet" -- see Load's doc comment.
	Get(ctx context.Context, key string) ([]byte, error)
	// Put writes data to key, creating or overwriting it.
	Put(ctx context.Context, key string, data []byte) error
}

// ManifestPath returns the object-storage key for tenant's manifest file, mirroring
// internal/modules/cube's `<tenant>/cubes/index.json` and internal/modules/viusage's
// `<tenant>/viusage/index.json` tenant-scoped placement convention (task #216 design
// decision 2: one aggregate per-tenant file, not one file per colHash directory).
func ManifestPath(tenant string) string {
	return tenant + "/column_manifest/index.json"
}

// Load fetches and decodes tenant's current manifest. Unlike cube.Registry.Load /
// viusage.Registry.Load, Load does NOT distinguish a genuine "not found" from any other
// Get error -- both are treated as an empty manifest. This is a deliberate simplification
// (NOTE-COLMANIFEST-1): those two registries require ErrNotFound-precision because
// misreading a real error as "empty" would silently destroy every other tracked entry via an
// unconditional overwrite -- a correctness bug for state their read paths actually depend on.
// This manifest is never read by any correctness-relevant path, so the equivalent failure mode
// here is merely "occasionally re-derives a slightly incomplete manifest," which is an
// acceptable trade against requiring every caller's Store to implement a not-found-signaling
// contract. Load returns (nil, nil) whenever store.Get fails for ANY reason (not-found,
// transient, or a bounded-timeout expiry -- see manifestOpTimeout) or the object is empty.
// Load returns a real, non-nil error only when the stored bytes exist but fail to JSON-decode
// (corruption) -- see SPEC-COLMANIFEST-3.
func Load(ctx context.Context, store Store, tenant string) ([]Entry, error) {
	opCtx, cancel := context.WithTimeout(ctx, manifestOpTimeout)
	defer cancel()
	data, err := store.Get(opCtx, ManifestPath(tenant))
	if err != nil {
		// Deliberately swallowed: see this function's own doc comment and NOTES.md
		// NOTE-COLMANIFEST-1 -- this manifest is advisory-only, so "not found," a genuine
		// transient Get failure, and a manifestOpTimeout expiry (NOTE-COLMANIFEST-2) are all
		// treated identically as "start from empty."
		return nil, nil //nolint:nilerr
	}
	if len(data) == 0 {
		return nil, nil
	}
	var idx manifestIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("colhashmanifest: decode %q: %w", ManifestPath(tenant), err)
	}
	return idx.Entries, nil
}

// RecordColumn records that source observed (colHash, colName) for tenant at nowSec,
// writing/updating the manifest at most once per genuinely new fact (task #216 design
// decision 3):
//   - a colHash never seen before for tenant: a new Entry is created and persisted.
//   - a colHash already recorded with the SAME source (or already SourceBoth): a pure
//     no-op, no Get/Put beyond the initial Load -- "write once per colHash when first seen."
//   - a colHash already recorded with the OTHER source: FirstSeenBy is upgraded to
//     SourceBoth and persisted -- this is new information worth recording.
//
// RecordColumn does NOT swallow a Put failure -- it returns the real error so the caller
// can log it. It is the CALLER's responsibility (never this function's) to ensure that
// error never fails the real VI/VCNT write it piggybacks on; see the call sites in
// internal/modules/valueindexconsumer and internal/modules/valuecountscompactor, both of
// which log-and-ignore.
func RecordColumn(
	ctx context.Context,
	store Store,
	tenant, colHash, colName, source string,
	nowSec uint64,
) error {
	if source != SourceVI && source != SourceVCNT {
		return fmt.Errorf("colhashmanifest: invalid source %q (want %q or %q)", source, SourceVI, SourceVCNT)
	}

	entries, err := Load(ctx, store, tenant)
	if err != nil {
		return err
	}

	idx := -1
	for i := range entries {
		if entries[i].Tenant == tenant && entries[i].ColumnHash == colHash {
			idx = i
			break
		}
	}

	switch {
	case idx < 0:
		entries = append(entries, Entry{
			Tenant:         tenant,
			ColumnHash:     colHash,
			ColumnName:     colName,
			FirstSeenBy:    source,
			FirstSeenAtSec: nowSec,
			LastSeenAtSec:  nowSec,
		})
	case entries[idx].FirstSeenBy == source || entries[idx].FirstSeenBy == SourceBoth:
		// Nothing new to record -- no Put.
		return nil
	default:
		entries[idx].FirstSeenBy = SourceBoth
		entries[idx].LastSeenAtSec = nowSec
	}

	data, err := json.Marshal(manifestIndex{Version: indexVersion, Entries: entries})
	if err != nil {
		return fmt.Errorf("colhashmanifest: encode: %w", err)
	}
	opCtx, cancel := context.WithTimeout(ctx, manifestOpTimeout)
	defer cancel()
	if err := store.Put(opCtx, ManifestPath(tenant), data); err != nil {
		return fmt.Errorf("colhashmanifest: put %q: %w", ManifestPath(tenant), err)
	}
	return nil
}
