package blockpack

// cube_backfill_runner.go — historical backfill for a cube from the value index (#508).
// Ports tempo's tempodb/encoding/vblockpack/cube_backfill.go orchestration (viBackfillSource,
// decodeCanonicalVI, buildVCNTSection, runCubeBackfillCore, RunCubeBackfill, LoadCubeEntry) into
// blockpack root, so tempo only builds S3 adapters and calls RunCubeBackfill/LoadCubeEntry.
// Logging/metrics stay tempo-side (blockpack has no logging dependency by design, matching
// cube_query_path.go's loadEntries convention) -- every decline here is silent; callers that
// want visibility inspect the returned error themselves.
// SPEC-CUBE-032: orchestration contract. NOTE-CUBE-032: decodeCanonicalVI's type-threaded fix
// (Decision 1, not ported verbatim). SPEC-CUBE-033 (#511): zero-dimension entries backfill via
// internal/modules/cube/backfill.go's processMinuteZeroDim, anchored on the mandatory
// DurationColumn AggAttr lookup instead of a dimension column.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// cubeVIBackfillSource implements CubeValueIndexSource over a LookupStore. LookupColumn lists
// and downloads value-index files for (tenant, column) across every canonical value type,
// decoding each hit's value bytes with decodeCanonicalVI using the type discovered from the
// list-prefix loop itself (Decision 1 fix, see decodeCanonicalVI's own doc comment). Ported
// (renamed, unexported) from tempo's viBackfillSource; built via the unexported
// newCubeVIBackfillSource constructor below -- kept unexported since RunCubeBackfill is the
// only real caller (holistic-review finding: an exported constructor here would be a 10th root
// symbol beyond the #508 plan's stated nine-symbol public surface, with no actual consumer).
type cubeVIBackfillSource struct {
	store       LookupStore
	indexPrefix string
}

// newCubeVIBackfillSource builds a CubeValueIndexSource that reads value-index files for
// backfill/cardinality-gate lookups through store. indexPrefix is the value-index object-key
// prefix (e.g. "indexes"); the caller supplies it, blockpack makes no default guess.
func newCubeVIBackfillSource(store LookupStore, indexPrefix string) CubeValueIndexSource {
	return &cubeVIBackfillSource{store: store, indexPrefix: indexPrefix}
}

func (s *cubeVIBackfillSource) LookupColumn(
	ctx context.Context,
	tenant, column string,
	minSec, maxSec uint64,
) ([]VIQueryResult, error) {
	colHash := VCNTColHash(column)

	var results []VIQueryResult
	for _, typeName := range []string{"string", "int64", "uint64", "bool", "float64"} {
		prefix := path.Join(tenant, s.indexPrefix, colHash, typeName) + "/"
		keys, err := s.store.List(ctx, prefix)
		if err != nil || len(keys) == 0 {
			continue
		}

		for _, k := range keys {
			meta, perr := VIParseFilenameV2(path.Base(k))
			if perr != nil {
				continue
			}
			if !meta.IsInTimeRange(minSec, maxSec) {
				continue
			}
			data, getErr := s.store.Get(ctx, k)
			if getErr != nil {
				continue
			}
			r, openErr := VIOpenReader(data)
			if openErr != nil {
				continue
			}
			tr := [2]uint64{minSec, maxSec}
			hits, qErr := r.Lookup(nil, &tr)
			if qErr != nil {
				continue
			}
			for _, h := range hits {
				// The backfill reads the column value from SourceRef, not Value: decode the
				// canonical value bytes to a string using the type this loop iteration already
				// knows, rather than re-guessing it from the bytes (Decision 1).
				results = append(results, VIQueryResult{
					SourceRef: decodeCanonicalVI(h.Value, typeName),
					Value:     h.Value,
					TimeSec:   h.TimeSec,
					TraceID:   h.TraceID,
					SpanID:    h.SpanID,
					RowIdx:    h.RowIdx,
				})
			}
		}
	}
	return results, nil
}

// decodeCanonicalVI converts canonical VI value bytes to a string, given the VI column's actual
// type (threaded from the caller's list-prefix loop, LookupColumn above).
//
// Decision 1 (#508 plan): the original tempo implementation byte-sniffed for "any byte < 0x20"
// to guess numeric vs. string, then decoded a guessed-numeric value's 8 bytes as int64 and took
// `v % 10` -- a single decimal digit, discarding the rest of the value -- and additionally
// mis-decoded a genuine float64 column's IEEE-754 bit pattern as if it were an int64. Both are
// real, untested, pre-existing value-corruption bugs (not introduced by this port), fixed here
// by threading the VI column's actual type through instead of guessing from the bytes.
func decodeCanonicalVI(b []byte, typeName string) string {
	if len(b) == 0 {
		return ""
	}
	switch typeName {
	case "int64":
		if len(b) == 8 {
			return strconv.FormatInt(int64(binary.LittleEndian.Uint64(b)), 10) //nolint:gosec // intentional two's complement re-interpretation
		}
	case "uint64":
		if len(b) == 8 {
			return strconv.FormatUint(binary.LittleEndian.Uint64(b), 10)
		}
	case "float64":
		if len(b) == 8 {
			return strconv.FormatFloat(math.Float64frombits(binary.LittleEndian.Uint64(b)), 'g', -1, 64)
		}
	case "bool":
		if len(b) == 1 {
			if b[0] != 0 {
				return "true"
			}
			return "false"
		}
	}
	return string(b) // string type, or a short/malformed numeric payload -- raw passthrough
}

// CompactedKeyChecker reports which of a candidate set of object keys are
// already marked compacted in blockpack_file_catalog (issue #522 Phase 2.1)
// -- the mandatory VCNT read-path fix for NOTE-VC-009: valuecounts.Compact
// sums per key with no source-identity dedup, so a compacted-but-undeleted
// source coexisting with its merged replacement for up to the reaper's
// 30-minute grace window would otherwise be double-counted. nil disables the
// filter (matches this package's LookupStore/vi nil-tolerance convention) --
// buildVCNTSection's one production caller (cube_query_path.go) always wires
// a real *pgcatalog.Store, since it already requires a non-nil pgPool to
// reach this call at all. *pgcatalog.Store satisfies this structurally.
type CompactedKeyChecker interface {
	ListCompactedKeys(ctx context.Context, keys []string) (map[string]struct{}, error)
}

// buildVCNTSection lists and downloads the .vcnt files covering each dim and merges them into
// one consolidated section (data + dir) via VCNTBuildSectionFromObjects -- the shape the
// cardinality gate consumes. Ported from tempo's cube_backfill.go (the store-agnostic core of
// fetchVCNTSection); the tempo-specific nil-client guard and level.Warn logging stay tempo-side
// -- blockpack has no logging dependency (matches loadEntries' "declines are silent by design"
// convention in cube_query_path.go). On any error or absent coverage this returns a nil/empty
// section, which the cardinality gate treats as "no coverage" and passes by default -- a VCNT
// read failure must never block cube creation, only inform it when data is present.
//
// SPEC-PGCATALOG-7 (issue #522 Phase 2.1, MANDATORY, final resolution of NOTE-VC-009): every
// candidate key surviving the ext/time-range filters below is checked against
// compactedChecker BEFORE being downloaded and summed -- any key already marked compacted is
// excluded. A compactedChecker error fails the WHOLE call closed (nil/empty section, "no VCNT
// signal"), never falls through to an unfiltered fetch: an unverifiable exclusion is treated
// exactly like absent coverage, never as a green light to risk double-counting.
func buildVCNTSection(
	ctx context.Context,
	store LookupStore,
	tenant string,
	dims []string,
	minSec, maxSec uint64,
	compactedChecker CompactedKeyChecker,
) ([]byte, []VCNTChunkDirEntry) {
	var candidateKeys []string
	for _, dim := range dims {
		colHash := VCNTColHash(dim)
		prefix := path.Join(tenant, "value_counts", colHash) + "/"
		keys, err := store.List(ctx, prefix)
		if err != nil || len(keys) == 0 {
			continue
		}
		for _, k := range keys {
			if path.Ext(k) != ".vcnt" {
				continue
			}
			if !VCNTFileOverlapsRange(path.Base(k), minSec, maxSec) {
				continue
			}
			candidateKeys = append(candidateKeys, k)
		}
	}
	if len(candidateKeys) == 0 {
		return nil, nil
	}

	var compacted map[string]struct{}
	if compactedChecker != nil {
		var checkErr error
		compacted, checkErr = compactedChecker.ListCompactedKeys(ctx, candidateKeys)
		if checkErr != nil {
			return nil, nil
		}
	}

	var objects [][]byte
	for _, k := range candidateKeys {
		if _, isCompacted := compacted[k]; isCompacted {
			continue
		}
		data, getErr := store.Get(ctx, k)
		if getErr != nil || len(data) == 0 {
			continue
		}
		objects = append(objects, data)
	}
	if len(objects) == 0 {
		return nil, nil
	}
	data, dir, _ := VCNTBuildSectionFromObjects(objects)
	return data, dir
}

// cubeBackfillMaxConsecutiveFailures bounds how many consecutive per-minute processMinute
// failures runCubeBackfillCore tolerates before aborting the whole run, rather than burning the
// entire (possibly very wide) backfill window on a structural failure that will deterministically
// recur on every remaining minute (e.g. a registry entry missing required AggAttrs). Ported
// verbatim from tempo's cube_backfill.go: 5 matches maxRetries (tempo's backendworker.go), the
// locked default for how many attempts a Postgres-claimed job gets before it is left permanently
// failed -- reusing it here keeps a single "how many failures before giving up" policy value
// across the codebase's related retry/circuit-breaker knobs. It resets on every minute that
// succeeds, so only an UNINTERRUPTED run of failures counts.
const cubeBackfillMaxConsecutiveFailures = 5

// runCubeBackfillCore is RunCubeBackfill's dependency-injected core: constructs the CubeRegistry
// from pgPool and runs the CubeBackfiller, persisting progress via CubeRegistry.UpdateWatermarks
// on every successful (non-error) per-minute progressFn callback -- including minutes with no
// data (a sparse cube's watermark still advances so the router doesn't re-attempt an
// already-covered, empty minute). Ported from tempo's cube_backfill.go with zero logging calls
// (blockpack's zero-logging convention); the circuit-breaker abort still returns a real error
// for the caller to inspect.
func runCubeBackfillCore(
	ctx context.Context,
	entry CubeRegistryEntry,
	src CubeValueIndexSource,
	pgPool *pgxpool.Pool,
	cfg CubeBackfillConfig,
	currentMinute uint32,
) error {
	registry := NewPgCubeRegistry(pgPool, entry.Tenant)
	bf := NewCubeBackfiller(entry, src, cfg)
	var consecutiveFailures int
	return bf.Run(ctx, currentMinute, func(prog CubeBackfillProgress) error {
		if prog.LastError != nil {
			consecutiveFailures++
			if consecutiveFailures >= cubeBackfillMaxConsecutiveFailures {
				return fmt.Errorf(
					"cube backfill: aborted after %d consecutive per-minute failures: %w",
					consecutiveFailures, prog.LastError,
				)
			}
			return nil
		}
		consecutiveFailures = 0

		wm := prog.Watermark
		if uwErr := registry.UpdateWatermarks(
			ctx, entry.CubeID, CubeRollupL0, wm.WatermarkMinute, wm.WatermarkMinute,
		); uwErr != nil {
			// A persist failure aborts the run rather than continuing to spend backfill I/O the
			// registry cannot yet account for.
			return uwErr
		}
		return nil
	})
}

// RunCubeBackfill runs a cube's historical backfill synchronously in the calling goroutine,
// reading from vi and writing finished cube files through files, until the full window is
// complete or ctx is done. pgPool backs the cube registry; a nil pgPool declines immediately
// (Edge Case 2) rather than panicking on a nil-pool dereference deeper in the call chain.
// indexPrefix is the value-index object-key prefix vi's files live under.
//
// Returns nil if and only if the full backfill window was genuinely exhausted -- CubeBackfiller.
// Run's own loop only returns nil after processing every minute in [currentMinute-1,
// currentMinute-cfg.WindowMinutes]; both context cancellation and a circuit-breaker abort return
// a non-nil error. Callers relying on "err == nil implies done" (e.g. a completion metric) may
// do so without a separate progress callback.
//
// A zero-dimension (ungrouped) entry backfills successfully like any other entry (#511): it has
// no dimension column to anchor a historical VI lookup on, so
// internal/modules/cube/backfill.go's processMinuteZeroDim instead anchors on the mandatory
// DurationColumn AggAttr lookup (every v2 cube materializes duration) as the span-enumeration
// source, collapsing every span into one (AllDimSentinel, AllDimSentinel) cell per minute. This
// mirrors that same file's processMinute, which RunCubeBackfill delegates to via
// runCubeBackfillCore/CubeBackfiller -- no zero-dim-specific guard exists here anymore.
func RunCubeBackfill(
	ctx context.Context,
	entry CubeRegistryEntry,
	vi LookupStore,
	files CubeObjectPutter,
	pgPool *pgxpool.Pool,
	cfg CubeBackfillConfig,
	currentMinute uint32,
	indexPrefix string,
) error {
	if pgPool == nil {
		return errors.New("blockpack: RunCubeBackfill: postgres not configured")
	}
	src := newCubeVIBackfillSource(vi, indexPrefix)
	cfg.Store = files
	return runCubeBackfillCore(ctx, entry, src, pgPool, cfg, currentMinute)
}

// LoadCubeEntry loads the actual CubeRegistryEntry for cubeID from the cube registry. Returns a
// real error if pgPool is nil, the registry cannot be loaded, or no entry with cubeID exists in
// it -- callers must treat any of these as a hard failure, not a caller-supplied placeholder.
// Ported verbatim from tempo's cube_backfill.go (already zero tempo dependency).
func LoadCubeEntry(ctx context.Context, pgPool *pgxpool.Pool, tenant, cubeID string) (CubeRegistryEntry, error) {
	if pgPool == nil {
		return CubeRegistryEntry{}, errors.New("cube registry: postgres not configured")
	}
	reg := NewPgCubeRegistry(pgPool, tenant)
	entries, _, loadErr := reg.Load(ctx)
	if loadErr != nil {
		return CubeRegistryEntry{}, fmt.Errorf("cube registry: load: %w", loadErr)
	}
	for _, e := range entries {
		if e.CubeID == cubeID {
			return e, nil
		}
	}
	return CubeRegistryEntry{}, fmt.Errorf("cube registry: entry %q not found for tenant %q", cubeID, tenant)
}
