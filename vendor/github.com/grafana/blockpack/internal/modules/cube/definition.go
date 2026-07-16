package cube

// NOTE: SPEC-CUBE-012 — RegistryEntry is the stable description of one pre-aggregated
// metrics cube. CubeID is deterministic: hex(SHA256(tenant+dims+filters)[:8]); the same
// pattern always maps to the same ID so concurrent creators converge on one entry.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefFilterOp is the serialized comparison operator stored in a RegistryEntry filter
// (the stable wire/JSON form used in index.json). Separate from the runtime FilterOp in
// accumulator.go which is a uint8 enum for in-memory predicate evaluation.
type DefFilterOp string

// Supported filter operators in a cube definition.
const (
	DefFilterOpGT  DefFilterOp = "GT"
	DefFilterOpGTE DefFilterOp = "GTE"
	DefFilterOpLT  DefFilterOp = "LT"
	DefFilterOpLTE DefFilterOp = "LTE"
	DefFilterOpEQ  DefFilterOp = "EQ"
)

// ColumnFilter is one baked-in span filter in a cube definition (e.g. duration > 300ms).
// Value is stored as a JSON-compatible type (float64 for numbers, string for strings).
type ColumnFilter struct {
	Value  any         `json:"value"`
	Column string      `json:"column"`
	Op     DefFilterOp `json:"op"`
}

// AllDimSentinel is the fixed placeholder dimension value used whenever a Definition has no
// real column for a dimension slot (SPEC-CUBE-033): a single-dimension cube's Dim2Column
// (there is no second dimension to key on), and a zero-dimension cube's Dim1Column AND
// Dim2Column (there is no dimension at all — every span accumulates into one shared cell).
// Using the SAME constant for both cases is deliberate: forward-ingest
// (CubeRegistryEntryToDefinition) and backfill (Backfiller.processMinute) must agree on this
// exact string, or CubeRollup treats their respective files' "no dimension" cells as two
// distinct series instead of merging them.
const AllDimSentinel = "__all__"

// RegistryEntry is the full, stable description of one active cube.
// It is stored in the tenant-level index.json and used by ingest, query, and compaction.
type RegistryEntry struct {
	// CubeID is the deterministic 8-byte hex identifier derived from tenant+dims+filters+aggAttrs.
	// SPEC-CUBE-020: hex(SHA256(computeDimsFiltersKey(tenant, sorted(dims), sorted(filters)) +
	// sorted(aggAttrs))[:8]) — the aggAttrs segment is unconditional, no empty-set special case.
	CubeID string `json:"cube_id"`
	// Tenant this cube belongs to.
	Tenant string `json:"tenant"`
	// Dimensions is the ordered pair of column names (1 or 2 elements).
	Dimensions []string `json:"dimensions"`
	// Watermarks tracks, per resolution level (keyed by RollupLevel — 1/60/1440), the
	// [MinMinute, MaxMinute] window this cube's data is COMPLETELY covered at that level.
	// Declared/populated by E-12a's Compactor.Execute on every successful rollup write; consumed
	// by E-6b's router to decline a query whose window isn't fully covered at the chosen
	// resolution rather than serving a partial/mixed-resolution answer (ruling 4(b)).
	Watermarks map[uint32]ResolutionWatermark `json:"watermarks,omitempty"`
	// Filters are the baked-in span filters; may be empty.
	Filters []ColumnFilter `json:"filters,omitempty"`
	// AggAttrs is the SET of materialized aggregate-attribute columns (ruling 3): the set
	// joins cube identity (ComputeCubeID's fourth hash segment), the aggregation FUNCTION
	// set does not — every cube always materializes count+sum+min+max+buckets for every
	// attribute in this set. Never legitimately empty: `duration` is always present.
	// A cube's attribute set is fixed for its entire life (never grows after creation).
	AggAttrs []string `json:"agg_attrs,omitempty"`
	// Resolution is the minutes-per-bucket (1 for L0, 60 for L1, 1440 for L2).
	Resolution uint32 `json:"resolution"`
	// CreatedAt is unix seconds when the cube was first registered.
	CreatedAt uint32 `json:"created_at"`
}

// ResolutionWatermark is one resolution level's complete-coverage window.
type ResolutionWatermark struct {
	MinMinute uint32 `json:"min_minute"`
	MaxMinute uint32 `json:"max_minute"`
}

// computeDimsFiltersKey returns the canonical (tenant, dims, filters) hash — the exact
// 3-segment SHA256 computation that was ComputeCubeID's entire body before aggAttrs joined
// cube identity (ruling 3). ComputeCubeID and the router's superset-matching grouping key
// (E-6b) are both built on this SAME function so they can never independently drift apart
// (ruling 5, single-source-of-truth).
func computeDimsFiltersKey(tenant string, dimensions []string, filters []ColumnFilter) string {
	dims := make([]string, len(dimensions))
	copy(dims, dimensions)
	sort.Strings(dims)

	// Canonical filter representation: "col:op:val" sorted.
	filterStrs := make([]string, 0, len(filters))
	for _, f := range filters {
		valJSON, _ := json.Marshal(f.Value)
		filterStrs = append(filterStrs, fmt.Sprintf("%s:%s:%s", f.Column, f.Op, string(valJSON)))
	}
	sort.Strings(filterStrs)

	h := sha256.Sum256([]byte(tenant + "\x00" + strings.Join(dims, "\x00") + "\x00" + strings.Join(filterStrs, "\x00")))
	return hex.EncodeToString(h[:8])
}

// ComputeCubeID returns the deterministic 8-byte hex ID for a (tenant, dimensions, filters,
// aggAttrs) combination. aggAttrs is the cube's attribute SET (ruling 3) and joins identity
// UNCONDITIONALLY — there is no empty-aggAttrs special case: aggAttrs can never legitimately
// be empty (`duration` is always present), so preserving the old 3-segment-only hash for an
// input that cannot legitimately occur would itself be a compat shim with nothing to be
// compatible with (team-lead veto, second-round correction).
//
// SPEC-CUBE-012: IDs are stable — the same inputs always produce the same ID.
func ComputeCubeID(tenant string, dimensions []string, filters []ColumnFilter, aggAttrs []string) string {
	key := computeDimsFiltersKey(tenant, dimensions, filters)

	attrs := make([]string, len(aggAttrs))
	copy(attrs, aggAttrs)
	sort.Strings(attrs)

	h := sha256.Sum256([]byte(key + "\x00" + strings.Join(attrs, "\x00")))
	return hex.EncodeToString(h[:8])
}

// IDFromBytes returns the deterministic [16]byte cube ID suitable for use in file headers.
// It is the first 8 bytes of SHA256, decoded from the hex CubeID string.
func IDFromBytes(cubeID string) ([16]byte, error) {
	b, err := hex.DecodeString(cubeID)
	if err != nil || len(b) != 8 {
		return [16]byte{}, fmt.Errorf("cube: invalid cube_id %q", cubeID)
	}
	var out [16]byte
	copy(out[:], b)
	return out, nil
}

// AggAttrsMismatchError signals a cube FILE's on-disk NumAggAttrs disagreeing with its own
// RegistryEntry's AggAttrs count — a registry-vs-file consistency violation (corruption, a buggy
// writer, or a stale/incorrect registry entry). This is a DIFFERENT failure class from
// validateDefinition's "duration always present" invariant (E-4, accumulator.go): that check
// gates NEW cube creation, before any file exists; this check catches an EXISTING file/entry pair
// that has already drifted apart — a question validateDefinition cannot answer even in principle,
// since it has no access to an already-written file's actual bytes (APPENDIX 3). Modeled on this
// codebase's existing T1b/NOTE-VI-078 index-vs-data-inconsistency error family — never silently
// parsed around.
type AggAttrsMismatchError struct {
	CubeID                string
	FileNumAggAttrs       uint8
	RegistryAggAttrsCount int
}

func (e *AggAttrsMismatchError) Error() string {
	return fmt.Sprintf(
		"cube: registry-vs-file mismatch for cube %q: file declares %d aggAttrs, registry entry declares %d",
		e.CubeID,
		e.FileNumAggAttrs,
		e.RegistryAggAttrsCount,
	)
}

// ValidateFileMatchesRegistry is a pure comparison, no I/O — the caller (E-10, tempo-side)
// already has both an opened file's decoded header (via (*Reader).NumAggAttrs) and the
// RegistryEntry it's about to route through in hand at the same time. This phase implements the
// COUNT check only (NumAggAttrs vs. len(AggAttrs)) — attribute IDENTITY checking is an aspiration
// conditioned on the format eventually carrying attribute names per-file, which it does not
// (ruling 3's fixed-for-life design keeps attribute names in the registry only); a future phase
// adding per-file attribute-name self-description could naturally extend this to compare
// identities, not just counts.
func ValidateFileMatchesRegistry(fileNumAggAttrs uint8, entry RegistryEntry) error {
	if int(fileNumAggAttrs) != len(entry.AggAttrs) {
		return &AggAttrsMismatchError{
			CubeID:                entry.CubeID,
			FileNumAggAttrs:       fileNumAggAttrs,
			RegistryAggAttrsCount: len(entry.AggAttrs),
		}
	}
	return nil
}

// defOpToFilterOp maps a RegistryEntry's serialized DefFilterOp to the runtime FilterOp
// NumericFilter consumes. A direct 1:1 mapping — the two enums exist separately only because
// DefFilterOp is the stable JSON wire form (definition.go's own doc comment) while FilterOp is an
// in-memory uint8 used by the hot per-span predicate path (accumulator.go).
func defOpToFilterOp(op DefFilterOp) (FilterOp, bool) {
	switch op {
	case DefFilterOpGT:
		return FilterOpGreater, true
	case DefFilterOpGTE:
		return FilterOpGreaterEqual, true
	case DefFilterOpLT:
		return FilterOpLess, true
	case DefFilterOpLTE:
		return FilterOpLessEqual, true
	case DefFilterOpEQ:
		return FilterOpEqual, true
	default:
		return 0, false
	}
}

// numericFilterValue extracts an int64 threshold from a ColumnFilter.Value of unknown concrete
// type. Value is documented as "float64 for numbers, string for strings" (ColumnFilter's own doc
// comment), but the ONLY real production writer of filters
// (tempo/tempodb/encoding/vblockpack/cubequerypath.go's extractFilters) always encodes the
// operand via traceql.Static.EncodeToString(false) — so in practice Value is always a string post
// round-trip through the registry's JSON persistence, and that string may itself represent a
// plain integer ("5"), a Go duration ("150ms" — the wire form for a TraceQL duration operand,
// e.g. `duration > 100ms`), or a float ("5.0"). int/int64/float64 are also accepted directly so a
// caller constructing a ColumnFilter in-process (never round-tripped through JSON) is not forced
// through a string first.
func numericFilterValue(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case float64:
		return int64(t), true
	case string:
		if i, err := strconv.ParseInt(t, 10, 64); err == nil {
			return i, true
		}
		if d, err := time.ParseDuration(t); err == nil {
			return d.Nanoseconds(), true
		}
		if f, err := strconv.ParseFloat(t, 64); err == nil {
			return int64(f), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// ColumnFilterToFilter converts one RegistryEntry-persisted ColumnFilter into a runtime Filter
// predicate — the single source of truth shared by BOTH real ingest paths that apply a cube's
// baked-in filters to real span data (#491 Phase E fix pass, review.md Issues 2/3):
//   - forward ingest: tempo's cubemanager.go passes this function directly as
//     LoadCubeDefinitions' filterFn parameter.
//   - backfill: Backfiller.processMinute (backfill.go) calls this directly per b.entry.Filters
//     entry (same package, no wrapper needed).
//
// A numeric threshold is preferred when cf.Value parses as one (via numericFilterValue) — this
// covers duration filters (`duration > 100ms`) and numeric attribute filters (e.g.
// `http.status_code = 200`) via NumericFilter, which reads through SpanValues.Int64. When cf.Value
// does not parse numerically and cf.Op is DefFilterOpEQ, this falls back to a string-equality
// Filter (StringFilter, reading through SpanValues.String) — the only operator for which string
// comparison is meaningful. Any other combination (a GT/GTE/LT/LTE op paired with a
// non-numeric-parseable value) is unrepresentable and returns nil; the caller skips a nil filter
// rather than treating it as a fatal error, matching this package's existing "skip malformed
// individual entries" tolerance (see LoadCubeDefinitions).
func ColumnFilterToFilter(cf ColumnFilter) Filter {
	if threshold, ok := numericFilterValue(cf.Value); ok {
		if op, opOK := defOpToFilterOp(cf.Op); opOK {
			return NumericFilter(cf.Column, op, threshold)
		}
		return nil
	}
	if cf.Op == DefFilterOpEQ {
		if s, ok := cf.Value.(string); ok {
			return StringFilter(cf.Column, s)
		}
	}
	return nil
}
