package cube

// filename.go — issue #522 Phase 3.1/3.2: filename format/parse for cube's L<level>-<min>-<max>-
// <id>.cube "timed" shape, ported from tempo's cube_compactor.go (cubeTimedFileRe/
// cubeTierToLevel) before that file is deleted per plan.md Phase 3.3 -- compaction-worker can't
// import it either way (tempo-side, minio-specific, wrong dependency direction). Mirrors
// valuecounts.FormatFilenameV2/ParseFilenameV2's shape (minutes instead of seconds, matching
// cube's own MinMinute/MaxMinute convention; .cube suffix instead of .vcnt).
//
// legacyTierToLevel's default case is the ONE deliberate correctness fix relative to tempo's
// original (planner-522, issue #522 Phase 3.1/3.2 review): tempo's cubeTierToLevel REJECTS
// (returns ok=false) any digit outside {0,1,2}, since every real tempo-written filename's leading
// digit was always a human-readable tier code needing translation, never the real Level. That
// assumption breaks for the new pairwise compaction-worker's own merge outputs, whose Level is
// always >= 10000 (plan.md Phase 3.0 point 4) and is ALREADY the real value, not a tier code
// needing translation. Rejecting it here (porting tempo's original default verbatim) would make
// every new-model merge output silently invisible to candidate-selection/reconcile forever.
// Passing it through unchanged is the fix -- see legacyTierToLevel's own doc comment.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/xid"
)

// FormatFilename returns a cube filename embedding merge-depth level and the file's wall-clock
// minute range: L<level>-<minMinute>-<maxMinute>-<id>.cube.
func FormatFilename(level uint32, minMinute, maxMinute uint32, id string) string {
	return fmt.Sprintf("L%d-%d-%d-%s.cube", level, minMinute, maxMinute, id)
}

// NewID returns a new unique ID string suitable for use in cube filenames.
func NewID() string {
	return xid.New().String()
}

// FileMeta holds the parsed metadata from a cube filename produced by FormatFilename.
type FileMeta struct {
	Filename  string
	ID        string
	Level     uint32
	MinMinute uint32
	MaxMinute uint32
}

// ParseFilename parses any "timed" cube filename -- L<levelOrTier>-<minMinute>-<maxMinute>-
// <id>.cube -- into its components, translating a legacy tier digit (0/1/2) to its real Level
// (1/60/1440) via legacyTierToLevel; any other digit (a new-model merge output, always >= 10000)
// passes through unchanged. Returns an error for any other shape, notably an accumulator-written
// untimed "L0-<xid>.cube" raw flush file (no embedded range at all) -- callers must fall back to
// reading the file's own header (cube.DecodeHeader) for that shape instead of guessing.
func ParseFilename(name string) (FileMeta, error) {
	base := strings.TrimSuffix(name, ".cube")
	if base == name {
		return FileMeta{}, fmt.Errorf("cube: filename %q missing .cube suffix", name)
	}
	if !strings.HasPrefix(base, "L") {
		return FileMeta{}, fmt.Errorf("cube: filename %q missing L<level>- prefix", name)
	}
	parts := strings.SplitN(base[1:], "-", 4)
	if len(parts) != 4 {
		return FileMeta{}, fmt.Errorf(
			"cube: filename %q is not this package's format (expected L<level>-<minMinute>-<maxMinute>-<id>.cube)",
			name,
		)
	}
	lv, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return FileMeta{}, fmt.Errorf("cube: filename %q level not an integer: %w", name, err)
	}
	//nolint:gosec // G115: lv parsed with ParseUint(..., 32) above, never exceeds uint32 range
	level := legacyTierToLevel(uint32(lv))
	minMinute, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return FileMeta{}, fmt.Errorf("cube: filename %q minMinute not an integer: %w", name, err)
	}
	maxMinute, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return FileMeta{}, fmt.Errorf("cube: filename %q maxMinute not an integer: %w", name, err)
	}
	if parts[3] == "" {
		return FileMeta{}, fmt.Errorf("cube: filename %q has empty id segment", name)
	}
	if minMinute > maxMinute {
		return FileMeta{}, fmt.Errorf("cube: filename %q has minMinute %d > maxMinute %d", name, minMinute, maxMinute)
	}
	//nolint:gosec // G115: parsed from this package's own filenames, never exceeds uint32 range
	return FileMeta{
		Filename: name, ID: parts[3],
		Level: level, MinMinute: uint32(minMinute), MaxMinute: uint32(maxMinute),
	}, nil
}

// legacyTierToLevel maps a legacy human-readable tier digit (0/1/2, from a pre-#522 "L0"/"L1"/
// "L2" merge/rollup filename) to its real Level value (1/60/1440, RollupL0/RollupL1/RollupL2).
// Any OTHER value is not a tier code at all -- it IS the real Level already (a new-model
// pairwise compaction-worker merge output, always >= 10000 per plan.md Phase 3.0 point 4) --
// passed through unchanged. See this file's own doc comment for why getting this backwards
// (rejecting anything outside {0,1,2}, tempo's original cubeTierToLevel behavior) would be a
// real bug for the new model, not just a missed optimization.
func legacyTierToLevel(tier uint32) uint32 {
	switch tier {
	case 0:
		return uint32(RollupL0)
	case 1:
		return uint32(RollupL1)
	case 2:
		return uint32(RollupL2)
	default:
		return tier
	}
}

// IsInTimeRange reports whether the file covers any part of [queryMinMinute, queryMaxMinute].
func (m *FileMeta) IsInTimeRange(queryMinMinute, queryMaxMinute uint32) bool {
	return m.MaxMinute >= queryMinMinute && m.MinMinute <= queryMaxMinute
}

// SortFileMetas sorts by (Level ASC, MinMinute ASC, MaxMinute ASC).
func SortFileMetas(metas []FileMeta) {
	sort.Slice(metas, func(i, j int) bool { return lessFileMeta(metas[i], metas[j]) })
}

func lessFileMeta(a, b FileMeta) bool {
	if a.Level != b.Level {
		return a.Level < b.Level
	}
	if a.MinMinute != b.MinMinute {
		return a.MinMinute < b.MinMinute
	}
	return a.MaxMinute < b.MaxMinute
}
