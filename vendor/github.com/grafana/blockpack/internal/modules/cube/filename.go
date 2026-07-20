package cube

// filename.go — issue #522 Phase 3.1/3.2: filename format/parse for cube's new pairwise
// compaction-worker merge outputs, mirroring valuecounts.FormatFilenameV2/ParseFilenameV2's
// established L<level>-<min>-<max>-<id> shape exactly (minutes instead of seconds, matching
// cube's own MinMinute/MaxMinute convention; .cube suffix instead of .vcnt).
//
// Deliberately NOT understood by tempo's cube_compactor.go's legacy cubeTierToLevel mapping
// (tier 0/1/2 -> Level 1/60/1440 only, everything else silently skipped) -- this is safe, not a
// gap: level here is always >= 10000 (plan.md Phase 3.0 point 4's confirmed offset, guaranteeing
// no collision with legacy 1/60/1440 values), and neither of the two real consumers of this
// filename shape depends on that legacy classifier at all. cube_query_path.go's QueryRange lists
// via the narrower Lister (raw key strings, Decision 2) and fetches every key regardless of any
// tier classification; compaction-planner/compaction-worker are Postgres-driven and never call
// cubeFileStore.List either. A level>=10000 file being invisible to the OLD scheduler's own
// listing is a feature (it can never be re-selected as an old-model merge candidate), not a bug.

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

// ParseFilename parses a cube filename produced by FormatFilename into its components. Returns
// an error for any other shape (including legacy L0/L1/L2-tier filenames written by the old
// scheduler/accumulator, or an accumulator-written untimed "L0-<xid>.cube" flush file) --
// callers must treat those identically to any other unparseable input (skip, don't guess).
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
	return FileMeta{
		Filename: name, ID: parts[3],
		Level: uint32(lv), MinMinute: uint32(minMinute), MaxMinute: uint32(maxMinute), //nolint:gosec // G115: parsed from this package's own filenames, never exceeds uint32 range
	}, nil
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
