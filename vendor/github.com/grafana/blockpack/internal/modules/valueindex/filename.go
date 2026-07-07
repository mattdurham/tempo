package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rs/xid"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// FormatFilename returns a value index filename for the given compaction level and ID.
// Format: L<level>-<id>.blockpack  (e.g. "L0-ce3sg9bh45cs7fvb.blockpack").
func FormatFilename(level int, id string) string {
	// FormatFilename uses the legacy (non-time-range) pattern intentionally; callers that
	// need wall-time-range in the filename should use FormatFilenameV2.
	return fmt.Sprintf("L%d-%s.blockpack", level, id)
}

// ParseFilename parses a value index filename into its compaction level and ID.
// Returns an error if the filename does not match the expected L<N>-<id>.blockpack format.
func ParseFilename(name string) (level int, id string, err error) {
	base := strings.TrimSuffix(name, ".blockpack")
	if !strings.HasPrefix(base, "L") {
		return 0, "", fmt.Errorf("valueindex: filename %q missing L<level>- prefix", name)
	}
	parts := strings.SplitN(base[1:], "-", 2)
	if len(parts) != 2 || parts[1] == "" {
		return 0, "", fmt.Errorf("valueindex: filename %q malformed (expected L<N>-<id>.blockpack)", name)
	}
	lv, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", fmt.Errorf("valueindex: filename %q level not an integer: %w", name, err)
	}
	return lv, parts[1], nil
}

// NewID returns a new unique ID string suitable for use in filenames.
func NewID() string {
	return xid.New().String()
}

// FormatFilenameV2 returns a value index filename that embeds wall-clock time range
// for efficient file discovery (NOTE-VI-030, issue #431).
// Format: L<level>-<wallMinSec>-<wallMaxSec>-<id>.blockpack
func FormatFilenameV2(level int, wallMinSec, wallMaxSec uint64, id string) string {
	return fmt.Sprintf(shared.ValueIndexFilenamePatternV2, level, wallMinSec, wallMaxSec, id)
}

// FileMeta holds the parsed metadata from a v2 value-index filename.
type FileMeta struct {
	Filename   string
	ID         string
	Level      int
	WallMinSec uint64
	WallMaxSec uint64
}

// ParseFilenameV2 parses a v2 value-index filename into its components.
// Returns (meta, nil) on success, or an error if name is not a v2 filename (the v1
// L<level>-<id> fallback was removed once every value-index file writer moved to v2 —
// see NOTE-VI-030).
func ParseFilenameV2(name string) (FileMeta, error) {
	base := strings.TrimSuffix(name, ".blockpack")
	if !strings.HasPrefix(base, "L") {
		return FileMeta{}, fmt.Errorf("valueindex: filename %q missing L<level>- prefix", name)
	}
	parts := strings.SplitN(base[1:], "-", 4)
	// v2: L<level>-<minTS>-<maxTS>-<id> (4 parts after stripping "L")
	if len(parts) != 4 {
		return FileMeta{}, fmt.Errorf("valueindex: filename %q is not a v2 filename (expected L<level>-<minTS>-<maxTS>-<id>.blockpack)", name)
	}
	lv, err := strconv.Atoi(parts[0])
	if err != nil {
		return FileMeta{}, fmt.Errorf("valueindex: filename %q level not an integer: %w", name, err)
	}
	minTS, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return FileMeta{}, fmt.Errorf("valueindex: filename %q wallMinSec not an integer: %w", name, err)
	}
	maxTS, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return FileMeta{}, fmt.Errorf("valueindex: filename %q wallMaxSec not an integer: %w", name, err)
	}
	return FileMeta{
		Filename:   name,
		Level:      lv,
		WallMinSec: minTS,
		WallMaxSec: maxTS,
		ID:         parts[3],
	}, nil
}

// IsInTimeRange reports whether the file covers any part of the query window [minTS, maxTS].
func (m *FileMeta) IsInTimeRange(queryMinSec, queryMaxSec uint64) bool {
	// File covers [WallMinSec, WallMaxSec]; query window [queryMinSec, queryMaxSec].
	// Overlap iff: file.maxSec >= query.minSec && file.minSec <= query.maxSec
	return m.WallMaxSec >= queryMinSec && m.WallMinSec <= queryMaxSec
}

// SortFileMetas sorts a slice of FileMeta by (Level ASC, WallMinSec ASC, WallMaxSec ASC).
// Used to order files for query traversal: L0 (freshest) before L1/L2.
func SortFileMetas(metas []FileMeta) {
	for i := 1; i < len(metas); i++ {
		for j := i; j > 0 && lessFileMeta(metas[j], metas[j-1]); j-- {
			metas[j], metas[j-1] = metas[j-1], metas[j]
		}
	}
}

func lessFileMeta(a, b FileMeta) bool {
	if a.Level != b.Level {
		return a.Level < b.Level
	}
	if a.WallMinSec != b.WallMinSec {
		return a.WallMinSec < b.WallMinSec
	}
	return a.WallMaxSec < b.WallMaxSec
}
