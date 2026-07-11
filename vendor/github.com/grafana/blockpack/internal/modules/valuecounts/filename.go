package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/xid"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// ColHash returns the per-column directory hash used in the .vcnt object key:
// <tenant>/value_counts/<column_hash>/L<level>-<id>.vcnt (issue #400).
func ColHash(colName string) string {
	sum := sha256.Sum256([]byte(colName))
	return hex.EncodeToString(sum[:16])
}

// FormatFilename returns a .vcnt filename for the given compaction level and ID.
// Format: L<level>-<id>.vcnt (e.g. "L0-ce3sg9bh45cs7fvb.vcnt").
func FormatFilename(level int, id string) string {
	return fmt.Sprintf(shared.ValueCountsFilenamePattern, level, id)
}

// ParseFilename parses a .vcnt filename into its compaction level and ID.
func ParseFilename(name string) (level int, id string, err error) {
	base := strings.TrimSuffix(name, ".vcnt")
	if base == name {
		return 0, "", fmt.Errorf("valuecounts: filename %q missing .vcnt suffix", name)
	}
	if !strings.HasPrefix(base, "L") {
		return 0, "", fmt.Errorf("valuecounts: filename %q missing L<level>- prefix", name)
	}
	parts := strings.SplitN(base[1:], "-", 2)
	if len(parts) != 2 || parts[1] == "" {
		return 0, "", fmt.Errorf("valuecounts: filename %q malformed (expected L<N>-<id>.vcnt)", name)
	}
	lv, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", fmt.Errorf("valuecounts: filename %q level not an integer: %w", name, err)
	}
	return lv, parts[1], nil
}

// NewID returns a new unique ID string suitable for use in .vcnt filenames.
func NewID() string {
	return xid.New().String()
}

// FormatFilenameV2 returns a .vcnt filename that embeds the file's wall-clock time range
// for O(1) input-side clustering during compaction (issue #494).
// Format: L<level>-<wallMinSec>-<wallMaxSec>-<id>.vcnt
func FormatFilenameV2(level int, wallMinSec, wallMaxSec uint64, id string) string {
	return fmt.Sprintf(shared.ValueCountsFilenamePatternV2, level, wallMinSec, wallMaxSec, id)
}

// FileMeta holds the parsed metadata from a v2 .vcnt filename.
type FileMeta struct {
	Filename   string
	ID         string
	Level      int
	WallMinSec uint64
	WallMaxSec uint64
}

// ParseFilenameV2 parses a v2 .vcnt filename into its components. Returns an error if name
// is not a v2 filename — there is no v1 fallback (issue #494, R4: no backward-compat
// parsing; a v1-shaped name here is treated identically to any other unparseable input by
// callers, e.g. compactColumn's skip-and-leave-in-place path).
func ParseFilenameV2(name string) (FileMeta, error) {
	base := strings.TrimSuffix(name, ".vcnt")
	if base == name {
		return FileMeta{}, fmt.Errorf("valuecounts: filename %q missing .vcnt suffix", name)
	}
	if !strings.HasPrefix(base, "L") {
		return FileMeta{}, fmt.Errorf("valuecounts: filename %q missing L<level>- prefix", name)
	}
	parts := strings.SplitN(base[1:], "-", 4)
	if len(parts) != 4 {
		return FileMeta{}, fmt.Errorf(
			"valuecounts: filename %q is not a v2 filename (expected L<level>-<minSec>-<maxSec>-<id>.vcnt)",
			name,
		)
	}
	lv, err := strconv.Atoi(parts[0])
	if err != nil {
		return FileMeta{}, fmt.Errorf("valuecounts: filename %q level not an integer: %w", name, err)
	}
	minSec, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return FileMeta{}, fmt.Errorf("valuecounts: filename %q wallMinSec not an integer: %w", name, err)
	}
	maxSec, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return FileMeta{}, fmt.Errorf("valuecounts: filename %q wallMaxSec not an integer: %w", name, err)
	}
	if parts[3] == "" {
		return FileMeta{}, fmt.Errorf("valuecounts: filename %q has empty id segment", name)
	}
	if minSec > maxSec {
		return FileMeta{}, fmt.Errorf(
			"valuecounts: filename %q has wallMinSec %d > wallMaxSec %d", name, minSec, maxSec,
		)
	}
	return FileMeta{Filename: name, Level: lv, WallMinSec: minSec, WallMaxSec: maxSec, ID: parts[3]}, nil
}

// IsInTimeRange reports whether the file covers any part of [queryMinSec, queryMaxSec].
func (m *FileMeta) IsInTimeRange(queryMinSec, queryMaxSec uint64) bool {
	return m.WallMaxSec >= queryMinSec && m.WallMinSec <= queryMaxSec
}

// SortFileMetas sorts by (Level ASC, WallMinSec ASC, WallMaxSec ASC).
func SortFileMetas(metas []FileMeta) {
	sort.Slice(metas, func(i, j int) bool { return lessFileMeta(metas[i], metas[j]) })
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
