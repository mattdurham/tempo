package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/rs/xid"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// ColHash returns the per-column directory hash used in the .vcnt object key:
// indexes/<tenant>/unique_values/<column_hash>/L<level>-<id>.vcnt (issue #400).
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
