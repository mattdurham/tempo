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
	return fmt.Sprintf(shared.ValueIndexFilenamePattern, level, id)
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
