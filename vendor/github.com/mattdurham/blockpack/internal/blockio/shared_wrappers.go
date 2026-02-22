package blockio

import "github.com/mattdurham/blockpack/internal/blockio/shared"

// Local wrappers for shared utilities.
// These allow the root blockio package to call shared functions using the
// original unexported names, minimizing code changes during deduplication.
// The canonical implementations live in shared/.
var (
	// Exported wrappers for bitset functions used by column_builder.go
	EncodePresenceRLE = shared.EncodePresenceRLE
	DecodePresenceRLE = shared.DecodePresenceRLE
)

// Exported validation functions from shared/limits.go.
var (
	ValidateSpanCount      = shared.ValidateSpanCount
	ValidateBlockCount     = shared.ValidateBlockCount
	ValidateColumnCount    = shared.ValidateColumnCount
	ValidateStringLen      = shared.ValidateStringLen
	ValidateBytesLen       = shared.ValidateBytesLen
	ValidateDictionarySize = shared.ValidateDictionarySize
	ValidateBlockSize      = shared.ValidateBlockSize
	ValidateMetadataSize   = shared.ValidateMetadataSize
	ValidateTraceCount     = shared.ValidateTraceCount
	ValidateNameLen        = shared.ValidateNameLen
	ValidateColumnType     = shared.ValidateColumnType
)

// Exported limit constants from shared/limits.go.
const (
	MaxSpans          = shared.MaxSpans
	MaxBlocks         = shared.MaxBlocks
	MaxColumns        = shared.MaxColumns
	MaxDictionarySize = shared.MaxDictionarySize
	MaxStringLen      = shared.MaxStringLen
	MaxBytesLen       = shared.MaxBytesLen
	MaxBlockSize      = shared.MaxBlockSize
	MaxMetadataSize   = shared.MaxMetadataSize
	MaxTraceCount     = shared.MaxTraceCount
	MaxNameLen        = shared.MaxNameLen
	FooterVersion     = shared.FooterVersion
)

// Exported overflow-safe arithmetic from shared/limits.go.
var (
	ErrIntegerOverflow = shared.ErrIntegerOverflow
	SafeAddInt         = shared.SafeAddInt
	SafeAddInt64       = shared.SafeAddInt64
	SafeAddUint64      = shared.SafeAddUint64
)
