package sectioncache

import (
	"fmt"
	"strconv"
)

// BlockColumnsKey returns the cache key for raw block column bytes at blockIdx.
// Format: fileID + "/block/" + blockIdx.
// Used by FilecacheAdapter (fmt.Sprintf) and TypedTieredCache (strconv.Itoa) — both
// produce identical output. FilecacheAdapter uses Sprintf for brevity; TypedTieredCache
// uses string concatenation + Itoa to avoid fmt overhead on the hot path.
func BlockColumnsKey(fileID string, blockIdx int) string {
	return fmt.Sprintf("%s/block/%d", fileID, blockIdx)
}

// BlockColumnsKeyFast returns the cache key for raw block column bytes using
// string concatenation + strconv.Itoa (zero-alloc for small integers; faster than fmt.Sprintf).
// Produces output identical to BlockColumnsKey.
func BlockColumnsKeyFast(fileID string, blockIdx int) string {
	return fileID + "/block/" + strconv.Itoa(blockIdx)
}

// IntrinsicKey returns the cache key for an intrinsic per-column blob.
// Format: fileID + "/intrinsic/" + name.
func IntrinsicKey(fileID, name string) string {
	return fmt.Sprintf("%s/intrinsic/%s", fileID, name)
}
