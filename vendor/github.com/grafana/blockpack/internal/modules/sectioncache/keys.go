package sectioncache

import (
	"fmt"
	"strconv"
	"strings"
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

// V8SectionKeyFast returns the cache key for a V8 per-column/per-index section
// blob using string concatenation + strconv.Itoa instead of fmt.Sprintf.
// Format: fileID + "\x00v8\x00" + tocType + "\x00" + subType + "\x00" + name.
// Produces output byte-identical to fmt.Sprintf("%s\x00v8\x00%d\x00%d\x00%s",
// fileID, tocType, subType, name).
//
// NOTE-189: this runs once per wanted column per block per query on the warm read
// path (the steady-state production case). fmt.Sprintf incurs reflection,
// interface boxing of the two uint32 args, and an internal []byte->string copy;
// concatenation + strconv.Itoa avoids all of that and pre-sizes a single
// allocation via strings.Builder.Grow. The four V8-section key builds in
// TypedTieredCache (single Get/Put, GetMultiV8Section, GetMultiV8SectionMixed)
// all route through here so the hot batch path no longer pays per-key fmt cost.
func V8SectionKeyFast(fileID string, tocType, subType uint32, name string) string {
	ts := strconv.Itoa(int(tocType))
	ss := strconv.Itoa(int(subType))
	var b strings.Builder
	// fileID + "\x00v8\x00" + ts + "\x00" + ss + "\x00" + name
	b.Grow(len(fileID) + 4 + len(ts) + 1 + len(ss) + 1 + len(name))
	b.WriteString(fileID)
	b.WriteString("\x00v8\x00")
	b.WriteString(ts)
	b.WriteByte(0)
	b.WriteString(ss)
	b.WriteByte(0)
	b.WriteString(name)
	return b.String()
}
