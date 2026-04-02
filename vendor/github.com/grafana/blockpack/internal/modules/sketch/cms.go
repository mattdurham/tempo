// Package sketch provides HyperLogLog, Count-Min Sketch, and BinaryFuse8
// implementations for per-block cardinality estimation, frequency estimation,
// and value membership testing.
//
// NOTE: Count-Min Sketch with w=64, d=4, uint16 saturating counters.
// SPEC-SK-06 through SPEC-SK-11 govern this file.
// Marshal/Unmarshal are 512 bytes fixed-size (SPEC-SK-09, SPEC-SK-10).
package sketch

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	cmsW         = 64              // width per row
	cmsD         = 4               // number of rows (hash functions)
	cmsMarshalSz = cmsW * cmsD * 2 // 512 bytes: 64 × 4 × 2 bytes per uint16
	// cmsKnuth is the multiplicative constant for deriving independent hash row values.
	cmsKnuth = 2654435761

	// CMSDepth is the exported depth (number of hash rows) for format validation.
	CMSDepth = cmsD
	// CMSWidth is the exported width (counters per row) for format validation.
	CMSWidth = cmsW
)

// CountMinSketch is a w=64, d=4 sketch with uint16 saturating counters.
// NOT safe for concurrent use. Callers must serialize Add/Estimate/Marshal calls.
// SPEC-SK-06: Add never panics.
// SPEC-SK-07: Estimate returns 0 for unseen values.
// SPEC-SK-08: Estimate >= actual count (over-estimate guarantee).
type CountMinSketch struct {
	rows [cmsD][cmsW]uint16
}

// NewCountMinSketch creates an empty CMS.
func NewCountMinSketch() *CountMinSketch {
	return &CountMinSketch{}
}

// Add increments the counters for v by count. Saturates at math.MaxUint16. (SPEC-SK-06)
func (c *CountMinSketch) Add(v string, count uint16) {
	h := cmsHash(v)
	for i := range cmsD {
		col := cmsCol(h, i)
		old := c.rows[i][col]
		if old > math.MaxUint16-count {
			c.rows[i][col] = math.MaxUint16
		} else {
			c.rows[i][col] = old + count
		}
	}
}

// Estimate returns the minimum counter value across all d rows. (SPEC-SK-07, SPEC-SK-08)
// Returns 0 for values never added (no false negatives for zero result).
func (c *CountMinSketch) Estimate(v string) uint16 {
	h := cmsHash(v)
	minVal := uint16(math.MaxUint16)
	for i := range cmsD {
		col := cmsCol(h, i)
		if c.rows[i][col] < minVal {
			minVal = c.rows[i][col]
		}
	}
	return minVal
}

// Merge adds all counters from other into c, element-wise, with uint16 saturation.
// Used to build file-level aggregated sketches from per-block sketches.
func (c *CountMinSketch) Merge(other *CountMinSketch) {
	for i := range cmsD {
		for j := range cmsW {
			sum := uint32(c.rows[i][j]) + uint32(other.rows[i][j])
			if sum > math.MaxUint16 {
				c.rows[i][j] = math.MaxUint16
			} else {
				c.rows[i][j] = uint16(sum) //nolint:gosec // safe: sum <= MaxUint16
			}
		}
	}
}

// Marshal serializes the CMS to 512 bytes, little-endian uint16. (SPEC-SK-09)
func (c *CountMinSketch) Marshal() []byte {
	b := make([]byte, cmsMarshalSz)
	pos := 0
	for i := range cmsD {
		for j := range cmsW {
			binary.LittleEndian.PutUint16(b[pos:], c.rows[i][j])
			pos += 2
		}
	}
	return b
}

// Unmarshal deserializes from b. Returns error if len(b) != 512. (SPEC-SK-10)
func (c *CountMinSketch) Unmarshal(b []byte) error {
	if len(b) != cmsMarshalSz {
		return fmt.Errorf("cms: Unmarshal: need %d bytes, got %d", cmsMarshalSz, len(b))
	}
	pos := 0
	for i := range cmsD {
		for j := range cmsW {
			c.rows[i][j] = binary.LittleEndian.Uint16(b[pos:])
			pos += 2
		}
	}
	return nil
}

// cmsHash computes a FNV-32a hash of s.
// NOTE-SK-03: seed_i = i * cmsKnuth, combined with col selection via mod w=64.
func cmsHash(s string) uint32 {
	const (
		offset32 uint32 = 2166136261
		prime32  uint32 = 16777619
	)
	h := offset32
	for i := range len(s) {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

// cmsCol derives the column index for row i from the base hash h.
func cmsCol(h uint32, i int) int {
	return int((h ^ uint32(i*cmsKnuth)) % cmsW) //nolint:gosec // safe: i is [0,3], cmsKnuth fits uint32
}
