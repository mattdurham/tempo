package cube

// NOTE: SPEC-CUBE-012 — RegistryEntry is the stable description of one pre-aggregated
// metrics cube. CubeID is deterministic: hex(SHA256(tenant+dims+filters)[:8]); the same
// pattern always maps to the same ID so concurrent creators converge on one entry.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// DefFilterOp is the serialized comparison operator stored in a RegistryEntry filter
// (the stable wire/JSON form used in index.json). Separate from the runtime FilterOp in
// accumulator.go which is a uint8 enum for in-memory predicate evaluation.
type DefFilterOp string

// Supported filter operators in a cube definition.
const (
	DefFilterOpGT  DefFilterOp = "GT"
	DefFilterOpGTE DefFilterOp = "GTE"
	DefFilterOpLT  DefFilterOp = "LT"
	DefFilterOpLTE DefFilterOp = "LTE"
	DefFilterOpEQ  DefFilterOp = "EQ"
)

// ColumnFilter is one baked-in span filter in a cube definition (e.g. duration > 300ms).
// Value is stored as a JSON-compatible type (float64 for numbers, string for strings).
type ColumnFilter struct {
	Value  any         `json:"value"`
	Column string      `json:"column"`
	Op     DefFilterOp `json:"op"`
}

// RegistryEntry is the full, stable description of one active cube.
// It is stored in the tenant-level index.json and used by ingest, query, and compaction.
type RegistryEntry struct {
	// CubeID is the deterministic 8-byte hex identifier derived from tenant+dims+filters.
	// SPEC-CUBE-012: hex(SHA256(tenant + sorted(dims) + sorted(filters))[:8]).
	CubeID string `json:"cube_id"`
	// Tenant this cube belongs to.
	Tenant string `json:"tenant"`
	// Dimensions is the ordered pair of column names (1 or 2 elements).
	Dimensions []string `json:"dimensions"`
	// Filters are the baked-in span filters; may be empty.
	Filters []ColumnFilter `json:"filters,omitempty"`
	// Resolution is the minutes-per-bucket (1 for L0, 60 for L1, 1440 for L2).
	Resolution uint32 `json:"resolution"`
	// CreatedAt is unix seconds when the cube was first registered.
	CreatedAt uint32 `json:"created_at"`
}

// ComputeCubeID returns the deterministic 8-byte hex ID for a (tenant, dimensions, filters)
// combination. The result is the first 8 bytes of SHA256 over the canonical string
// representation, hex-encoded to 16 characters.
//
// SPEC-CUBE-012: IDs are stable — the same inputs always produce the same ID.
func ComputeCubeID(tenant string, dimensions []string, filters []ColumnFilter) string {
	dims := make([]string, len(dimensions))
	copy(dims, dimensions)
	sort.Strings(dims)

	// Canonical filter representation: "col:op:val" sorted.
	filterStrs := make([]string, 0, len(filters))
	for _, f := range filters {
		valJSON, _ := json.Marshal(f.Value)
		filterStrs = append(filterStrs, fmt.Sprintf("%s:%s:%s", f.Column, f.Op, string(valJSON)))
	}
	sort.Strings(filterStrs)

	h := sha256.Sum256([]byte(tenant + "\x00" + strings.Join(dims, "\x00") + "\x00" + strings.Join(filterStrs, "\x00")))
	return hex.EncodeToString(h[:8])
}

// IDFromBytes returns the deterministic [16]byte cube ID suitable for use in file headers.
// It is the first 8 bytes of SHA256, decoded from the hex CubeID string.
func IDFromBytes(cubeID string) ([16]byte, error) {
	b, err := hex.DecodeString(cubeID)
	if err != nil || len(b) != 8 {
		return [16]byte{}, fmt.Errorf("cube: invalid cube_id %q", cubeID)
	}
	var out [16]byte
	copy(out[:], b)
	return out, nil
}
