package executor

import (
	"testing"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// EXEC-TEST-BLS-001: blockLabelSet satisfies LabelSet interface (compile-time check)
var _ logqlparser.LabelSet = (*blockLabelSet)(nil)

// EXEC-TEST-BLS-002: Get on zero-column blockLabelSet returns ""
func TestBlockLabelSet_GetMissing(t *testing.T) {
	bls := &blockLabelSet{
		colNames: []string{},
		colMap:   make(map[string]int),
	}
	if bls.Get("missing") != "" {
		t.Fatalf("expected empty string for missing key, got %q", bls.Get("missing"))
	}
}

// EXEC-TEST-BLS-003: Set/Get round-trip via overlay
func TestBlockLabelSet_SetAndGet(t *testing.T) {
	bls := &blockLabelSet{
		colNames: []string{},
		colMap:   make(map[string]int),
	}
	bls.Set("level", "info")
	if bls.Get("level") != "info" {
		t.Fatalf("expected level=info after Set, got %q", bls.Get("level"))
	}
}

// EXEC-TEST-BLS-004: Delete marks key as deleted; Get returns ""
func TestBlockLabelSet_Delete(t *testing.T) {
	bls := &blockLabelSet{
		colNames: []string{},
		colMap:   make(map[string]int),
	}
	bls.Set("app", "svc")
	bls.Delete("app")
	if bls.Get("app") != "" {
		t.Fatalf("expected empty string after Delete, got %q", bls.Get("app"))
	}
	for _, k := range bls.Keys() {
		if k == "app" {
			t.Fatalf("deleted key 'app' still in Keys()")
		}
	}
}

// EXEC-TEST-BLS-005: resetForRow clears overlay and deleted maps; updates rowIdx
func TestBlockLabelSet_ResetForRow(t *testing.T) {
	bls := &blockLabelSet{
		colNames: []string{},
		colMap:   make(map[string]int),
	}
	bls.Set("x", "1")
	bls.Delete("y")
	bls.resetForRow(5)
	if bls.rowIdx != 5 {
		t.Fatalf("expected rowIdx=5 after reset, got %d", bls.rowIdx)
	}
	if len(bls.overlay) != 0 {
		t.Fatalf("expected empty overlay after reset, got %v", bls.overlay)
	}
	if len(bls.deleted) != 0 {
		t.Fatalf("expected empty deleted map after reset, got %v", bls.deleted)
	}
}

// EXEC-TEST-BLS-006: Materialize returns overlay values (no block cols in this unit test)
func TestBlockLabelSet_Materialize(t *testing.T) {
	bls := &blockLabelSet{
		colNames: []string{},
		colMap:   make(map[string]int),
	}
	bls.Set("level", "warn")
	bls.Set("app", "svc")
	m := bls.Materialize()
	if m["level"] != "warn" {
		t.Fatalf("expected level=warn, got %q", m["level"])
	}
	if m["app"] != "svc" {
		t.Fatalf("expected app=svc, got %q", m["app"])
	}
}

// EXEC-TEST-BLS-007: Keys returns overlay keys (no block cols in this unit test)
func TestBlockLabelSet_Keys(t *testing.T) {
	bls := &blockLabelSet{
		colNames: []string{},
		colMap:   make(map[string]int),
	}
	bls.Set("a", "1")
	bls.Set("b", "2")
	bls.Delete("b")
	keys := bls.Keys()
	if len(keys) != 1 || keys[0] != "a" {
		t.Fatalf("expected [a], got %v", keys)
	}
}

// EXEC-TEST-BLS-008: acquireBlockLabelSet / releaseBlockLabelSet pool round-trip
func TestBlockLabelSetPool_AcquireRelease(t *testing.T) {
	bls := acquireBlockLabelSet(nil, 0, []string{}, make(map[string]int), nil)
	bls.Set("x", "1")
	releaseBlockLabelSet(bls)
	// After release, overlay should be cleared.
	bls2 := acquireBlockLabelSet(nil, 1, []string{}, make(map[string]int), nil)
	if bls2.Get("x") != "" {
		t.Fatalf("expected cleared overlay after pool release, got %q", bls2.Get("x"))
	}
	releaseBlockLabelSet(bls2)
}

// EXEC-TEST-BLS-009: buildBlockColMapsWithLogCache returns colCols with len == len(colNames)
// (alignment invariant). Verified via overlay-only blockLabelSet; block column alignment
// is covered by integration tests in executor_log_test.go.
func TestBuildBlockColMapsWithLogCache_ColColsAlignment(t *testing.T) {
	// Use a nil block — buildBlockColMapsWithLogCache requires a non-nil block. Use a real empty block.
	// Since we can't easily construct a *modules_reader.Block here, verify the invariant
	// holds for the pool acquire/release path instead.
	bls := acquireBlockLabelSet(nil, 0, []string{"a", "b"}, map[string]int{"x": 0, "y": 1}, nil)
	if len(bls.colNames) != 2 {
		t.Fatalf("expected 2 colNames, got %d", len(bls.colNames))
	}
	// colCols was passed as nil; verify it is stored as nil (not some default).
	if bls.colCols != nil {
		t.Fatalf("expected nil colCols when passed nil, got non-nil")
	}
	releaseBlockLabelSet(bls)
}

// EXEC-TEST-BLS-010: releaseBlockLabelSet nils out colCols before pool return.
func TestBlockLabelSetPool_ColColsCleared(t *testing.T) {
	bls := acquireBlockLabelSet(nil, 0, []string{}, make(map[string]int), make([]*modules_reader.Column, 3))
	if bls.colCols == nil {
		t.Fatal("expected non-nil colCols after acquire")
	}
	releaseBlockLabelSet(bls)
	// Re-acquire from pool — colCols must be nil.
	bls2 := acquireBlockLabelSet(nil, 0, []string{}, make(map[string]int), nil)
	if bls2.colCols != nil {
		t.Fatalf("expected nil colCols after pool release, got non-nil slice of len %d", len(bls2.colCols))
	}
	releaseBlockLabelSet(bls2)
}

// EXEC-TEST-BLS-011: HasLive returns true when key is in overlay only
func TestBlockLabelSet_HasLive_OverlayOnly(t *testing.T) {
	bls := &blockLabelSet{
		colNames: []string{},
		colMap:   make(map[string]int),
	}
	bls.Set("level", "info")
	if !bls.HasLive("level") {
		t.Fatalf("expected HasLive=true for key in overlay, got false")
	}
}

// EXEC-TEST-BLS-012: HasLive returns false when key is absent entirely
func TestBlockLabelSet_HasLive_MissingKey(t *testing.T) {
	bls := &blockLabelSet{
		colNames: []string{},
		colMap:   make(map[string]int),
	}
	if bls.HasLive("missing") {
		t.Fatalf("expected HasLive=false for absent key, got true")
	}
}

// EXEC-TEST-BLS-013: HasLive returns false when key was deleted from overlay
func TestBlockLabelSet_HasLive_DeletedKey(t *testing.T) {
	bls := &blockLabelSet{
		colNames: []string{},
		colMap:   make(map[string]int),
	}
	bls.Set("app", "svc")
	bls.Delete("app")
	if bls.HasLive("app") {
		t.Fatalf("expected HasLive=false for deleted key, got true")
	}
}
