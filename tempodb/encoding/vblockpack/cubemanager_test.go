package vblockpack

// cubemanager_test.go — coverage for cubemanager.go's filterValidCubeDefs, the lockstep
// (activeDefs, accs) pairing loadDefs relies on, tempoSpanValues.Float64's dual OTLP-encoding
// acceptance, and (as of #491 Phase E fix pass, go-presubmit.md #2) addTrace/flush/rotateLocked's
// lock-release-before-S3-write behavior. addTrace/flush/rotateLocked only touch cm.store, which
// IS already the CubeObjectPutter interface, so they can be exercised directly against a
// constructed *cubeManager with a fake store, no minio required. loadDefs is exercised
// end-to-end in cubemanager_configure_test.go via ConfigureCubeManager's generic backend path
// (cm.objStore widened to the blockpack.CubeObjectStore interface, backend-agnostic VI/cube
// task) against a real local.NewBackend — no minio needed there either now.

import (
	"sync"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	commonpbv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	tracepbv1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

// pairingSpanValues is a minimal blockpack.CubeSpanValues test double that reports a value for
// exactly ONE column name (matchColumn) — used to prove which Definition an accumulator was
// ACTUALLY constructed from, not just that lengths/IDs line up (a def-accumulator swap wouldn't
// be caught by an ID-only check, since defs[] itself is untouched by such a bug).
type pairingSpanValues struct {
	matchColumn string
	value       string
}

func (p pairingSpanValues) String(col string) (string, bool) {
	if col == "__all__" {
		return "__all__", true
	}
	if col == p.matchColumn {
		return p.value, true
	}
	return "", false
}

func (p pairingSpanValues) Int64(string) (int64, bool)     { return 0, false }
func (p pairingSpanValues) Float64(string) (float64, bool) { return 0, false }

// TestFilterValidCubeDefs_ParallelSliceInvariant (#491, E-4 ripple finding #44):
// filterValidCubeDefs builds cm.defs/cm.accs in LOCKSTEP — a definition failing
// NewCubeAccumulator's mandatory-duration validation is excluded from BOTH slices together,
// never shifting index correspondence between them. Mutation check: an implementation that
// appends to one slice unconditionally but the other only when valid would produce a length
// mismatch, and/or a misaligned pairing, either of which this test catches.
func TestFilterValidCubeDefs_ParallelSliceInvariant(t *testing.T) {
	duration := blockpack.CubeAggAttrDef{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}

	valid1 := blockpack.CubeDefinition{
		Dim1Column: "dim1-a",
		Dim2Column: "__all__",
		AggAttrs:   []blockpack.CubeAggAttrDef{duration},
		ID:         [16]byte{1},
		Resolution: 1,
	}
	invalid := blockpack.CubeDefinition{
		Dim1Column: "dim1-invalid",
		Dim2Column: "__all__",
		// Missing duration — must fail NewCubeAccumulator's validation and be excluded from
		// BOTH activeDefs and accs.
		AggAttrs:   []blockpack.CubeAggAttrDef{{Column: "http.status_code", Type: blockpack.CubeAggAttrTypeInt64}},
		ID:         [16]byte{2},
		Resolution: 1,
	}
	valid2 := blockpack.CubeDefinition{
		Dim1Column: "dim1-b",
		Dim2Column: "__all__",
		AggAttrs:   []blockpack.CubeAggAttrDef{duration},
		ID:         [16]byte{3},
		Resolution: 1,
	}

	defs, accs := filterValidCubeDefs([]blockpack.CubeDefinition{valid1, invalid, valid2}, 100, "tenant-a")

	if len(defs) != 2 || len(accs) != 2 {
		t.Fatalf("want 2 valid defs/accs (invalid one excluded from BOTH), got defs=%d accs=%d", len(defs), len(accs))
	}
	if defs[0].ID != valid1.ID {
		t.Fatalf("defs[0].ID = %x, want %x (valid1)", defs[0].ID, valid1.ID)
	}
	if defs[1].ID != valid2.ID {
		t.Fatalf("defs[1].ID = %x, want %x (valid2) — invalid def must be excluded, not shift indices incorrectly", defs[1].ID, valid2.ID)
	}

	// Cross-check the PAIRING itself (not just lengths/IDs): each accs[i] must have been
	// constructed from defs[i]'s OWN Dim1Column — a def/accumulator swap would still pass the ID
	// checks above (defs[] itself is untouched by such a bug) but fail here, since a span
	// carrying only "dim1-a" is counted by the accumulator whose Dim1Column is "dim1-a", never
	// by one built from a different definition.
	wantColumns := []string{"dim1-a", "dim1-b"}
	for i, acc := range accs {
		before := acc.CellCount()
		counted, err := acc.Add(pairingSpanValues{matchColumn: wantColumns[i], value: "x"})
		if err != nil {
			t.Fatalf("accs[%d].Add: %v", i, err)
		}
		if !counted {
			t.Fatalf("accs[%d].Add with column %q not counted — accumulator not built from defs[%d] (pairing broken)",
				i, wantColumns[i], i)
		}
		if acc.CellCount() != before+1 {
			t.Fatalf("accs[%d].CellCount() = %d, want %d", i, acc.CellCount(), before+1)
		}
	}
}

// TestFilterValidCubeDefs_AllValid_KeepsEverything is the non-degenerate baseline: no
// definitions are dropped when all pass validation.
func TestFilterValidCubeDefs_AllValid_KeepsEverything(t *testing.T) {
	duration := blockpack.CubeAggAttrDef{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}
	defs := []blockpack.CubeDefinition{
		{Dim1Column: "a", Dim2Column: "__all__", AggAttrs: []blockpack.CubeAggAttrDef{duration}, ID: [16]byte{1}, Resolution: 1},
		{Dim1Column: "b", Dim2Column: "__all__", AggAttrs: []blockpack.CubeAggAttrDef{duration}, ID: [16]byte{2}, Resolution: 1},
	}
	gotDefs, gotAccs := filterValidCubeDefs(defs, 100, "tenant-a")
	if len(gotDefs) != 2 || len(gotAccs) != 2 {
		t.Fatalf("want 2/2, got defs=%d accs=%d", len(gotDefs), len(gotAccs))
	}
}

// TestFilterValidCubeDefs_AllInvalid_ReturnsEmptyNotNilLengthMismatch confirms the degenerate
// all-rejected case still returns length-matched (zero-length) slices, not a nil/mismatched pair.
func TestFilterValidCubeDefs_AllInvalid_ReturnsEmptyNotNilLengthMismatch(t *testing.T) {
	defs := []blockpack.CubeDefinition{
		{Dim1Column: "a", Dim2Column: "__all__", AggAttrs: nil, ID: [16]byte{1}, Resolution: 1},
	}
	gotDefs, gotAccs := filterValidCubeDefs(defs, 100, "tenant-a")
	if len(gotDefs) != 0 || len(gotAccs) != 0 {
		t.Fatalf("want 0/0 for an all-invalid input, got defs=%d accs=%d", len(gotDefs), len(gotAccs))
	}
}

// TestTempoSpanValues_Float64_AcceptsIntValueEncoding (#491, E-4 ripple finding #45): a numeric
// aggAttr may legitimately arrive as an OTLP AnyValue_IntValue (e.g. integer byte counts or status
// codes used as a metrics attribute) rather than AnyValue_DoubleValue — attrFloat64's documented
// dual-encoding acceptance must actually work, not just compile. The parity harness's
// fixtureFloatAttr (cube_metrics_parity_test.go) only ever constructs DoubleValue, leaving this
// branch completely dark before this test.
func TestTempoSpanValues_Float64_AcceptsIntValueEncoding(t *testing.T) {
	span := &tracepbv1.Span{
		Attributes: []*commonpbv1.KeyValue{
			{
				Key:   "request.size",
				Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_IntValue{IntValue: 4096}},
			},
		},
	}
	sv := &tempoSpanValues{span: span}
	got, ok := sv.Float64("span.request.size")
	if !ok {
		t.Fatal("Float64 must return ok=true for an IntValue-encoded numeric attribute")
	}
	if got != 4096 {
		t.Fatalf("Float64 = %v, want 4096", got)
	}
}

// TestTempoSpanValues_Float64_AcceptsDoubleValueEncoding is the sibling positive case — already
// implicitly exercised by the parity harness, made explicit and isolated here.
func TestTempoSpanValues_Float64_AcceptsDoubleValueEncoding(t *testing.T) {
	span := &tracepbv1.Span{
		Attributes: []*commonpbv1.KeyValue{
			{
				Key:   "request.size",
				Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_DoubleValue{DoubleValue: 120.5}},
			},
		},
	}
	sv := &tempoSpanValues{span: span}
	got, ok := sv.Float64("span.request.size")
	if !ok {
		t.Fatal("Float64 must return ok=true for a DoubleValue-encoded numeric attribute")
	}
	if got != 120.5 {
		t.Fatalf("Float64 = %v, want 120.5", got)
	}
}

// TestTempoSpanValues_Float64_MissingAttributeReturnsFalse mirrors Int64's own "absent -> skip"
// convention (accumulator.go's addAggAttrs relies on this: no value observed must not increment
// SampleCount/Sum for that aggAttr).
func TestTempoSpanValues_Float64_MissingAttributeReturnsFalse(t *testing.T) {
	span := &tracepbv1.Span{}
	sv := &tempoSpanValues{span: span}
	if _, ok := sv.Float64("span.request.size"); ok {
		t.Fatal("Float64 must return ok=false for a missing attribute")
	}
}

// fakeCubeObjectPutter is a minimal blockpack.CubeObjectPutter (Put-only) fake recording every
// write, for exercising addTrace/flush/rotateLocked's S3-write behavior without a real S3 backend.
// Unlike cm.objStore (a concrete *minioObjectStore), cm.store is already the CubeObjectPutter
// INTERFACE, so this is directly usable against the real cubeManager type.
type fakeCubeObjectPutter struct {
	mu   sync.Mutex
	puts map[string][]byte
}

func newFakeCubeObjectPutter() *fakeCubeObjectPutter {
	return &fakeCubeObjectPutter{puts: make(map[string][]byte)}
}

func (f *fakeCubeObjectPutter) Put(path string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.puts[path] = cp
	return nil
}

func (f *fakeCubeObjectPutter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.puts)
}

// TestCubeManager_AddTrace_MinuteRotation_FlushesRotatedOutAccumulator (#491 Phase E fix pass,
// go-presubmit.md #2): a minute rollover inside addTrace must flush the ROTATED-OUT accumulator
// (the one that had data) via the fake store, and cm.accs afterward must hold a fresh, empty
// accumulator for the new minute — a DIFFERENT pointer from the one that existed before rotation,
// proving the swap-then-flush-outside-the-lock restructure actually replaces the slot rather than
// resetting/reusing the same accumulator a concurrent addTrace could still be mutating.
func TestCubeManager_AddTrace_MinuteRotation_FlushesRotatedOutAccumulator(t *testing.T) {
	def := blockpack.CubeDefinition{
		Dim1Column: "dim1-a",
		Dim2Column: "__all__",
		AggAttrs:   []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}},
		ID:         [16]byte{0xAB},
		Resolution: 1,
	}
	// currentMin=0 is guaranteed stale relative to wallMinute()'s real current value, so the very
	// first addTrace call below deterministically triggers a rotation.
	acc, err := blockpack.NewCubeAccumulator(def, 0)
	if err != nil {
		t.Fatalf("NewCubeAccumulator: %v", err)
	}
	if _, addErr := acc.Add(pairingSpanValues{matchColumn: "dim1-a", value: "svc-a"}); addErr != nil {
		t.Fatalf("Add: %v", addErr)
	}
	if acc.CellCount() == 0 {
		t.Fatal("test setup error: accumulator must have data before rotation")
	}

	store := newFakeCubeObjectPutter()
	cm := &cubeManager{
		store:      store,
		tenant:     "t",
		defs:       []blockpack.CubeDefinition{def},
		accs:       []*blockpack.CubeAccumulator{acc},
		currentMin: 0,
	}
	oldAcc := cm.accs[0]

	// An empty trace still exercises addTrace's rotation check (it happens unconditionally,
	// before the per-span loop) without needing to construct a realistic OTLP span.
	cm.addTrace(&tempopb.Trace{})

	if cm.accs[0] == oldAcc {
		t.Fatal("cm.accs[0] must be a fresh accumulator pointer after rotation, not the same one")
	}
	if cm.accs[0].CellCount() != 0 {
		t.Fatalf("fresh accumulator after rotation must start empty, got CellCount=%d", cm.accs[0].CellCount())
	}
	if store.count() != 1 {
		t.Fatalf("expected exactly 1 flushed cube file (the rotated-out accumulator with data), got %d", store.count())
	}
}

// TestCubeManager_Flush_WritesNonEmptyAccumulatorsOnly (#491 Phase E fix pass, go-presubmit.md
// #2): flush's snapshot-then-flush-outside-the-lock restructure must still preserve the original
// behavior of skipping empty accumulators and writing every non-empty one.
func TestCubeManager_Flush_WritesNonEmptyAccumulatorsOnly(t *testing.T) {
	emptyDef := blockpack.CubeDefinition{
		Dim1Column: "dim1-empty",
		Dim2Column: "__all__",
		AggAttrs:   []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}},
		ID:         [16]byte{1},
		Resolution: 1,
	}
	populatedDef := blockpack.CubeDefinition{
		Dim1Column: "dim1-populated",
		Dim2Column: "__all__",
		AggAttrs:   []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}},
		ID:         [16]byte{2},
		Resolution: 1,
	}
	emptyAcc, err := blockpack.NewCubeAccumulator(emptyDef, 100)
	if err != nil {
		t.Fatalf("NewCubeAccumulator(empty): %v", err)
	}
	populatedAcc, err := blockpack.NewCubeAccumulator(populatedDef, 100)
	if err != nil {
		t.Fatalf("NewCubeAccumulator(populated): %v", err)
	}
	if _, addErr := populatedAcc.Add(pairingSpanValues{matchColumn: "dim1-populated", value: "svc-a"}); addErr != nil {
		t.Fatalf("Add: %v", addErr)
	}

	store := newFakeCubeObjectPutter()
	cm := &cubeManager{
		store:      store,
		tenant:     "t",
		defs:       []blockpack.CubeDefinition{emptyDef, populatedDef},
		accs:       []*blockpack.CubeAccumulator{emptyAcc, populatedAcc},
		currentMin: 100,
	}

	cm.flush("t")

	if store.count() != 1 {
		t.Fatalf("expected exactly 1 flushed cube file (only the populated accumulator), got %d", store.count())
	}
}

// TestCubeManager_ConcurrentAddTraceAndFlush_NoDataRace is the concurrency regression test for
// NEW Issue R1 (re-review, #491 Phase E, Iteration 3): flush() must transfer accumulator
// ownership under cm.mu — via the same swap-in-a-fresh-accumulator pattern rotateLocked already
// used — BEFORE performing FlushTo's Encode()/Reset() outside the lock.
//
// Pre-fix, flush() only snapshotted WHICH accumulators had data under cm.mu, then called FlushTo
// (Encode()+Reset()) on those SAME *blockpack.CubeAccumulator pointers AFTER releasing the lock,
// while they remained live in cm.accs. A concurrent addTrace() (which locks cm.mu and calls
// Add() on the identical pointer, mutating the same a.cells/a.dict maps FlushTo's unlocked
// Encode()/Reset() was iterating/reassigning) could run at the same wall-clock moment —
// Accumulator is documented "not safe for concurrent use". This test hammers addTrace and flush
// concurrently against the same cubeManager; it must be run with `go test -race` to have any
// teeth, since a single-goroutine test structurally cannot observe this class of bug (this is
// exactly why the two pre-existing single-threaded tests above did not catch it).
//
// Mutation check (recorded in implementation-status.md): reverting flush() to the pre-fix
// snapshot-only version (drop the swapOutAccumulatorsLocked call, restore the old CellCount-only
// snapshot loop) makes this test fail reliably under -race with "DATA RACE" /
// "fatal error: concurrent map iteration and map write" — confirming the test actually exercises
// the hazard rather than passing vacuously.
func TestCubeManager_ConcurrentAddTraceAndFlush_NoDataRace(t *testing.T) {
	def := blockpack.CubeDefinition{
		Dim1Column: cubeColSpanName,
		Dim2Column: cubeDimAll,
		AggAttrs:   []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}},
		ID:         [16]byte{0xCC},
		Resolution: 1,
	}
	minute := wallMinute()
	acc, err := blockpack.NewCubeAccumulator(def, minute)
	if err != nil {
		t.Fatalf("NewCubeAccumulator: %v", err)
	}

	store := newFakeCubeObjectPutter()
	cm := &cubeManager{
		store:      store,
		tenant:     "t",
		defs:       []blockpack.CubeDefinition{def},
		accs:       []*blockpack.CubeAccumulator{acc},
		currentMin: minute,
	}

	// A trace with one real span carrying a name (satisfies Dim1Column=cubeColSpanName) and a
	// valid duration (satisfies the mandatory duration AggAttr) — every addTrace call below
	// exercises a real Add() on the shared accumulator, not a no-op.
	trace := &tempopb.Trace{
		ResourceSpans: []*tracepbv1.ResourceSpans{
			{
				ScopeSpans: []*tracepbv1.ScopeSpans{
					{
						Spans: []*tracepbv1.Span{
							{
								Name:              "op",
								StartTimeUnixNano: 1,
								EndTimeUnixNano:   2,
							},
						},
					},
				},
			},
		},
	}

	const goroutinePairs = 8
	const iterationsPerGoroutine = 200
	var wg sync.WaitGroup
	wg.Add(goroutinePairs * 2)
	for i := 0; i < goroutinePairs; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterationsPerGoroutine; j++ {
				cm.addTrace(trace)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterationsPerGoroutine; j++ {
				cm.flush("t")
			}
		}()
	}
	wg.Wait()
}
