package vblockpack

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"
	"testing"

	blockpack "github.com/grafana/blockpack"
)

// fakeSchedStore is a minimal blockpack.CubeFileStore fake for scheduler tests. List() reflects
// deletions and Puts so a caller re-listing mid-pass (the scheduler does this between rollup
// stages) sees a consistent view.
type fakeSchedStore struct {
	files   map[string][]byte
	deleted map[string]bool
	listed  []blockpack.CubeFileInfo
}

func newFakeSchedStore() *fakeSchedStore {
	return &fakeSchedStore{files: make(map[string][]byte), deleted: make(map[string]bool)}
}

func (s *fakeSchedStore) addFile(info blockpack.CubeFileInfo, data []byte) {
	s.listed = append(s.listed, info)
	cp := make([]byte, len(data))
	copy(cp, data)
	s.files[info.Key] = cp
}

func (s *fakeSchedStore) List(_ context.Context, _, _ string) ([]blockpack.CubeFileInfo, error) {
	out := make([]blockpack.CubeFileInfo, 0, len(s.listed))
	for _, f := range s.listed {
		if s.deleted[f.Key] {
			continue
		}
		out = append(out, f)
	}
	return out, nil
}

func (s *fakeSchedStore) Get(_ context.Context, key string) (*blockpack.CubeReader, error) {
	data, ok := s.files[key]
	if !ok {
		return nil, fmt.Errorf("fakeSchedStore: no such key %q", key)
	}
	return blockpack.OpenCubeReaderFromBytes(data)
}

// Put records the written file's info so a subsequent List() call within the same test pass sees
// it — the scheduler's L1->L2 and eviction stages depend on re-listing after an earlier stage's
// Execute() call. Mirrors real production cubeFileStore.List's two paths exactly: a merged
// rollup output's Level/MinMinute/MaxMinute come from its filename (cubeTimedFileRe, since a
// rolled-up cell's Minute is rebucketed to the target resolution's bucket start — see rollup.go's
// MergedCell doc — so the file's OWN header no longer spans the full input range); an
// accumulator-written file (no embedded range in its name) falls back to reading its header.
func (s *fakeSchedStore) Put(key string, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	s.files[key] = cp

	base := path.Base(key)
	if m := cubeTimedFileRe.FindStringSubmatch(base); m != nil {
		tier, _ := strconv.ParseUint(m[1], 10, 32)
		level, ok := cubeTierToLevel(tier)
		if !ok {
			return fmt.Errorf("fakeSchedStore: unrecognized tier in filename %q", base)
		}
		minM, _ := strconv.ParseUint(m[2], 10, 32)
		maxM, _ := strconv.ParseUint(m[3], 10, 32)
		s.listed = append(s.listed, blockpack.CubeFileInfo{
			Key: key, Level: level, MinMinute: uint32(minM), MaxMinute: uint32(maxM), //nolint:gosec
		})
		return nil
	}

	minM, maxM, res, err := blockpack.CubeReadHeader(cp)
	if err != nil {
		return err
	}
	s.listed = append(s.listed, blockpack.CubeFileInfo{Key: key, MinMinute: minM, MaxMinute: maxM, Level: res})
	return nil
}

func (s *fakeSchedStore) Delete(_ context.Context, key string) error {
	s.deleted[key] = true
	return nil
}

// fakeSchedObjectStore is a minimal blockpack.CubeObjectStore fake backing the registry, mirroring
// the conditional-PUT-with-etag discipline the real Registry.UpdateWatermarks retries against.
type fakeSchedObjectStore struct {
	mu   sync.Mutex
	data []byte
	etag string
}

func (s *fakeSchedObjectStore) Get(_ context.Context, _ string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(s.data))
	copy(cp, s.data)
	return cp, s.etag, nil
}

func (s *fakeSchedObjectStore) ConditionalPut(_ context.Context, _ string, data []byte, etag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.etag != etag {
		return blockpack.CubeErrConflict
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	s.data = cp
	s.etag = etag + "x"
	return nil
}

// schedSpan is a minimal blockpack.CubeSpanValues fake used to drive a real
// blockpack.CubeAccumulator (real write path) when building fixture cube files.
type schedSpan struct {
	strs map[string]string
	ints map[string]int64
}

func (m schedSpan) String(col string) (string, bool) { v, ok := m.strs[col]; return v, ok }
func (m schedSpan) Int64(col string) (int64, bool)   { v, ok := m.ints[col]; return v, ok }
func (m schedSpan) Float64(col string) (float64, bool) {
	v, ok := m.ints[col]
	return float64(v), ok
}

// buildSchedL0Bytes writes one span (fixed dim1="auth", dim2="200", duration=1ms; every test in
// this file only varies minute, which is what drives boundary-completeness behavior) into a real
// blockpack.CubeAccumulator and encodes it — the same write path production ingest uses — rather
// than hand-building a fixture header/cell.
func buildSchedL0Bytes(t *testing.T, id [16]byte, minute uint32) []byte {
	t.Helper()
	def := blockpack.CubeDefinition{
		Dim1Column: "service.name",
		Dim2Column: "status_code",
		AggAttrs:   []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}},
		ID:         id,
		Resolution: 1,
	}
	acc, err := blockpack.NewCubeAccumulator(def, minute)
	if err != nil {
		t.Fatalf("NewCubeAccumulator: %v", err)
	}
	span := schedSpan{
		strs: map[string]string{"service.name": "auth", "status_code": "200"},
		ints: map[string]int64{blockpack.CubeDurationColumn: 1_000_000},
	}
	if _, err := acc.Add(span); err != nil {
		t.Fatalf("Add: %v", err)
	}
	data, err := acc.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return data
}

// schedTestCube sets up a registered cube plus its file store/compactor for scheduler tests.
// Deliberately kept on blockpack.NewCubeRegistry's blob backend (issue #504's test-migration
// review, 2026-07-15): this file's tests drive processCube/CubeCompactor directly against a
// standalone Registry+fixture, never through CubeScheduler.processTenant's own Postgres-backed
// registry construction (ConfigureCubeScheduler's pgPool) -- a real Postgres testcontainer
// would add test weight for zero additional coverage of the rollup/compaction logic under
// test here.
type schedTestCube struct {
	id        [16]byte
	hexID     string
	store     *fakeSchedStore
	reg       *blockpack.CubeRegistry
	compactor *blockpack.CubeCompactor
}

func newSchedTestCube(t *testing.T, cfg blockpack.CubeCompactorConfig) *schedTestCube {
	t.Helper()
	hexID := blockpack.CubeComputeID("t", []string{"service.name", "status_code"}, nil, []string{blockpack.CubeDurationColumn})
	id, err := blockpack.CubeIDFromHex(hexID)
	if err != nil {
		t.Fatalf("CubeIDFromHex: %v", err)
	}

	objStore := &fakeSchedObjectStore{}
	reg := blockpack.NewCubeRegistry(objStore, "t")
	entry := blockpack.CubeRegistryEntry{
		CubeID:     hexID,
		Tenant:     "t",
		Dimensions: []string{"service.name", "status_code"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	if err := reg.Add(context.Background(), entry); err != nil {
		t.Fatalf("reg.Add: %v", err)
	}

	store := newFakeSchedStore()
	compactor := blockpack.NewCubeCompactor(store, reg, cfg)
	return &schedTestCube{id: id, hexID: hexID, store: store, reg: reg, compactor: compactor}
}

func (c *schedTestCube) entry(t *testing.T) blockpack.CubeRegistryEntry {
	t.Helper()
	entries, _, err := c.reg.Load(context.Background())
	if err != nil {
		t.Fatalf("reg.Load: %v", err)
	}
	for _, e := range entries {
		if e.CubeID == c.hexID {
			return e
		}
	}
	t.Fatalf("cube %q not found in registry", c.hexID)
	return blockpack.CubeRegistryEntry{}
}

// TestCubeScheduler_RollsUpCompletedHourBoundary: an hour with L0 files, once fully elapsed
// (nowMinute >= hourStart+60), is rolled up into a single new L1 file and the L1 watermark is
// observable via the registry.
func TestCubeScheduler_RollsUpCompletedHourBoundary(t *testing.T) {
	tc := newSchedTestCube(t, blockpack.CubeCompactorConfig{})
	for m := uint32(0); m < 60; m++ {
		data := buildSchedL0Bytes(t, tc.id, m)
		tc.store.addFile(blockpack.CubeFileInfo{Key: fmt.Sprintf("l0-%d", m), MinMinute: m, MaxMinute: m, Level: 1}, data)
	}

	processCube(context.Background(), "t", tc.entry(t), tc.store, tc.compactor, 1_000_000, 60)

	files, err := tc.store.List(context.Background(), "t", tc.hexID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	l1Count := 0
	for _, f := range files {
		if f.Level == 60 {
			l1Count++
			if f.MinMinute != 0 || f.MaxMinute != 59 {
				t.Fatalf("expected L1 file to span [0,59], got [%d,%d]", f.MinMinute, f.MaxMinute)
			}
		}
	}
	if l1Count != 1 {
		t.Fatalf("expected exactly 1 L1 file after rollup, got %d", l1Count)
	}
}

// TestCubeScheduler_RollsUpCompletedDayBoundary: a day with L1 files, once fully elapsed
// (nowMinute >= dayStart+1440), is rolled up into a single new L2 file.
func TestCubeScheduler_RollsUpCompletedDayBoundary(t *testing.T) {
	tc := newSchedTestCube(t, blockpack.CubeCompactorConfig{})
	for h := uint32(0); h < 24; h++ {
		hourStart := h * 60
		data := buildSchedL0Bytes(t, tc.id, hourStart)
		tc.store.addFile(blockpack.CubeFileInfo{
			Key: fmt.Sprintf("l1-%d", h), MinMinute: hourStart, MaxMinute: hourStart + 59, Level: 60,
		}, data)
	}

	processCube(context.Background(), "t", tc.entry(t), tc.store, tc.compactor, 1_000_000, 1440)

	files, err := tc.store.List(context.Background(), "t", tc.hexID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	l2Count := 0
	for _, f := range files {
		if f.Level == 1440 {
			l2Count++
			if f.MinMinute != 0 || f.MaxMinute != 1439 {
				t.Fatalf("expected L2 file to span [0,1439], got [%d,%d]", f.MinMinute, f.MaxMinute)
			}
		}
	}
	if l2Count != 1 {
		t.Fatalf("expected exactly 1 L2 file after daily rollup, got %d", l2Count)
	}
}

// TestCubeScheduler_SkipsIncompleteCurrentHour (mutation-verification required): an hour that has
// NOT yet fully elapsed (nowMinute < hourStart+60) must never be rolled up, even though it already
// has L0 files. Mutation-verified: an implementation using `nowMinute > hourStart` (or any
// off-by-one relative to the documented `>=hourStart+60` condition) must FAIL this test, since at
// nowMinute=59 the hour [0,59] has not fully elapsed (elapses only at minute 60) but has strictly
// more than hourStart(=0).
func TestCubeScheduler_SkipsIncompleteCurrentHour(t *testing.T) {
	tc := newSchedTestCube(t, blockpack.CubeCompactorConfig{})
	for m := uint32(0); m < 60; m++ {
		data := buildSchedL0Bytes(t, tc.id, m)
		tc.store.addFile(blockpack.CubeFileInfo{Key: fmt.Sprintf("l0-%d", m), MinMinute: m, MaxMinute: m, Level: 1}, data)
	}

	// nowMinute=59: the hour [0,59] has NOT fully elapsed (needs nowMinute>=60).
	processCube(context.Background(), "t", tc.entry(t), tc.store, tc.compactor, 1_000_000, 59)

	files, err := tc.store.List(context.Background(), "t", tc.hexID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, f := range files {
		if f.Level == 60 {
			t.Fatalf("incomplete hour must not be rolled up, but found an L1 file [%d,%d]", f.MinMinute, f.MaxMinute)
		}
	}
	l0Count := 0
	for _, f := range files {
		if f.Level == 1 {
			l0Count++
		}
	}
	if l0Count != 60 {
		t.Fatalf("expected all 60 L0 inputs to remain untouched, got %d", l0Count)
	}
}

// TestCubeScheduler_ReproducesLegacyBug_NeverRollsUpJustTwoFilesRegardlessOfBoundary
// (mutation-verification required) reproduces the exact scenario the deleted
// CubeCompactorService's L1-rollup loop mishandled: that loop's ONLY gate was file count
// (`len(keys) < 2` to skip), so an hour with as few as 2 L0 files got rolled up the moment a
// second file appeared, regardless of whether the hour had actually finished — permanently
// undercounting that hour's L1 data. The new scheduler must gate on genuine time-boundary
// completeness, never file count. Mutation-verified: an implementation that removes the boundary
// gate (rolls up whenever PlanCubeL1Rollup finds >=1 key, mirroring the old ungated loop) must
// roll this up and fail; the fix must not.
func TestCubeScheduler_ReproducesLegacyBug_NeverRollsUpJustTwoFilesRegardlessOfBoundary(t *testing.T) {
	tc := newSchedTestCube(t, blockpack.CubeCompactorConfig{})
	// Only 2 L0 files, at the very start of an hour that has barely begun — exactly the scenario
	// the old ungated `len(keys) < 2` check would have rolled up.
	data0 := buildSchedL0Bytes(t, tc.id, 0)
	data1 := buildSchedL0Bytes(t, tc.id, 1)
	tc.store.addFile(blockpack.CubeFileInfo{Key: "l0-0", MinMinute: 0, MaxMinute: 0, Level: 1}, data0)
	tc.store.addFile(blockpack.CubeFileInfo{Key: "l0-1", MinMinute: 1, MaxMinute: 1, Level: 1}, data1)

	// nowMinute=2: the hour [0,59] has barely started, nowhere near its nowMinute>=60 completion.
	processCube(context.Background(), "t", tc.entry(t), tc.store, tc.compactor, 1_000_000, 2)

	files, err := tc.store.List(context.Background(), "t", tc.hexID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, f := range files {
		if f.Level == 60 {
			t.Fatalf("2 L0 files in a barely-started hour must never trigger a rollup, but found an L1 file [%d,%d]", f.MinMinute, f.MaxMinute)
		}
	}
}

// TestCubeScheduler_EvictsAgedL0AfterRetentionAndRollup: an L0 file that has already been rolled
// into L1 AND is past L0RetentionMinutes is deleted; one that is past retention but NOT yet
// covered by the L1 watermark is not.
func TestCubeScheduler_EvictsAgedL0AfterRetentionAndRollup(t *testing.T) {
	tc := newSchedTestCube(t, blockpack.CubeCompactorConfig{L0RetentionMinutes: 100})
	for m := uint32(0); m < 60; m++ {
		data := buildSchedL0Bytes(t, tc.id, m)
		tc.store.addFile(blockpack.CubeFileInfo{Key: fmt.Sprintf("l0-%d", m), MinMinute: m, MaxMinute: m, Level: 1}, data)
	}
	// An L0 file for an hour NOT covered by any rollup (minute 1000, isolated) — past retention
	// but never rolled up, so eviction must decline it.
	unrolledData := buildSchedL0Bytes(t, tc.id, 1000)
	tc.store.addFile(blockpack.CubeFileInfo{Key: "l0-unrolled", MinMinute: 1000, MaxMinute: 1000, Level: 1}, unrolledData)

	// nowMinute far enough past both the hour boundary (60) and retention (100) that the rolled-up
	// L0 files should be evicted, but not far enough to make the isolated file's own hour complete
	// (its hour [960,1019] needs nowMinute>=1020 to roll up at all).
	processCube(context.Background(), "t", tc.entry(t), tc.store, tc.compactor, 1_000_000, 1010)

	files, err := tc.store.List(context.Background(), "t", tc.hexID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	rolledUpL0Remaining := 0
	unrolledStillPresent := false
	for _, f := range files {
		if f.Level != 1 {
			continue
		}
		if f.Key == "l0-unrolled" {
			unrolledStillPresent = true
			continue
		}
		rolledUpL0Remaining++
	}
	if rolledUpL0Remaining != 0 {
		t.Fatalf("expected all rolled-up-and-aged L0 files evicted, %d remain", rolledUpL0Remaining)
	}
	if !unrolledStillPresent {
		t.Fatalf("an L0 file never rolled up into L1 must never be evicted, but it was deleted")
	}
}

// TestCubeScheduler_RunOnce_PanicInOneTenantDoesNotAbortSiblings (#491 Phase E fix pass,
// go-presubmit.md #4): runOnce's errgroup panic-isolation pattern (recover + err=nil in the
// per-tenant g.Go closure) is exercised for real here via the processTenantFn seam — a panicking
// fake for tenant "b" must not prevent "a" and "c" from being processed, and must not crash the
// test itself (errgroup.WithContext would otherwise cancel gctx for siblings the instant any
// goroutine returns a non-nil error; recover() prevents a panic from ever becoming one).
func TestCubeScheduler_RunOnce_PanicInOneTenantDoesNotAbortSiblings(t *testing.T) {
	var mu sync.Mutex
	var called []string

	s := &CubeScheduler{
		tenants: []string{"a", "b", "c"},
		cfg:     CubeSchedulerConfig{Concurrency: 4},
		nowFunc: func() uint32 { return 1000 },
		processTenantFn: func(_ context.Context, tenant string, _ uint32) {
			mu.Lock()
			called = append(called, tenant)
			mu.Unlock()
			if tenant == "b" {
				panic("boom: simulated panic for tenant b")
			}
		},
	}

	// Must not panic/crash the test.
	s.runOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(called) != 3 {
		t.Fatalf("expected all 3 tenants to be invoked despite tenant b panicking, got %v", called)
	}
	seen := map[string]bool{}
	for _, tenant := range called {
		seen[tenant] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !seen[want] {
			t.Fatalf("tenant %q was never invoked (sibling tenant likely aborted by the panic): %v", want, called)
		}
	}
}

// TestCubeScheduler_UpdatesWatermarkOnSuccess_ObservableViaRegistry: after a successful hourly
// rollup, the registry's Watermarks[RollupL1] entry is observable and covers the rolled-up range.
func TestCubeScheduler_UpdatesWatermarkOnSuccess_ObservableViaRegistry(t *testing.T) {
	tc := newSchedTestCube(t, blockpack.CubeCompactorConfig{})
	for m := uint32(0); m < 60; m++ {
		data := buildSchedL0Bytes(t, tc.id, m)
		tc.store.addFile(blockpack.CubeFileInfo{Key: fmt.Sprintf("l0-%d", m), MinMinute: m, MaxMinute: m, Level: 1}, data)
	}

	processCube(context.Background(), "t", tc.entry(t), tc.store, tc.compactor, 1_000_000, 60)

	updated := tc.entry(t)
	wm, ok := updated.Watermarks[60]
	if !ok {
		t.Fatalf("expected Watermarks[60] (RollupL1) to be set after successful rollup")
	}
	if wm.MinMinute != 0 || wm.MaxMinute != 59 {
		t.Fatalf("expected watermark [0,59], got [%d,%d]", wm.MinMinute, wm.MaxMinute)
	}
}
