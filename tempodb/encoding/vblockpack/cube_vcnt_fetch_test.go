package vblockpack

// cube_vcnt_fetch_test.go — coverage for buildVCNTSection, the store-agnostic core
// of the cube-cardinality-gate VCNT read path (#483). Exercises listing, .vcnt
// filtering, and consolidated-section building against an in-memory valueIndexStore
// without a live object store.

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"testing"

	blockpack "github.com/grafana/blockpack"
)

// memVCNTStore is an in-memory valueIndexStore holding raw object bytes by key.
// Only List and Get are meaningfully implemented — buildVCNTSection uses only those.
type memVCNTStore struct {
	objects map[string][]byte
}

func (m *memVCNTStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *memVCNTStore) Get(_ context.Context, key string) ([]byte, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, errors.New("not found: " + key)
	}
	return data, nil
}

func (m *memVCNTStore) Size(key string) (int64, error) {
	data, ok := m.objects[key]
	if !ok {
		return 0, errors.New("not found: " + key)
	}
	return int64(len(data)), nil
}

func (m *memVCNTStore) ReadAt(key string, p []byte, off int64) (int, error) {
	data, ok := m.objects[key]
	if !ok {
		return 0, errors.New("not found: " + key)
	}
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// vcntObjKey builds the S3 key a .vcnt object for column would live under, mirroring
// blockpack.VCNTObjectKey's layout (<tenant>/indexes/unique_values/<colHash>/L0-<id>.vcnt).
func vcntObjKey(tenant, column, id string) string {
	return blockpack.VCNTObjectKey(tenant, defaultValueIndexPref, column, id)
}

// vcntObjKeyV2 builds the S3 key a v2-format .vcnt object would live under, embedding an
// explicit wall-clock range for pruning tests.
func vcntObjKeyV2(tenant, column, id string, wallMinSec, wallMaxSec uint64) string {
	colHash := blockpack.VCNTColHash(column)
	filename := blockpack.VCNTFormatFilenameV2(0, wallMinSec, wallMaxSec, id)
	return path.Join(tenant, defaultValueIndexPref, "unique_values", colHash, filename)
}

// countingVCNTStore wraps memVCNTStore and records every key passed to Get, so a test can
// assert a file was never fetched (not merely "absent from the output section" — a bug that
// filters post-fetch instead of pre-fetch could satisfy that weaker assertion by accident).
type countingVCNTStore struct {
	*memVCNTStore
	gotKeys []string
}

func (c *countingVCNTStore) Get(ctx context.Context, key string) ([]byte, error) {
	c.gotKeys = append(c.gotKeys, key)
	return c.memVCNTStore.Get(ctx, key)
}

func vcntObj(t *testing.T, column string, values map[string]int64) []byte {
	t.Helper()
	var recs []blockpack.VCNTRecord
	for v, c := range values {
		recs = append(recs, blockpack.VCNTRecord{
			ColumnName: column,
			Value:      []byte(v),
			TimeStart:  60,
			TimeEnd:    60,
			Count:      c,
		})
	}
	blockpack.SortVCNTRecords(recs)
	// EncodeVCNTFile is the self-describing format the real block-builder writes
	// (issue #490 A-Tempo-1) and the only format DecodeVCNTObject accepts post-#490 A-3.
	return blockpack.EncodeVCNTFile(recs, 0)
}

// TEST-483-fetch-1: buildVCNTSection lists+fetches per-dim .vcnt objects and merges
// them into one section whose per-dim distinct counts match what was written.
func TestBuildVCNTSection_MergesPerDim(t *testing.T) {
	tenant := "tenant-a"
	store := &memVCNTStore{objects: map[string][]byte{
		vcntObjKey(tenant, "resource.service.name", "L0-aaa"): vcntObj(t, "resource.service.name", map[string]int64{"api": 5, "web": 3}),
		vcntObjKey(tenant, "span:kind", "L0-bbb"):             vcntObj(t, "span:kind", map[string]int64{"server": 8}),
	}}

	data, dir := buildVCNTSection(context.Background(), store, tenant, []string{"resource.service.name", "span:kind"}, 0, 120)
	if len(data) == 0 || len(dir) == 0 {
		t.Fatalf("expected a non-empty section, got data=%d dir=%d", len(data), len(dir))
	}

	// Assert real record content survived, not merely a non-empty byte count: a section
	// built entirely from silently-skipped undecodable objects (VCNTBuildSectionFromObjects
	// tolerates per-object decode failures) can still produce non-zero data/dir lengths from
	// the empty-but-valid encoding, so length alone cannot distinguish "records merged" from
	// "every input was skipped."
	for _, want := range []struct {
		column string
		value  string
		count  int64
	}{
		{"resource.service.name", "api", 5},
		{"resource.service.name", "web", 3},
		{"span:kind", "server", 8},
	} {
		est, err := blockpack.VCNTSelectivityInRange(data, dir, want.column, []byte(want.value), 0, 120)
		if err != nil {
			t.Fatalf("VCNTSelectivityInRange(%s=%s): %v", want.column, want.value, err)
		}
		if !est.Covered {
			t.Fatalf("VCNTSelectivityInRange(%s=%s): not covered, want Count=%d", want.column, want.value, want.count)
		}
		if est.Count != want.count {
			t.Fatalf("VCNTSelectivityInRange(%s=%s): Count=%d, want %d", want.column, want.value, est.Count, want.count)
		}
	}

	// Re-run the exact read the cardinality gate performs via a second, independent
	// section builder call to confirm both dims are present and gate-queryable.
	sameData, sameDir, _ := blockpack.VCNTBuildSectionFromObjects([][]byte{
		store.objects[vcntObjKey(tenant, "resource.service.name", "L0-aaa")],
		store.objects[vcntObjKey(tenant, "span:kind", "L0-bbb")],
	})
	if len(sameData) != len(data) || len(sameDir) != len(dir) {
		t.Fatalf("buildVCNTSection output differs from direct build: got data=%d dir=%d want data=%d dir=%d",
			len(data), len(dir), len(sameData), len(sameDir))
	}
}

// TEST-483-fetch-2: non-.vcnt keys under the same prefix are ignored (defensive: the
// prefix could in principle collect sibling files).
func TestBuildVCNTSection_IgnoresNonVCNTKeys(t *testing.T) {
	tenant := "tenant-b"
	colHash := blockpack.VCNTColHash("span:kind")
	prefix := path.Join(tenant, defaultValueIndexPref, "unique_values", colHash)
	store := &memVCNTStore{objects: map[string][]byte{
		vcntObjKey(tenant, "span:kind", "L0-ccc"): vcntObj(t, "span:kind", map[string]int64{"server": 1}),
		path.Join(prefix, "junk.tmp"):             []byte("garbage that must be ignored"),
	}}

	data, dir := buildVCNTSection(context.Background(), store, tenant, []string{"span:kind"}, 0, 120)
	if len(data) == 0 || len(dir) == 0 {
		t.Fatalf("expected a section from the one valid .vcnt file, got data=%d dir=%d", len(data), len(dir))
	}

	// Confirm the one real .vcnt object's record survived alongside the ignored junk key
	// (not merely that a non-empty section was produced — see TestBuildVCNTSection_MergesPerDim).
	est, err := blockpack.VCNTSelectivityInRange(data, dir, "span:kind", []byte("server"), 0, 120)
	if err != nil {
		t.Fatalf("VCNTSelectivityInRange: %v", err)
	}
	if !est.Covered || est.Count != 1 {
		t.Fatalf("VCNTSelectivityInRange(span:kind=server): Covered=%v Count=%d, want Covered=true Count=1",
			est.Covered, est.Count)
	}
}

// TEST-483-fetch-3: no coverage → nil section (gate passes by default), and this must
// not error even though Get would fail for a missing key.
func TestBuildVCNTSection_NoCoverageReturnsNil(t *testing.T) {
	store := &memVCNTStore{objects: map[string][]byte{}}
	data, dir := buildVCNTSection(context.Background(), store, "tenant-c", []string{"resource.service.name"}, 0, 120)
	if data != nil || dir != nil {
		t.Fatalf("expected nil section for absent coverage, got data=%d dir=%d", len(data), len(dir))
	}
}

// TEST-495-fetch-1: a v2-shaped .vcnt file whose embedded range provably does not overlap
// the query window must never be fetched (issue #495) — asserted via the actual Get call
// recorded by countingVCNTStore, not merely via the output section (R7a mechanism assertion).
func TestBuildVCNTSection_OutOfRangeV2FileNeverFetched(t *testing.T) {
	tenant := "tenant-a"
	key := vcntObjKeyV2(tenant, "span:kind", "id1", 500, 600)
	store := &countingVCNTStore{memVCNTStore: &memVCNTStore{objects: map[string][]byte{
		key: vcntObj(t, "span:kind", map[string]int64{"server": 1}),
	}}}

	data, dir := buildVCNTSection(context.Background(), store, tenant, []string{"span:kind"}, 0, 100)
	if data != nil || dir != nil {
		t.Fatalf("expected nil section for out-of-range v2 file, got data=%d dir=%d", len(data), len(dir))
	}
	for _, got := range store.gotKeys {
		if got == key {
			t.Fatalf("out-of-range v2 file %q must never be passed to Get", key)
		}
	}
}

// TEST-495-fetch-2: a v2-shaped .vcnt file whose embedded range overlaps the query window
// must still be fetched (sibling positive case to TestBuildVCNTSection_OutOfRangeV2FileNeverFetched).
func TestBuildVCNTSection_InRangeV2FileStillFetched(t *testing.T) {
	tenant := "tenant-a"
	key := vcntObjKeyV2(tenant, "span:kind", "id1", 500, 600)
	store := &countingVCNTStore{memVCNTStore: &memVCNTStore{objects: map[string][]byte{
		key: vcntObj(t, "span:kind", map[string]int64{"server": 1}),
	}}}

	data, dir := buildVCNTSection(context.Background(), store, tenant, []string{"span:kind"}, 100, 700)
	if len(data) == 0 || len(dir) == 0 {
		t.Fatalf("expected a non-empty section for in-range v2 file, got data=%d dir=%d", len(data), len(dir))
	}
	found := false
	for _, got := range store.gotKeys {
		if got == key {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected in-range v2 file %q to be passed to Get", key)
	}

	// vcntObj hardcodes each record's own TimeStart/TimeEnd to 60, independent of the
	// filename's embedded v2 range (500,600) used above purely for file-level pruning -- so
	// the record-level selectivity check below uses a window covering 60, not 100,700.
	est, err := blockpack.VCNTSelectivityInRange(data, dir, "span:kind", []byte("server"), 0, 120)
	if err != nil {
		t.Fatalf("VCNTSelectivityInRange: %v", err)
	}
	if !est.Covered || est.Count != 1 {
		t.Fatalf("VCNTSelectivityInRange(span:kind=server): Covered=%v Count=%d, want Covered=true Count=1",
			est.Covered, est.Count)
	}
}

// TEST-495-fetch-3: a v1-shaped .vcnt file (unknown range) must always be fetched regardless
// of the query window (issue #495 R7b) — pins the "unknown range, never dropped" fallback at
// the integration level, not just the helper's own unit test.
func TestBuildVCNTSection_V1ShapedFileStillFetchedRegardlessOfWindow(t *testing.T) {
	tenant := "tenant-a"
	key := vcntObjKey(tenant, "span:kind", "L0-v1id")
	store := &countingVCNTStore{memVCNTStore: &memVCNTStore{objects: map[string][]byte{
		key: vcntObj(t, "span:kind", map[string]int64{"server": 1}),
	}}}

	data, dir := buildVCNTSection(context.Background(), store, tenant, []string{"span:kind"}, 900, 1000)
	if len(data) == 0 || len(dir) == 0 {
		t.Fatalf("expected a non-empty section for v1-shaped file, got data=%d dir=%d", len(data), len(dir))
	}
	found := false
	for _, got := range store.gotKeys {
		if got == key {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected v1-shaped file %q to be fetched despite unrelated window", key)
	}
}

// TEST-495-fetch-4: query windows that merely touch a v2 file's boundary (WallMaxSec ==
// queryMinSec, or WallMinSec == queryMaxSec) must still be treated as overlapping and fetched
// (issue #495 R7c) — pins the inclusive-both-ends formula at the integration level.
func TestBuildVCNTSection_BoundaryTouchingWindowsIncluded(t *testing.T) {
	tenant := "tenant-a"
	key := vcntObjKeyV2(tenant, "span:kind", "id1", 100, 200)

	for _, window := range []struct{ minSec, maxSec uint64 }{
		{200, 300},
		{0, 100},
	} {
		store := &countingVCNTStore{memVCNTStore: &memVCNTStore{objects: map[string][]byte{
			key: vcntObj(t, "span:kind", map[string]int64{"server": 1}),
		}}}
		data, dir := buildVCNTSection(context.Background(), store, tenant, []string{"span:kind"}, window.minSec, window.maxSec)
		if len(data) == 0 || len(dir) == 0 {
			t.Fatalf("window [%d,%d]: expected boundary-touching file to be fetched, got data=%d dir=%d",
				window.minSec, window.maxSec, len(data), len(dir))
		}
		found := false
		for _, got := range store.gotKeys {
			if got == key {
				found = true
			}
		}
		if !found {
			t.Fatalf("window [%d,%d]: expected boundary-touching file %q to be passed to Get", window.minSec, window.maxSec, key)
		}
	}
}
