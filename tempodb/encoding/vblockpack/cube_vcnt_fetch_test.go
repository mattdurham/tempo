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
