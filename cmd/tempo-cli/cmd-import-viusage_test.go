package main

import (
	"bytes"
	"context"
	"path"
	"strings"
	"testing"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
)

// putObject writes to a real, ephemeral, local-filesystem backend (t.TempDir())
// -- no fakes, no external infra, matching this project's "local backend should
// always work" convention. local.Write's MkdirAll only creates keypath's
// directories (not name's), so the directory portion of objPath must be passed
// as keypath, mirroring how readObject's production code reads back the exact
// same full path (Read has no such restriction -- it just opens the joined
// path directly, so readObject itself needs no equivalent split).
func putObject(t *testing.T, w backend.RawWriter, objPath string, data []byte) {
	t.Helper()
	dir, name := path.Split(objPath)
	keyPath := backend.KeyPath(strings.Split(strings.TrimSuffix(dir, "/"), "/"))
	if err := w.Write(context.Background(), name, keyPath, bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatalf("writing %s: %v", objPath, err)
	}
}

func TestReadUsageIndex_DecodesEntries(t *testing.T) {
	dir := t.TempDir()
	r, w, _, err := local.New(&local.Config{Path: dir})
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	t.Cleanup(r.Shutdown)

	putObject(t, w, "tenant-a/viusage/index.json", []byte(`{
		"version": 1,
		"entries": [
			{
				"tenant": "tenant-a",
				"column_hash": "abc123",
				"column_name": "span.http.method",
				"column_type": "string",
				"backfill": {"triggered": true, "done": false, "last_catalog_row_id": 42},
				"first_seen_sec": 100,
				"created_at": 100
			}
		]
	}`))

	entries, err := readUsageIndex(context.Background(), r, "tenant-a")
	if err != nil {
		t.Fatalf("readUsageIndex: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	e := entries[0]
	if e.Tenant != "tenant-a" || e.ColumnHash != "abc123" || e.ColumnType != "string" {
		t.Errorf("unexpected entry: %+v", e)
	}
	if !e.Backfill.Triggered || e.Backfill.LastCatalogRowID != 42 {
		t.Errorf("backfill state not decoded correctly: %+v", e.Backfill)
	}
}

func TestReadUsageIndex_MissingObject_ReturnsNilNoError(t *testing.T) {
	r, _, _, err := local.New(&local.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	t.Cleanup(r.Shutdown)

	entries, err := readUsageIndex(context.Background(), r, "tenant-with-no-activity")
	if err != nil {
		t.Fatalf("readUsageIndex on missing object should not error, got: %v", err)
	}
	if entries != nil {
		t.Errorf("expected nil entries for missing object, got %v", entries)
	}
}

func TestReadCubeIndex_DecodesEntries(t *testing.T) {
	dir := t.TempDir()
	r, w, _, err := local.New(&local.Config{Path: dir})
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	t.Cleanup(r.Shutdown)

	putObject(t, w, "tenant-a/cubes/index.json", []byte(`{
		"version": 1,
		"cubes": [
			{
				"cube_id": "deadbeef",
				"tenant": "tenant-a",
				"dimensions": ["resource.service.name", "span.name"],
				"agg_attrs": ["duration"],
				"resolution": 1,
				"created_at": 100
			}
		]
	}`))

	entries, err := readCubeIndex(context.Background(), r, "tenant-a")
	if err != nil {
		t.Fatalf("readCubeIndex: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	e := entries[0]
	if e.CubeID != "deadbeef" || e.Tenant != "tenant-a" || len(e.Dimensions) != 2 {
		t.Errorf("unexpected cube entry: %+v", e)
	}
}

func TestReadCubeIndex_MissingObject_ReturnsNilNoError(t *testing.T) {
	r, _, _, err := local.New(&local.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	t.Cleanup(r.Shutdown)

	entries, err := readCubeIndex(context.Background(), r, "tenant-with-no-cubes")
	if err != nil {
		t.Fatalf("readCubeIndex on missing object should not error, got: %v", err)
	}
	if entries != nil {
		t.Errorf("expected nil entries for missing object, got %v", entries)
	}
}

// TestImportViusageCmd_Run_DryRunDefault_MakesNoNetworkCalls verifies --commit
// defaults to false: Run must never construct a Postgres pool (and thus never
// dial anything) unless --commit is explicitly set. This is the safety-net
// regression guard for this reviewed-only artifact's dry-run-by-default
// contract -- an accidental real invocation without --commit must be a no-op.
//
// The index must contain at least one entry: with zero entries, the
// import*Entries loops never call pool.Exec at all, so a bypassed guard would
// pass this test for the wrong reason (no query attempted either way) --
// confirmed by mutation-testing this exact test (temporarily inverted the
// `if !cmd.Commit` guard to `if cmd.Commit`; with an empty entries list the
// test still incorrectly passed, since postgres.NewPool's connection is lazy
// and no Exec call was ever made to trigger a real dial; only after adding a
// non-empty entry here did the same mutation correctly make the test fail).
func TestImportViusageCmd_Run_DryRunDefault_MakesNoNetworkCalls(t *testing.T) {
	dir := t.TempDir()
	r, w, _, err := local.New(&local.Config{Path: dir})
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	t.Cleanup(r.Shutdown)
	putObject(t, w, "tenant-a/viusage/index.json", []byte(`{
		"version": 1,
		"entries": [
			{"tenant": "tenant-a", "column_hash": "abc123", "column_name": "span.name", "column_type": "string"}
		]
	}`))

	cmd := &importViusageCmd{
		backendOptions: backendOptions{Backend: "local", Bucket: dir},
		PostgresDSN:    "postgres://invalid-host-that-does-not-resolve:5432/db",
		Tenant:         "tenant-a",
		Commit:         false,
	}
	if err := cmd.Run(&globalOptions{}); err != nil {
		t.Fatalf("dry-run Run should not error even with an unreachable PostgresDSN, got: %v", err)
	}
}
