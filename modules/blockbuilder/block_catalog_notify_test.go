package blockbuilder

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
	"github.com/grafana/tempo/tempodb/wal"
)

// newTestWriter constructs a real tempodb.Writer (via tempodb.New(), the same production
// factory backend-worker/job-planner/block-builder all use) with cfg.Postgres pointed at a
// genuinely fresh testcontainer -- its concrete type already implements tempodb.PgPoolProvider
// (issue #522 #157/#159), exactly what notifyBlockpackFileCatalog's own type-assertion relies on.
func newTestWriter(t *testing.T, withPostgres bool) tempodb.Writer {
	t.Helper()
	tempDir := t.TempDir()
	cfg := &tempodb.Config{
		Backend: backend.Local,
		Local:   &local.Config{Path: path.Join(tempDir, "traces")},
		Block:   &common.BlockConfig{BloomFP: .01, BloomShardSizeBytes: 100_000, Version: "vblockpack"},
		WAL:     &wal.Config{Filepath: path.Join(tempDir, "wal")},
	}
	if withPostgres {
		ctx := context.Background()
		container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("blockbuilder_notify_test"),
			tcpostgres.WithUsername("blockbuilder_notify_test"),
			tcpostgres.WithPassword("blockbuilder_notify_test"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "docker") {
				t.Skipf("Docker unavailable in this environment, skipping Postgres-backed test: %v", err)
			}
			t.Fatalf("starting postgres testcontainer: %v", err)
		}
		t.Cleanup(func() { _ = container.Terminate(context.Background()) })
		dsn, dsnErr := container.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, dsnErr)
		cfg.Postgres = &postgres.Config{DSN: dsn}
	}

	_, w, _, err := tempodb.New(cfg, nil, log.NewNopLogger())
	require.NoError(t, err)
	return w
}

func testBlockMeta(tenant, version string) *backend.BlockMeta {
	return &backend.BlockMeta{
		BlockID:         backend.NewUUID(),
		TenantID:        tenant,
		Version:         version,
		CompactionLevel: 0,
		StartTime:       time.Unix(1000, 0),
		EndTime:         time.Unix(2000, 0),
		Size_:           12345,
	}
}

// TestNotifyBlockpackFileCatalog_VblockpackWithPostgres_InsertsRow proves the happy path: a
// vblockpack block, written by a Writer with Postgres configured, gets a real
// blockpack_file_catalog row (subsystem="trace") so compaction-planner can see it as a fresh
// trace_compaction candidate.
func TestNotifyBlockpackFileCatalog_VblockpackWithPostgres_InsertsRow(t *testing.T) {
	w := newTestWriter(t, true)
	provider, ok := w.(tempodb.PgPoolProvider)
	require.True(t, ok, "test writer must implement PgPoolProvider")
	pg := provider.PgPool()
	require.NotNil(t, pg)

	meta := testBlockMeta("tenant-a", vblockpack.VersionString)
	notifyBlockpackFileCatalog(context.Background(), log.NewNopLogger(), w, meta)

	rows, err := pg.FileCatalogStore().ListLiveKeys(context.Background(), "trace", "tenant-a")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "tenant-a/"+meta.BlockID.String()+"/data.blockpack", rows[0].ObjectKey)
	require.Equal(t, int64(1000), rows[0].MinSec)
	require.Equal(t, int64(2000), rows[0].MaxSec)
	require.Equal(t, int64(12345), rows[0].SizeBytes)
}

// TestNotifyBlockpackFileCatalog_NonVblockpackVersion_DoesNothing proves vparquet/standard
// blocks are never cataloged here, matching this project's "vparquet stays untouched forever"
// scoping (mirrors filecatalog.Lister.reconcileTenant's identical Version filter).
func TestNotifyBlockpackFileCatalog_NonVblockpackVersion_DoesNothing(t *testing.T) {
	w := newTestWriter(t, true)
	provider, ok := w.(tempodb.PgPoolProvider)
	require.True(t, ok)
	pg := provider.PgPool()

	meta := testBlockMeta("tenant-a", "vParquet4")
	notifyBlockpackFileCatalog(context.Background(), log.NewNopLogger(), w, meta)

	rows, err := pg.FileCatalogStore().ListLiveKeys(context.Background(), "trace", "tenant-a")
	require.NoError(t, err)
	require.Empty(t, rows, "a non-vblockpack block must never be cataloged")
}

// TestNotifyBlockpackFileCatalog_NoPostgresConfigured_DoesNotPanic proves the safe no-op when
// Postgres isn't configured on this deployment -- notifyBlockpackFileCatalog's PgPoolProvider
// type-assertion or nil-pool check must short-circuit cleanly, never panic or error the flush.
func TestNotifyBlockpackFileCatalog_NoPostgresConfigured_DoesNotPanic(t *testing.T) {
	w := newTestWriter(t, false)
	meta := testBlockMeta("tenant-a", vblockpack.VersionString)
	require.NotPanics(t, func() {
		notifyBlockpackFileCatalog(context.Background(), log.NewNopLogger(), w, meta)
	})
}
