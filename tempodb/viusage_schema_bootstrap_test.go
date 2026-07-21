package tempodb

// viusage_schema_bootstrap_test.go — regression guard for issue #522's viusage_entries
// bootstrap-ordering fix (New()'s pg.ApplySchemas call, tempodb.go). Proves New() alone,
// against a genuinely fresh Postgres instance with NO other schema-apply step, leaves
// viusage_entries in a state where rw.pg.ViUsageEntryStore() -- the exact construction
// ConfigureViUsage's registryFor (vi_usage_hook.go) and NewViBackfillDepsWithPgRegistry
// (vi_backfill.go) both use against this same pool -- can actually UpsertEntry without
// "relation \"viusage_entries\" does not exist".

import (
	"context"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"
)

func TestNew_AppliesViUsageSchema_PgViUsageEntryStoreUsableImmediately(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)
	ctx := context.Background()

	store := rw.pg.ViUsageEntryStore()
	entry, err := store.UpsertEntry(ctx, "tenant-a", "col-hash-a", "string",
		func() blockpack.Entry {
			return blockpack.Entry{Tenant: "tenant-a", ColumnHash: "col-hash-a", ColumnType: "string", ColumnName: "span.name"}
		},
		func(_ *blockpack.Entry) error { return nil },
	)
	require.NoError(t, err, "viusage_entries must already exist after New() alone, with no separate schema-apply step")
	require.Equal(t, "span.name", entry.ColumnName)

	entries, err := store.Load(ctx, "tenant-a")
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
