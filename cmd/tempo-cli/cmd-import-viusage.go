package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/tempodb/backend"
)

// importViusageCmd is a REVIEWED, READY-TO-USE ARTIFACT for a hypothetical future
// production Postgres rollout -- it is NOT run as part of this task, and is not run
// against tempo-dev-test-03's own cutover. For tempo-dev-test-03 specifically, the
// accepted plan is "start fresh in Postgres": losing in-progress dev-test-03
// backfill state is a wasted-work risk, not a data-loss risk, since
// BackfillState.CoversRange already treats "never triggered" as "no coverage" --
// a column simply re-triggers and re-backfills the next time it's used. This
// importer exists only so a later, real production rollout has a reviewed,
// ready-to-invoke path to carry forward existing S3 registry state instead of
// discarding it.
//
// Reads <tenant>/viusage/index.json and <tenant>/cubes/index.json per tenant via
// the tenant's already-configured backend.Reader (same construction every other
// tempo-cli command uses -- no new S3/GCS/Azure client code), decodes each
// Entry/RegistryEntry, and writes INSERT ... ON CONFLICT (...) DO NOTHING into
// viusage_entries/cube_entries. DO NOTHING (not DO UPDATE) is deliberate: a live
// Postgres registry could already be ahead of a stale S3 snapshot by the time an
// operator runs this, and this importer must never regress a row that's already
// moved on.
//
// UseTimestamps (source JSON field) is simply dropped during import -- Part 0
// removed the field from blockpack.Entry entirely; there is no destination column
// for it and no remaining semantic purpose (the repeated-use threshold it fed no
// longer exists).
type importViusageCmd struct {
	backendOptions

	PostgresDSN string `required:"" help:"Postgres DSN to import into (see modules/postgres.Config.DSN)"`
	Tenant      string `arg:"" help:"tenant ID to import (run once per tenant)"`
	Commit      bool   `help:"actually write to Postgres. Without this flag, only prints what would be imported" default:"false"`
}

// usageIndexWire mirrors viusage's private on-disk JSON shape
// (internal/modules/viusage/registry.go's usageIndex) -- {"version":N,"entries":[...]}.
// Duplicated here rather than exported from blockpack: this importer only needs the
// wire shape, not any behavior, and blockpack's CLAUDE.md requires explicit
// permission before adding new public API surface.
type usageIndexWire struct {
	Version int               `json:"version"`
	Entries []blockpack.Entry `json:"entries"`
}

// cubeIndexWire mirrors cube's private on-disk JSON shape
// (internal/modules/cube/entry_store.go's cubeIndex) -- {"version":N,"cubes":[...]}.
type cubeIndexWire struct {
	Version int                           `json:"version"`
	Cubes   []blockpack.CubeRegistryEntry `json:"cubes"`
}

func (cmd *importViusageCmd) Run(g *globalOptions) error {
	ctx := context.Background()

	r, _, _, err := loadRawBackend(&cmd.backendOptions, g)
	if err != nil {
		return fmt.Errorf("loading backend: %w", err)
	}
	defer r.Shutdown()

	usageEntries, err := readUsageIndex(ctx, r, cmd.Tenant)
	if err != nil {
		return fmt.Errorf("reading viusage index for tenant %s: %w", cmd.Tenant, err)
	}
	cubeEntries, err := readCubeIndex(ctx, r, cmd.Tenant)
	if err != nil {
		return fmt.Errorf("reading cube index for tenant %s: %w", cmd.Tenant, err)
	}

	fmt.Printf("tenant %s: %d viusage entries, %d cube entries found\n", cmd.Tenant, len(usageEntries), len(cubeEntries))

	if !cmd.Commit {
		fmt.Println("--dry-run (default): no writes performed. Pass --commit to actually import.")
		for _, e := range usageEntries {
			fmt.Printf("  [dry-run] viusage_entries: tenant=%s col_hash=%s col_type=%s column_name=%s\n",
				e.Tenant, e.ColumnHash, e.ColumnType, e.ColumnName)
		}
		for _, e := range cubeEntries {
			fmt.Printf("  [dry-run] cube_entries: tenant=%s cube_id=%s dimensions=%v\n", e.Tenant, e.CubeID, e.Dimensions)
		}
		return nil
	}

	pool, err := postgres.NewPool(ctx, &postgres.Config{DSN: cmd.PostgresDSN})
	if err != nil {
		return fmt.Errorf("connecting to postgres %s: %w", postgres.RedactDSN(cmd.PostgresDSN), err)
	}
	defer pool.Close()

	imported, err := importUsageEntries(ctx, pool, usageEntries)
	if err != nil {
		return fmt.Errorf("importing viusage entries: %w", err)
	}
	fmt.Printf("imported %d/%d viusage entries (rest already present, DO NOTHING)\n", imported, len(usageEntries))

	imported, err = importCubeEntries(ctx, pool, cubeEntries)
	if err != nil {
		return fmt.Errorf("importing cube entries: %w", err)
	}
	fmt.Printf("imported %d/%d cube entries (rest already present, DO NOTHING)\n", imported, len(cubeEntries))

	return nil
}

func readUsageIndex(ctx context.Context, r backend.RawReader, tenant string) ([]blockpack.Entry, error) {
	data, err := readObject(ctx, r, tenant+"/viusage/index.json")
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	var idx usageIndexWire
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("decoding viusage index: %w", err)
	}
	return idx.Entries, nil
}

func readCubeIndex(ctx context.Context, r backend.RawReader, tenant string) ([]blockpack.CubeRegistryEntry, error) {
	data, err := readObject(ctx, r, tenant+"/cubes/index.json")
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	var idx cubeIndexWire
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("decoding cube index: %w", err)
	}
	return idx.Cubes, nil
}

// readObject reads path via the tenant's already-configured backend.RawReader,
// returning (nil, nil) if the object does not exist yet (a tenant with no
// viusage/cube activity has no index.json at all -- not an error).
func readObject(ctx context.Context, r backend.RawReader, path string) ([]byte, error) {
	rc, _, err := r.Read(ctx, path, backend.KeyPath{}, nil)
	if err != nil {
		if errors.Is(err, backend.ErrDoesNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer rc.Close()

	buf := make([]byte, 0)
	chunk := make([]byte, 32*1024)
	for {
		n, rerr := rc.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if rerr != nil {
			break
		}
	}
	return buf, nil
}

func importUsageEntries(ctx context.Context, pool *pgxpool.Pool, entries []blockpack.Entry) (int, error) {
	imported := 0
	for _, e := range entries {
		tag, err := pool.Exec(ctx, `
			INSERT INTO viusage_entries (
				tenant, col_hash, col_type, column_name, first_seen_sec, created_at,
				lease_owner_id, lease_expires_at, watermark_sec, window_start_sec,
				window_end_sec, triggered, backfill_in_progress, done, last_catalog_row_id
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
			ON CONFLICT (tenant, col_hash, col_type) DO NOTHING`,
			e.Tenant, e.ColumnHash, e.ColumnType, e.ColumnName, e.FirstSeenSec, e.CreatedAt,
			e.Backfill.LeaseOwnerID, e.Backfill.LeaseExpiresAt, e.Backfill.WatermarkSec,
			e.Backfill.WindowStartSec, e.Backfill.WindowEndSec, e.Backfill.Triggered,
			e.Backfill.BackfillInProgress, e.Backfill.Done, e.Backfill.LastCatalogRowID,
		)
		if err != nil {
			return imported, fmt.Errorf("inserting viusage entry %s/%s/%s: %w", e.Tenant, e.ColumnHash, e.ColumnType, err)
		}
		imported += int(tag.RowsAffected())
	}
	return imported, nil
}

func importCubeEntries(ctx context.Context, pool *pgxpool.Pool, entries []blockpack.CubeRegistryEntry) (int, error) {
	imported := 0
	for _, e := range entries {
		dimensions, err := json.Marshal(e.Dimensions)
		if err != nil {
			return imported, fmt.Errorf("marshaling dimensions for cube %s: %w", e.CubeID, err)
		}
		filters, err := json.Marshal(e.Filters)
		if err != nil {
			return imported, fmt.Errorf("marshaling filters for cube %s: %w", e.CubeID, err)
		}
		aggAttrs, err := json.Marshal(e.AggAttrs)
		if err != nil {
			return imported, fmt.Errorf("marshaling agg_attrs for cube %s: %w", e.CubeID, err)
		}
		watermarks, err := json.Marshal(e.Watermarks)
		if err != nil {
			return imported, fmt.Errorf("marshaling watermarks for cube %s: %w", e.CubeID, err)
		}

		tag, err := pool.Exec(ctx, `
			INSERT INTO cube_entries (cube_id, tenant, dimensions, filters, agg_attrs, resolution, created_at, watermarks)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (cube_id) DO NOTHING`,
			e.CubeID, e.Tenant, dimensions, filters, aggAttrs, e.Resolution, e.CreatedAt, watermarks,
		)
		if err != nil {
			return imported, fmt.Errorf("inserting cube entry %s: %w", e.CubeID, err)
		}
		imported += int(tag.RowsAffected())
	}
	return imported, nil
}
