package vblockpack

// pg_entrystore.go — Postgres-backed blockpack.EntryStore/CubeEntryStore
// implementations (2026-07-11 opt-in Postgres backend). Row-oriented: one
// Postgres row per (tenant, col_hash, col_type) for viusage, one row per
// cube_id for cube, eliminating the single-shared-blob contention point the
// JSON-blob ObjectStore path has (every column's UpdateWatermark call today
// serializes against EVERY OTHER column's update for the same tenant via one
// shared index.json). Uses a single transaction with SELECT ... FOR UPDATE to
// get the same atomicity RecordUseAndMaybeTrigger's evaluate-and-mutate step
// needs, without a retry loop, for keys that already have a row (Postgres's
// row lock makes concurrent UpsertEntry calls for the SAME existing key queue
// rather than conflict-and-retry). SELECT ... FOR UPDATE cannot lock a row
// that doesn't exist yet, though, so the create-path additionally relies on
// INSERT ... ON CONFLICT DO NOTHING plus a re-load-under-FOR-UPDATE fallback
// for whichever concurrent caller loses that race (see UpsertEntry) to avoid
// a duplicate-key error and to still apply the loser's own mutation exactly
// once. READ COMMITTED (pgx's default) is sufficient -- the atomicity
// boundary is the transaction wrapping ONE row's lock, not a multi-row
// invariant.

import (
	"context"
	"errors"
	"fmt"

	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgViUsageEntryStore satisfies blockpack.EntryStore over a *pgxpool.Pool.
type pgViUsageEntryStore struct{ pool *pgxpool.Pool }

func newPgViUsageEntryStore(pool *pgxpool.Pool) *pgViUsageEntryStore {
	return &pgViUsageEntryStore{pool: pool}
}

const viusageSelectAllSQL = `
	SELECT tenant, col_hash, col_type, column_name, first_seen_sec, created_at,
		lease_owner_id, lease_expires_at, watermark_sec, window_start_sec,
		window_end_sec, triggered, backfill_in_progress, done, last_catalog_row_id
	FROM viusage_entries WHERE tenant = $1`

const viusageSelectOneForUpdateSQL = `
	SELECT tenant, col_hash, col_type, column_name, first_seen_sec, created_at,
		lease_owner_id, lease_expires_at, watermark_sec, window_start_sec,
		window_end_sec, triggered, backfill_in_progress, done, last_catalog_row_id
	FROM viusage_entries WHERE tenant = $1 AND col_hash = $2 AND col_type = $3 FOR UPDATE`

// viusageInsertSQL's ON CONFLICT target is viusage_entries' own PRIMARY KEY
// (tenant, col_hash, col_type) -- see registries.sql. DO NOTHING lets two
// concurrent UpsertEntry calls for the same not-yet-existing key both attempt
// this INSERT without a duplicate-key error; the loser re-loads (and locks)
// the winner's committed row instead (see UpsertEntry).
const viusageInsertSQL = `
	INSERT INTO viusage_entries (
		tenant, col_hash, col_type, column_name, first_seen_sec, created_at,
		lease_owner_id, lease_expires_at, watermark_sec, window_start_sec,
		window_end_sec, triggered, backfill_in_progress, done, last_catalog_row_id
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
	ON CONFLICT (tenant, col_hash, col_type) DO NOTHING`

const viusageUpdateSQL = `
	UPDATE viusage_entries SET
		column_name = $4, first_seen_sec = $5, created_at = $6,
		lease_owner_id = $7, lease_expires_at = $8, watermark_sec = $9,
		window_start_sec = $10, window_end_sec = $11, triggered = $12,
		backfill_in_progress = $13, done = $14, last_catalog_row_id = $15
	WHERE tenant = $1 AND col_hash = $2 AND col_type = $3`

// viusageRowScanner is satisfied by both pgx.Rows and pgx.Row.
type viusageRowScanner interface {
	Scan(dest ...any) error
}

func scanViUsageEntry(row viusageRowScanner) (blockpack.Entry, error) {
	var e blockpack.Entry
	err := row.Scan(
		&e.Tenant, &e.ColumnHash, &e.ColumnType, &e.ColumnName, &e.FirstSeenSec, &e.CreatedAt,
		&e.Backfill.LeaseOwnerID, &e.Backfill.LeaseExpiresAt, &e.Backfill.WatermarkSec,
		&e.Backfill.WindowStartSec, &e.Backfill.WindowEndSec, &e.Backfill.Triggered,
		&e.Backfill.BackfillInProgress, &e.Backfill.Done, &e.Backfill.LastCatalogRowID,
	)
	return e, err
}

func (s *pgViUsageEntryStore) Load(ctx context.Context, tenant string) ([]blockpack.Entry, error) {
	rows, err := s.pool.Query(ctx, viusageSelectAllSQL, tenant)
	if err != nil {
		return nil, fmt.Errorf("pg viusage entrystore: load: %w", err)
	}
	defer rows.Close()

	var out []blockpack.Entry
	for rows.Next() {
		e, scanErr := scanViUsageEntry(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("pg viusage entrystore: scan: %w", scanErr)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *pgViUsageEntryStore) UpsertEntry(
	ctx context.Context, tenant, colHash, colType string,
	createIfMissing func() blockpack.Entry,
	mutate func(*blockpack.Entry) error,
) (blockpack.Entry, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return blockpack.Entry{}, fmt.Errorf("pg viusage entrystore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit

	entry, found, err := loadViUsageEntryForUpdate(ctx, tx, tenant, colHash, colType)
	if err != nil {
		return blockpack.Entry{}, err
	}
	if !found {
		if createIfMissing == nil {
			return blockpack.Entry{}, fmt.Errorf("pg viusage entrystore: entry %s/%s/%s not found", tenant, colHash, colType)
		}
		entry = createIfMissing()
		inserted, err := insertViUsageEntryIfAbsent(ctx, tx, entry)
		if err != nil {
			return blockpack.Entry{}, err
		}
		if !inserted {
			// Lost the insert race: a concurrent UpsertEntry call committed the
			// same key first. Re-load under FOR UPDATE to see (and lock) their
			// row so this caller's mutate below still gets applied, exactly
			// once, on top of the real row -- never silently dropped.
			entry, found, err = loadViUsageEntryForUpdate(ctx, tx, tenant, colHash, colType)
			if err != nil {
				return blockpack.Entry{}, err
			}
			if !found {
				return blockpack.Entry{}, fmt.Errorf("pg viusage entrystore: entry %s/%s/%s vanished after losing insert race", tenant, colHash, colType)
			}
		}
	}
	if err := mutate(&entry); err != nil {
		return blockpack.Entry{}, err
	}
	if err := updateViUsageEntry(ctx, tx, entry); err != nil {
		return blockpack.Entry{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return blockpack.Entry{}, fmt.Errorf("pg viusage entrystore: commit: %w", err)
	}
	return entry, nil
}

func loadViUsageEntryForUpdate(ctx context.Context, tx pgx.Tx, tenant, colHash, colType string) (blockpack.Entry, bool, error) {
	row := tx.QueryRow(ctx, viusageSelectOneForUpdateSQL, tenant, colHash, colType)
	e, err := scanViUsageEntry(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return blockpack.Entry{}, false, nil
		}
		return blockpack.Entry{}, false, fmt.Errorf("pg viusage entrystore: load for update: %w", err)
	}
	return e, true, nil
}

// insertViUsageEntryIfAbsent reports whether this call's INSERT actually won
// (true) or was absorbed by ON CONFLICT DO NOTHING because a concurrent
// caller's row for the same key already committed (false).
func insertViUsageEntryIfAbsent(ctx context.Context, tx pgx.Tx, e blockpack.Entry) (bool, error) {
	tag, err := tx.Exec(ctx, viusageInsertSQL,
		e.Tenant, e.ColumnHash, e.ColumnType, e.ColumnName, e.FirstSeenSec, e.CreatedAt,
		e.Backfill.LeaseOwnerID, e.Backfill.LeaseExpiresAt, e.Backfill.WatermarkSec,
		e.Backfill.WindowStartSec, e.Backfill.WindowEndSec, e.Backfill.Triggered,
		e.Backfill.BackfillInProgress, e.Backfill.Done, e.Backfill.LastCatalogRowID,
	)
	if err != nil {
		return false, fmt.Errorf("pg viusage entrystore: insert: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func updateViUsageEntry(ctx context.Context, tx pgx.Tx, e blockpack.Entry) error {
	_, err := tx.Exec(ctx, viusageUpdateSQL,
		e.Tenant, e.ColumnHash, e.ColumnType, e.ColumnName, e.FirstSeenSec, e.CreatedAt,
		e.Backfill.LeaseOwnerID, e.Backfill.LeaseExpiresAt, e.Backfill.WatermarkSec,
		e.Backfill.WindowStartSec, e.Backfill.WindowEndSec, e.Backfill.Triggered,
		e.Backfill.BackfillInProgress, e.Backfill.Done, e.Backfill.LastCatalogRowID,
	)
	if err != nil {
		return fmt.Errorf("pg viusage entrystore: update: %w", err)
	}
	return nil
}

var _ blockpack.EntryStore = (*pgViUsageEntryStore)(nil)
