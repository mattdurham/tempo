package valueindexconsumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/rqlite/gorqlite"
	"github.com/rs/xid"

	"github.com/grafana/blockpack/internal/modules/blockevents"
)

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// jobTable is the rqlite table backing the value-index job queue. It must match
// blockevents' jobTable so the publisher and consumer agree on the schema. See
// NOTE-VI-021.
const jobTable = "value_index_jobs"

// rqliteDB is the minimal rqlite surface the consumer needs.
//
// Writes use gorqlite's raw types directly. Reads go through claimedPaths
// instead of returning *gorqlite.QueryResult, because QueryResult's fields are
// unexported and its Next/Scan dereference an internal connection — a test fake
// cannot construct or iterate one. The connAdapter wraps a real
// *gorqlite.Connection to satisfy this interface; tests substitute a fake that
// returns scripted paths. See NOTE-VI-021.
type rqliteDB interface {
	WriteParameterizedContext(
		ctx context.Context,
		statements []gorqlite.ParameterizedStatement,
	) ([]gorqlite.WriteResult, error)
	WriteParameterized(statements []gorqlite.ParameterizedStatement) ([]gorqlite.WriteResult, error)
	// claimedPaths runs the read-back SELECT and returns the file_path of each
	// row this worker now owns, in order.
	claimedPaths(ctx context.Context, workerID string, limit int) ([]string, error)
	Close()
}

// connAdapter wraps a *gorqlite.Connection to satisfy rqliteDB, translating the
// read-back SELECT into a []string of file paths so the orchestration is
// testable against a fake that needs no real rqlite server.
type connAdapter struct {
	conn *gorqlite.Connection
}

func (a connAdapter) WriteParameterizedContext(
	ctx context.Context,
	s []gorqlite.ParameterizedStatement,
) ([]gorqlite.WriteResult, error) {
	return a.conn.WriteParameterizedContext(ctx, s)
}

func (a connAdapter) WriteParameterized(s []gorqlite.ParameterizedStatement) ([]gorqlite.WriteResult, error) {
	return a.conn.WriteParameterized(s)
}

func (a connAdapter) Close() { a.conn.Close() }

func (a connAdapter) claimedPaths(ctx context.Context, workerID string, limit int) ([]string, error) {
	results, err := a.conn.QueryParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query: `SELECT file_path FROM ` + jobTable + `
				WHERE status = 'in_progress' AND worker_id = ?
				ORDER BY inserted_at ASC
				LIMIT ?`,
			Arguments: []any{workerID, limit},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	qr := results[0]
	if qr.Err != nil {
		return nil, qr.Err
	}
	var paths []string
	for qr.Next() {
		var p string
		if err := qr.Scan(&p); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, nil
}

// RqliteConsumer is a pull-based Consumer backed by the rqlite job table.
//
// Unlike the Redis Streams consumer this is not a push-delivered queue: each
// Poll atomically claims up to BatchSize rows by flipping their status to
// 'in_progress' and stamping worker_id + claimed_at, then returns them. Ack
// deletes the claimed rows. Because the claim UPDATE also reclaims rows whose
// 'in_progress' claim is older than ClaimIdleThreshold, stale claims from a
// dead worker are recovered inline — no separate reaper. rqlite's Raft layer
// serializes concurrent workers' claim writes, so two workers never claim the
// same row. See NOTE-VI-021.
type RqliteConsumer struct {
	db       rqliteDB
	workerID string
	cfg      Config
}

// NewRqliteConsumer dials rqlite, ensures the job schema exists, and returns a
// ready Consumer.
func NewRqliteConsumer(cfg Config) (*RqliteConsumer, error) {
	cfg = cfg.withDefaults()
	if cfg.RqliteURL == "" {
		return nil, errors.New("valueindexconsumer: rqlite_url required")
	}
	conn, err := gorqlite.Open(cfg.RqliteURL)
	if err != nil {
		return nil, fmt.Errorf("valueindexconsumer: open rqlite: %w", err)
	}
	c := newRqliteConsumer(connAdapter{conn: conn}, cfg)
	if err := c.ensureSchema(); err != nil {
		conn.Close()
		return nil, err
	}
	return c, nil
}

// newRqliteConsumer wires a consumer around an already-constructed rqliteDB.
// cfg is assumed to have defaults applied. The worker_id defaults to a fresh
// xid when ConsumerName is empty, so each instance owns a distinct claim
// identity (used for stale-claim diagnosis and reclaim).
func newRqliteConsumer(db rqliteDB, cfg Config) *RqliteConsumer {
	workerID := cfg.ConsumerName
	if workerID == "" {
		workerID = "vic-" + xid.New().String()
	}
	return &RqliteConsumer{db: db, workerID: workerID, cfg: cfg}
}

// ensureSchema creates the job table and status index if absent. Idempotent and
// safe to run concurrently from every pod (rqlite Raft serializes the writes).
func (c *RqliteConsumer) ensureSchema() error {
	_, err := c.db.WriteParameterized([]gorqlite.ParameterizedStatement{
		{Query: `CREATE TABLE IF NOT EXISTS ` + jobTable + ` (
			file_path   TEXT NOT NULL PRIMARY KEY,
			inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
			claimed_at  TEXT,
			worker_id   TEXT,
			status      TEXT NOT NULL DEFAULT 'pending'
		)`},
		{Query: `CREATE INDEX IF NOT EXISTS idx_status_inserted
			ON ` + jobTable + ` (status, inserted_at)`},
	})
	if err != nil {
		return fmt.Errorf("valueindexconsumer: ensure schema: %w", err)
	}
	return nil
}

// Poll atomically claims up to BatchSize pending (or stale in_progress) rows for
// this worker and returns them. It does not block waiting for rows: an empty
// table yields an empty slice and nil error so the caller can run periodic work
// (the elapsed-time flush check, NOTE-VI-020). The claim and the read are two
// statements: the UPDATE flips status under Raft serialization, the SELECT then
// reads back exactly the rows this worker now owns. Because the UPDATE stamps
// worker_id, the follow-up SELECT filtered by worker_id returns only rows
// claimed by this Poll, never rows another worker holds.
//
// Stale reclaim is inline: the claim predicate also matches rows whose
// in_progress claim is older than ClaimIdleThreshold, so a dead worker's
// orphans are picked up by the next live Poll with no separate reaper.
//
// The job ID (Message.ID) is the file_path itself — the table's primary key —
// so Ack can delete by it directly.
func (c *RqliteConsumer) Poll(ctx context.Context) ([]Message, error) {
	staleCutoff := fmt.Sprintf("-%d seconds", int(c.cfg.ClaimIdleThreshold.Seconds()))

	// Claim: flip up to BatchSize eligible rows to in_progress for this worker.
	// SQLite's UPDATE ... WHERE file_path IN (SELECT ... LIMIT N) bounds the
	// number claimed; ORDER BY inserted_at ASC processes oldest first (FIFO).
	_, err := c.db.WriteParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query: `UPDATE ` + jobTable + `
				SET status = 'in_progress', claimed_at = datetime('now'), worker_id = ?
				WHERE file_path IN (
					SELECT file_path FROM ` + jobTable + `
					WHERE status = 'pending'
					   OR (status = 'in_progress' AND claimed_at < datetime('now', ?))
					ORDER BY inserted_at ASC
					LIMIT ?
				)`,
			Arguments: []any{c.workerID, staleCutoff, c.cfg.BatchSize},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("valueindexconsumer: claim: %w", err)
	}

	// Read back the rows this worker now owns.
	paths, err := c.db.claimedPaths(ctx, c.workerID, c.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("valueindexconsumer: read claimed: %w", err)
	}

	var msgs []Message
	for _, path := range paths {
		if path == "" {
			continue
		}
		msgs = append(msgs, Message{
			ID:    path,
			Event: blockevents.Message{Action: blockevents.ActionCreate, Path: path},
		})
	}
	return msgs, nil
}

// Ack deletes the fully-processed job rows by file_path (their Message.ID).
// After delete the row is gone, so it is never re-claimed; the table is
// self-trimming. A missing row (already deleted by a previous Ack) is a no-op.
func (c *RqliteConsumer) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	stmts := make([]gorqlite.ParameterizedStatement, len(ids))
	for i, id := range ids {
		stmts[i] = gorqlite.ParameterizedStatement{
			Query:     `DELETE FROM ` + jobTable + ` WHERE file_path = ?`,
			Arguments: []any{id},
		}
	}
	if _, err := c.db.WriteParameterizedContext(ctx, stmts); err != nil {
		return fmt.Errorf("valueindexconsumer: ack delete: %w", err)
	}
	return nil
}

// Close releases the rqlite connection.
func (c *RqliteConsumer) Close() error {
	c.db.Close()
	return nil
}
