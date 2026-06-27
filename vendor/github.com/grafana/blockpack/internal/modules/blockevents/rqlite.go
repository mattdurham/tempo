package blockevents

import (
	"context"
	"fmt"
	"sync"

	"github.com/rqlite/gorqlite"
)

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// jobTable is the rqlite table backing the value-index job queue. See
// NOTE-VI-021. The schema is created idempotently via ensureJobSchema, which
// both the publisher and the consumer call on startup; rqlite's Raft
// serialization makes concurrent CREATE TABLE IF NOT EXISTS from every pod safe.
const jobTable = "value_index_jobs"

// ensureJobSchema creates the job table and its status index if they do not
// already exist. It is idempotent (IF NOT EXISTS) and safe to call concurrently
// from every pod: rqlite's Raft layer serializes the writes so the table is
// created exactly once. Shared by the rqlite publisher and consumer.
//
// The schema is the durable hand-off between producer (block builder /
// compactor) and the stateless per-file workers: one row per blockpack file
// awaiting indexing, claimed by status transition, deleted on completion.
func ensureJobSchema(db rqliteWriter) error {
	_, err := db.WriteParameterized([]gorqlite.ParameterizedStatement{
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
		return fmt.Errorf("blockevents: ensure job schema: %w", err)
	}
	return nil
}

// rqliteWriter is the minimal rqlite surface the publisher needs.
// *gorqlite.Connection satisfies it; tests substitute a fake so no real rqlite
// server is required.
type rqliteWriter interface {
	WriteParameterizedContext(
		ctx context.Context,
		statements []gorqlite.ParameterizedStatement,
	) ([]gorqlite.WriteResult, error)
	WriteParameterized(statements []gorqlite.ParameterizedStatement) ([]gorqlite.WriteResult, error)
	Close()
}

// RqlitePublisher publishes block events as rows in the rqlite job table.
//
// Like RedisStreamsPublisher it is non-blocking on the hot path: Publish
// enqueues onto a bounded channel drained by a single background goroutine that
// performs the INSERT. If the channel is full the message is dropped (a lost
// create event is recoverable — the S3 reconciler re-inserts files with no
// value index). The drop count is observable via Dropped.
//
// Unlike the Redis stream there is no MAXLEN trimming: a row is deleted by the
// worker when the file is fully indexed (Consumer.Ack), so the table is
// self-trimming. ON CONFLICT DO NOTHING makes a re-published create idempotent.
type RqlitePublisher struct {
	db rqliteWriter

	buf chan Message
	wg  sync.WaitGroup

	dropped int64

	mu     sync.Mutex
	closed bool
}

// NewRqlitePublisher dials rqlite, ensures the job schema exists, and starts the
// background drain goroutine.
func NewRqlitePublisher(cfg Config) (*RqlitePublisher, error) {
	cfg = cfg.withDefaults()
	if cfg.RqliteURL == "" {
		return nil, fmt.Errorf("blockevents: rqlite_url required")
	}
	conn, err := gorqlite.Open(cfg.RqliteURL)
	if err != nil {
		return nil, fmt.Errorf("blockevents: open rqlite: %w", err)
	}
	if err := ensureJobSchema(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return newRqlitePublisher(conn, cfg), nil
}

// newRqlitePublisher wires a publisher around an already-constructed
// rqliteWriter. cfg is assumed to have defaults applied and the schema ensured.
func newRqlitePublisher(db rqliteWriter, cfg Config) *RqlitePublisher {
	p := &RqlitePublisher{
		db:  db,
		buf: make(chan Message, cfg.BufferSize),
	}
	p.wg.Add(1)
	go p.run()
	return p
}

// Publish enqueues msg without blocking. Returns ErrPublisherClosed after Close.
func (p *RqlitePublisher) Publish(_ context.Context, msg Message) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPublisherClosed
	}
	select {
	case p.buf <- msg:
	default:
		p.dropped++
	}
	p.mu.Unlock()
	return nil
}

// run drains the buffer and inserts each message as a pending job row. It exits
// after the buffer is closed and fully drained. INSERT ... ON CONFLICT DO
// NOTHING makes a duplicate create event (e.g. re-published after a transient
// failure, or by the S3 reconciler) a no-op rather than an error.
func (p *RqlitePublisher) run() {
	defer p.wg.Done()
	for msg := range p.buf {
		// Use a background context: the per-call ctx may already be canceled by
		// the time the message is drained. Errors are intentionally ignored on
		// the hot path — a failed publish is recoverable via the S3 reconciler.
		_, _ = p.db.WriteParameterizedContext(context.Background(), []gorqlite.ParameterizedStatement{
			{
				Query: `INSERT INTO ` + jobTable + ` (file_path)
					VALUES (?)
					ON CONFLICT (file_path) DO NOTHING`,
				Arguments: []any{msg.Path},
			},
		})
	}
}

// Dropped returns the number of messages dropped because the buffer was full.
func (p *RqlitePublisher) Dropped() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dropped
}

// Close stops accepting messages, drains the buffer, waits for the background
// goroutine to finish, and closes the rqlite connection. Close is idempotent.
func (p *RqlitePublisher) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.buf)
	p.mu.Unlock()

	p.wg.Wait()
	p.db.Close()
	return nil
}
