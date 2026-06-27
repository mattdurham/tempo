package valueindexconsumer

import (
	"context"

	"github.com/grafana/blockpack/internal/modules/blockevents"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// Message is a single queue delivery: a block-event payload plus the opaque
// queue-assigned ID used to acknowledge it. The ID is queue-implementation
// specific (e.g. a Redis Streams entry ID) and is treated as an opaque token by
// the service.
type Message struct {
	Event blockevents.Message
	ID    string
}

// Consumer pulls block-event messages off a queue and acknowledges them.
//
// Implementations must be safe for use by a single service goroutine; the
// service does not call Consumer methods concurrently.
type Consumer interface {
	// Poll returns up to a batch of messages, blocking until at least one is
	// available or ctx is done. It returns an empty slice (and nil error) when
	// the poll times out with no messages, so the caller can run periodic work
	// (e.g. fire the flush timer) between polls.
	Poll(ctx context.Context) ([]Message, error)
	// Ack acknowledges that the given message IDs have been fully processed.
	// After Ack the queue must not redeliver them.
	Ack(ctx context.Context, ids ...string) error
	// Close releases queue resources.
	Close() error
}

// ColumnEntry is one extracted observation for one configured column.
//
// Value is the typed column value (string, int64, uint64, float64, bool, []byte)
// matching ColType; the service hands it to valueindex.Writer.AddEntry which
// canonicalises it. SourceRef is the blockpack object path the entry came from,
// BlockID the zero-based block index within that file (NOTE-VI-014), and TimeSec
// the span's wall time in seconds.
type ColumnEntry struct {
	ColName   string
	Value     any
	SourceRef string
	TraceID   [16]byte
	ColType   shared.ColumnType
	BlockID   uint32
	TimeSec   uint64
}

// Extractor reads a blockpack file and streams per-column entries to a callback.
// Implementations process one inner block at a time so peak memory is bounded
// by one block's decoded columns rather than the entire file.
type Extractor interface {
	// Extract reads the blockpack at event.Path and calls yield for each
	// (span, column) observation. Extract stops and returns the error if yield
	// returns one. A column absent from a block simply produces no calls.
	Extract(ctx context.Context, event blockevents.Message, yield func(ColumnEntry) error) error
}
