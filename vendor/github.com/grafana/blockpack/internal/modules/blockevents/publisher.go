package blockevents

import "context"

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// Action is the kind of lifecycle event that occurred for a blockpack file.
type Action string

// ActionCreate signals that a new blockpack file was written to object storage.
const ActionCreate Action = "create"

// Message is a single blockpack lifecycle event.
//
// It is intentionally minimal: the consumer reads tenant, columns and entries
// from the blockpack file referenced by Path.
type Message struct {
	// Action is the event kind. Currently only ActionCreate is emitted.
	Action Action `json:"action"`
	// Path is the fully-qualified object-storage path of the blockpack file,
	// e.g. "s3://bucket/<tenant>/<block-id>/data.blockpack".
	Path string `json:"path"`
}

// Publisher publishes blockpack lifecycle events to a queue.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Publisher interface {
	// Publish emits a single event. Implementations on the hot path must not
	// block: a full buffer should drop the message rather than stall the caller.
	// Publish returns an error only for non-recoverable setup problems (e.g. a
	// closed publisher); a dropped message is not an error.
	Publish(ctx context.Context, msg Message) error
	// Close flushes any in-flight state and releases resources. After Close the
	// publisher must reject further Publish calls.
	Close() error
}
