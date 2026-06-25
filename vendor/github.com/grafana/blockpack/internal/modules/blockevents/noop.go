package blockevents

import "context"

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// NoopPublisher discards every event. It is the default when no queue is
// configured, and is zero-cost — no allocation, no goroutine, no I/O.
type NoopPublisher struct{}

// NewNoopPublisher returns a Publisher that discards all events.
func NewNoopPublisher() *NoopPublisher { return &NoopPublisher{} }

// Publish discards msg and always succeeds.
func (*NoopPublisher) Publish(context.Context, Message) error { return nil }

// Close always succeeds.
func (*NoopPublisher) Close() error { return nil }
