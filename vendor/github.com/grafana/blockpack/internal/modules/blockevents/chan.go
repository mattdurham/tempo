package blockevents

import (
	"context"
	"errors"
	"sync"
)

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// ErrPublisherClosed is returned by Publish after the publisher has been closed.
var ErrPublisherClosed = errors.New("blockevents: publisher closed")

// ChanPublisher is an in-process Publisher that delivers events on a channel.
// It is intended for tests and in-process consumers, not production use.
//
// Like the production publisher it is non-blocking: if the channel buffer is
// full the message is dropped and Dropped is incremented.
type ChanPublisher struct {
	ch      chan Message
	mu      sync.Mutex
	closed  bool
	dropped int64
}

// NewChanPublisher returns a ChanPublisher with an internal buffer of the given
// size. A size <= 0 yields an unbuffered channel.
func NewChanPublisher(buffer int) *ChanPublisher {
	if buffer < 0 {
		buffer = 0
	}
	return &ChanPublisher{ch: make(chan Message, buffer)}
}

// C returns the receive side of the event channel.
func (p *ChanPublisher) C() <-chan Message { return p.ch }

// Publish enqueues msg without blocking. If the buffer is full the message is
// dropped (and counted by Dropped). Publish returns ErrPublisherClosed if the
// publisher has been closed.
func (p *ChanPublisher) Publish(_ context.Context, msg Message) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPublisherClosed
	}
	select {
	case p.ch <- msg:
	default:
		p.dropped++
	}
	p.mu.Unlock()
	return nil
}

// Dropped returns the number of messages dropped because the buffer was full.
func (p *ChanPublisher) Dropped() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dropped
}

// Close closes the channel. Subsequent Publish calls return ErrPublisherClosed.
// Close is idempotent.
func (p *ChanPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	close(p.ch)
	return nil
}
