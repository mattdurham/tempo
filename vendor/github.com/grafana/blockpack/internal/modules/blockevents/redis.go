package blockevents

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
)

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// streamWriter is the minimal Redis surface the publisher needs. *redis.Client
// satisfies it; tests substitute a fake so no real Redis is required.
type streamWriter interface {
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	Close() error
}

// RedisStreamsPublisher publishes block events to a Redis stream.
//
// It is non-blocking on the hot path: Publish enqueues onto a bounded channel
// drained by a single background goroutine that performs the XADD. If the
// channel is full the message is dropped (a lost create event is recoverable —
// the value index can rebuild from scratch). The drop count is observable via
// Dropped for metrics/debugging.
type RedisStreamsPublisher struct {
	client streamWriter

	buf    chan Message
	stream string
	wg     sync.WaitGroup

	maxLen int64

	dropped int64

	mu     sync.Mutex
	closed bool
}

// NewPublisher returns a Publisher for the given config. When cfg.Enabled is
// false it returns a NoopPublisher. Otherwise it dials Redis and starts the
// background drain goroutine.
func NewPublisher(cfg Config) (Publisher, error) {
	if !cfg.Enabled {
		return NewNoopPublisher(), nil
	}
	cfg = cfg.withDefaults()
	if cfg.RedisAddr == "" {
		return nil, fmt.Errorf("blockevents: redis_addr required when enabled")
	}
	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	return newRedisStreamsPublisher(client, cfg), nil
}

// newRedisStreamsPublisher wires a publisher around an already-constructed
// streamWriter. cfg is assumed to have defaults applied.
func newRedisStreamsPublisher(client streamWriter, cfg Config) *RedisStreamsPublisher {
	maxLen := cfg.MaxLen
	if maxLen < 0 {
		maxLen = 0
	}
	p := &RedisStreamsPublisher{
		client: client,
		stream: cfg.StreamName,
		maxLen: maxLen,
		buf:    make(chan Message, cfg.BufferSize),
	}
	p.wg.Add(1)
	go p.run()
	return p
}

// Publish enqueues msg without blocking. Returns ErrPublisherClosed after Close.
func (p *RedisStreamsPublisher) Publish(_ context.Context, msg Message) error {
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

// run drains the buffer and writes each message to the Redis stream. It exits
// after the buffer is closed and fully drained.
func (p *RedisStreamsPublisher) run() {
	defer p.wg.Done()
	for msg := range p.buf {
		args := &redis.XAddArgs{
			Stream: p.stream,
			Values: map[string]any{
				"action": string(msg.Action),
				"path":   msg.Path,
			},
		}
		if p.maxLen > 0 {
			args.MaxLen = p.maxLen
			args.Approx = true
		}
		// Use a background context: the per-call ctx may already be canceled
		// by the time the message is drained. Errors are intentionally ignored
		// on the hot path — a failed publish is recoverable downstream.
		_ = p.client.XAdd(context.Background(), args).Err()
	}
}

// Dropped returns the number of messages dropped because the buffer was full.
func (p *RedisStreamsPublisher) Dropped() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dropped
}

// Close stops accepting messages, drains the buffer, waits for the background
// goroutine to finish, and closes the Redis client. Close is idempotent.
func (p *RedisStreamsPublisher) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.buf)
	p.mu.Unlock()

	p.wg.Wait()
	return p.client.Close()
}
