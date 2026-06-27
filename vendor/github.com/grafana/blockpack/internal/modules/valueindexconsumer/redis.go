package valueindexconsumer

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"

	"github.com/grafana/blockpack/internal/modules/blockevents"
)

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// claimScanStart is the XAUTOCLAIM cursor that both begins a PEL scan and is
// returned by Redis once the whole pending-entries list has been scanned. See
// NOTE-VI-019.
const claimScanStart = "0-0"

// streamReader is the minimal Redis Streams surface the consumer needs.
// *redis.Client satisfies it; tests substitute a fake so no real Redis is
// required.
type streamReader interface {
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd
	XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
	Close() error
}

// RedisConsumer is a Redis Streams consumer-group Consumer. Multiple instances
// in the same group share the stream: each message is delivered to exactly one
// consumer, and is redelivered (via the group's pending-entries list) if the
// consumer crashes before acking.
type RedisConsumer struct {
	client streamReader
	// claimCursor is the XAUTOCLAIM scan position for the startup reclaim of
	// stale pending messages. It begins at "0-0" and advances each Poll until
	// it wraps back to "0-0", at which point reclaimDone is set and Poll falls
	// through to normal XREADGROUP ">" delivery. See NOTE-VI-019.
	claimCursor string
	cfg         Config
	reclaimDone bool
}

// NewRedisConsumer dials Redis, ensures the consumer group exists, and returns a
// ready Consumer. The group is created with MKSTREAM so it works before the
// publisher has written anything.
func NewRedisConsumer(cfg Config) (*RedisConsumer, error) {
	cfg = cfg.withDefaults()
	if cfg.RedisAddr == "" {
		return nil, errors.New("valueindexconsumer: redis_addr required")
	}
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = "vic-" + xid.New().String()
	}
	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	c := newRedisConsumer(client, cfg)
	if err := c.ensureGroup(context.Background()); err != nil {
		_ = client.Close()
		return nil, err
	}
	return c, nil
}

// newRedisConsumer wires a consumer around an already-constructed streamReader.
// cfg is assumed to have defaults applied.
func newRedisConsumer(client streamReader, cfg Config) *RedisConsumer {
	return &RedisConsumer{client: client, cfg: cfg, claimCursor: claimScanStart}
}

// ensureGroup creates the consumer group, tolerating BUSYGROUP (already exists).
func (c *RedisConsumer) ensureGroup(ctx context.Context) error {
	// "$" would skip history; "0" reads from the start so messages published
	// before the group existed are still consumed.
	err := c.client.XGroupCreateMkStream(ctx, c.cfg.StreamName, c.cfg.ConsumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("valueindexconsumer: create group: %w", err)
	}
	return nil
}

// Poll fetches a batch of messages for this consumer, blocking up to
// PollTimeout. It returns an empty slice when there is nothing to do so the
// caller can run periodic work.
//
// Before serving new messages, Poll first drains stale pending messages
// orphaned by dead/restarted consumer instances (see reclaimStep). Once the
// reclaim scan completes, Poll falls through to normal XREADGROUP ">"
// (never-delivered) delivery.
func (c *RedisConsumer) Poll(ctx context.Context) ([]Message, error) {
	if !c.reclaimDone {
		msgs, err := c.reclaimStep(ctx)
		if err != nil {
			return nil, err
		}
		// Return whatever this reclaim step produced, even if empty: the next
		// Poll continues the scan (or starts fresh delivery once reclaimDone).
		// We do not block in reclaim, so an empty step is a cheap fast path.
		if len(msgs) > 0 || !c.reclaimDone {
			return msgs, nil
		}
		// Scan just completed with no remaining claims; fall through to a
		// normal blocking read so this Poll still does useful work.
	}

	res, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.cfg.ConsumerGroup,
		Consumer: c.cfg.ConsumerName,
		Streams:  []string{c.cfg.StreamName, ">"},
		Count:    int64(c.cfg.BatchSize),
		Block:    c.cfg.PollTimeout,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // timed out with no messages
	}
	if err != nil {
		return nil, fmt.Errorf("valueindexconsumer: xreadgroup: %w", err)
	}

	var msgs []Message
	for _, stream := range res {
		for _, m := range stream.Messages {
			msgs = c.appendParsed(ctx, msgs, m)
		}
	}
	return msgs, nil
}

// reclaimStep performs one XAUTOCLAIM cursor step, claiming up to
// DefaultClaimBatchSize stale pending entries (idle ≥ ClaimIdleThreshold) from
// dead/restarted consumers to this consumer. It advances claimCursor; when the
// cursor wraps back to "0-0" the scan is complete and reclaimDone is set.
// Claimed messages are returned as ordinary Messages so the service reprocesses
// them; the value-index compactor dedups any duplicate entries (NOTE-VI-019).
func (c *RedisConsumer) reclaimStep(ctx context.Context) ([]Message, error) {
	entries, next, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   c.cfg.StreamName,
		Group:    c.cfg.ConsumerGroup,
		Consumer: c.cfg.ConsumerName,
		MinIdle:  c.cfg.ClaimIdleThreshold,
		Start:    c.claimCursor,
		Count:    DefaultClaimBatchSize,
	}).Result()
	if errors.Is(err, redis.Nil) {
		c.reclaimDone = true
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("valueindexconsumer: xautoclaim: %w", err)
	}

	c.claimCursor = next
	// XAUTOCLAIM returns "0-0" as the cursor once the whole PEL has been scanned.
	if next == "" || next == claimScanStart {
		c.reclaimDone = true
	}

	var msgs []Message
	for _, m := range entries {
		msgs = c.appendParsed(ctx, msgs, m)
	}
	return msgs, nil
}

// appendParsed parses one stream entry and appends a Message, or acks-and-skips
// a malformed entry so it is not redelivered forever. Shared by Poll and
// reclaimStep.
func (c *RedisConsumer) appendParsed(ctx context.Context, msgs []Message, m redis.XMessage) []Message {
	ev, ok := parseEvent(m.Values)
	if !ok {
		_ = c.client.XAck(ctx, c.cfg.StreamName, c.cfg.ConsumerGroup, m.ID).Err()
		return msgs
	}
	return append(msgs, Message{ID: m.ID, Event: ev})
}

// Ack acknowledges processed message IDs to the consumer group.
func (c *RedisConsumer) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := c.client.XAck(ctx, c.cfg.StreamName, c.cfg.ConsumerGroup, ids...).Err(); err != nil {
		return fmt.Errorf("valueindexconsumer: xack: %w", err)
	}
	return nil
}

// Close releases the Redis client.
func (c *RedisConsumer) Close() error { return c.client.Close() }

// parseEvent reconstructs a blockevents.Message from a stream entry's field map.
// It mirrors the publisher's XADD encoding ("action", "path"). Returns false if
// required fields are missing or have the wrong type.
func parseEvent(values map[string]any) (blockevents.Message, bool) {
	action, ok := values["action"].(string)
	if !ok || action == "" {
		return blockevents.Message{}, false
	}
	p, ok := values["path"].(string)
	if !ok || p == "" {
		return blockevents.Message{}, false
	}
	return blockevents.Message{Action: blockevents.Action(action), Path: p}, true
}
