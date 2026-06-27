package valueindexconsumer

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/rs/xid"

	"github.com/grafana/blockpack/internal/modules/blockevents"
)

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// streamReader is the minimal Redis Streams surface the consumer needs.
// *redis.Client satisfies it; tests substitute a fake so no real Redis is
// required.
type streamReader interface {
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
	Close() error
}

// RedisConsumer is a Redis Streams consumer-group Consumer. Multiple instances
// in the same group share the stream: each message is delivered to exactly one
// consumer, and is redelivered (via the group's pending-entries list) if the
// consumer crashes before acking.
type RedisConsumer struct {
	client streamReader
	cfg    Config
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
	return &RedisConsumer{client: client, cfg: cfg}
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

// Poll fetches up to BatchSize new (never-delivered) messages for this consumer,
// blocking up to PollTimeout. It returns an empty slice when the read times out
// so the caller can run periodic work.
func (c *RedisConsumer) Poll(ctx context.Context) ([]Message, error) {
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
			ev, ok := parseEvent(m.Values)
			if !ok {
				// Malformed message: ack it so it is not redelivered forever.
				_ = c.client.XAck(ctx, c.cfg.StreamName, c.cfg.ConsumerGroup, m.ID).Err()
				continue
			}
			msgs = append(msgs, Message{ID: m.ID, Event: ev})
		}
	}
	return msgs, nil
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
