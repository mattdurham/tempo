package blockevents

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// Default configuration values for block-event publishing.
const (
	// DefaultStreamName is the Redis stream key used when none is configured.
	DefaultStreamName = "blockpack-events"
	// DefaultBufferSize is the size of the publisher's internal non-blocking
	// buffer. It trades memory for tolerance to short Redis stalls before
	// messages start being dropped.
	DefaultBufferSize = 4096
	// DefaultMaxLen approximately caps the Redis stream length so it cannot grow
	// without bound. Old entries are trimmed (MAXLEN ~ N). A value <= 0 disables
	// trimming.
	DefaultMaxLen = 1_000_000
)

// Config configures block-event publishing. It maps to the optional
// `block_events` YAML block. When Enabled is false a NoopPublisher is used.
//
//	block_events:
//	  enabled: false
//	  redis_addr: "redis:6379"
//	  stream_name: "blockpack-events"
type Config struct {
	// RedisAddr is the host:port of the Redis server backing the stream.
	RedisAddr string `yaml:"redis_addr"`
	// StreamName is the Redis stream key events are appended to. Defaults to
	// DefaultStreamName when empty.
	StreamName string `yaml:"stream_name"`
	// BufferSize overrides the internal non-blocking buffer size. Defaults to
	// DefaultBufferSize when <= 0.
	BufferSize int `yaml:"buffer_size"`
	// MaxLen approximately caps the stream length. Defaults to DefaultMaxLen
	// when 0; a negative value disables trimming.
	MaxLen int64 `yaml:"max_len"`
	// Enabled turns on event publishing. When false NewPublisher returns a
	// NoopPublisher and RedisAddr/StreamName are ignored.
	Enabled bool `yaml:"enabled"`
}

// withDefaults returns a copy of c with empty/zero fields filled in.
func (c Config) withDefaults() Config {
	if c.StreamName == "" {
		c.StreamName = DefaultStreamName
	}
	if c.BufferSize <= 0 {
		c.BufferSize = DefaultBufferSize
	}
	if c.MaxLen == 0 {
		c.MaxLen = DefaultMaxLen
	}
	return c
}
