package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "github.com/grafana/blockpack/internal/arena"

// Option is a functional option for Reader construction.
type Option func(*readerOptions)

type readerOptions struct {
	// arena is used for index/presence array allocation during block decoding.
	// Dictionaries always use heap allocation regardless.
	arena *arena.Arena
}

// WithArena sets the arena allocator for query-time allocations.
// Dictionary fields (StringDict, BytesDict, etc.) always use heap allocation.
func WithArena(a *arena.Arena) Option {
	return func(o *readerOptions) { o.arena = a }
}
