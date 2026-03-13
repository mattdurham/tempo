package common

import (
	"bytes"
	"context"
	"hash"
	"hash/fnv"
	"sort"
)

// originalTraceQLQueryKey is a context key for storing the original TraceQL query string.
type originalTraceQLQueryKey struct{}

// traceQLMostRecentKey is a context key for the most_recent hint.
type traceQLMostRecentKey struct{}

// WithOriginalTraceQLQuery stores the original TraceQL query string (with Tempo hints stripped)
// in the context so that encoding backends (e.g. vblockpack) can use the full query instead of
// a reconstructed approximation built from extracted conditions.
// mostRecent should be true when the original query included "with (most_recent=true)".
func WithOriginalTraceQLQuery(ctx context.Context, query string, mostRecent bool) context.Context {
	ctx = context.WithValue(ctx, originalTraceQLQueryKey{}, query)
	ctx = context.WithValue(ctx, traceQLMostRecentKey{}, mostRecent)
	return ctx
}

// OriginalTraceQLQuery retrieves the original TraceQL query string from the context.
// Returns ("", false) when not set.
func OriginalTraceQLQuery(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(originalTraceQLQueryKey{}).(string)
	return v, ok && v != ""
}

// TraceQLMostRecent returns true if the original query included "with (most_recent=true)".
func TraceQLMostRecent(ctx context.Context) bool {
	v, _ := ctx.Value(traceQLMostRecentKey{}).(bool)
	return v
}

// This file contains types that need to be referenced by both the ./encoding and ./encoding/vX packages.
// It primarily exists here to break dependency loops.

// ID in TempoDB
type ID []byte

type IDMapEntry[T any] struct {
	ID    ID
	Entry T
}

// IDMap is a helper for recording and checking for IDs. Not safe for concurrent use.
type IDMap[T any] struct {
	m map[uint64]IDMapEntry[T]
	h hash.Hash64
}

func NewIDMap[T any](estimatedCount int) *IDMap[T] {
	return &IDMap[T]{
		m: make(map[uint64]IDMapEntry[T], estimatedCount),
		h: fnv.New64(),
	}
}

// tokenForID returns a token for use in a hash map given a span id and span kind
// buffer must be a 4 byte slice and is reused for writing the span kind to the hashing function
// kind is used along with the actual id b/c in zipkin traces span id is not guaranteed to be unique
// as it is shared between client and server spans.
func (m *IDMap[T]) tokenFor(id ID) uint64 {
	m.h.Reset()
	_, _ = m.h.Write(id)
	return m.h.Sum64()
}

func (m *IDMap[T]) Set(id ID, val T) {
	m.m[m.tokenFor(id)] = IDMapEntry[T]{id, val}
}

func (m *IDMap[T]) Has(id ID) bool {
	_, ok := m.m[m.tokenFor(id)]
	return ok
}

func (m *IDMap[T]) Get(id ID) (T, bool) {
	v, ok := m.m[m.tokenFor(id)]
	return v.Entry, ok
}

func (m *IDMap[T]) Len() int {
	return len(m.m)
}

func (m *IDMap[T]) EntriesSortedByID() []IDMapEntry[T] {
	// Copy and sort entries by ID
	entries := make([]IDMapEntry[T], 0, len(m.m))
	for _, e := range m.m {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].ID, entries[j].ID) == -1
	})

	return entries
}
