package lokibench

import "github.com/grafana/loki/v3/pkg/logproto"

// sliceEntryIterator implements iter.EntryIterator over a pre-collected slice of
// log entries. It is used by LokiConverter to return per-stream results.
// pos starts at -1 (before the first element); Next() advances it before access.
type sliceEntryIterator struct {
	labels     string
	streamHash uint64
	entries    []logproto.Entry
	pos        int
}

func newSliceEntryIterator(labels string, hash uint64, entries []logproto.Entry) *sliceEntryIterator {
	return &sliceEntryIterator{
		labels:     labels,
		streamHash: hash,
		entries:    entries,
		pos:        -1,
	}
}

func (it *sliceEntryIterator) Next() bool {
	it.pos++
	return it.pos < len(it.entries)
}

func (it *sliceEntryIterator) At() logproto.Entry { return it.entries[it.pos] }
func (it *sliceEntryIterator) Err() error         { return nil }
func (it *sliceEntryIterator) Labels() string     { return it.labels }
func (it *sliceEntryIterator) StreamHash() uint64 { return it.streamHash }
func (it *sliceEntryIterator) Close() error       { return nil }
