package rw

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// bytesProvider is a minimal in-memory ReaderProvider backed by a byte slice.
// Used by the writer's ValueIndexSink path to open a Reader over just-flushed bytes
// without an external storage round-trip.
type bytesProvider struct {
	data []byte
}

// NewBytesProvider returns a ReaderProvider backed by the given byte slice.
// The caller must not modify data after calling this function.
func NewBytesProvider(data []byte) ReaderProvider {
	return &bytesProvider{data: data}
}

func (b *bytesProvider) Size() (int64, error) { return int64(len(b.data)), nil }

func (b *bytesProvider) ReadAt(p []byte, off int64, _ DataType) (int, error) {
	if off >= int64(len(b.data)) {
		return 0, nil
	}
	n := copy(p, b.data[off:])
	return n, nil
}
