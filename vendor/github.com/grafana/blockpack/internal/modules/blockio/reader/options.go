package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Option is a functional option for Reader construction.
type Option func(*readerOptions)

type readerOptions struct{}
