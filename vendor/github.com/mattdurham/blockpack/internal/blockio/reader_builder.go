package blockio

import (
	"github.com/mattdurham/blockpack/internal/blockio/reader"
)

// ReaderOption is an alias to reader.ReaderOption for backward compatibility
type ReaderOption = reader.ReaderOption

// WithCaching is re-exported from reader package for backward compatibility
var WithCaching = reader.WithCaching

// WithDataAwareCaching is re-exported from reader package for backward compatibility
var WithDataAwareCaching = reader.WithDataAwareCaching

// WithBasicCaching is re-exported from reader package for backward compatibility
var WithBasicCaching = reader.WithBasicCaching

// WithTracking is re-exported from reader package for backward compatibility
var WithTracking = reader.WithTracking

// NewReaderFromProvider is re-exported from reader package for backward compatibility
func NewReaderFromProvider(provider ReaderProvider, opts ...ReaderOption) (*Reader, error) {
	return reader.NewReaderFromProvider(provider, opts...)
}

// NewReaderFromBytes is re-exported from reader package for backward compatibility
func NewReaderFromBytes(data []byte, opts ...ReaderOption) (*Reader, error) {
	return reader.NewReaderFromBytes(data, opts...)
}
