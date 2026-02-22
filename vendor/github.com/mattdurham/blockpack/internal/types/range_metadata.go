package ondisk

import (
	"sync"
)

// RangeMetadata stores bucket boundaries for range-encoded columns.
// It provides thread-safe access to boundaries for float64, bytes, and string columns.
// All methods perform deep copies to prevent external modification.
type RangeMetadata struct {
	// Bucket boundaries by column name
	float64Boundaries map[string][]float64
	bytesBoundaries   map[string][][]byte
	stringBoundaries  map[string][]string
	mu                sync.RWMutex
}

// NewRangeMetadata creates a new metadata store for range column boundaries.
func NewRangeMetadata() *RangeMetadata {
	return &RangeMetadata{
		float64Boundaries: make(map[string][]float64),
		bytesBoundaries:   make(map[string][][]byte),
		stringBoundaries:  make(map[string][]string),
	}
}

// SetFloat64Boundaries stores bucket boundaries for a float64 column.
// The boundaries are deep copied to prevent external modification.
func (rm *RangeMetadata) SetFloat64Boundaries(column string, boundaries []float64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Deep copy to avoid external modification
	copied := make([]float64, len(boundaries))
	copy(copied, boundaries)
	rm.float64Boundaries[column] = copied
}

// GetFloat64Boundaries retrieves bucket boundaries for a float64 column.
// Returns a deep copy of the boundaries and true if found, nil and false otherwise.
func (rm *RangeMetadata) GetFloat64Boundaries(column string) ([]float64, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	boundaries, ok := rm.float64Boundaries[column]
	if !ok {
		return nil, false
	}

	// Return copy to prevent external modification
	copied := make([]float64, len(boundaries))
	copy(copied, boundaries)
	return copied, true
}

// SetBytesBoundaries stores bucket boundaries for a bytes column.
// The boundaries are deep copied to prevent external modification.
func (rm *RangeMetadata) SetBytesBoundaries(column string, boundaries [][]byte) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Deep copy including slices
	copied := make([][]byte, len(boundaries))
	for i, b := range boundaries {
		copied[i] = make([]byte, len(b))
		copy(copied[i], b)
	}
	rm.bytesBoundaries[column] = copied
}

// GetBytesBoundaries retrieves bucket boundaries for a bytes column.
// Returns a deep copy of the boundaries and true if found, nil and false otherwise.
func (rm *RangeMetadata) GetBytesBoundaries(column string) ([][]byte, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	boundaries, ok := rm.bytesBoundaries[column]
	if !ok {
		return nil, false
	}

	// Deep copy
	copied := make([][]byte, len(boundaries))
	for i, b := range boundaries {
		copied[i] = make([]byte, len(b))
		copy(copied[i], b)
	}
	return copied, true
}

// SetStringBoundaries stores bucket boundaries for a string column.
// The boundaries are deep copied to prevent external modification.
func (rm *RangeMetadata) SetStringBoundaries(column string, boundaries []string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	copied := make([]string, len(boundaries))
	copy(copied, boundaries)
	rm.stringBoundaries[column] = copied
}

// GetStringBoundaries retrieves bucket boundaries for a string column.
// Returns a deep copy of the boundaries and true if found, nil and false otherwise.
func (rm *RangeMetadata) GetStringBoundaries(column string) ([]string, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	boundaries, ok := rm.stringBoundaries[column]
	if !ok {
		return nil, false
	}

	copied := make([]string, len(boundaries))
	copy(copied, boundaries)
	return copied, true
}
