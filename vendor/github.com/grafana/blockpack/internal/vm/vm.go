package vm

import (
	"fmt"
	"time"

	"github.com/grafana/blockpack/internal/quantile"
)

// VM is a stack-based virtual machine for executing TraceQL bytecode
type VM struct {
	provider AttributeProvider

	program *Program
	stack   []Value
	sp      int // Stack pointer
	pc      int // Program counter (current instruction index)
}

// AggBucket holds aggregation state for a single group.
// A JSON-friendly payload used to exist but was removed; this is the sole bucket struct.
type AggBucket struct {
	Quantiles  map[string]*quantile.QuantileSketch // field_name -> quantile sketch
	Histograms map[string]*HistogramData           // field_name -> histogram data
	GroupKey   GroupKey
	Sum        float64
	Count      int64
	Rate       float64
	Min        float64
	Max        float64
	SumSq      float64 // Sum of squares for stddev calculation
}

// Merge combines another AggBucket into this one.
// This is used to merge aggregation results from multiple files.
// Quantile sketches are properly merged to maintain accuracy.
func (b *AggBucket) Merge(other *AggBucket) {
	if other == nil {
		return
	}

	// Sum the counts
	b.Count += other.Count

	// Sum the sums
	b.Sum += other.Sum

	// Take the minimum
	if other.Count > 0 {
		if b.Count == other.Count {
			// This is the first bucket being merged
			b.Min = other.Min
		} else if other.Min < b.Min {
			b.Min = other.Min
		}
	}

	// Take the maximum
	if other.Count > 0 {
		if b.Count == other.Count {
			// This is the first bucket being merged
			b.Max = other.Max
		} else if other.Max > b.Max {
			b.Max = other.Max
		}
	}

	// Merge quantile sketches
	if other.Quantiles != nil {
		if b.Quantiles == nil {
			b.Quantiles = make(map[string]*quantile.QuantileSketch)
		}
		for field, otherSketch := range other.Quantiles {
			if otherSketch == nil {
				continue
			}
			if b.Quantiles[field] == nil {
				// Clone the other sketch instead of sharing the reference
				b.Quantiles[field] = otherSketch.Clone()
			} else {
				// Merge the other sketch into ours
				b.Quantiles[field].Merge(otherSketch)
			}
		}
	}

	// Rate is recalculated after merge, not merged directly
}

// MergeAggregationResults combines multiple aggregation result sets into one.
// This is used to merge results from multiple blockpack files.
// Returns a new map with merged buckets.
func MergeAggregationResults(results ...map[string]*AggBucket) map[string]*AggBucket {
	if len(results) == 0 {
		return make(map[string]*AggBucket)
	}

	if len(results) == 1 {
		return results[0]
	}

	merged := make(map[string]*AggBucket)

	for _, resultSet := range results {
		for key, bucket := range resultSet {
			if existing, ok := merged[key]; ok {
				// Merge into existing bucket
				existing.Merge(bucket)
			} else {
				// Create new bucket with copied values
				merged[key] = &AggBucket{
					GroupKey:  bucket.GroupKey,
					Count:     bucket.Count,
					Sum:       bucket.Sum,
					Min:       bucket.Min,
					Max:       bucket.Max,
					Rate:      bucket.Rate,
					Quantiles: bucket.Quantiles,
				}
			}
		}
	}

	return merged
}

// GroupKey represents a unique combination of group-by field values
type GroupKey struct {
	Values []Value // Ordered by GROUP BY fields in query
}

// QuantileSketch is a stub for quantile operations (not used in SQL path)
// QuantileSketch is an alias for quantile.QuantileSketch for backward compatibility
type QuantileSketch = quantile.QuantileSketch

// HistogramData holds histogram buckets and counts
type HistogramData struct {
	Buckets []float64 // Bucket boundaries
	Counts  []int64   // Count in each bucket
}

// JSONValue is a stub for JSON operations (not used in SQL path)
type JSONValue struct {
	Data interface{}
	Raw  []byte
}

// DateBinInfo holds date_bin configuration
type DateBinInfo struct {
	Interval        time.Duration // e.g., 5*time.Minute for "5m"
	OriginTimestamp int64         // Query start time (Unix nanoseconds)
}

// NewVM creates a new VM instance
func NewVM(program *Program) *VM {
	return &VM{
		program: program,
		stack:   make([]Value, 256), // Pre-allocate stack
		sp:      0,
	}
}

func (vm *VM) pop() Value {
	if vm.sp == 0 {
		return Value{Type: TypeNil, Data: nil}
	}
	vm.sp--
	return vm.stack[vm.sp]
}

func (vm *VM) push(val Value) {
	if vm.sp >= len(vm.stack) {
		vm.stack = append(vm.stack, val)
		vm.sp++
	} else {
		vm.stack[vm.sp] = val
		vm.sp++
	}
}

func (vm *VM) loadAttribute(attrIdx int) Value {
	if attrIdx < 0 || attrIdx >= len(vm.program.Attributes) {
		return Value{Type: TypeNil, Data: nil}
	}

	attrName := vm.program.Attributes[attrIdx]

	// Use provider if available (blockpack execution)
	if vm.provider != nil {
		return vm.provider.GetAttribute(attrName)
	}

	// OTLP execution path not yet implemented
	// Programs compiled by CompileTraceQLFilter() MUST be executed with an AttributeProvider
	// (e.g., via blockpack ColumnDataProvider). Direct OTLP span execution is not supported.
	panic(
		fmt.Sprintf(
			"attribute lookup without provider: %s (OTLP execution path not implemented - use ColumnPredicate with blockpack provider)",
			attrName,
		),
	)
}

// AttributeProvider enables VM execution over alternate data sources (e.g., blockpack).
type AttributeProvider interface {
	GetAttribute(attrPath string) Value
	GetStartTime() (uint64, bool)
}
