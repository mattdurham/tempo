package vm

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/mattdurham/blockpack/internal/quantile"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// VM is a stack-based virtual machine for executing TraceQL bytecode
type VM struct {
	provider AttributeProvider
	err      error

	program     *Program
	ctx         *spanContext
	aggState    map[string]*AggBucket // serialized group key -> bucket
	stack       []Value
	groupByVals []Value // current span's group-by values (ordered)
	sp          int     // Stack pointer
	pc          int     // Program counter (current instruction index)

	// Aggregation state
	aggMode bool // true when in aggregation mode
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

// serializeBufPool provides byte buffers for GroupKey serialization
var serializeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

// Serialize converts GroupKey to string for use as map key
// Optimized to use sync.Pool and direct byte appending instead of fmt.Sprintf
func (gk GroupKey) Serialize() string {
	if len(gk.Values) == 0 {
		return ""
	}

	bufPtr := serializeBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	for i, val := range gk.Values {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendGroupValue(buf, val)
	}

	result := string(buf)

	// Only return reasonably-sized buffers to pool to avoid memory bloat
	// If capacity exceeds 4KB, replace with fresh buffer
	if cap(buf) > 4096 {
		buf = make([]byte, 0, 256)
	}
	*bufPtr = buf
	serializeBufPool.Put(bufPtr)
	return result
}

// appendGroupValue appends a Value to a byte buffer for serialization.
// Optimized to avoid fmt.Sprintf allocations for all common types.
func appendGroupValue(buf []byte, val Value) []byte {
	// Append type
	buf = strconv.AppendInt(buf, int64(val.Type), 10)
	buf = append(buf, ':')

	// Append data based on type with checked assertions
	switch val.Type {
	case TypeNil:
		buf = append(buf, "nil"...)
	case TypeBool:
		if b, ok := val.Data.(bool); ok {
			if b {
				buf = append(buf, "true"...)
			} else {
				buf = append(buf, "false"...)
			}
		} else {
			buf = append(buf, fmt.Sprintf("%v", val.Data)...)
		}
	case TypeInt:
		// Support int, int32, int64 variants
		switch v := val.Data.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		default:
			buf = append(buf, fmt.Sprintf("%v", val.Data)...)
		}
	case TypeFloat:
		if f, ok := val.Data.(float64); ok {
			buf = strconv.AppendFloat(buf, f, 'g', -1, 64)
		} else {
			buf = append(buf, fmt.Sprintf("%v", val.Data)...)
		}
	case TypeString:
		if s, ok := val.Data.(string); ok {
			// Use strconv.AppendQuote to properly escape strings and prevent collisions
			buf = strconv.AppendQuote(buf, s)
		} else {
			buf = append(buf, fmt.Sprintf("%v", val.Data)...)
		}
	case TypeDuration:
		// Support both int64 (nanoseconds) and time.Duration
		switch v := val.Data.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
			buf = append(buf, "ns"...)
		case time.Duration:
			buf = strconv.AppendInt(buf, int64(v), 10)
			buf = append(buf, "ns"...)
		default:
			buf = append(buf, fmt.Sprintf("%v", val.Data)...)
		}
	case TypeBytes:
		if bytes, ok := val.Data.([]byte); ok {
			// Encode bytes as hex string
			buf = append(buf, "0x"...)
			const hexDigits = "0123456789abcdef"
			for _, b := range bytes {
				buf = append(buf, hexDigits[b>>4], hexDigits[b&0xf])
			}
		} else {
			buf = append(buf, fmt.Sprintf("%v", val.Data)...)
		}
	case TypeArray:
		if arr, ok := val.Data.([]Value); ok {
			// Recursively serialize array elements
			buf = append(buf, '[')
			for i, elem := range arr {
				if i > 0 {
					buf = append(buf, ',')
				}
				buf = appendGroupValue(buf, elem)
			}
			buf = append(buf, ']')
		} else {
			buf = append(buf, fmt.Sprintf("%v", val.Data)...)
		}
	case TypeJSON:
		// JSON values are rare in group keys but must be escaped to prevent collisions
		// Use strconv.Quote to escape any delimiter characters
		jsonStr := fmt.Sprintf("%v", val.Data)
		buf = strconv.AppendQuote(buf, jsonStr)
	default:
		// Fallback to fmt for unknown types, with escaping to prevent collisions
		fallbackStr := fmt.Sprintf("%v", val.Data)
		buf = strconv.AppendQuote(buf, fallbackStr)
	}

	return buf
}

// QuantileSketch is a stub for quantile operations (not used in SQL path)
// QuantileSketch is an alias for quantile.QuantileSketch for backward compatibility
type QuantileSketch = quantile.QuantileSketch

// NewQuantileSketch creates a new quantile sketch with the given accuracy
func NewQuantileSketch(accuracy float64) *QuantileSketch {
	return quantile.NewQuantileSketch(accuracy)
}

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

// Execute runs the bytecode against a span and returns the result
// Using closure-based execution for performance (inspired by PlanetScale)
func (vm *VM) Execute(span *tracev1.Span, resource *resourcev1.Resource) (bool, error) {
	vm.provider = nil
	vm.sp = 0 // Reset stack pointer
	vm.err = nil
	vm.ctx = &spanContext{
		span:     span,
		resource: resource,
	}

	// Simple loop executing instruction closures
	// Each closure returns the next instruction index
	vm.pc = 0
	for vm.pc < len(vm.program.Instructions) {
		vm.pc = vm.program.Instructions[vm.pc](vm)
		if vm.err != nil {
			return false, vm.err
		}
	}

	// Return instruction sets pc to len(instructions)
	if vm.sp == 0 {
		return false, nil
	}
	result := vm.pop()
	return vm.toBool(result), nil
}

// ExecuteWithProvider runs bytecode against a custom attribute provider (e.g., blockpack) without constructing OTLP spans.
func (vm *VM) ExecuteWithProvider(provider AttributeProvider) (bool, error) {
	vm.sp = 0
	vm.err = nil
	vm.ctx = nil
	vm.provider = provider
	// CRITICAL: Clear group-by values between spans to prevent accumulation
	if vm.groupByVals != nil {
		vm.groupByVals = vm.groupByVals[:0]
	}

	vm.pc = 0
	for vm.pc < len(vm.program.Instructions) {
		vm.pc = vm.program.Instructions[vm.pc](vm)
		if vm.err != nil {
			vm.provider = nil
			return false, vm.err
		}
	}
	vm.provider = nil

	if vm.sp == 0 {
		return false, nil
	}
	result := vm.pop()
	return vm.toBool(result), nil
}

// ExecuteAggregation runs bytecode in aggregation mode
// Processes one span, updating internal aggregation state
func (vm *VM) ExecuteAggregation(span *tracev1.Span, resource *resourcev1.Resource) error {
	vm.provider = nil
	vm.sp = 0                           // Reset stack pointer
	vm.groupByVals = vm.groupByVals[:0] // Clear group-by values
	vm.err = nil
	vm.ctx = &spanContext{
		span:     span,
		resource: resource,
	}

	// Execute instruction closures
	vm.pc = 0
	for vm.pc < len(vm.program.Instructions) {
		vm.pc = vm.program.Instructions[vm.pc](vm)
		if vm.err != nil {
			return vm.err
		}
	}

	return nil
}

// GetAggregationResults extracts final aggregation state after processing all spans
func (vm *VM) GetAggregationResults() map[string]*AggBucket {
	return vm.aggState
}

// ResetAggregation clears aggregation state for reuse
func (vm *VM) ResetAggregation() {
	vm.aggState = make(map[string]*AggBucket)
	vm.groupByVals = nil
	vm.aggMode = false
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

// spanContext holds the context for attribute lookups.
type spanContext struct {
	span     *tracev1.Span
	resource *resourcev1.Resource
}

// AttributeProvider enables VM execution over alternate data sources (e.g., blockpack).
type AttributeProvider interface {
	GetAttribute(attrPath string) Value
	GetStartTime() (uint64, bool)
}

// Note: these helpers can be hot in expression-heavy queries.
// Kept on VM to allow future caching/configuration alongside stateful helpers.
// toBool converts a value to boolean
func (vm *VM) toBool(val Value) bool {
	switch val.Type {
	case TypeBool:
		return val.Data.(bool)
	case TypeInt:
		return val.Data.(int64) != 0
	case TypeFloat:
		return val.Data.(float64) != 0
	case TypeString:
		return val.Data.(string) != ""
	case TypeNil:
		return false
	case TypeBytes:
		return len(val.Data.([]byte)) > 0
	case TypeDuration:
		return val.Data.(int64) != 0
	case TypeArray:
		return len(val.Data.([]Value)) > 0
	case TypeJSON:
		jsonVal, ok := val.Data.(*JSONValue)
		return ok && jsonVal != nil && jsonVal.Data != nil
	default:
		return false
	}
}
