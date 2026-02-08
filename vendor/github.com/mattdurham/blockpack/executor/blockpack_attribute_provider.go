package executor

import (
	"strings"

	blockpackio "github.com/mattdurham/blockpack/blockpack/io"
	blockpack "github.com/mattdurham/blockpack/blockpack/types"
	"github.com/mattdurham/blockpack/vm"
)

// blockpackAttributeProvider provides VM attribute access to blockpack storage.
// Fast-path caching is used for commonly accessed columns to avoid map lookups.
type blockpackAttributeProvider struct {
	block *blockpackio.Block // Store reference to block for GetColumn lookups
	idx   int

	// Fast-path cache for commonly accessed columns (avoids map lookups)
	traceIDCol    *blockpack.Column
	spanIDCol     *blockpack.Column
	spanNameCol   *blockpack.Column
	durationCol   *blockpack.Column
	startTimeCol  *blockpack.Column
	spanKindCol   *blockpack.Column
	spanStatusCol *blockpack.Column
}

func newBlockpackAttributeProvider(block *blockpackio.Block, idx int) *blockpackAttributeProvider {
	return &blockpackAttributeProvider{
		block:         block,
		idx:           idx,
		traceIDCol:    block.GetColumn("trace:id"),
		spanIDCol:     block.GetColumn("span:id"),
		spanNameCol:   block.GetColumn("span:name"),
		durationCol:   block.GetColumn("span:duration"),
		startTimeCol:  block.GetColumn("span:start"),
		spanKindCol:   block.GetColumn("span:kind"),
		spanStatusCol: block.GetColumn("span:status"),
	}
}

// GetAttribute retrieves attribute values from blockpack storage for VM execution.
//
// Column Naming Convention:
// The blockpack format uses colon notation (:) for intrinsics to separate them from
// user-defined attributes, following OpenTelemetry's data model distinction:
//
// Span Intrinsics (use colon notation):
//   - span:name, span:status, span:kind, trace:id, span:id, etc.
//   - These are defined by the OpenTelemetry specification as core span fields
//   - See: https://opentelemetry.io/docs/specs/otel/trace/api/#span
//
// User-defined Attributes (use dot notation):
//   - span.foo, resource.foo (custom attributes set by users)
//   - These are arbitrary key-value pairs attached to spans/resources
//   - See: https://opentelemetry.io/docs/specs/otel/common/#attributes
//
// Why this matters:
// The colon notation prevents naming collisions with user-defined attributes.
// For example, span:name (intrinsic) vs span.name (user attribute named "name").
//
// This function translates query paths to column names:
//   - Query: "resource.foo" → Column: "resource.foo"
//   - Query: "span:name" → Column: "span:name" (intrinsic, no translation)
func (p *blockpackAttributeProvider) GetAttribute(attrPath string) vm.Value {

	switch attrPath {
	case "span:name":
		if v, ok := p.getString("span:name"); ok {
			return vm.Value{Type: vm.TypeString, Data: v}
		}
	case "span:duration":
		if v, ok := p.getUint("span:duration"); ok {
			return vm.Value{Type: vm.TypeDuration, Data: int64(v)}
		}
	case "span:start":
		if v, ok := p.getUint("span:start"); ok {
			return vm.Value{Type: vm.TypeInt, Data: int64(v)}
		}
	case "span:kind":
		if v, ok := p.getInt("span:kind"); ok {
			return vm.Value{Type: vm.TypeInt, Data: v}
		}
	case "span:status":
		if v, ok := p.getInt("span:status"); ok {
			return vm.Value{Type: vm.TypeInt, Data: v}
		}
	case "span:status_message", "statusMessage":
		if v, ok := p.getString("span:status_message"); ok {
			return vm.Value{Type: vm.TypeString, Data: v}
		}
	case "trace:id":
		if v, ok := p.getBytes("trace:id"); ok {
			return vm.Value{Type: vm.TypeBytes, Data: v}
		}
	case "span:id":
		if v, ok := p.getBytes("span:id"); ok {
			return vm.Value{Type: vm.TypeBytes, Data: v}
		}
	case "span:parent_id", "parentID":
		if v, ok := p.getBytes("span:parent_id"); ok {
			return vm.Value{Type: vm.TypeBytes, Data: v}
		}
	case "span:trace_state", "traceState":
		if v, ok := p.getString("span:trace_state"); ok {
			return vm.Value{Type: vm.TypeString, Data: v}
		}
	}

	if suffix, ok := strings.CutPrefix(attrPath, "event."); ok {
		return p.getEventAttribute(suffix)
	}
	if suffix, ok := strings.CutPrefix(attrPath, "link."); ok {
		return p.getLinkAttribute(suffix)
	}
	if suffix, ok := strings.CutPrefix(attrPath, "instrumentation."); ok {
		return p.getInstrumentationAttribute(suffix)
	}

	// User-defined span attributes: use dot notation (no collision with colon intrinsics)
	// Example: "span.foo" → "span.foo"
	if strings.HasPrefix(attrPath, "span.") {
		if v, ok := p.getGenericValue(attrPath); ok {
			return v
		}
		// Return nil value (not error) if column doesn't exist or has no value
		return vm.Value{Type: vm.TypeNil, Data: nil}
	}
	// User-defined resource attributes: use dot notation (no collision with colon intrinsics)
	// Example: "resource.foo" → "resource.foo"
	if strings.HasPrefix(attrPath, "resource.") {
		if v, ok := p.getGenericValue(attrPath); ok {
			return v
		}
		// Return nil value (not error) if column doesn't exist or has no value
		return vm.Value{Type: vm.TypeNil, Data: nil}
	}

	return vm.Value{Type: vm.TypeNil, Data: nil}
}

func (p *blockpackAttributeProvider) GetStartTime() (uint64, bool) {
	// Fast path: use cached column
	if p.startTimeCol != nil {
		return p.startTimeCol.Uint64Value(p.idx)
	}
	// Fallback to map lookup
	if v, ok := p.getUint("span:start"); ok {
		return v, true
	}
	return 0, false
}

func (p *blockpackAttributeProvider) getEventAttribute(name string) vm.Value {
	columnName := "event." + name
	switch name {
	case "name":
		columnName = "event:name"
	case "time_since_start":
		columnName = "event:time_since_start"
	}
	col := p.block.GetColumn(columnName)
	if col == nil {
		return vm.Value{Type: vm.TypeNil}
	}
	values, err := decodeArrayColumn(col, p.idx)
	if err != nil {
		return vm.Value{Type: vm.TypeArray, Data: []vm.Value{}}
	}
	return vm.Value{Type: vm.TypeArray, Data: arrayValuesToVM(values)}
}

func (p *blockpackAttributeProvider) getLinkAttribute(name string) vm.Value {
	columnName := "link." + name
	switch name {
	case "trace_id":
		columnName = "link:trace_id"
	case "span_id":
		columnName = "link:span_id"
	}
	col := p.block.GetColumn(columnName)
	if col == nil {
		return vm.Value{Type: vm.TypeNil}
	}
	values, err := decodeArrayColumn(col, p.idx)
	if err != nil {
		return vm.Value{Type: vm.TypeArray, Data: []vm.Value{}}
	}
	return vm.Value{Type: vm.TypeArray, Data: arrayValuesToVM(values)}
}

func (p *blockpackAttributeProvider) getInstrumentationAttribute(name string) vm.Value {
	columnName := "instrumentation." + name
	switch name {
	case "name":
		columnName = "instrumentation:name"
	case "version":
		columnName = "instrumentation:version"
	}
	col := p.block.GetColumn(columnName)
	if col == nil {
		return vm.Value{Type: vm.TypeNil}
	}
	values, err := decodeArrayColumn(col, p.idx)
	if err != nil {
		return vm.Value{Type: vm.TypeArray, Data: []vm.Value{}}
	}
	return vm.Value{Type: vm.TypeArray, Data: arrayValuesToVM(values)}
}

func (p *blockpackAttributeProvider) getGenericValue(columnName string) (vm.Value, bool) {
	col := p.block.GetColumn(columnName)
	if col == nil {
		return vm.Value{Type: vm.TypeNil, Data: nil}, false
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		if v, ok := col.StringValue(p.idx); ok {
			return vm.Value{Type: vm.TypeString, Data: v}, true
		}
	case blockpack.ColumnTypeInt64:
		if v, ok := col.Int64Value(p.idx); ok {
			return vm.Value{Type: vm.TypeInt, Data: v}, true
		}
	case blockpack.ColumnTypeUint64:
		if v, ok := col.Uint64Value(p.idx); ok {
			return vm.Value{Type: vm.TypeInt, Data: int64(v)}, true
		}
	case blockpack.ColumnTypeBool:
		if v, ok := col.BoolValue(p.idx); ok {
			return vm.Value{Type: vm.TypeBool, Data: v}, true
		}
	case blockpack.ColumnTypeFloat64:
		if v, ok := col.Float64Value(p.idx); ok {
			return vm.Value{Type: vm.TypeFloat, Data: v}, true
		}
	case blockpack.ColumnTypeBytes:
		if v, ok := col.BytesValueView(p.idx); ok {
			// Try to decode as array first (arrays are stored as encoded bytes)
			if values, err := blockpack.DecodeArray(v); err == nil {
				return vm.Value{Type: vm.TypeArray, Data: arrayValuesToVM(values)}, true
			}
			// Fall back to bytes if not an array (DecodeArray returns error for non-arrays)
			return vm.Value{Type: vm.TypeBytes, Data: v}, true
		}
	}

	return vm.Value{Type: vm.TypeNil, Data: nil}, false
}

func (p *blockpackAttributeProvider) getString(name string) (string, bool) {
	// Fast path for common columns
	if name == "span.name" && p.spanNameCol != nil {
		return p.spanNameCol.StringValue(p.idx)
	}

	col := p.block.GetColumn(name)
	if col == nil {
		return "", false
	}
	return col.StringValue(p.idx)
}

func (p *blockpackAttributeProvider) getInt(name string) (int64, bool) {
	// Fast path for common columns
	switch name {
	case "span.kind":
		if p.spanKindCol != nil {
			return p.spanKindCol.Int64Value(p.idx)
		}
	case "span.status":
		if p.spanStatusCol != nil {
			return p.spanStatusCol.Int64Value(p.idx)
		}
	}

	col := p.block.GetColumn(name)
	if col == nil {
		return 0, false
	}
	return col.Int64Value(p.idx)
}

func (p *blockpackAttributeProvider) getUint(name string) (uint64, bool) {
	// Fast path for common columns
	switch name {
	case "span:duration":
		if p.durationCol != nil {
			return p.durationCol.Uint64Value(p.idx)
		}
	case "span:start":
		if p.startTimeCol != nil {
			return p.startTimeCol.Uint64Value(p.idx)
		}
	}

	col := p.block.GetColumn(name)
	if col == nil {
		return 0, false
	}
	return col.Uint64Value(p.idx)
}

func (p *blockpackAttributeProvider) getBytes(name string) ([]byte, bool) {
	// Fast path for common columns
	switch name {
	case "trace:id":
		if p.traceIDCol != nil {
			return p.traceIDCol.BytesValueView(p.idx)
		}
	case "span:id":
		if p.spanIDCol != nil {
			return p.spanIDCol.BytesValueView(p.idx)
		}
	}

	col := p.block.GetColumn(name)
	if col == nil {
		return nil, false
	}
	return col.BytesValueView(p.idx)
}

func (p *blockpackAttributeProvider) traceID() []byte {
	if v, ok := p.getBytes("trace:id"); ok {
		return v
	}
	return nil
}

func (p *blockpackAttributeProvider) spanID() []byte {
	if v, ok := p.getBytes("span:id"); ok {
		return v
	}
	return nil
}
