package executor

import (
	blockpack "github.com/mattdurham/blockpack/blockpack/types"
	"github.com/mattdurham/blockpack/vm"
)

func decodeArrayColumn(col *blockpack.Column, idx int) ([]blockpack.ArrayValue, error) {
	if col == nil {
		return nil, nil
	}
	data, ok := col.BytesValueView(idx)
	if !ok || len(data) == 0 {
		return nil, nil
	}
	return blockpack.DecodeArray(data)
}

func arrayValuesToVM(values []blockpack.ArrayValue) []vm.Value {
	if len(values) == 0 {
		return []vm.Value{}
	}
	out := make([]vm.Value, 0, len(values))
	for _, val := range values {
		switch val.Type {
		case blockpack.ArrayTypeString:
			out = append(out, vm.Value{Type: vm.TypeString, Data: val.Str})
		case blockpack.ArrayTypeInt64:
			out = append(out, vm.Value{Type: vm.TypeInt, Data: val.Int})
		case blockpack.ArrayTypeFloat64:
			out = append(out, vm.Value{Type: vm.TypeFloat, Data: val.Float})
		case blockpack.ArrayTypeBool:
			out = append(out, vm.Value{Type: vm.TypeBool, Data: val.Bool})
		case blockpack.ArrayTypeBytes:
			out = append(out, vm.Value{Type: vm.TypeBytes, Data: val.Bytes})
		case blockpack.ArrayTypeDuration:
			out = append(out, vm.Value{Type: vm.TypeDuration, Data: val.Int})
		}
	}
	return out
}
