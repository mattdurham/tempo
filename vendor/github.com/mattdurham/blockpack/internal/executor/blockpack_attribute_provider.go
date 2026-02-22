package executor

import (
	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	blockpack "github.com/mattdurham/blockpack/internal/types"
)

// blockpackAttributeProvider provides VM attribute access to blockpack storage.
// Fast-path caching is used for commonly accessed columns to avoid map lookups.
type blockpackAttributeProvider struct {
	block *blockpackio.Block // Store reference to block for GetColumn lookups

	// Fast-path cache for commonly accessed columns (avoids map lookups)
	traceIDCol    *blockpack.Column
	spanIDCol     *blockpack.Column
	spanNameCol   *blockpack.Column
	durationCol   *blockpack.Column
	startTimeCol  *blockpack.Column
	spanKindCol   *blockpack.Column
	spanStatusCol *blockpack.Column
	idx           int
}

func newBlockpackAttributeProvider(block *blockpackio.Block, idx int) *blockpackAttributeProvider {
	return &blockpackAttributeProvider{
		block:         block,
		idx:           idx,
		traceIDCol:    block.GetColumn(IntrinsicTraceID),
		spanIDCol:     block.GetColumn(IntrinsicSpanID),
		spanNameCol:   block.GetColumn(IntrinsicSpanName),
		durationCol:   block.GetColumn(IntrinsicSpanDuration),
		startTimeCol:  block.GetColumn(IntrinsicSpanStart),
		spanKindCol:   block.GetColumn(IntrinsicSpanKind),
		spanStatusCol: block.GetColumn(IntrinsicSpanStatus),
	}
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

func (p *blockpackAttributeProvider) getUint(name string) (uint64, bool) {
	// Fast path for common columns
	switch name {
	case IntrinsicSpanDuration:
		if p.durationCol != nil {
			return p.durationCol.Uint64Value(p.idx)
		}
	case IntrinsicSpanStart:
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
	case IntrinsicTraceID:
		if p.traceIDCol != nil {
			return p.traceIDCol.BytesValueView(p.idx)
		}
	case IntrinsicSpanID:
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
	if v, ok := p.getBytes(IntrinsicTraceID); ok {
		return v
	}
	return nil
}

func (p *blockpackAttributeProvider) spanID() []byte {
	if v, ok := p.getBytes(IntrinsicSpanID); ok {
		return v
	}
	return nil
}
