package executor

// SpanNode represents a span in the trace graph with parent-child relationships.
type SpanNode struct {
	TraceID  string
	SpanID   string
	ParentID string
	Span     *BlockpackSpanMatch // Reference to original span data
}

// TraceSpanGraph represents the parent-child relationships of spans in a single trace.
type TraceSpanGraph struct {
	TraceID   string
	SpansByID map[string]*SpanNode // Fast lookup by span ID
	RootSpans []*SpanNode          // Spans with no parent (parent_id == "")
	AllSpans  []*SpanNode          // All spans in the trace
}

// BuildTraceGraph constructs parent-child graphs for each trace.
// Returns a map of trace_id -> TraceSpanGraph.
func BuildTraceGraph(spans []BlockpackSpanMatch) map[string]*TraceSpanGraph {
	// Group spans by trace ID
	traceSpans := make(map[string][]*BlockpackSpanMatch)
	for i := range spans {
		span := &spans[i]
		traceSpans[span.TraceID] = append(traceSpans[span.TraceID], span)
	}

	// Build graph for each trace
	graphs := make(map[string]*TraceSpanGraph)
	for traceID, spanList := range traceSpans {
		graph := &TraceSpanGraph{
			TraceID:   traceID,
			SpansByID: make(map[string]*SpanNode),
			RootSpans: make([]*SpanNode, 0),
			AllSpans:  make([]*SpanNode, 0, len(spanList)),
		}

		// Create nodes
		for _, span := range spanList {
			// Get parent_id from span fields (prefer intrinsic column name)
			parentID := ""
			if parentVal, ok := span.Fields.GetField("span:parent_id"); ok {
				if parentStr, ok := parentVal.(string); ok {
					parentID = parentStr
				}
			} else if parentVal, ok := span.Fields.GetField("parent_id"); ok {
				// Support legacy alias for backwards compatibility
				if parentStr, ok := parentVal.(string); ok {
					parentID = parentStr
				}
			}

			node := &SpanNode{
				TraceID:  span.TraceID,
				SpanID:   span.SpanID,
				ParentID: parentID,
				Span:     span,
			}

			graph.SpansByID[span.SpanID] = node
			graph.AllSpans = append(graph.AllSpans, node)

			// Track root spans
			if parentID == "" {
				graph.RootSpans = append(graph.RootSpans, node)
			}
		}

		graphs[traceID] = graph
	}

	return graphs
}

// DescendantOf returns RHS spans that are descendants of any LHS span.
// A span is a descendant if any of its ancestors (walking up parent chain) matches LHS.
//
// Example: { .parent } >> { .child }
//   - LHS: spans with .parent attribute
//   - RHS: spans with .child attribute
//   - Result: RHS spans whose ancestors include any LHS span
func DescendantOf(lhsSpans []*SpanNode, rhsSpans []*SpanNode, graph *TraceSpanGraph, negate bool) []*SpanNode {
	matches := descendantOfImpl(lhsSpans, rhsSpans, graph)

	if !negate {
		return matches
	}

	// Negation: return RHS spans NOT in matches
	matchSet := make(map[string]bool)
	for _, span := range matches {
		matchSet[span.SpanID] = true
	}

	result := make([]*SpanNode, 0)
	for _, rhsSpan := range rhsSpans {
		if !matchSet[rhsSpan.SpanID] {
			result = append(result, rhsSpan)
		}
	}

	return result
}

func descendantOfImpl(lhsSpans []*SpanNode, rhsSpans []*SpanNode, graph *TraceSpanGraph) []*SpanNode {
	result := make([]*SpanNode, 0)

	// Build set of LHS span IDs for O(1) lookup
	lhsSet := make(map[string]bool)
	for _, span := range lhsSpans {
		lhsSet[span.SpanID] = true
	}

	// For each RHS span, walk up parent chain
	for _, rhsSpan := range rhsSpans {
		current := rhsSpan
		for current.ParentID != "" {
			parent := graph.SpansByID[current.ParentID]
			if parent == nil {
				break // Parent not in trace
			}

			// Check if parent is in LHS
			if lhsSet[parent.SpanID] {
				result = append(result, rhsSpan)
				break
			}

			current = parent
		}
	}

	return result
}

// ChildOf returns RHS spans that are direct children of any LHS span.
// Direct child means parent_id matches LHS span_id.
//
// Example: { .parent } > { .child }
func ChildOf(lhsSpans []*SpanNode, rhsSpans []*SpanNode, graph *TraceSpanGraph, negate bool) []*SpanNode {
	matches := childOfImpl(lhsSpans, rhsSpans, graph)

	if !negate {
		return matches
	}

	// Negation: return RHS spans NOT in matches
	matchSet := make(map[string]bool)
	for _, span := range matches {
		matchSet[span.SpanID] = true
	}

	result := make([]*SpanNode, 0)
	for _, rhsSpan := range rhsSpans {
		if !matchSet[rhsSpan.SpanID] {
			result = append(result, rhsSpan)
		}
	}

	return result
}

func childOfImpl(lhsSpans []*SpanNode, rhsSpans []*SpanNode, _ *TraceSpanGraph) []*SpanNode {
	result := make([]*SpanNode, 0)

	// Build set of LHS span IDs
	lhsSet := make(map[string]bool)
	for _, span := range lhsSpans {
		lhsSet[span.SpanID] = true
	}

	// Check if RHS span's parent is in LHS
	for _, rhsSpan := range rhsSpans {
		if rhsSpan.ParentID != "" && lhsSet[rhsSpan.ParentID] {
			result = append(result, rhsSpan)
		}
	}

	return result
}

// SiblingOf returns RHS spans that share a parent with any LHS span.
// Siblings have the same parent_id (but are not the same span).
//
// Example: { .span1 } ~ { .span2 }
func SiblingOf(lhsSpans []*SpanNode, rhsSpans []*SpanNode, graph *TraceSpanGraph, negate bool) []*SpanNode {
	matches := siblingOfImpl(lhsSpans, rhsSpans, graph)

	if !negate {
		return matches
	}

	// Negation: return RHS spans NOT in matches
	matchSet := make(map[string]bool)
	for _, span := range matches {
		matchSet[span.SpanID] = true
	}

	result := make([]*SpanNode, 0)
	for _, rhsSpan := range rhsSpans {
		if !matchSet[rhsSpan.SpanID] {
			result = append(result, rhsSpan)
		}
	}

	return result
}

func siblingOfImpl(lhsSpans []*SpanNode, rhsSpans []*SpanNode, _ *TraceSpanGraph) []*SpanNode {
	result := make([]*SpanNode, 0)

	// Build mapping from parent IDs to LHS span IDs
	parentToLhsSpanIDs := make(map[string]map[string]bool)

	for _, lhsSpan := range lhsSpans {
		if lhsSpan.ParentID == "" {
			continue
		}

		siblings, ok := parentToLhsSpanIDs[lhsSpan.ParentID]
		if !ok {
			siblings = make(map[string]bool)
			parentToLhsSpanIDs[lhsSpan.ParentID] = siblings
		}
		siblings[lhsSpan.SpanID] = true
	}

	// Find RHS spans that share a parent with at least one different LHS span
	for _, rhsSpan := range rhsSpans {
		if rhsSpan.ParentID == "" {
			continue
		}

		siblings, ok := parentToLhsSpanIDs[rhsSpan.ParentID]
		if !ok {
			continue
		}

		// Exclude only true self-pairings: allow rhsSpan if there exists
		// some LHS span with the same parent and a different SpanID.
		if len(siblings) > 1 || !siblings[rhsSpan.SpanID] {
			result = append(result, rhsSpan)
		}
	}

	return result
}

// AncestorOf returns RHS spans that are ancestors of any LHS span.
// This is the inverse of DescendantOf.
//
// Example: { .child } << { .parent }
func AncestorOf(lhsSpans []*SpanNode, rhsSpans []*SpanNode, graph *TraceSpanGraph, negate bool) []*SpanNode {
	matches := ancestorOfImpl(lhsSpans, rhsSpans, graph)

	if !negate {
		return matches
	}

	// Negation: return RHS spans NOT in matches
	matchSet := make(map[string]bool)
	for _, span := range matches {
		matchSet[span.SpanID] = true
	}

	result := make([]*SpanNode, 0)
	for _, rhsSpan := range rhsSpans {
		if !matchSet[rhsSpan.SpanID] {
			result = append(result, rhsSpan)
		}
	}

	return result
}

func ancestorOfImpl(lhsSpans []*SpanNode, rhsSpans []*SpanNode, graph *TraceSpanGraph) []*SpanNode {
	// Build set of RHS span IDs for O(1) lookup
	rhsSet := make(map[string]bool)
	for _, span := range rhsSpans {
		rhsSet[span.SpanID] = true
	}

	// For each LHS span, walk up parent chain and mark matching RHS spans
	seen := make(map[string]bool)
	for _, lhsSpan := range lhsSpans {
		current := lhsSpan
		for current.ParentID != "" {
			parent := graph.SpansByID[current.ParentID]
			if parent == nil {
				break // Parent not in trace
			}

			// Check if parent is in RHS
			if rhsSet[parent.SpanID] {
				seen[parent.SpanID] = true
				// Don't break - continue walking to find all ancestors
			}

			current = parent
		}
	}

	// Build result slice in RHS order for deterministic behavior
	result := make([]*SpanNode, 0)
	for _, rhsSpan := range rhsSpans {
		if seen[rhsSpan.SpanID] {
			result = append(result, rhsSpan)
		}
	}

	return result
}

// ParentOf returns RHS spans that are direct parents of any LHS span.
// This is the inverse of ChildOf.
//
// Example: { .child } < { .parent }
func ParentOf(lhsSpans []*SpanNode, rhsSpans []*SpanNode, graph *TraceSpanGraph, negate bool) []*SpanNode {
	matches := parentOfImpl(lhsSpans, rhsSpans, graph)

	if !negate {
		return matches
	}

	// Negation: return RHS spans NOT in matches
	matchSet := make(map[string]bool)
	for _, span := range matches {
		matchSet[span.SpanID] = true
	}

	result := make([]*SpanNode, 0)
	for _, rhsSpan := range rhsSpans {
		if !matchSet[rhsSpan.SpanID] {
			result = append(result, rhsSpan)
		}
	}

	return result
}

func parentOfImpl(lhsSpans []*SpanNode, rhsSpans []*SpanNode, graph *TraceSpanGraph) []*SpanNode {
	// Build set of RHS span IDs for O(1) lookup
	rhsSet := make(map[string]bool)
	for _, span := range rhsSpans {
		rhsSet[span.SpanID] = true
	}

	// For each LHS span, check if its parent is in RHS and mark it
	seen := make(map[string]bool)
	for _, lhsSpan := range lhsSpans {
		if lhsSpan.ParentID != "" {
			parent := graph.SpansByID[lhsSpan.ParentID]
			if parent != nil && rhsSet[parent.SpanID] {
				// Mark for deduplication
				seen[parent.SpanID] = true
			}
		}
	}

	// Build result slice in RHS order for deterministic behavior
	result := make([]*SpanNode, 0)
	for _, rhsSpan := range rhsSpans {
		if seen[rhsSpan.SpanID] {
			result = append(result, rhsSpan)
		}
	}

	return result
}
