# Discovery: AddAttribute Method, Spanset Struct, and Pipeline Evaluation

## File Structure

The relevant code is located in:
- `/Users/mdurham/source/tempo-mrd/pkg/traceql/storage.go` - Spanset struct and SpansetAttribute definition
- `/Users/mdurham/source/tempo-mrd/pkg/traceql/ast.go` - Pipeline struct and evaluate method
- `/Users/mdurham/source/tempo-mrd/pkg/traceql/engine.go` - Engine logic where attributeMatched is used

## Key Components

### 1. Spanset Struct Definition
**File:** `/Users/mdurham/source/tempo-mrd/pkg/traceql/storage.go:204-222`

The Spanset struct includes:
- **Attributes field type:** `[]*SpansetAttribute` - a slice of pointers to SpansetAttribute structs

### 2. SpansetAttribute Type Definition
**File:** `/Users/mdurham/source/tempo-mrd/pkg/traceql/storage.go:194-197`

```go
type SpansetAttribute struct {
	Name string
	Val  Static
}
```

The `Attributes` field is a slice of `*SpansetAttribute`, where each attribute has a name (string) and a value (Static).

### 3. AddAttribute Method on Spanset
**File:** `/Users/mdurham/source/tempo-mrd/pkg/traceql/storage.go:224-226`

```go
func (s *Spanset) AddAttribute(key string, value Static) {
	s.Attributes = append(s.Attributes, &SpansetAttribute{Name: key, Val: value})
}
```

**Purpose:** This method appends a new SpansetAttribute to the Spanset's Attributes slice. It takes a key (string) and a value (Static type) and creates a new SpansetAttribute struct with those values, then appends it to the attributes list.

### 4. Pipeline evaluate Method
**File:** `/Users/mdurham/source/tempo-mrd/pkg/traceql/ast.go:235-250`

```go
func (p Pipeline) evaluate(input []*Spanset) (result []*Spanset, err error) {
	result = input

	for _, element := range p.Elements {
		result, err = element.evaluate(result)
		if err != nil {
			return nil, err
		}

		if len(result) == 0 {
			return []*Spanset{}, nil
		}
	}

	return result, nil
}
```

**For Empty Pipeline (`{}`/no stages):**
- If the pipeline has no Elements (empty pipeline), the for loop does NOT execute
- The method immediately returns `result = input` on line 249
- **Yes, it returns the input unchanged** - no modifications are made to the spansets

### 5. attributeMatched Constant and Usage
**File:** `/Users/mdurham/source/tempo-mrd/pkg/traceql/storage.go:192`

```go
const attributeMatched = "__matched"
```

**Where AddAttribute is called with attributeMatched:**
**File:** `/Users/mdurham/source/tempo-mrd/pkg/traceql/engine.go:116`

```go
evalSS[i].AddAttribute(attributeMatched, NewStaticInt(l))
```

This occurs in the `ExecuteSearch` method within the SecondPass function (lines 97-128). Specifically:
- After the pipeline evaluates the spansets (line 102): `evalSS, err := rootExpr.Pipeline.evaluate([]*Spanset{inSS})`
- If evaluation succeeds and returns non-empty results, a loop processes each evaluated spanset
- Line 116 calls `AddAttribute(attributeMatched, NewStaticInt(l))` where `l` is `len(evalSS[i].Spans)`
- This records the number of matched spans for each spanset result

## Important Finding: Empty Pipeline Behavior

**Does empty pipeline compilation result in AddAttribute being called with attributeMatched?**

The answer is context-dependent:
1. **The Pipeline.evaluate method itself (ast.go:235-250):** For an empty pipeline, it returns the input unchanged without calling AddAttribute
2. **The Engine.ExecuteSearch flow (engine.go:97-128):** AFTER the pipeline evaluates (regardless of whether it's empty), the engine itself calls `AddAttribute(attributeMatched, NewStaticInt(len(spans)))` in the SecondPass callback at line 116

So for an empty pipeline `{}`:
- The pipeline evaluation returns input unchanged
- But the engine layer then ADDS the attributeMatched attribute when storing results

## Summary

1. **AddAttribute** appends a SpansetAttribute (name-value pair) to the Spanset's Attributes slice
2. **Attributes field** is typed as `[]*SpansetAttribute` - a slice of pointers to SpansetAttribute structs
3. **Pipeline.evaluate** for an empty pipeline returns input unchanged - it's a pass-through operation
4. **attributeMatched** (`"__matched"`) is added by the Engine layer (not the pipeline itself) after evaluation, recording the span count in each result
