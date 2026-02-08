# TraceQL Support

This package implements TraceQL parsing and compilation for the blockpack trace storage system.

## Supported Features

The TraceQL parser supports basic filter expressions:

### Syntax

```traceql
{ <filter-expression> }
```

An empty query `{}` or `""` matches all spans.

### Operators

**Comparison Operators:**
- `=` - Equals
- `!=` - Not equals
- `>` - Greater than
- `>=` - Greater than or equal
- `<` - Less than
- `<=` - Less than or equal
- `=~` - Regex match
- `!~` - Regex not match

**Logical Operators:**
- `&&` - AND
- `||` - OR

**Nil Checks:**
- `!= nil` - Field is not null
- `= nil` - Field is null

### Attribute Scopes

**Unscoped** (checks both resource and span scopes):
```traceql
{ .foo = "bar" }
// Equivalent to: ("resource.foo" = 'bar' OR "span.foo" = 'bar')
```

**Scoped Attributes:**
```traceql
{ resource.service.name = "myservice" }
{ span.http.status_code = 500 }
{ event:name = "exception" }
{ link:spanID = "abc123" }
{ instrumentation:name = "my-lib" }
```

### Intrinsic Fields

**Span Intrinsics:**
- `name` - Span name
- `duration` - Span duration (with time units: ns, us, ms, s, m, h)
- `status` - Span status (values: `error`, `ok`, `unset`)
- `statusMessage` - Status message string
- `kind` - Span kind (values: `client`, `server`, `producer`, `consumer`, `internal`, `unspecified`)

**IDs:**
- `trace:id` or `traceID` - Trace ID
- `span:id` or `spanID` - Span ID
- `span:parent_id` or `parentID` - Parent span ID

### Data Types

**Strings:**
```traceql
{ name = "hello" }
```

**Numbers:**
```traceql
{ .count > 100 }
{ .score = 456.78 }
```

**Booleans:**
```traceql
{ .enabled = true }
{ .disabled = false }
```

**Durations:**
```traceql
{ duration > 100s }
{ duration < 5m }
{ event:timeSinceStart > 2ms }
```

Supported units: `ns`, `us`, `ms`, `s`, `m`, `h`

**Status Enums:**
```traceql
{ status = error }
{ status = ok }
{ status = unset }
```

**Kind Enums:**
```traceql
{ kind = client }
{ kind = server }
{ kind = producer }
{ kind = consumer }
{ kind = internal }
{ kind = unspecified }
```

## Examples

### Simple Filters

```traceql
{ name = "hello" }
{ duration > 100s }
{ status = error }
{ kind = client }
```

### Scoped Attributes

```traceql
{ resource.cluster = "prod" }
{ span.http.status_code = 500 }
{ .service.name = "api-server" }  // Unscoped - checks both resource and span
```

### Logical Combinations

```traceql
// AND
{ name = "hello" && duration > 100s }

// OR
{ status = error || span.http.status_code >= 500 }

// Complex
{ resource.cluster = "prod" && (span.http.status_code >= 500 || name = "error") }
```

### Pattern Matching

```traceql
{ name =~ "api-.*" }
{ .url =~ "https://.*\\.example\\.com/.*" }
{ resource.service.name !~ "test-.*" }
```

### Nil Checks

```traceql
{ .foo != nil }  // Field exists
{ span.bar = nil }  // Field does not exist or is null
```

### Array Attributes

Array attributes can be queried for membership:
```traceql
{ resource.tags = "production" }  // Checks if array contains "production"
{ span.errors = 404 }  // Checks if array contains 404
```

## NOT Supported

The following TraceQL features are **not** supported by this implementation:

- **Structural Queries**: Span relationships (`>> child`, `~ sibling`)
- **Trace-Level Selectors**: `{ }` with trace-level aggregations
- **Metrics Queries**: `rate()`, `count_over_time()`, `avg_over_time()`, etc.
  - Note: These are supported by Tempo's metrics API on parquet data
  - Our TraceQL parser only handles filter expressions, not metrics aggregations
- **Span Aggregations**: `count()`, `avg()`, `min()`, `max()`, `sum()` (spanset aggregations)
- **Grouping**: `by()` operator (used with metrics queries)
- **Pipeline**: Multiple stages with `|`
- **Math Operations**: `+`, `-`, `*`, `/`, `%`, `^`

### TraceQL Metrics in Tempo (Not Implemented Here)

Tempo's full TraceQL implementation supports **metrics queries** for time-series aggregations:

```traceql
# Count spans over time, grouped by service
{} | count_over_time() by (resource.service.name)

# Average duration over time, grouped by service
{} | avg_over_time(duration) by (resource.service.name)

# P95 duration over time
{} | quantile_over_time(duration, 0.95) by (resource.service.name)
```

These metrics queries are **time-series aggregations** (return values per time interval), which differ from SQL GROUP BY aggregations (return single aggregate values). See `/benchmark/AGGREGATION_VALIDATION.md` for details on the differences.

## Usage

### From Go Code

```go
import (
    "github.com/mattdurham/blockpack/internal/traceqlparser"
    "github.com/mattdurham/blockpack/internal/sql"
)

// Parse TraceQL to AST
ast, err := traceqlparser.ParseTraceQL(`{ name = "hello" && duration > 100s }`)
if err != nil {
    panic(err)
}

// Convert to SQL
whereClause, err := ast.ToSQL()
// Returns: "name" = 'hello' AND "duration" > INTERVAL '100s'

// Compile TraceQL directly to VM Program
program, err := sql.CompileTraceQL(`{ status = error }`)

// Auto-detect SQL vs TraceQL
program, err := sql.CompileTraceQLOrSQL(query)
// If query starts with "SELECT" -> SQL
// Otherwise -> TraceQL
```

### From HTTP API

The server automatically detects TraceQL vs SQL:

```bash
# TraceQL query
curl "http://localhost:8080/api/search?q={status=error}"

# SQL query
curl "http://localhost:8080/api/search?q=SELECT * FROM spans WHERE \"status\" = 2"
```

## Implementation Details

The TraceQL parser:
1. Parses TraceQL syntax to an AST
2. Converts the AST to SQL WHERE clause
3. Uses the existing PostgreSQL parser to parse the SQL
4. Compiles to VM bytecode using the existing SQL compilation pipeline

This approach reuses all existing SQL compilation, optimization, and execution infrastructure.

## Testing

Run the tests:
```bash
go test ./internal/traceqlparser -v
go test ./sql -run TraceQL -v
```

Test coverage includes:
- Parser correctness
- SQL generation
- Integration with SQL compiler
- Edge cases (nil, regex, durations, enums)
- Complex expressions with operator precedence
