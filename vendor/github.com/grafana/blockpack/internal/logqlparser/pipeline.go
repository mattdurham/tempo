package logqlparser

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// Pipeline stages are pure functions — no shared mutable state. Process is safe
// for concurrent use on the same Pipeline value (SPEC-002).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"text/template"

	"github.com/go-logfmt/logfmt"
)

// UnwrapValueKey is the reserved label key used by UnwrapStage to store the extracted
// numeric value for metric aggregation (NOTE-006).
// SPEC-010: Subsequent stages and metric aggregation read this key for the unwrapped value.
const UnwrapValueKey = "__unwrap_value__"

// PipelineStageFunc is a single pipeline stage.
// SPEC-001: Receives ts (nanosecond timestamp), log line, and a mutable LabelSet.
// Returns transformed line, LabelSet, and keep bool. False means drop the row.
// Implementations MUST NOT retain references to the input LabelSet beyond the call.
type PipelineStageFunc func(ts uint64, line string, labels LabelSet) (string, LabelSet, bool)

// Pipeline chains zero or more PipelineStageFunc values applied per row.
// SPEC-002: Process short-circuits on first false return.
type Pipeline struct {
	Stages           []PipelineStageFunc
	parserFreeStages []PipelineStageFunc // same as Stages but with logfmt/JSON parser stages removed
	// HasLineFormat is true when any stage is a line_format stage.
	// When true, ProcessSkipParsers is unsafe: line_format calls Materialize() which
	// only includes decoded columns. If the parser was skipped, body-parsed fields
	// (caller, msg, etc.) are never put in the overlay → template produces "".
	// The executor must use Process() instead to run logfmt and populate the overlay.
	HasLineFormat bool
	// HasParserStage is true when any stage is a logfmt or JSON parser stage.
	// When true, the executor must call HideLogColumns() on the blockLabelSet before
	// running Process() so that ingest-time log.* column values do not shadow
	// re-parsed body values. This ensures first-wins logfmt semantics and correct
	// behavior for non-logfmt bodies (e.g. JSON) that would fail logfmt parsing.
	HasParserStage bool
}

// Process applies all stages in order to the given row.
// SPEC-002: Returns (line, labels, false) on first stage that drops the row.
// A nil or empty Pipeline is a no-op: returns (line, labels, true).
// Process is safe for concurrent use on the same Pipeline value.
func (p *Pipeline) Process(ts uint64, line string, labels LabelSet) (string, LabelSet, bool) {
	if p == nil || len(p.Stages) == 0 {
		return line, labels, true
	}
	for _, stage := range p.Stages {
		var keep bool
		line, labels, keep = stage(ts, line, labels)
		if !keep {
			return line, labels, false
		}
	}
	return line, labels, true
}

// ProcessSkipParsers applies all non-parser stages (label filters, format stages, etc.)
// skipping any logfmt/JSON parser stages. Use when the block's body fields are already
// stored as log.* columns and populated into labels via blockLabelSet (e.g. acquireBlockLabelSet) —
// in that case the parser stages are pure no-ops and skipping them avoids redundant body decoding.
// Safe for concurrent use on the same Pipeline value.
func (p *Pipeline) ProcessSkipParsers(ts uint64, line string, labels LabelSet) (string, LabelSet, bool) {
	if p == nil || len(p.parserFreeStages) == 0 {
		return line, labels, true
	}
	for _, stage := range p.parserFreeStages {
		var keep bool
		line, labels, keep = stage(ts, line, labels)
		if !keep {
			return line, labels, false
		}
	}
	return line, labels, true
}

// JSONStage returns a PipelineStageFunc that parses the log line as a JSON object
// and extracts all top-level key-value pairs as labels.
// SPEC-003: If the line is not valid JSON or not a JSON object, labels are unchanged
// and the row is kept (silent failure policy — NOTE-007).
func JSONStage() PipelineStageFunc {
	return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		// Hide ingest-time log.* columns before parsing so body re-parse uses
		// first-wins semantics. Runs immediately before the parser stage.
		labels.HideBodyParsedColumns()
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			// Not valid JSON — keep row unchanged (SPEC-003, NOTE-007)
			return line, labels, true
		}
		for k, v := range obj {
			// SPEC-11.5 no-op: live labels win; pipeline fills gaps for undecoded columns.
			// HasLive skips only keys that have an immediately accessible value (overlay or
			// decoded block column). Undecoded block columns return HasLive=false, so JSON
			// extraction populates them into the overlay — line_format then sees the value.
			if labels.HasLive(k) {
				continue
			}
			switch s := v.(type) {
			case string:
				labels.Set(k, s)
			default:
				labels.Set(k, fmt.Sprint(v))
			}
		}
		return line, labels, true
	}
}

// LogfmtStage returns a PipelineStageFunc that parses the log line as logfmt
// (key=value pairs) and extracts the pairs as labels.
// SPEC-004: Uses github.com/go-logfmt/logfmt decoder.
// Invalid logfmt keeps labels unchanged; row is NOT dropped (NOTE-007).
func LogfmtStage() PipelineStageFunc {
	return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		// Hide ingest-time log.* columns before parsing so body re-parse uses
		// first-wins semantics and does not fall back to last-wins ingest values.
		// This runs immediately before the parser, so prior non-parser stages
		// (e.g. label_format) already executed and are unaffected.
		labels.HideBodyParsedColumns()
		dec := logfmt.NewDecoder(bytes.NewBufferString(line))
		for dec.ScanRecord() {
			for dec.ScanKeyval() {
				k := string(dec.Key())
				// SPEC-11.5 no-op: live labels win; pipeline fills gaps for undecoded columns.
				// HasLive skips only keys that have an immediately accessible value (overlay or
				// decoded block column). Undecoded block columns return HasLive=false, so logfmt
				// extraction populates them into the overlay — line_format then sees the value.
				if labels.HasLive(k) {
					continue
				}
				labels.Set(k, string(dec.Value()))
			}
		}
		// Loki non-strict logfmt: __error__ is always "" regardless of parse errors.
		// __error__ is set unconditionally — not guarded by HasLive.
		labels.Set("__error__", "")
		return line, labels, true
	}
}

// LabelFormatStage returns a PipelineStageFunc that renames labels using the given mappings.
// SPEC-005: For each dst->src mapping, copies labels[src] to labels[dst] then deletes labels[src]
// (unless dst == src). If src is absent, dst is set to "". Never drops rows.
func LabelFormatStage(mappings map[string]string) PipelineStageFunc {
	return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		for dst, src := range mappings {
			val := labels.Get(src) // empty string if absent (SPEC-005)
			labels.Set(dst, val)
			if dst != src {
				labels.Delete(src)
			}
		}
		return line, labels, true
	}
}

// LineFormatStage returns a PipelineStageFunc that replaces the log line using
// the pre-compiled text/template executed against a materialized labels map.
// SPEC-006: Template is pre-compiled at query compile time (NOTE-005).
// If template execution fails, the original line is kept; row is NOT dropped (SPEC-021).
func LineFormatStage(tmpl *template.Template) PipelineStageFunc {
	return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, labels.Materialize()); err != nil {
			// Keep original line on template error (SPEC-006, SPEC-021)
			return line, labels, true
		}
		return buf.String(), labels, true
	}
}

// DropStage returns a PipelineStageFunc that removes the named fields from labels.
// SPEC-007: Fields not present in labels are silently skipped. Never drops rows.
func DropStage(fields []string) PipelineStageFunc {
	return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		for _, f := range fields {
			labels.Delete(f)
		}
		return line, labels, true
	}
}

// KeepStage returns a PipelineStageFunc that removes all labels NOT in the given fields list.
// SPEC-008: Fields in fields not present in labels are silently skipped. Never drops rows.
func KeepStage(fields []string) PipelineStageFunc {
	keepSet := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		keepSet[f] = struct{}{}
	}
	return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		for _, k := range labels.Keys() {
			if _, ok := keepSet[k]; !ok {
				labels.Delete(k)
			}
		}
		return line, labels, true
	}
}

// LabelFilterStage returns a PipelineStageFunc that filters rows based on a label comparison.
// SPEC-009: String ops: OpEqual, OpNotEqual, OpRegex, OpNotRegex.
// Numeric ops: OpGT, OpLT, OpGTE, OpLTE — parse label value as float64; if unparseable, drop.
// Returns false (drop) if the filter condition is NOT satisfied (SPEC-021).
func LabelFilterStage(name, value string, op FilterOp) PipelineStageFunc {
	// Pre-compile regex for regex ops
	var re *regexp.Regexp
	if op == OpRegex || op == OpNotRegex {
		var err error
		// Loki anchors label-filter regexes as full-string matches: ^(?:value)$
		// Without anchoring, "warn|error" would match "warning" as a substring.
		re, err = regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			// Invalid regex — always drop (fail safe)
			return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
				return line, labels, false
			}
		}
	}

	// Pre-parse numeric threshold for numeric ops
	var numThreshold float64
	if op == OpGT || op == OpLT || op == OpGTE || op == OpLTE {
		var err error
		numThreshold, err = strconv.ParseFloat(value, 64)
		if err != nil {
			// Invalid numeric threshold — always drop
			return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
				return line, labels, false
			}
		}
	}

	return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		labelVal := labels.Get(name)
		switch op {
		case OpEqual:
			return line, labels, labelVal == value
		case OpNotEqual:
			return line, labels, labelVal != value
		case OpRegex:
			return line, labels, re.MatchString(labelVal)
		case OpNotRegex:
			return line, labels, !re.MatchString(labelVal)
		case OpGT, OpLT, OpGTE, OpLTE:
			parsed, err := strconv.ParseFloat(labelVal, 64)
			if err != nil {
				// Non-numeric value — drop (SPEC-009, SPEC-021)
				return line, labels, false
			}
			var keep bool
			switch op {
			case OpGT:
				keep = parsed > numThreshold
			case OpLT:
				keep = parsed < numThreshold
			case OpGTE:
				keep = parsed >= numThreshold
			case OpLTE:
				keep = parsed <= numThreshold
			}
			return line, labels, keep
		}
		return line, labels, false
	}
}

// OrLabelFilterStage combines a primary label filter with OR alternatives.
// The entry passes if the primary OR any of the alternatives match.
func OrLabelFilterStage(primary PipelineStageFunc, alts []PipelineStageFunc) PipelineStageFunc {
	return func(ts uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		outLine, outLabels, ok := primary(ts, line, labels)
		if ok {
			return outLine, outLabels, true
		}
		for _, alt := range alts {
			outLine, outLabels, ok = alt(ts, line, labels)
			if ok {
				return outLine, outLabels, true
			}
		}
		return outLine, outLabels, false
	}
}

// UnwrapStage returns a PipelineStageFunc that reads labels[field], parses it as float64,
// and stores the result in labels[UnwrapValueKey].
// SPEC-010: If the label is absent or not parseable as float64, the row is dropped.
// NOTE-006: UnwrapValueKey is read by metric aggregation after pipeline execution.
// NOTE: Get("") returns "" for absent keys; ParseFloat("", 64) errors — same drop semantics.
func UnwrapStage(field string) PipelineStageFunc {
	return func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		raw := labels.Get(field)
		// SPEC-010: absent label (Get returns "") fails ParseFloat — drop.
		val, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			// Non-numeric or absent — drop (SPEC-010, SPEC-021)
			return line, labels, false
		}
		labels.Set(UnwrapValueKey, strconv.FormatFloat(val, 'f', -1, 64))
		return line, labels, true
	}
}
