// Package main generates an asciicast v2 (.cast) file that animates the
// BlockPack TraceQL query execution pipeline.
//
// Usage:
//
//	go run ./cmd/animate > animation.cast
//	agg animation.cast animation.gif
//
// Or in one step (if agg is on PATH):
//
//	go run ./cmd/animate | agg - animation.gif
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Terminal dimensions for the cast.
const (
	termWidth  = 60
	termHeight = 40
)

// ANSI escape codes.
const (
	reset    = "\033[0m"
	bold     = "\033[1m"
	dim      = "\033[2m"
	italic   = "\033[3m"
	green    = "\033[92m"
	yellow   = "\033[93m"
	cyan     = "\033[96m"
	red      = "\033[91m"
	white    = "\033[97m"
	gray     = "\033[90m"
	clearScr = "\033[2J\033[H"
)

// Pipeline stages — label max 14 chars, file is basename only.
var stages = []struct {
	label string
	file  string
}{
	{"Parser", "parser.go"},
	{"VM Compiler", "traceql_compiler.go"},
	{"Pred. Builder", "predicates.go"},
	{"Time Range", "planner.go"},
	{"Range Index", "plan_blocks.go"},
	{"Bloom Filter", "plan_blocks.go"},
	{"Executor", "stream.go"},
	{"Block I/O", "reader.go"},
	{"Span Eval", "traceql_compiler.go"},
	{"Results", "api.go"},
}

// stageDetails returns the expanded detail block for each pipeline stage.
func stageDetails(stage int) []string {
	switch stage {
	case -1: // overview
		return []string{
			"",
			gray + "  BlockPack executes TraceQL queries" + reset,
			gray + "  against columnar span storage." + reset,
			gray + "  Blocks are pruned at each stage before" + reset,
			gray + "  evaluating predicates per span." + reset,
			"",
			gray + "  Invariant: ONE I/O per block." + reset,
			gray + "  Column filtering is always in-memory." + reset,
		}
	case 0: // Parser
		return []string{
			"",
			cyan + "  ParseTraceQL" + reset + gray + "(query string)" + reset,
			gray + "  → (*ParsedPipeline, error)" + reset,
			"",
			"  " + dim + "1." + reset + " Tokenize query string",
			"  " + dim + "2." + reset + " Build AST (SpansetFilter nodes)",
			"  " + dim + "3." + reset + " Identify: filter / metrics / structural",
			"",
			"  Output → " + green + "ParsedPipeline" + reset + "{",
			"    " + cyan + "Filter" + reset + ":      SpansetFilter",
			"    " + cyan + "Expressions" + reset + ": []AttributeCondition",
			"  }",
		}
	case 1: // VM Compiler
		return []string{
			"",
			cyan + "  CompileTraceQLFilter" + reset + gray + "(pipeline)" + reset,
			gray + "  → (*Program, error)" + reset,
			"",
			"  " + dim + "1." + reset + " Walk AST, emit bytecode",
			"  " + dim + "2." + reset + " Build " + yellow + "ColumnPredicate" + reset + " closure",
			"  " + dim + "3." + reset + " Build " + yellow + "RangeNode" + reset + " tree (pruning hints)",
			"",
			"  Output → " + green + "Program" + reset + "{",
			"    " + cyan + "ColumnPredicate" + reset + ": func(col) bool",
			"    " + cyan + "RangeNodes" + reset + ":     []RangeNode",
			"  }",
		}
	case 2: // Predicate Builder
		return []string{
			"",
			cyan + "  BuildPredicates" + reset + gray + "(prog *Program)" + reset,
			gray + "  → *QueryPredicates" + reset,
			"",
			"  " + dim + "1." + reset + " Walk RangeNode tree",
			"  " + dim + "2." + reset + " " + cyan + "normalizeAttributePath" + reset + "()",
			"       maps TraceQL syntax → column names",
			"  " + dim + "3." + reset + " Wire-encode value (encodeValue)",
			"",
			"  Output → " + green + "QueryPredicates" + reset + "{",
			"    " + cyan + "Ranges" + reset + ":    []planner.Predicate",
			"    " + cyan + "BloomKeys" + reset + ": []string",
			"  }",
		}
	case 3: // Planner: Time Range
		return []string{
			"",
			cyan + "  Plan" + reset + gray + "(blocks []BlockMeta, preds)" + reset,
			gray + "  → []BlockMeta" + reset,
			"",
			"  Stage 1/4 — " + yellow + "Time-range reject" + reset,
			"",
			"  Blocks carry [MinNano, MaxNano].",
			"  Non-overlapping blocks are dropped.",
			"",
			"  " + red + "✗ [blk-001][blk-002]" + reset + gray + " — outside window" + reset,
			"  " + green + "✓ [blk-003][blk-004][blk-005][blk-006]" + reset,
		}
	case 4: // Planner: Range Index
		return []string{
			"",
			cyan + "  pruneByIndex" + reset + gray + "(blocks, preds)" + reset,
			gray + "  → []BlockMeta" + reset,
			"",
			"  Stage 2/4 — " + yellow + "KLL Range Index" + reset,
			"",
			"  Each block: KLL sketch min/max per col.",
			"  Drop if [min,max] can't overlap range.",
			"",
			"  " + red + "✗ [blk-004]" + reset + gray + " — duration out of range" + reset,
			"  " + green + "✓ [blk-003][blk-005][blk-006]" + reset,
		}
	case 5: // Planner: Bloom Filter
		return []string{
			"",
			cyan + "  pruneByBloom" + reset + gray + "(blocks, preds)" + reset,
			gray + "  → []BlockMeta" + reset,
			"",
			"  Stage 3/4 — " + yellow + "BinaryFuse8 Bloom" + reset,
			"",
			"  Per-column BinaryFuse8 filter.",
			"  Drop if filter rejects predicate value.",
			"",
			"  " + red + "✗ [blk-005]" + reset + gray + " — not in bloom" + reset,
			"  " + green + "✓ [blk-003][blk-006]" + reset + "  (67% pruned)",
		}
	case 6: // Executor
		return []string{
			"",
			cyan + "  Collect" + reset + gray + "(ctx, preds, fn)" + reset,
			"",
			"  " + dim + "A." + reset + " " + yellow + "Intrinsic fast-path" + reset,
			"       label/service → zero I/O",
			"  " + dim + "B." + reset + " " + yellow + "Full block scan" + reset,
			"       fetch → decode → evaluate",
			"",
			"  → " + green + "path B" + reset + " (duration requires decode)",
			"  Blocks: " + cyan + "[blk-003]" + reset + " → " + cyan + "[blk-006]" + reset,
		}
	case 7: // Block I/O
		return []string{
			"",
			cyan + "  GetBlockWithBytes" + reset + gray + "(ctx, id)" + reset,
			"",
			bold + yellow + "  ONE I/O per block — always." + reset,
			"",
			"  Object storage: 50-100ms latency.",
			"  Per-col reads caused 10-120x API calls",
			"  — removed.",
			"",
			"  " + green + "io_ops < 500" + reset + "  |  " + green + "bytes/io > 100KB" + reset,
			"  → " + cyan + "parseBlockColumnsReuse" + reset + "() in-memory",
		}
	case 8: // Span Evaluation
		return []string{
			"",
			cyan + "  ColumnPredicate" + reset + gray + "(col *Column) bool" + reset,
			gray + "  ← closure compiled by VM" + reset,
			"",
			"  " + dim + "1." + reset + " " + cyan + "parseBlockColumnsReuse" + reset + "()",
			"       lazy-decode only wantColumns",
			"  " + dim + "2." + reset + " Invoke closure per span row",
			"  " + dim + "3." + reset + " " + yellow + "status_code==200" + reset + " AND " + yellow + "duration>5ms" + reset,
			"  " + dim + "4." + reset + " Collect SpanMatch structs",
			"",
			"  Lazy: unneeded cols never decoded.",
		}
	case 9: // Results
		return []string{
			"",
			cyan + "  SpanMatchCallback" + reset + gray + "(match SpanMatch) bool" + reset,
			"",
			"  Spans delivered via callback as found.",
			"  Return " + yellow + "false" + reset + " to stop early.",
			"",
			"  " + green + "BlockpackResult" + reset + "{",
			"    " + cyan + "Traces" + reset + ":  []TraceMatch",
			"    " + cyan + "Metrics" + reset + ": []MetricSample",
			"  }",
			"",
			bold + green + "  ✓ Query complete." + reset,
		}
	}
	return nil
}

// divider returns a full-width horizontal rule.
func divider() string {
	return gray + strings.Repeat("─", termWidth) + reset + "\n"
}

// visibleLen returns the printable column width of s (excluding ANSI escape codes).
// It iterates over runes so that multi-byte Unicode glyphs each count as one column.
func visibleLen(s string) int {
	n := 0
	inEscape := false
	for _, r := range s {
		if inEscape {
			if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
				inEscape = false
			}
			continue
		}
		if r == '\033' {
			inEscape = true
			continue
		}
		n++
	}
	return n
}

// buildFrame renders a complete terminal frame for the given active stage.
// Pass active=-1 for the overview frame.
func buildFrame(active int) string {
	var b strings.Builder

	// ── header ──────────────────────────────────────────────────────────────
	b.WriteString(divider())
	title := bold + white + " BlockPack — TraceQL Query Flow" + reset
	right := gray + "blockpack" + reset
	gap := max(1, termWidth-visibleLen(" BlockPack — TraceQL Query Flow")-visibleLen("blockpack"))
	b.WriteString(title + strings.Repeat(" ", gap) + right + "\n")
	b.WriteString(divider())

	// ── query ────────────────────────────────────────────────────────────────
	b.WriteString(gray + " Query: " + reset + yellow + bold +
		`{ span.http.status_code = 200 && duration > 5ms }` + reset + "\n")
	b.WriteString(divider())

	// ── pipeline stages ───────────────────────────────────────────────────────
	for i, s := range stages {
		var line string
		if i == active {
			arrow := green + bold + " ══► " + reset
			label := green + bold + fmt.Sprintf("[%d] %-14s", i+1, strings.ToUpper(s.label)) + reset
			file := cyan + italic + " " + s.file + reset
			line = arrow + label + file
		} else if i < active {
			arrow := dim + "  ✓   " + reset
			label := dim + fmt.Sprintf("[%d] %-14s", i+1, s.label) + reset
			file := dim + gray + " " + s.file + reset
			line = arrow + label + file
		} else {
			arrow := gray + "  ─── "
			label := gray + fmt.Sprintf("[%d] %-14s", i+1, s.label)
			file := gray + " " + s.file + reset
			line = arrow + label + file
		}
		b.WriteString(line + "\n")
	}
	b.WriteString(divider())

	// ── detail pane ──────────────────────────────────────────────────────────
	details := stageDetails(active)
	const detailHeight = 14
	for i := range detailHeight {
		if i < len(details) {
			b.WriteString(details[i] + "\n")
		} else {
			b.WriteString("\n")
		}
	}

	return b.String()
}

// castHeader writes the asciicast v2 header to w.
func castHeader(w *os.File) {
	h := map[string]any{
		"version": 2,
		"width":   termWidth,
		"height":  termHeight,
		"title":   "BlockPack: TraceQL Query Execution Flow",
		"env": map[string]string{
			"TERM":  "xterm-256color",
			"SHELL": "/bin/bash",
		},
	}
	enc, _ := json.Marshal(h)
	_, _ = fmt.Fprintln(w, string(enc))
}

// castEvent writes a single output event at the given timestamp.
func castEvent(w *os.File, t float64, data string) {
	enc, _ := json.Marshal([]any{t, "o", data})
	_, _ = fmt.Fprintln(w, string(enc))
}

func main() {
	w := os.Stdout

	castHeader(w)

	t := 0.0
	hold := 2.5  // seconds to hold each stage frame
	intro := 3.0 // hold for intro / overview frame

	// Overview frame.
	castEvent(w, t, clearScr+buildFrame(-1))
	t += intro

	// Step through each stage.
	for i := range stages { //nolint:intrange
		castEvent(w, t, clearScr+buildFrame(i))
		t += hold
	}

	// Final pause on results frame (already shown — just extend hold).
	castEvent(w, t, "")
}
