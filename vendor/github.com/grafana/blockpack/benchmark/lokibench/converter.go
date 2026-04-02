package lokibench

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/grafana/blockpack"
	"github.com/grafana/blockpack/internal/logqlparser"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_queryplanner "github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	lokilog "github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// LokiConverter bridges blockpack's StreamLogQL API to Loki's logql.Querier
// interface. It delegates block selection, I/O, and label/line-filter
// evaluation to the shared executor pipeline, then applies Loki-specific
// Pipeline/SampleExtractor evaluation per matching row.
type LokiConverter struct {
	reader *blockpack.Reader
	// linesProcessed counts log entries returned by SelectLogs and log rows
	// consumed by SelectSamples across all calls since the last ResetLines call.
	linesProcessed atomic.Int64
	// lastStats holds the most recent query's block selection statistics.
	// Updated atomically on every SelectLogs and SelectSamples call.
	lastStats atomic.Pointer[blockpack.QueryStats]
	// forceFullPipeline disables the fast path that bypasses sp.Process for
	// fullyPushed queries. Used by TestSyntheticDeepCompare which validates
	// entry metadata (StructuredMetadata/Parsed) against the chunk store.
	forceFullPipeline bool
}

// LinesProcessed returns the total lines processed since the last ResetLines call.
func (q *LokiConverter) LinesProcessed() int64 { return q.linesProcessed.Load() }

// ResetLines resets the lines-processed counter to zero.
func (q *LokiConverter) ResetLines() { q.linesProcessed.Store(0) }

// LastStats returns the block selection statistics from the most recent query,
// or nil if no query has been executed yet.
func (q *LokiConverter) LastStats() *blockpack.QueryStats {
	return q.lastStats.Load()
}

// NewLokiConverter creates a LokiConverter bridging the given blockpack Reader
// to Loki's logql.Querier interface.
func NewLokiConverter(r *blockpack.Reader) *LokiConverter {
	return &LokiConverter{reader: r}
}

// WithParallelism is a no-op retained for API compatibility. Parallelism is
// now handled by the shared executor pipeline.
func (q *LokiConverter) WithParallelism(_ int) *LokiConverter {
	return q
}

// SelectLogs implements logql.Querier.
//
// It builds a LogQL selector query from the label matchers, delegates block
// selection and label matching to blockpack.StreamLogQL, then applies Loki's
// Pipeline per matching row for line filters, parsers, and label filters.
func (q *LokiConverter) SelectLogs(
	ctx context.Context,
	params logql.SelectLogParams,
) (iter.EntryIterator, error) {
	if err := ctx.Err(); err != nil {
		return iter.NoopEntryIterator, err
	}

	logSel, err := params.LogSelector()
	if err != nil {
		return iter.NoopEntryIterator, nil
	}

	pipeline, err := logSel.Pipeline()
	if err != nil {
		return iter.NoopEntryIterator, nil
	}

	start := params.GetStart()
	end := params.GetEnd()
	direction := params.Direction

	startNano := uint64(start.UnixNano()) //nolint:gosec
	endNano := uint64(end.UnixNano())     //nolint:gosec

	// Parse and compile the pushed-down query for blockpack's internal executor.
	// CollectLogs is called directly to skip the SpanMatch/logEntryFields layer.
	// NOTE-014: buildPushdownQuery re-emits label filter stages as native predicates
	// so the executor evaluates them via column scans and block-level pruning.
	pqQuery, fullyPushed := buildPushdownQuery(logSel.String())
	bpSel, bpParseErr := logqlparser.Parse(pqQuery)
	if bpParseErr != nil {
		return nil, fmt.Errorf("LokiConverter.SelectLogs: parse pushed-down query %q: %w", pqQuery, bpParseErr)
	}
	program, bpPipeline, bpCompileErr := logqlparser.CompileAll(bpSel)
	if bpCompileErr != nil {
		return nil, fmt.Errorf("LokiConverter.SelectLogs: compile pushed-down query %q: %w", pqQuery, bpCompileErr)
	}

	collectDir := modules_queryplanner.Forward
	if direction == logproto.BACKWARD {
		collectDir = modules_queryplanner.Backward
	}
	collectOpts := modules_executor.CollectOptions{
		TimeRange: modules_queryplanner.TimeRange{MinNano: startNano, MaxNano: endNano},
		Direction: collectDir,
	}

	if ctx.Err() != nil {
		return iter.NoopEntryIterator, ctx.Err()
	}
	entries, qs, err := modules_executor.CollectLogs(q.reader, program, bpPipeline, collectOpts)
	q.lastStats.Store(&qs)
	if err != nil {
		return nil, fmt.Errorf("LokiConverter.SelectLogs: %w", err)
	}

	streamEntries := make(map[string][]logproto.Entry)

	if fullyPushed && !q.forceFullPipeline {
		// Fast path: all filtering was handled by blockpack's executor (stream matchers,
		// line filters, label filters pushed as column predicates, parser stages transparent).
		// Every returned entry is guaranteed to match. Skip the entire Loki pipeline:
		// no syntax.ParseLabels, no pipeline.ForStream, no sp.Process, no lbs.String().
		// Entry metadata uses blockpack's pre-parsed log.* columns as StructuredMetadata.
		for _, entry := range entries {
			if ctx.Err() != nil {
				break
			}
			labelsStr := entry.LokiLabels
			streamEntries[labelsStr] = append(streamEntries[labelsStr], logproto.Entry{
				Timestamp:          time.Unix(0, int64(entry.TimestampNanos)), //nolint:gosec
				Line:               entry.Line,
				StructuredMetadata: logAttrsToLabelAdapters(entry.LogAttrs),
			})
		}
	} else {
		// Slow path: complex pipeline stages (negation filters, drop/keep, label_format,
		// line_format) require the Loki pipeline for authoritative evaluation.
		streamPipelineCache := make(map[string]lokilog.StreamPipeline, 4)
		for _, entry := range entries {
			if ctx.Err() != nil {
				break
			}

			labelsStr := entry.LokiLabels
			sp, cached := streamPipelineCache[labelsStr]
			if !cached {
				streamLbls, parseErr := syntax.ParseLabels(labelsStr)
				if parseErr != nil {
					continue
				}
				sp = pipeline.ForStream(streamLbls)
				streamPipelineCache[labelsStr] = sp
			}

			smLabels := logproto.FromLabelAdaptersToLabels(logAttrsToLabelAdapters(entry.LogAttrs))
			newLine, lbs, matched := sp.Process(int64(entry.TimestampNanos), []byte(entry.Line), smLabels) //nolint:gosec
			if !matched {
				continue
			}

			combinedKey := lbs.String()
			streamEntries[combinedKey] = append(streamEntries[combinedKey], logproto.Entry{
				Timestamp:          time.Unix(0, int64(entry.TimestampNanos)), //nolint:gosec
				Line:               string(newLine),
				StructuredMetadata: logproto.FromLabelsToLabelAdapters(lbs.StructuredMetadata()),
				Parsed:             logproto.FromLabelsToLabelAdapters(lbs.Parsed()),
			})
		}
	}

	var totalLines int64
	its := make([]iter.EntryIterator, 0, len(streamEntries))
	for labelsStr, streamEnts := range streamEntries {
		totalLines += int64(len(streamEnts))
		// CollectLogs returns entries in timestamp order; no per-stream sort needed.
		its = append(its, newSliceEntryIterator(labelsStr, xxhash.Sum64String(labelsStr), streamEnts))
	}
	q.linesProcessed.Add(totalLines)

	return iter.NewSortEntryIterator(its, direction), nil
}

// SelectSamples implements logql.Querier. It uses SampleExtractor from the
// sample expression to correctly evaluate unwrap and count-based queries.
func (q *LokiConverter) SelectSamples(
	ctx context.Context,
	params logql.SelectSampleParams,
) (iter.SampleIterator, error) {
	if err := ctx.Err(); err != nil {
		return iter.NoopSampleIterator, err
	}

	expr, err := params.Expr()
	if err != nil {
		return iter.NoopSampleIterator, nil
	}
	extractors, err := expr.Extractors()
	if err != nil {
		return iter.NoopSampleIterator, nil
	}
	if len(extractors) == 0 {
		return iter.NoopSampleIterator, nil
	}
	extractor := extractors[0]

	logSel, err := expr.Selector()
	if err != nil {
		return iter.NoopSampleIterator, nil
	}

	start := params.GetStart()
	end := params.GetEnd()
	startNano := uint64(start.UnixNano()) //nolint:gosec
	endNano := uint64(end.UnixNano())     //nolint:gosec

	// Parse and compile for direct CollectLogs call — see SelectLogs.
	// fullyPushed is ignored for SelectSamples: unwrap is a barrier stage so
	// fullyPushed will always be false for metric queries.
	pqQuery, _ := buildPushdownQuery(logSel.String())
	bpSel, bpParseErr := logqlparser.Parse(pqQuery)
	if bpParseErr != nil {
		return nil, fmt.Errorf("LokiConverter.SelectSamples: parse pushed-down query %q: %w", pqQuery, bpParseErr)
	}
	program, bpPipeline, bpCompileErr := logqlparser.CompileAll(bpSel)
	if bpCompileErr != nil {
		return nil, fmt.Errorf("LokiConverter.SelectSamples: compile pushed-down query %q: %w", pqQuery, bpCompileErr)
	}

	collectOpts := modules_executor.CollectOptions{
		TimeRange: modules_queryplanner.TimeRange{MinNano: startNano, MaxNano: endNano},
	}

	if ctx.Err() != nil {
		return iter.NoopSampleIterator, ctx.Err()
	}
	entries, qs, err := modules_executor.CollectLogs(q.reader, program, bpPipeline, collectOpts)
	q.lastStats.Store(&qs)
	if err != nil {
		return nil, fmt.Errorf("LokiConverter.SelectSamples: %w", err)
	}

	streamExtractorCache := make(map[string]lokilog.StreamSampleExtractor, 4)
	streamSamples := make(map[string][]logproto.Sample)

	for _, entry := range entries {
		if ctx.Err() != nil {
			break
		}

		labelsStr := entry.LokiLabels
		se, cached := streamExtractorCache[labelsStr]
		if !cached {
			streamLbls, parseErr := syntax.ParseLabels(labelsStr)
			if parseErr != nil {
				continue
			}
			se = extractor.ForStream(streamLbls)
			streamExtractorCache[labelsStr] = se
		}

		smLabels := logproto.FromLabelAdaptersToLabels(logAttrsToLabelAdapters(entry.LogAttrs))
		extracted, matched := se.Process(int64(entry.TimestampNanos), []byte(entry.Line), smLabels) //nolint:gosec
		if !matched {
			continue
		}
		for _, es := range extracted {
			key := es.Labels.String()
			streamSamples[key] = append(streamSamples[key], logproto.Sample{
				Timestamp: int64(entry.TimestampNanos), //nolint:gosec
				Value:     es.Value,
				Hash:      xxhash.Sum64String(entry.Line),
			})
		}
	}

	var totalSamples int64
	its := make([]logproto.Series, 0, len(streamSamples))
	for labelsStr, samples := range streamSamples {
		totalSamples += int64(len(samples))
		// CollectLogs returns entries in timestamp order; no per-series sort needed.
		its = append(its, logproto.Series{
			Labels:     labelsStr,
			Samples:    samples,
			StreamHash: xxhash.Sum64String(labelsStr),
		})
	}
	q.linesProcessed.Add(totalSamples)
	return iter.NewMultiSeriesIterator(its), nil
}

// extractLogFields reads common log fields from a SpanFieldsProvider.
func extractLogFields(fields blockpack.SpanFieldsProvider) (ts uint64, body, labelsStr string) {
	if v, ok := fields.GetField("log:timestamp"); ok {
		if u, ok := v.(uint64); ok {
			ts = u
		}
	}
	if v, ok := fields.GetField("log:body"); ok {
		if s, ok := v.(string); ok {
			body = s
		}
	}
	if v, ok := fields.GetField("resource.__loki_labels__"); ok {
		if s, ok := v.(string); ok {
			labelsStr = s
		}
	}
	return ts, body, labelsStr
}

// extractStructuredMetadata reads log.* fields from a SpanFieldsProvider and
// returns them as LabelAdapter pairs sorted alphabetically by name.
//
// NOTE: This iterates all block columns per row via IterateFields. A caching
// approach (discover log.* names once, then GetField per row) would be faster
// but is unsafe because IterateFields only returns columns present at the
// current row — sparse columns would be missed on the discovery row.
func extractStructuredMetadata(fields blockpack.SpanFieldsProvider) []logproto.LabelAdapter {
	var sm []logproto.LabelAdapter
	fields.IterateFields(func(name string, value any) bool {
		if strings.HasPrefix(name, "log.") {
			if val, ok := value.(string); ok && val != "" {
				sm = append(sm, logproto.LabelAdapter{Name: name[4:], Value: val})
			}
		}
		return true
	})
	slices.SortFunc(sm, func(a, b logproto.LabelAdapter) int { return cmp.Compare(a.Name, b.Name) })
	return sm
}

// logAttrsToLabelAdapters converts LogEntry.LogAttrs to a sorted []logproto.LabelAdapter.
// LogAttrs names carry the "log." prefix (e.g. "log.level"); that prefix is stripped.
// Returns nil when logAttrs is empty, avoiding allocation for the common case.
func logAttrsToLabelAdapters(logAttrs modules_executor.LogAttrs) []logproto.LabelAdapter {
	if logAttrs.Len() == 0 {
		return nil
	}
	la := make([]logproto.LabelAdapter, 0, logAttrs.Len())
	for i, name := range logAttrs.Names {
		la = append(la, logproto.LabelAdapter{Name: name[4:], Value: logAttrs.Values[i]}) // strip "log."
	}
	slices.SortFunc(la, func(a, b logproto.LabelAdapter) int { return cmp.Compare(a.Name, b.Name) })
	return la
}

// buildPushdownQuery parses a LogQL query and reconstructs it with stream
// matchers, line filters, and pushed-down label filter stages. Returns both the
// pushed-down query string and whether ALL pipeline stages were handled
// (fullyPushed=true means the Loki pipeline can be skipped entirely).
//
// Pipeline stage handling:
//   - logfmt, json: dropped (transparent — native log.* columns exist)
//   - label filters (=, =~, >, >=, <, <=): pushed as native column predicates
//   - negation filters (!=, !~): pushed as row-level predicates UNLESS preceded
//     by drop/keep (a dropped label is absent in Loki, so negation matches all
//     rows; pushing would cause false negatives)
//   - drop, keep: skipped (don't modify surviving label values); subsequent
//     positive filters are still pushed, but fullyPushed is set to false
//   - label_format, line_format, unwrap: true barriers — stop pushdown
//
// fullyPushed is false when: any barrier stage hit, any drop/keep seen,
// any label filter appears without a preceding parser stage (body-parsed
// column may over-match), or parsing fails.
//
// Falls back to the original query string (fullyPushed=false) if parsing fails.
func buildPushdownQuery(query string) (pushed string, fullyPushed bool) {
	sel, err := logqlparser.Parse(query)
	if err != nil {
		return query, false
	}
	var sb strings.Builder
	writeMatchersAndLineFilters(&sb, sel)
	fullyPushed = true
	seenParser := false
	seenDropKeep := false
	for _, stage := range sel.Pipeline {
		switch stage.Type {
		case logqlparser.StageLogfmt, logqlparser.StageJSON:
			// Transparent: native log.* columns exist without parsing; drop stage.
			seenParser = true
			continue
		case logqlparser.StageDrop, logqlparser.StageKeep:
			// Drop/keep remove labels but don't modify values of surviving labels.
			// Safe to look past for POSITIVE filters on surviving labels.
			// Negation filters after drop/keep are unsafe: a dropped label is absent
			// in Loki (negation matches all rows), but blockpack still has the column
			// value and would incorrectly exclude rows (false negatives).
			fullyPushed = false
			seenDropKeep = true
			continue
		case logqlparser.StageLabelFilter:
			if stage.LabelFilter == nil {
				fullyPushed = false
				continue
			}
			// Without a preceding parser (logfmt/json), label filters only match
			// stream labels in Loki, but blockpack's column resolver also checks
			// log.* body-parsed columns — causing false positives. Still push the
			// predicate for block-level pruning, but require sp.Process for
			// authoritative row-level evaluation.
			if !seenParser {
				fullyPushed = false
			}
			// After drop/keep, negation filters are unsafe to push (see above).
			isNegation := stage.LabelFilter.Op == logqlparser.OpNotEqual ||
				stage.LabelFilter.Op == logqlparser.OpNotRegex
			if seenDropKeep && isNegation {
				fullyPushed = false
				continue // don't push — Loki pipeline handles it
			}
			frag := labelFilterFragment(stage.LabelFilter)
			if frag == "" {
				fullyPushed = false
				continue
			}
			sb.WriteString(" | ")
			sb.WriteString(frag)
			for _, olf := range stage.OrFilters {
				if olfrag := labelFilterFragment(olf); olfrag != "" {
					sb.WriteString(" or ")
					sb.WriteString(olfrag)
				}
			}
		default:
			// True barrier: label_format, line_format, unwrap — these modify
			// values or lines, unsafe to push predicates past them.
			return sb.String(), false
		}
	}
	return sb.String(), fullyPushed
}

// labelFilterFragment returns the LogQL fragment for a single LabelFilter,
// or "" if the filter should be skipped (negations are unsafe for block-level
// bloom pruning; a block without the column satisfies a negation).
func labelFilterFragment(lf *logqlparser.LabelFilter) string {
	switch lf.Op {
	case logqlparser.OpEqual:
		return lf.Name + `="` + lf.Value + `"`
	case logqlparser.OpRegex:
		return lf.Name + `=~"` + lf.Value + `"`
	case logqlparser.OpNotEqual:
		// Row-level ScanNotEqual handles negation correctly (rows where column
		// is absent are included). Block-level bloom pruning is not affected —
		// the planner only uses positive predicates for bloom/fuse checks.
		return lf.Name + `!="` + lf.Value + `"`
	case logqlparser.OpNotRegex:
		return lf.Name + `!~"` + lf.Value + `"`
	case logqlparser.OpGT:
		return lf.Name + " > " + lf.Value
	case logqlparser.OpGTE:
		return lf.Name + " >= " + lf.Value
	case logqlparser.OpLT:
		return lf.Name + " < " + lf.Value
	case logqlparser.OpLTE:
		return lf.Name + " <= " + lf.Value
	}
	return ""
}

// writeMatchersAndLineFilters writes the stream selector and line filters
// of sel into sb, without any pipeline stages.
func writeMatchersAndLineFilters(sb *strings.Builder, sel *logqlparser.LogSelector) {
	sb.WriteString("{")
	for i, m := range sel.Matchers {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(m.Name)
		switch m.Type {
		case logqlparser.MatchEqual:
			sb.WriteString("=")
		case logqlparser.MatchNotEqual:
			sb.WriteString("!=")
		case logqlparser.MatchRegex:
			sb.WriteString("=~")
		case logqlparser.MatchNotRegex:
			sb.WriteString("!~")
		}
		sb.WriteString(`"`)
		sb.WriteString(m.Value)
		sb.WriteString(`"`)
	}
	sb.WriteString("}")
	for _, lf := range sel.LineFilters {
		switch lf.Type {
		case logqlparser.FilterContains:
			sb.WriteString(` |= "`)
		case logqlparser.FilterNotContains:
			sb.WriteString(` != "`)
		case logqlparser.FilterRegex:
			sb.WriteString(` |~ "`)
		case logqlparser.FilterNotRegex:
			sb.WriteString(` !~ "`)
		}
		sb.WriteString(lf.Pattern)
		sb.WriteString(`"`)
	}
}

// openBlockpackReader reads a blockpack file entirely into memory and returns a Reader.
func openBlockpackReader(path string) (*blockpack.Reader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("openBlockpackReader: %w", err)
	}
	return blockpack.NewReaderFromProvider(&bytesProvider{data: data})
}

// bytesProvider implements blockpack.ReaderProvider over an in-memory byte slice.
type bytesProvider struct {
	data []byte
}

func (p *bytesProvider) Size() (int64, error) { return int64(len(p.data)), nil }

func (p *bytesProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(buf, p.data[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

// latencyProvider wraps a ReaderProvider and injects per-read latency to
// simulate object storage round-trips. Every ReadAt call is delayed — in object
// storage each read is a separate HTTP request regardless of data type.
// It also tracks total bytes read and I/O count for benchmark reporting.
type latencyProvider struct {
	inner     blockpack.ReaderProvider
	latency   time.Duration
	bytesRead atomic.Int64
	ioCount   atomic.Int64
}

func (p *latencyProvider) Size() (int64, error) { return p.inner.Size() }

func (p *latencyProvider) ReadAt(buf []byte, off int64, dt blockpack.DataType) (int, error) {
	if p.latency > 0 {
		time.Sleep(p.latency)
	}
	n, err := p.inner.ReadAt(buf, off, dt)
	p.bytesRead.Add(int64(n))
	p.ioCount.Add(1)
	return n, err
}

// BytesRead returns total bytes read since creation (or last Reset).
func (p *latencyProvider) BytesRead() int64 { return p.bytesRead.Load() }

// IOCount returns total ReadAt calls since creation (or last Reset).
func (p *latencyProvider) IOCount() int64 { return p.ioCount.Load() }

// Reset zeroes the bytes-read and IO counters.
func (p *latencyProvider) Reset() {
	p.bytesRead.Store(0)
	p.ioCount.Store(0)
}

// openBlockpackReaderWithLatency reads a blockpack file into memory and returns
// a Reader whose block reads are delayed by latency, plus the tracking provider
// for querying bytes read and I/O count. This models object storage (S3/GCS) at
// the I/O layer rather than in the querier.
func openBlockpackReaderWithLatency(path string, latency time.Duration) (*blockpack.Reader, *latencyProvider, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("openBlockpackReaderWithLatency: %w", err)
	}
	lp := &latencyProvider{inner: &bytesProvider{data: data}, latency: latency}
	r, err := blockpack.NewReaderFromProvider(lp)
	if err != nil {
		return nil, nil, fmt.Errorf("openBlockpackReaderWithLatency: %w", err)
	}
	return r, lp, nil
}
