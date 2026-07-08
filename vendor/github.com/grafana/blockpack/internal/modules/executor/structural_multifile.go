package executor

// structural_multifile.go — Option A multi-file trace materialization (plan-d.md D3, issue #489).
//
// Relocated here from root package blockpack per team-lead's D3B checkpoint ruling (2026-07-07):
// D3B/D4/D6 all live in internal/modules/executor and need ResolvedSpan/StructuralReaderProvider
// directly (single-source-of-truth requirement); an unexported root-package type is unreachable
// from here, and executor importing root back would be an import cycle (root already imports
// executor). See structural_traceresolve.go's package doc comment for the sibling ruling on
// ResolveTraceGroupSourceRef.
//
// A structural query's trace commonly spans multiple compaction-boundary files (team-lead ruling
// 4: this is the COMMON case, not an edge case). MaterializeTraceGroupMultiFile resolves EVERY
// distinct SourceRef in a TraceGroup against its own reader (via the caller-supplied
// StructuralReaderProvider) with bounded concurrency, delegating each file's own resolve/skew
// detection to ResolveTraceGroupSourceRef — the SAME primitive root's materializeTraceGroup now
// delegates to — so skew semantics never drift between GetTraceByID's single-file path and this
// multi-file path.

import (
	"context"
	"errors"
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/valueindex"
	"golang.org/x/sync/errgroup"
)

// ErrStructuralMultiFileCoverageGap indicates a SourceRef referenced by a structural query's
// TraceGroup could not be opened for reading via the caller-supplied StructuralReaderProvider — a
// genuine multi-file coverage gap (team-lead ruling 4: "typed error ONLY for genuinely missing/
// unreadable SourceRefs"), never a silent drop that would produce a wrong or incomplete
// structural-query answer. Distinct from ResolveTraceGroupSourceRef's own index/data skew errors
// (a SourceRef that DOES open but whose named block/row doesn't check out) — those propagate
// unchanged per-file below and are never wrapped in this sentinel.
var ErrStructuralMultiFileCoverageGap = errors.New("executor: structural multi-file source ref unreadable")

// StructuralReaderProvider resolves a SourceRef (the S3 object key stamped on each SpanEntry,
// NOTE-VI-076) to an already-open *modules_reader.Reader for that file. The caller (tempo's
// querier, via root's D5 QueryStructuralFromIndex) owns reader caching/pooling — Option A
// (team-lead ruling, 2026-07-07) reuses whatever cache the caller already maintains for other
// block reads rather than building a second one here. Root's Reader is a type alias for
// modules_reader.Reader, so root callers pass their own *blockpack.Reader values here unchanged.
type StructuralReaderProvider func(ctx context.Context, sourceRef string) (*modules_reader.Reader, error)

// ResolvedSpan is one SpanEntry resolved to its own SourceRef's reader plus exact block/row — the
// per-span "which reader resolved this" record both MaterializeTraceGroupMultiFile and D3B's
// candidate-verification step (targeted row reads) consume. Defined once here so D3B imports/
// reuses this shape rather than re-deriving its own for the same concept (single-source-of-truth,
// plan-d.md process learnings).
type ResolvedSpan struct {
	Reader    *modules_reader.Reader
	SourceRef string
	Span      valueindex.SpanEntry
	BlockIdx  int
	RowIdx    uint16
}

// defaultMaxConcurrentReaderOpens bounds StructuralReaderProvider fan-out when a caller passes
// maxConcurrentReaderOpens <= 0.
const defaultMaxConcurrentReaderOpens = 8

// MaterializeTraceGroupMultiFile resolves every SpanEntry in group against its OWN SourceRef's
// reader (via readerFor), unlike root's materializeTraceGroup single-reader, sourceRef-FILTER
// behavior (which drops sibling-file spans — correct for, and left completely unchanged for,
// GetTraceByID's own per-block parallel-call pattern). Distinct SourceRefs are resolved with
// bounded concurrency (maxConcurrentReaderOpens, clamped to defaultMaxConcurrentReaderOpens when
// <= 0).
//
// A SourceRef readerFor fails to open is a genuine coverage gap
// (ErrStructuralMultiFileCoverageGap) — never a silent drop. A SourceRef that DOES open but whose
// claimed spans fail to resolve is index/data skew, reported via ResolveTraceGroupSourceRef's own
// existing error (reused per-file, unchanged, so skew semantics never drift from GetTraceByID's
// own path). Either failure fails the WHOLE call — no partial/degraded result is ever returned.
func MaterializeTraceGroupMultiFile(
	ctx context.Context,
	readerFor StructuralReaderProvider,
	group valueindex.TraceGroup,
	traceID [16]byte,
	maxConcurrentReaderOpens int,
) ([]ResolvedSpan, error) {
	if len(group.Spans) == 0 {
		return nil, nil
	}
	if maxConcurrentReaderOpens <= 0 {
		maxConcurrentReaderOpens = defaultMaxConcurrentReaderOpens
	}

	refOrder := distinctSourceRefsInOrder(group.Spans)
	slots := make([][]ResolvedSpan, len(refOrder))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentReaderOpens)
	for i, ref := range refOrder {
		g.Go(func() (err error) {
			// SPEC-ROOT-001: goroutine panics must not crash the process. readerFor is a
			// caller-supplied callback (StructuralReaderProvider) this package does not control --
			// a bug in the caller's implementation must fail this one query, not take down the
			// whole host process. Mirrors the established pattern in
			// internal/modules/valueindexcompactor/service.go's own g.Go closure, except a
			// returned error (not a swallowed nil) is correct here since g.Wait()'s caller needs to
			// see the failure.
			defer func() {
				if rec := recover(); rec != nil {
					err = fmt.Errorf("structural multi-file resolve sourceRef %q: panic: %v", ref, rec)
				}
			}()
			reader, err := readerFor(gctx, ref)
			if err != nil {
				return fmt.Errorf("%w: sourceRef %q: %w", ErrStructuralMultiFileCoverageGap, ref, err)
			}
			if reader == nil {
				return fmt.Errorf("%w: sourceRef %q: readerFor returned a nil reader", ErrStructuralMultiFileCoverageGap, ref)
			}
			rows, rErr := ResolveTraceGroupSourceRef(reader, group, traceID, ref)
			if rErr != nil {
				return fmt.Errorf("structural multi-file resolve sourceRef %q: %w", ref, rErr)
			}
			out := make([]ResolvedSpan, 0, len(rows))
			for _, row := range rows {
				out = append(out, ResolvedSpan{
					Reader:    reader,
					SourceRef: ref,
					Span:      row.Span,
					BlockIdx:  row.BlockIdx,
					RowIdx:    uint16(row.RowIdx), //nolint:gosec // RowIdx originates from SpanEntry.RowIdx (uint16)
				})
			}
			slots[i] = out
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	var total int
	for _, s := range slots {
		total += len(s)
	}
	resolved := make([]ResolvedSpan, 0, total)
	for _, s := range slots {
		resolved = append(resolved, s...)
	}
	return resolved, nil
}

// distinctSourceRefsInOrder returns the distinct SourceRefs in spans, in first-seen order —
// driving MaterializeTraceGroupMultiFile's slot indexing so results are deterministic regardless
// of goroutine completion order.
func distinctSourceRefsInOrder(spans []valueindex.SpanEntry) []string {
	seen := make(map[string]struct{}, 4)
	refOrder := make([]string, 0, 4)
	for _, span := range spans {
		if _, ok := seen[span.SourceRef]; !ok {
			seen[span.SourceRef] = struct{}{}
			refOrder = append(refOrder, span.SourceRef)
		}
	}
	return refOrder
}
