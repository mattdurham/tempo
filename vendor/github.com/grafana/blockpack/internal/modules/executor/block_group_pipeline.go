package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"

	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// blockGroupReader is the minimal interface blockGroupPipeline requires from a Reader.
// *modules_reader.Reader satisfies this interface.
//
// SPEC-STREAM-11: ReadGroup must be safe for concurrent calls; BlockMeta is read-only.
type blockGroupReader interface {
	ReadGroup(cr modules_shared.CoalescedRead) (map[int][]byte, error)
	BlockMeta(blockIdx int) modules_shared.BlockMeta
}

// defaultPipelineWorkers is the number of concurrent ReadGroup goroutines.
// NOTE-058: W=8 is chosen for I/O latency hiding, not CPU utilization.
// T_io≈75ms (median S3), T_parse<5ms → optimal saturation W≈15; W=8 absorbs tail
// latency without unbounded memory. Peak in-memory: up to W groups (W-1 in pending
// reorder map + 1 being processed) × 8MB = 64MB for W=8.
// SPEC-STREAM-11: semaphore-gated dispatcher ensures at most W groups dispatched ahead
// of nextExpected, so pending reorder map holds at most W-1 out-of-order groups.
const defaultPipelineWorkers = 8

// groupResult carries the output of one ReadGroup call from a worker goroutine to the consumer.
type groupResult struct {
	err        error
	data       map[int][]byte
	groupIdx   int
	blockCount int
	byteCount  int64
}

// blockGroupPipeline dispatches ReadGroup calls concurrently across workerCount goroutines
// and feeds completed groups to a sequential parse+process goroutine via a bounded channel.
//
// NOTE-058: W-concurrent I/O, sequential parse, bounded memory. This is an I/O-bound pipeline;
// the optimal W hides S3 latency (~50-100ms) by keeping W groups in flight while parse processes
// the current group (~5ms). Peak in-memory: up to W groups (W-1 in pending reorder map +
// 1 being processed in processGroup). SPEC-STREAM-11: semaphore-gated, bounded in-memory groups.
//
// processGroup is called sequentially on the caller's goroutine. It must not be called
// concurrently (Reader.ParseBlockFromBytes is not goroutine-safe on the same *Reader).
//
// Returns (fetchedGroups, fetchedBlocks, bytesRead, error).
// If processGroup returns errLimitReached, blockGroupPipeline stops dispatch, drains
// in-flight reads, and returns (fetchedGroups, fetchedBlocks, bytesRead, nil).
// Any other non-nil error from processGroup is returned as-is.
// ReadGroup errors are propagated as-is.
//
// processGroup receives groups in groupIdx order (ascending), regardless of completion order.
// SPEC-STREAM-11: ordered delivery via pending reorder buffer.
func blockGroupPipeline(
	ctx context.Context,
	r blockGroupReader,
	groups []modules_shared.CoalescedRead,
	workerCount int,
	processGroup func(groupIdx int, groupRaw map[int][]byte) error,
) (fetchedGroups, fetchedBlocks int, bytesRead int64, err error) {
	if len(groups) == 0 {
		return 0, 0, 0, nil
	}
	// Edge case: non-positive workerCount treated as 1.
	if workerCount <= 0 {
		workerCount = 1
	}

	// Internal context: canceled on early exit (errLimitReached) or error.
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// jobs carries group indices to the I/O workers. Unbuffered so workers block until
	// the dispatcher has a job ready. The dispatcher stops on innerCtx cancellation.
	jobs := make(chan int)

	// results carries completed group data to the consumer.
	// Capacity = workerCount: at most workerCount completed groups buffered between
	// I/O workers and the consumer, providing backpressure. SPEC-STREAM-11: bounded memory.
	results := make(chan groupResult, workerCount)

	// sem bounds dispatcher advance: at most workerCount groups can be dispatched
	// but not yet processed by processGroup. Pre-filled with workerCount tokens.
	// CRIT-BGP-1 / SPEC-STREAM-11: token is acquired before dispatch and released
	// only AFTER processGroup returns, so peak in-memory groups ≤ W.
	sem := make(chan struct{}, workerCount)
	for range workerCount {
		sem <- struct{}{}
	}

	// Workers: pull group indices from jobs, call ReadGroup, push groupResult.
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			var currentGi int // captured by the recovery closure to report the correct group
			defer func() {
				if rec := recover(); rec != nil {
					// SPEC-ROOT-001: goroutine panics must not crash the process.
					slog.Error("blockGroupPipeline: worker goroutine panic",
						"panic", rec, "stack", string(debug.Stack()), "group", currentGi)
					results <- groupResult{
						groupIdx: currentGi,
						err:      fmt.Errorf("blockGroupPipeline: worker panic in group %d: %v", currentGi, rec),
					}
				}
			}()
			for gi := range jobs {
				currentGi = gi
				data, readErr := r.ReadGroup(groups[gi])
				gr := groupResult{groupIdx: gi, data: data, err: readErr}
				if readErr == nil {
					gr.blockCount = len(groups[gi].BlockIDs)
					for _, bi := range groups[gi].BlockIDs {
						gr.byteCount += int64(r.BlockMeta(bi).Length) //nolint:gosec
					}
				}
				results <- gr
			}
		}()
	}

	// Close results when all workers exit so the consumer's range loop terminates.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Dispatcher goroutine: feeds job indices to the workers.
	// Acquires a semaphore token before each dispatch to bound in-flight groups.
	// Stops early when innerCtx is canceled (early exit or error).
	// Closes jobs when done so workers drain and exit.
	go func() {
		defer close(jobs)
		for i := range len(groups) {
			// Acquire dispatch slot before sending to workers.
			// CRIT-BGP-1 / SPEC-STREAM-11: at most workerCount groups in-flight.
			select {
			case <-sem:
			case <-innerCtx.Done():
				return
			}
			select {
			case jobs <- i:
			case <-innerCtx.Done():
				sem <- struct{}{} // return token — dispatcher is exiting
				return
			}
		}
	}()

	// Consumer: drain results in groupIdx order, calling processGroup sequentially.
	// A pending map buffers out-of-order completions for ordered delivery.
	// SPEC-STREAM-11: processGroup called in ascending groupIdx order.
	pending := make(map[int]groupResult, workerCount)
	nextExpected := 0

	for gr := range results {
		if gr.err != nil {
			// ReadGroup failed: cancel dispatch + workers, drain channel, return error.
			// Token abandonment is safe here: cancel() causes the dispatcher goroutine to exit
			// via innerCtx.Done() before acquiring further tokens, so no goroutine blocks on <-sem.
			cancel()
			for range results { //nolint:revive // intentional drain to unblock workers
			}
			return fetchedGroups, fetchedBlocks, bytesRead,
				fmt.Errorf("ReadGroup group %d: %w", gr.groupIdx, gr.err)
		}

		fetchedGroups++
		fetchedBlocks += gr.blockCount
		bytesRead += gr.byteCount

		// Buffer the result and flush all consecutive ready groups.
		pending[gr.groupIdx] = gr
		for {
			ready, ok := pending[nextExpected]
			if !ok {
				break
			}
			delete(pending, nextExpected)
			nextExpected++

			// CRIT-BGP-1 / SPEC-STREAM-11: release dispatch slot only AFTER processGroup
			// returns, so the dispatcher cannot enqueue a new group while ready.data is
			// still held in memory by processGroup. This ensures peak in-memory groups ≤ W.
			procErr := processGroup(ready.groupIdx, ready.data)
			sem <- struct{}{} // token released after processGroup completes (or errors)

			if procErr != nil {
				cancel()
				for range results { //nolint:revive // intentional drain to unblock workers
				}
				if procErr == errLimitReached {
					return fetchedGroups, fetchedBlocks, bytesRead, ctx.Err()
				}
				return fetchedGroups, fetchedBlocks, bytesRead, procErr
			}
		}
	}

	// CRIT-BGP-2: if the outer context was canceled (not just inner errLimitReached),
	// return ctx.Err() so callers distinguish cancellation from normal completion.
	if ctx.Err() != nil {
		return fetchedGroups, fetchedBlocks, bytesRead, ctx.Err()
	}
	return fetchedGroups, fetchedBlocks, bytesRead, nil
}
