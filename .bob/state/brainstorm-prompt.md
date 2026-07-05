Task: Investigate and root-cause the "whole file download instead of block" performance problem on tempo-dev-test-03's blockpack query path. This is a read-heavy investigation — produce a diagnosis with evidence, not a design doc for a hypothetical feature.

## Background

Benchmark TraceQL search queries against tempo-dev-test-03 (tenant 11638, query window now-3h to now-1h) are timing out (~30-33s, hitting context deadline exceeded). Established so far this session:

1. Value-index coverage IS working — querier logs show "vblockpack: index fetch: coverage found" and `index.used`/`index.hits` span attributes are populated. The value-index itself is not the problem.
2. Fetches observed during slow queries pulled 60-65MB objects. A single internal BLOCK should be "at most a few megabytes" (explicit user statement) — a .blockpack FILE can be gigabytes (bounded by `max_output_file_size`, currently 10GB in tempo-dev-test-03's backend-worker blockpack compactor config). Working hypothesis: the querier's fetch path pulls entire compacted FILES rather than just the specific internal BLOCK(s) needed, and this is the root cause of the timeouts (and likely contributed to today's one querier OOMKill, 16:24:31-16:26:40 EDT).
3. Known blockIDs implicated in slow queries (tenant 11638): 93b44dd6-09cc-5088-bce8-1e1f675bf534, 925a3b53-c371-519b-96c0-6655c0d4511f, b6478933-c598-5a12-b601-3ea6b651d013, 882b9288-eaa1-5280-927f-f5edf2f2745e, dc40aa41-8b83-5e90-8eac-c8c8cb84656c. S3 key convention: `tenantID/blockID/data.blockpack` (tempo-mrd's tempodb/encoding/vblockpack/blockevents.go:8, DataFileName const in version.go:12).
4. `blockpack-explorer` pod is deployed in tempo-dev-test-03 (service `blockpack-explorer.tempo-dev-test-03.svc:8090`, source at /home/mdurham/source/blockpack_collection/blockpack-worktrees/blockpack-explorer). It scans s3://dev-us-east-0-tempo-dev-test-03/11638 into a local SQLite index (files + toc_entries tables — see internal/explorer/db.go for exact schema) and exposes:
   - GET /status
   - POST /query — arbitrary SQL against files/toc_entries tables
   - GET /file — serves/reads S3 file content, supports Range requests
   - POST /fetch and GET /fetch/{id} — async S3 download jobs
   Use `kubectl port-forward svc/blockpack-explorer -n tempo-dev-test-03 8090:8090` to reach it.

## lth memory context already surfaced (highly relevant — verify these still hold, don't just trust them)

- INVARIANT: blockpack reader `WantAll()` eagerly decodes EVERY column in a block and causes 300MB+ memory spikes — query-driven paths should use `WantOnly(cols)`. Zero-value `WantColumns{}` == `WantOnly(empty)`. File: internal/modules/blockio/reader/reader.go (blockpack repo).
- L4 finding (2026-06-04, project mattdurham/tempo): "Queries efficiently use `GetBlockWithBytes()` (one full block read to memory, then in-memory parsing), while compaction wires `tempoBlockProvider.ReadAt()` directly to S3 ranged GETs." This was framed as the query path being the FAST one relative to compaction's N+1 ranged-GET problem — but it also means the query path, by design, may read an entire block/file into memory rather than doing a scoped ranged read. This is the single most load-bearing clue for this investigation: confirm what "block" means in `GetBlockWithBytes()` — is it the per-blockID S3 object (data.blockpack, potentially gigabytes after heavy compaction) or a small internal chunk? Find `GetBlockWithBytes` in the current codebase and read it directly; don't rely on the memory's phrasing, it may be stale.
- L4 finding (2026-06-19): range pruning on string bounds at the querier level can skip S3 fetches entirely for non-matching blocks — check whether this pruning is actually being applied for the timed-out queries, or whether it's being bypassed.
- L4 finding (2026-06-09): four serial per-block S3 GETs for intrinsic columns causing latency — likely not the primary cause here (this is about per-block latency for many small blocks, not one huge fetch) but worth ruling out as a contributing factor.
- INVARIANT (rw.DataType cache eviction tiers): Block payloads (DataTypeBlock) are tier3, evicted first. If large full-file reads are being cached as tier3, they'd be evicted fast — check if that's forcing frequent expensive re-fetches instead of a one-time cost.

## What to actually determine (in priority order)

1. **Ground truth on file/block sizes.** Use blockpack-explorer's `/query` endpoint against `toc_entries` and `files` tables for the 5 implicated blockIDs. Get: total file size, number of internal blocks/chunks per file (per the TOC), and the size of each individual internal block. This either confirms or kills the "file has many small blocks packed together" hypothesis vs. "file IS one giant block."
2. **Ground truth on what the querier actually requests.** Find where in tempo-mrd's `tempodb/encoding/vblockpack/*` (backend_block.go's `startBlockSpan`/Fetch path, value_index_query.go's `tryIndexFetch()`, and whatever `GetBlockWithBytes` resolves to today — grep for it, the memory reference may be stale) the actual S3 GetObject/ranged-read call happens. Does it pass a Range header at all? If so, is the range computed from TOC offsets for just the needed block, or does it default to full-object?
3. **Correlate.** Given the answers to 1 and 2: is the 60-65MB fetch observed in logs equal to the FULL file size for that blockID (confirming whole-file fetch), or does it match a legitimate single-block size from the TOC (meaning the "few megabytes" assumption about block size needs revisiting instead, or something else is fetching 60MB deliberately)? Do not assume — check the actual numbers.
4. If whole-file fetch IS confirmed as the bug: identify exactly which function/call site needs to change to do a scoped ranged read using TOC offsets instead, and whether the TOC itself is already fetched separately (small header read) before the full-body fetch, or whether TOC + body come down together.
5. Also independently sanity-check `max_output_file_size` (10GB) — is it a real contributing misconfiguration (i.e., should be lowered to keep files closer to a "few MB per block" world) or is it orthogonal to the fetch-path bug? Don't assume either way.

## Constraints

- blockpack repo (/home/mdurham/source/blockpack_collection/blockpack): main branch only, no new branches, no push, no PR without being asked.
- tempo-mrd repo (/home/mdurham/source/blockpack_collection/tempo): branch `agentic-tempo` (already checked out here), same no-push/no-PR rule. There is pre-existing uncommitted WIP in this checkout from earlier VI/VCNT compaction work today (`cmd/tempo/app/value_index.go`, `go.mod`, `tempodb/encoding/common/config.go`, several `vendor/github.com/grafana/blockpack/*` files) — do not touch or revert these, they are intentional and unrelated to this investigation.
- Verify claims independently (read actual logs/data/code, don't trust self-reported summaries or stale memory phrasing).
- This is diagnosis-first: do NOT write a code fix yet. Output a brainstorm.md with the confirmed root cause, evidence (real numbers from blockpack-explorer TOC queries and code line references), and 2-3 candidate fix approaches with tradeoffs. The actual fix implementation happens in a later PLAN/EXECUTE phase after the team lead reviews this.
- No spec-driven modules are expected to need doc updates yet at this investigation stage — that comes later if/when a code fix module (e.g. vblockpack) needs it.
