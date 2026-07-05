# Trace-Index Stage 6 Call-Site Review

Generated: 2026-07-05
Scope: `tempodb/encoding/vblockpack/backend_block.go`, `wal_block.go`, `integration_test.go`, `roundtrip_test.go` only.
Out of scope (per instructions, not reviewed): vibuilder bounded-concurrency changes, `vendor/` refresh.

Verification performed:
- Read `vendor/github.com/grafana/blockpack/reader.go` `GetTraceByID` / `getTraceByIDViaIndex` to confirm actual fallback semantics.
- Read `vendor/github.com/grafana/blockpack/internal/modules/valueindex/discovery.go` (`DiscoverIndexFiles`) to confirm time-window semantics.
- Read `tempodb/encoding/common/interfaces.go` to confirm `SearchOptions` has no time-range field (claim in the new comment).
- Read `tempodb/backend/block_meta.go` to confirm `StartTime`/`EndTime` are derived from span times (min/max), not ingestion time.
- `go build ./tempodb/encoding/vblockpack/...` and `go vet ./tempodb/encoding/vblockpack/...` — both clean.
- `go test ./tempodb/encoding/vblockpack/... -run FindTraceByID -v` — all 3 tests pass (`TestBackendBlockFindTraceByID`, `TestBlockpackBlock_FindTraceByID_StillFullScansWithoutIndexWiring`, `TestWalBlock_FindTraceByID_PermanentlySkipsIndex`).
- `grep` for other callers of `blockpack.GetTraceByID` in the tempo tree — only these two call sites exist; no missed call sites.

---

## Findings

### 1. Nil-lister path is genuinely behavior-neutral — CONFIRMED, no issue

`blockpack.GetTraceByID` (vendor/github.com/grafana/blockpack/reader.go:400) gates the index attempt on `lister != nil && tenant != ""`. Both call sites pass `lister = nil`:

- `backend_block.go:432-436` passes `nil` for lister but a real, non-empty `b.meta.TenantID` for tenant. This is fine — the `&&` short-circuits on `lister != nil` first, so the non-empty tenant is inert today. It is also forward-compatible: if a `LookupStore` is wired in later, the tenant value is already correctly threaded through.
- `wal_block.go:323` passes `nil` lister and `""` tenant — belt-and-suspenders, doubly ensures the index path can never activate for a WAL block.

Either way, execution always falls through to `getTraceByIDFullScan(r, traceID)`, identical to the pre-change call `blockpack.GetTraceByID(r, traceIDHex)`. No behavior change confirmed by reading the vendor implementation, not just by inference from the doc comment.

**Severity:** N/A (verified correct, no issue).

---

### 2. Time-window derivation (backend_block.go) — sound reasoning, but two related low-severity notes

`backend_block.go:432-436` passes `uint64(b.meta.StartTime.Unix())` / `uint64(b.meta.EndTime.Unix())` as `queryMinSec`/`queryMaxSec`.

Verified:
- `common.SearchOptions` (tempodb/encoding/common/interfaces.go:38-49) indeed has no time-range field — the comment's stated rationale for falling back to block meta wall-clock times is accurate.
- `block_meta.go:258-266` confirms `StartTime`/`EndTime` are extended per-span as spans are added to the block (i.e., true span wall-clock range, not ingestion/write time), which is consistent with how `DiscoverIndexFiles` filters value-index files by their embedded `WallMinSec`/`WallMaxSec` (vendor `discovery.go:26-35`). The reasoning holds structurally.
- Since `lister == nil` at this call site, `queryMinSec`/`queryMaxSec` are dead arguments today — `getTraceByIDViaIndex` (and therefore `DiscoverIndexFiles`) is never reached, so no valid results can be excluded by this window in the current nil-lister fallback path. Point (2) in the review brief is satisfied: correctness is unaffected today.

Two forward-looking observations for when the follow-up lister wiring lands (not bugs today, since the values are unused, but worth tracking against the `#428` TODO so they aren't rediscovered the hard way):

- **LOW** — `backend_block.go:434-435`: The `//nolint:gosec` comments assert "block start/end times are always positive," justifying the `int64→uint64` conversion. This holds for any block that has actually ingested spans, but if `b.meta.StartTime`/`EndTime` were ever a zero-value `time.Time` (e.g., a malformed/empty meta), `.Unix()` returns a large negative `int64`, which wraps to a huge `uint64` on conversion. Today this is inert (dead argument). Once a real lister is wired, a zero-value `StartTime` would silently pass a nonsensical index-discovery window instead of erroring. Consider a defensive `IsZero()` guard (e.g., falling back to `(0, math.MaxUint64)`, matching the WAL block's convention) at the point the lister is actually wired.
- **LOW** — related to the point above: nothing in this change enforces `StartTime <= EndTime`. Given the project's own history of write/read time-floor mismatches causing dropped results (noted precedent: the VI/VCNT minute-flooring bug), it's worth a note in the eventual lister-wiring follow-up to double check the block-meta window's rounding/flooring matches whatever granularity `valueindexcompactor` uses for `WallMinSec`/`WallMaxSec`, not just that both are "seconds since epoch." Not a defect in this diff — the parameters are unused here — but flagging since this is exactly the shape of bug this codebase has hit before.

---

### 3. wal_block.go "permanently nil" design — sound and well-documented

`wal_block.go:317-323`: the inline comment explicitly states a WAL block can never have index coverage (never consumed by `valueindexconsumer` or compacted by `valueindexcompactor`) and explicitly instructs future contributors not to "fix" this by wiring a lister in, citing the wasted `DiscoverIndexFiles` List call as the cost of getting it wrong. The doc comment on `TestWalBlock_FindTraceByID_PermanentlySkipsIndex` (roundtrip_test.go:87-93) reinforces this with the word "PERMANENTLY" and points back to the code comment. This directly and clearly addresses the brief's concern about a future contributor mistakenly "fixing" this call site.

One minor inconsistency worth flagging:

- **LOW** — `wal_block.go:323` passes `0, math.MaxUint64` for the time window (i.e., "unbounded"), while `backend_block.go` passes the block's actual wall-clock range. Both are correct given `lister == nil` makes the window inert either way, but the asymmetry could look like an oversight to a future reader who doesn't realize both windows are dead code paths (both are already covered by clear inline comments, so this is very low risk of confusion, but a one-line comment noting "unbounded because coverage is permanently absent, unlike the backend-block case" would remove any ambiguity).

**Severity:** LOW (documentation clarity only — functionally correct and already well explained).

---

### 4. Regression tests — assert real end-to-end behavior, not just compilation

Both new tests exercise the full public path (`CreateBlock`/`AppendTrace` → `FindTraceByID`) rather than mocking internals, so they validate actual behavior, not just that the code compiles:

- `TestBlockpackBlock_FindTraceByID_StillFullScansWithoutIndexWiring` (integration_test.go:358-421): builds a real backend block via `CreateBlock`, calls the real `blockpackBlock.FindTraceByID`, and asserts on the trace's actual span name (`"no-index-span"`) — this would catch either a full failure to find the trace or a wrong/corrupted result, not just a nil-vs-non-nil check.
- `TestWalBlock_FindTraceByID_PermanentlySkipsIndex` (roundtrip_test.go:90-125): builds a real WAL block via `createWALBlock`/`AppendTrace`, calls the real `walBlock.FindTraceByID`, and asserts a non-nil trace with non-empty `ResourceSpans`.
- Confirmed both pass: `go test ./tempodb/encoding/vblockpack/... -run FindTraceByID -v` → 3/3 pass, including the pre-existing `TestBackendBlockFindTraceByID`.

**Caveat (LOW, informational, not a defect):** Since the call sites already hard-code `nil` for the lister parameter, these tests cannot independently detect a future accidental change to pass a non-nil-but-broken lister that happens to also fall through to full scan (e.g., a lister that always errors) — they only pin the current, correct end state. This is an acceptable and normal limitation of a black-box regression test and does not need to be fixed; noting it only because it slightly limits how much protection the tests provide against the specific "someone wires in a broken lister" future-regression scenario the code comments warn about.

---

### 5. Leftover debug code / scope creep

None found:
- `git diff --stat` on the 4 files shows a small, focused diff (13 / 67 / 39 / 10 lines) consistent with "update two call sites + add two regression tests."
- No `fmt.Println`/`Debug`/stray `println` in the diff.
- Exactly one `TODO` in the diff (`backend_block.go:427`), and it is a deliberate, well-justified, tracked marker referencing `blockpack issue #428 Stage 6 plan` for the explicitly out-of-scope follow-up (lister wiring) — not an accidentally-unresolved TODO that belongs in this changeset.
- No other callers of `blockpack.GetTraceByID` exist in the tempo tree that were missed.
- `go build` / `go vet` clean on the package.

**Severity:** N/A — no issues found.

---

## Summary

**Total Issues:** 4 (all LOW, all forward-looking/informational; none block this change)
- CRITICAL: 0
- HIGH: 0
- MEDIUM: 0
- LOW: 4
  1. `backend_block.go:434-435` — zero-value `StartTime`/`EndTime` would produce a nonsensical (but currently inert) index-discovery window if the lister is wired later without a guard.
  2. Related — no `StartTime <= EndTime` invariant enforced; flagging given this codebase's prior history of write/read time-floor mismatches, purely as a heads-up for the follow-up lister-wiring work.
  3. `wal_block.go:323` vs `backend_block.go:434-435` — asymmetric time-window values (`0, MaxUint64` vs. block wall-clock range) are both correct (inert either way) but could look inconsistent to a future reader; a one-line comment would remove ambiguity.
  4. Regression tests can't distinguish "nil lister" from "a future non-nil-but-always-falls-through lister" — acceptable black-box test limitation, not a defect.

**Domains with findings:**
- Security: 0
- Bug Diagnosis: 0
- Error Handling: 0
- Code Quality: 0
- Performance: 0
- Go Idioms: 0
- Architecture: 0
- Documentation: 1 (item 3, wording clarity)
- Comment Accuracy: 0 (all comments verified accurate against vendor implementation and `common.SearchOptions`)
- Reference Integrity: 0 (issue #428 reference is external/GitHub, not a local spec file; not independently verifiable from this repo checkout, but consistently referenced in both code and test comments)
- Spec-Driven Verification: N/A — these 4 tempo files are not spec-driven (no local SPECS.md/NOTES.md/TESTS.md/BENCHMARKS.md in `tempodb/encoding/vblockpack/`); the vendored `blockpack` module is spec-driven but is out of scope per the review instructions.

This report is objective findings only, per instructions — no ship/no-ship recommendation given.
