# Implementation Status

## Status: COMPLETE

## Summary
Fixed all 4 issues listed in `.bob/state/fix-prompt.md` (1 MEDIUM, 3 LOW). No architectural
changes, no new functionality, no public API changes. Revendored blockpack into tempo and
confirmed byte-identical vendor. Re-ran affected test suites in both repos — all green.

## Issues Fixed

### 1. MEDIUM — Stale "bounded-recent-first" wording (tempo repo)
- `tempodb/encoding/vblockpack/slice_errors.go:35` — `ErrSearchNoCoverage`'s `Error()` string
  changed from `"vblockpack: search index has no coverage and no bounded-recent-first path was
  authorized"` to `"vblockpack: search index has no coverage"`.
- `tempodb/encoding/vblockpack/decline_response.go:94` — the corresponding HTTP 422 message
  changed from `"search index has no coverage yet (materialized index still building for this
  window) and no bounded-recent-first path was authorized"` to `"search index has no coverage
  yet (materialized index still building for this window)"`.
- Grepped both repos (live code + tests, not just docs) for `bounded-recent-first`: only
  remaining hits are in `.bob/state/*.md` planning docs (review.md, plan-scan-fallback.md,
  fix-prompt.md) — no `decline_response_test.go`/`fetch_test.go` assertions referenced the old
  literal string, so no test changes were needed.

### 2. LOW — gofumpt formatting (blockpack repo)
- Ran `gofumpt -w internal/modules/valueindex/bucketquery_ranged_test.go`. The 6-element
  `entries` composite literal (lines 441-443) is now one-per-line. Confirmed `gofumpt -l` no
  longer flags the file. Diff is whitespace-only (no logic change).

### 3. LOW — Missing overflow guard on widening loop (blockpack repo)
- `internal/modules/vibuilder/builder_bounded_and.go`: added `"math"` to the import block and
  added an overflow guard immediately after `overFetch *= 2` inside the anchor+confirm widening
  loop:
  ```go
  overFetch *= 2
  if overFetch <= 0 || overFetch < limit {
      overFetch = math.MaxInt // overflow guard; unreachable at any realistic limit value
  }
  ```
  Matches the style of the existing guard on the initial `overFetch := limit *
  anchorOverFetchFactor` computation a few lines above.

### 4. LOW — Stale plan doc note (tempo repo, docs only)
- `.bob/state/plan-scan-fallback.md`, Phase 6b section: added a "Correction
  (post-implementation, per task #193's fix)" note directly after the `ErrMatchAllRequiresLimit`
  sentinel definition/wiring instructions, mirroring the existing "Correction
  (post-implementation...)" pattern already used elsewhere in that same section (the slice-job
  window-scoping mechanism). States plainly that `ErrMatchAllRequiresLimit` was fully superseded
  before shipping, was never a real sentinel in the codebase, and that `spanLimit <= 0` on a
  match-all query now falls through to the ordinary unbounded scan (preserving
  `MaxTraces=0`'s "unlimited" contract) instead of hard-erroring. Documentation-only; no code
  touched.

## Revendoring (blockpack -> tempo)
- Quarantined 55 stale `integration/*/e2e_integration_test*/var` directories via
  `go run ./hack/renamedir` (permission-denied-for-traversal, owned-by-different-uid dirs that
  block `go mod vendor`'s `all` package scan).
- Ran `go mod vendor -e` in tempo (picks up the local `replace github.com/grafana/blockpack =>
  ../blockpack` directive).
- Restored all 55 quarantined directories (confirmed 55/55 restored).
- Confirmed byte-identical vendor: `diff -rq --exclude='*_test.go' blockpack/
  tempo/vendor/github.com/grafana/blockpack/` — zero differences. The only "Only in blockpack/"
  entries are expected non-vendored items (`.bob`, `.claude`, `cmd/`, `benchmark/`, `docs/`,
  `internal/otlpconvert`, `internal/parity`, `internal/parquetconv`, `go.mod`/`go.sum`, etc.) —
  none of these are imported by tempo's `vblockpack` package.
- `go.mod`/`vendor/modules.txt` pseudo-version bumped
  (`v0.0.0-20260711132147-dfa6b3e7` -> `v0.0.0-20260711175239-588cd183`) as an expected side
  effect of `go mod vendor` picking up blockpack's current working-tree state via the local
  `replace` directive.

## Verification

### tempo
- `go test ./tempodb/encoding/vblockpack/...` — PASS (41.1s)
- `go build ./tempodb/... ./modules/frontend/... ./modules/querier/...` — clean
- `go vet ./tempodb/encoding/vblockpack/...` — clean

### blockpack
- `go test ./internal/modules/valueindex/... ./internal/modules/vibuilder/...` — PASS
- `go build ./...` — clean
- `go vet ./internal/modules/valueindex/... ./internal/modules/vibuilder/...` — clean
- `gofumpt -l` on both touched files — clean (no output)
- `golangci-lint run ./internal/modules/vibuilder/... ./internal/modules/valueindex/...` — 4
  pre-existing unrelated findings in `bucketfile_test.go` (gosec G115 x3, govet shadow x1), none
  in the two files touched by this fix cycle (`builder_bounded_and.go`,
  `bucketquery_ranged_test.go`)

## Files Changed

### tempo
- `tempodb/encoding/vblockpack/slice_errors.go` (issue #1)
- `tempodb/encoding/vblockpack/decline_response.go` (issue #1)
- `.bob/state/plan-scan-fallback.md` (issue #4, docs only)
- `vendor/github.com/grafana/blockpack/**` (revendor, byte-identical to blockpack source)
- `go.mod`, `vendor/modules.txt` (pseudo-version bump from revendor)

### blockpack
- `internal/modules/valueindex/bucketquery_ranged_test.go` (issue #2, gofumpt formatting only)
- `internal/modules/vibuilder/builder_bounded_and.go` (issue #3, overflow guard + `math` import)

## Constraints Honored
- No rewrites of unrelated code.
- No new functionality introduced.
- No public API changes.
- Only the 4 listed items were touched; `plan-scan-fallback.md` was NOT used as a guide for what
  to fix, only as the location for issue #4's correction note.
