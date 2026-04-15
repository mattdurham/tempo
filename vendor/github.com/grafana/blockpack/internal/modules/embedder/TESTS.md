# embedder — Test Plan

This file documents the test coverage for the `embedder` package.

---

## Overview

The `embedder` package tests cover:
- HTTP backend: probe, embed, EmbedBatch with chunking and concurrency
- Retry behavior on transient errors (5xx, timeout, EOF)
- Error handling for 4xx responses (no retry)
- Text assembly from span fields (priority ordering, truncation)
- L2 normalization and zero-vector handling
- Nil embedder handling
- Model download and atomic write

---

## EMBED-TEST-001: HTTP Backend Interface Compliance
**Function:** `TestHTTPBackend_ImplementsBackend`
**File:** `internal/modules/embedder/backend_http_test.go`
**What it tests:** httpBackend satisfies the Backend interface; compile-time assertion passes.
**Spec invariant:** SPEC-010 — Backend interface must be implemented by httpBackend.

---

## EMBED-TEST-002: HTTP Backend Single Embed
**Function:** `TestHTTPBackend_Embed`
**File:** `internal/modules/embedder/backend_http_test.go`
**What it tests:** Embed() sends a single text to the TEI server and returns a float32 vector.
**Spec invariant:** SPEC-010 — Embed encodes one text into an L2-normalized float32 vector.

---

## EMBED-TEST-003: HTTP Backend Batch Embed
**Function:** `TestHTTPBackend_EmbedBatch`
**File:** `internal/modules/embedder/backend_http_test.go`
**What it tests:** EmbedBatch() sends multiple texts, receives one vector per text in order.
**Spec invariant:** SPEC-010 — EmbedBatch returns one vector per input text in input order.

---

## EMBED-TEST-004: HTTP Backend Empty Batch
**Function:** `TestHTTPBackend_EmptyBatch`
**File:** `internal/modules/embedder/backend_http_test.go`
**What it tests:** EmbedBatch with zero texts returns empty result without network call.
**Spec invariant:** SPEC-010 — Empty batch must be handled without error.

---

## EMBED-TEST-005: HTTP Backend Server Error Propagation
**Function:** `TestHTTPBackend_ServerError`
**File:** `internal/modules/embedder/backend_http_test.go`
**What it tests:** 5xx response from the embed server is propagated as an error.
**Spec invariant:** SPEC-010 — non-transient server errors surface to callers.

---

## EMBED-TEST-006: HTTP Backend Wrong Vector Count
**Function:** `TestHTTPBackend_WrongVectorCount`
**File:** `internal/modules/embedder/backend_http_test.go`
**What it tests:** When the server returns a different number of vectors than texts, an error is returned.
**Spec invariant:** SPEC-010 — EmbedBatch must return exactly one vector per input text.

---

## EMBED-TEST-007: Text Assembly Field Priority Ordering
**Function:** `TestAssembleText_fieldOrdering`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** Fields with higher weight appear first in the assembled text.
**Spec invariant:** SPEC-010 — primary fields appear before secondary fields.

---

## EMBED-TEST-008: Text Assembly Truncation
**Function:** `TestAssembleText_truncation`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** Assembled text is truncated to MaxTextLength; no panic on oversized input.
**Spec invariant:** SPEC-010 — assembled text must not exceed MaxTextLength characters.

---

## EMBED-TEST-009: L2 Normalize Unit Vector
**Function:** `TestL2Normalize_unitVector`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** L2Normalize of a unit vector returns the same vector (norm == 1).
**Spec invariant:** SPEC-010 — all returned vectors must be L2-normalized.

---

## EMBED-TEST-010: L2 Normalize Zero Vector
**Function:** `TestL2Normalize_zeroVector`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** L2Normalize of a zero vector does not panic; returns zero vector.
**Spec invariant:** SPEC-010 — edge case: zero-magnitude vector must not cause division by zero.

---

## EMBED-TEST-011: AssembleAllFields Priority and Alphabetical Ordering
**Function:** `TestAssembleAllFields_priorityOrder`, `TestAssembleAllFields_nonPriorityAlphabetical`
**File:** `internal/modules/embedder/all_fields_test.go`
**What it tests:** Priority fields appear in weight order; remaining fields appear alphabetically.
**Spec invariant:** SPEC-010 — deterministic field ordering for reproducible embeddings.

---

## EMBED-TEST-012: Model Download Atomic Write
**Function:** `TestEnsureModel_atomicWrite`
**File:** `internal/modules/embedder/download_test.go`
**What it tests:** EnsureModel writes to a temp file and renames atomically; partial writes are not visible.
**Spec invariant:** SPEC-010 — model file must be fully written before being visible to readers.

---

## EMBED-TEST-013: AssembleText Primary Field Only
**Function:** `TestAssembleText_primaryOnly`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** When only a primary-weight field is present, assembled text is the bare field value (no key= prefix).
**Spec invariant:** SPEC-010 — primary fields are emitted without key= prefix.

---

## EMBED-TEST-014: AssembleText Missing Fields Skipped
**Function:** `TestAssembleText_missingFieldsSkipped`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** Fields listed in the spec but absent from the input map are silently omitted.
**Spec invariant:** SPEC-010 — missing fields must not appear in assembled text.

---

## EMBED-TEST-015: AssembleText Empty Field Value Skipped
**Function:** `TestAssembleText_emptyFieldValueSkipped`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** Fields present in the input map with empty string values are omitted from output.
**Spec invariant:** SPEC-010 — empty field values must not appear in assembled text.

---

## EMBED-TEST-016: AssembleText Multiple Context Fields
**Function:** `TestAssembleText_multipleContext`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** Multiple context-weight fields all appear after the primary field in the assembled text.
**Spec invariant:** SPEC-010 — primary fields appear before context fields.

---

## EMBED-TEST-017: AssembleText No Fields
**Function:** `TestAssembleText_noFields`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** Empty input map and nil spec produce an empty string without panic.
**Spec invariant:** SPEC-010 — zero-field input must not panic and must return empty string.

---

## EMBED-TEST-018: AssembleText Unknown Weight Falls to Secondary
**Function:** `TestAssembleText_unknownWeightFallsToSecondary`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** An unrecognized weight value is treated as secondary (key=value format).
**Spec invariant:** SPEC-010 — unrecognized weights fall back to secondary behavior.

---

## EMBED-TEST-019: L2Normalize Already Normalized Vector
**Function:** `TestL2Normalize_alreadyNormalized`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** A unit vector [1, 0, 0] is returned unchanged (norm remains 1.0).
**Spec invariant:** SPEC-010 — normalizing an already-unit vector must be a no-op in magnitude.

---

## EMBED-TEST-020: L2Normalize Preserves Length
**Function:** `TestL2Normalize_preservesLength`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** The output slice length matches the input length after normalization.
**Spec invariant:** SPEC-010 — output dimensionality must equal input dimensionality.

---

## EMBED-TEST-021: L2Normalize Does Not Modify Input
**Function:** `TestL2Normalize_doesNotModifyInput`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** l2Normalize must not mutate the caller's input slice.
**Spec invariant:** SPEC-010 — normalization is non-destructive to the source vector.

---

## EMBED-TEST-022: AssembleText Batch Ordering
**Function:** `TestAssembleText_batchOrdering`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** AssembleText produces consistent, deterministic output for each span in a batch.
**Spec invariant:** SPEC-010 — text assembly is stateless and deterministic per input.

---

## EMBED-TEST-023: L2Normalize Numerical Stability
**Function:** `TestL2Normalize_numericalStability`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** Very small vectors (e.g. [1e-20, 0, 0]) do not produce NaN or panic; result is unit length or zero.
**Spec invariant:** SPEC-010 — normalization must be numerically stable at floating-point extremes.

---

## EMBED-TEST-024: Config Defaults
**Function:** `TestConfig_defaults`
**File:** `internal/modules/embedder/embedder_test.go`
**What it tests:** Default BatchSize and MaxTextLength are applied when Config fields are zero-valued.
**Spec invariant:** SPEC-010 — zero-value Config must produce safe, usable defaults.

---

## EMBED-TEST-025: AssembleAllFields Shipmate Fields High Priority
**Function:** `TestAssembleAllFields_shipmateFieldsHighPriority`
**File:** `internal/modules/embedder/all_fields_test.go`
**What it tests:** Shipmate-specific fields (prompt.text, tool.error, task.subject, agent_type, tool.command) appear in priority order before service/k8s/custom fields.
**Spec invariant:** SPEC-010 — domain-specific high-priority fields must be ranked above generic identity fields.

---

## EMBED-TEST-026: AssembleAllFields Skips Excluded Fields
**Function:** `TestAssembleAllFields_skipsExcludedFields`
**File:** `internal/modules/embedder/all_fields_test.go`
**What it tests:** Structural fields (trace:id, span:id, span:parent_id, __embedding__, __embedding_text__) are excluded from assembled text.
**Spec invariant:** SPEC-010 — structural/internal span fields must not pollute embedding text.

---

## EMBED-TEST-027: AssembleAllFields Empty Fields
**Function:** `TestAssembleAllFields_emptyFields`
**File:** `internal/modules/embedder/all_fields_test.go`
**What it tests:** Empty input map produces empty string without panic.
**Spec invariant:** SPEC-010 — zero-field input must be handled gracefully.

---

## EMBED-TEST-028: AssembleAllFields Empty Values Skipped
**Function:** `TestAssembleAllFields_emptyValuesSkipped`
**File:** `internal/modules/embedder/all_fields_test.go`
**What it tests:** Fields with empty string values are omitted from assembled text.
**Spec invariant:** SPEC-010 — empty values must not appear in assembled text.

---

## EMBED-TEST-029: AssembleAllFields Real World Span
**Function:** `TestAssembleAllFields_realWorldSpan`
**File:** `internal/modules/embedder/all_fields_test.go`
**What it tests:** A realistic OTel span with many attributes is assembled correctly: structural fields excluded, priority fields first, custom fields last.
**Spec invariant:** SPEC-010 — AssembleAllFields must produce correct output for production-representative input.
