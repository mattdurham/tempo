# Security Review - security-reviewer

## Review Scope
Files reviewed:
- tempodb/encoding/vblockpack/roundtrip_test.go
- go.mod (dependency change)

---

### Issue 1: External dependency on personal/fork repository via pseudo-version commit hash
**Severity:** MEDIUM
**Category:** security
**Files:** go.mod:89

**WHAT:** `github.com/mattdurham/blockpack v0.0.0-20260219155254-414f47287d07` is a direct dependency pointing to a personal GitHub account repository at a specific commit pseudo-version. It is not a published, semantically versioned release.

**WHY:** Dependencies on personal/fork repositories at commit pseudo-versions carry supply chain risk:
1. The repository owner can force-push or delete the commit, breaking reproducible builds.
2. There is no code review trail or release process the way there would be for a proper module release.
3. The module is not in the Go module proxy cache with the same guarantees as officially released modules.
4. In a production codebase like Tempo, this increases the attack surface for a dependency confusion or typosquatting attack if the repo is ever renamed or transferred.
This is not a vulnerability in the changed files themselves (roundtrip_test.go), but the dependency promotion from indirect to direct increases the surface area of concern for security review.

**WHERE:** go.mod:89

---

### Issue 2: Test data uses hardcoded predictable trace/span IDs
**Severity:** LOW
**Category:** security
**Files:** tempodb/encoding/vblockpack/roundtrip_test.go:27, 36

**WHAT:** The test uses `traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}` and `SpanId: []byte{1, 0, 0, 0, 0, 0, 0, 1}`.

**WHY:** In a test context this is fine and intentional — predictable IDs make assertions clear and reproducible. No security risk in test code. Noted only for completeness. This is consistent with the pattern used in other test files in the same package (create_test.go, integration_test.go use the same hardcoded IDs).

**WHERE:** roundtrip_test.go:27, 36

---

## Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 0     |
| HIGH     | 0     |
| MEDIUM   | 1     |
| LOW      | 1     |

Total issues found: 2
