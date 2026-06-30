# Remaining HIGH Priority Issues - Post Security Hardening

**Created**: 2026-02-11
**Status**: TODO
**Priority**: HIGH (but non-critical)
**Context**: After completing security hardening (6 CRITICAL + 12 HIGH issues resolved)

---

## Overview

All critical security issues have been resolved. The codebase is production-ready with comprehensive thread-safety, input validation, and error handling. The remaining HIGH priority issues focus on performance optimization, architectural improvements, and developer experience.

**Remaining**: 20 HIGH + 42 MEDIUM + 32 LOW issues

---

## HIGH Priority Issues (20 remaining)

### Performance Optimizations

#### HIGH-16: MinHash Cache Key Optimization
**Impact**: Reduce CPU usage on cache lookups
**Current**: String concatenation on every lookup
**Solution**: Hash-based cache keys with collision handling
**Effort**: 3-4 hours
**Files**: `internal/types/minhash_cache.go`

#### MEDIUM-1: Two-Pass RLE Encoding
**Impact**: Faster encoding
**Current**: Two passes (count runs, then encode)
**Solution**: Single-pass streaming encoder
**Effort**: 4-5 hours
**Files**: RLE encoding logic in blockio

#### HIGH: I/O Retry Logic
**Impact**: Resilience to transient failures
**Solution**: Add retry with exponential backoff
**Effort**: 3-4 hours

#### HIGH: File I/O Timeouts
**Impact**: Prevent hanging on slow I/O
**Solution**: Add timeout support to I/O operations
**Effort**: 2-3 hours

---

### Architectural Improvements

#### HIGH-9: Format Constants Consolidation
**Impact**: Maintainability, single source of truth
**Current**: Constants in 3 locations (format_constants.go, format_exports.go, format/ package)
**Solution**: Move all to format/ package, add deprecated aliases
**Effort**: 3-4 hours
**Breaking**: No (with aliases)
**Files**:
- `internal/blockio/format_constants.go`
- `internal/blockio/format_exports.go`
- `internal/blockio/format/`

#### HIGH-7: Duplicate Executor Types
**Impact**: Code duplication, maintenance burden
**Solution**: Consolidate executor implementations
**Effort**: 4-5 hours
**Breaking**: Potentially (internal only)
**Files**: Multiple executor implementations

#### HIGH-8: Confusing Aliases
**Impact**: Developer confusion
**Solution**: Document purpose or remove/deprecate
**Effort**: 2 hours
**Files**: Various type aliases throughout codebase

#### HIGH: Context.Context Support
**Impact**: Cancellation, timeouts, request tracing
**Solution**: Add context.Context parameters to key functions
**Effort**: 8-10 hours
**Breaking**: YES (major API change)
**Note**: Requires migration plan and versioning strategy

---

### Error Handling & Developer Experience

#### HIGH-6: Missing Error Context
**Impact**: Difficult debugging
**Solution**: Add operation/location context to all error returns
**Example**:
```go
// Before:
return nil, err

// After:
return nil, fmt.Errorf("read column %s at offset %d: %w", name, offset, err)
```
**Effort**: 2-3 hours
**Pattern**: Apply to I/O operations, parsing, construction

---

## MEDIUM Priority Issues (42 remaining)

### Documentation
- Add godoc comments to all exported types
- Document Reader, Writer, Executor APIs
- Add usage examples
- Update outdated comments

### Code Quality
- Reduce code duplication
- Extract magic number constants
- Improve comment formatting
- Better variable naming

### Performance
- O(n) improvements in hot paths
- Reduce allocations
- Optimize string operations

**Total Effort**: 10-15 hours

---

## LOW Priority Issues (32 remaining)

- Variable shadowing cleanup
- Consistent error message formatting
- Minor refactoring opportunities
- Style consistency

**Total Effort**: 5-8 hours

---

## Implementation Strategy

### Phase 1: Quick Wins (5-7 hours)
1. **HIGH-6**: Error context improvements
2. **HIGH-8**: Alias documentation
3. **Documentation**: Key exports

**Value**: Better debugging, clearer code
**Risk**: Low
**Breaking**: No

---

### Phase 2: Performance (8-12 hours)
1. **HIGH-16**: Cache optimization
2. **MEDIUM-1**: RLE optimization
3. **HIGH**: I/O retry logic
4. **HIGH**: Timeouts

**Value**: Measurable performance gains
**Risk**: Medium (need benchmarks)
**Breaking**: No

---

### Phase 3: Architecture (10-15 hours)
1. **HIGH-9**: Constants consolidation
2. **HIGH-7**: Executor consolidation
3. **HIGH-8**: Alias cleanup (if removal needed)

**Value**: Long-term maintainability
**Risk**: Medium (refactoring)
**Breaking**: Minimal (with deprecation)

---

### Phase 4: Context Support (8-10 hours)
1. Design migration strategy
2. Add context parameters to core functions
3. Update all callers
4. Document migration path

**Value**: Production-grade cancellation/tracing
**Risk**: High (breaking change)
**Breaking**: YES (needs major version bump)

---

## Priority Recommendation

**Start with Phase 1** (Quick Wins):
- Immediate value
- Low risk
- No breaking changes
- Improves developer experience

**Then Phase 2** (Performance):
- Measurable improvements
- Can be benchmarked
- No breaking changes

**Defer Phase 4** (Context Support):
- Requires careful API design
- Breaking change needs versioning plan
- Can be separate major version release

---

## Success Criteria

### Phase 1
- [ ] All error messages include operation context
- [ ] All aliases documented or deprecated
- [ ] Key exported types have godoc comments
- [ ] Examples added for complex APIs

### Phase 2
- [ ] Cache lookups 50%+ faster (benchmark)
- [ ] RLE encoding 2x faster (benchmark)
- [ ] I/O operations retry on transient failures
- [ ] Timeout protection on slow I/O

### Phase 3
- [ ] Single source of truth for constants
- [ ] No duplicate executor code
- [ ] All aliases have clear purpose or deprecated
- [ ] Migration paths documented

### Phase 4
- [ ] Context support in all blocking operations
- [ ] Cancellation works correctly
- [ ] Migration guide published
- [ ] Backward compatibility maintained (deprecated APIs)

---

## Notes

- All CRITICAL issues already resolved (6/6) ✅
- 12 of 30 HIGH issues already resolved (40%) ✅
- Codebase is production-ready as-is ✅
- Remaining work is optimization and polish, not security/reliability

---

**Task Created**: 2026-02-11
**Estimated Total Effort**: 35-50 hours (across all phases)
**Recommended Approach**: Incremental - tackle phases in order as time permits
