# Security Policy

## Supported Versions

ahocorasick is currently in early development (v0.x.x). We provide security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1.0 | :x:                |

Future stable releases (v1.0+) will follow semantic versioning with LTS support.

## Reporting a Vulnerability

We take security seriously. If you discover a security vulnerability in ahocorasick, please report it responsibly.

### How to Report

**DO NOT** open a public GitHub issue for security vulnerabilities.

Instead, please report security issues by:

1. **Private Security Advisory** (preferred):
   https://github.com/coregx/ahocorasick/security/advisories/new

2. **Email** to maintainers:
   Create a private GitHub issue or contact via discussions

### What to Include

Please include the following information in your report:

- **Description** of the vulnerability
- **Steps to reproduce** the issue (include pattern set if applicable)
- **Affected versions** (which versions are impacted)
- **Potential impact** (DoS, memory exhaustion, unexpected behavior, etc.)
- **Suggested fix** (if you have one)
- **Your contact information** (for follow-up questions)

### Response Timeline

- **Initial Response**: Within 48-72 hours
- **Triage & Assessment**: Within 1 week
- **Fix & Disclosure**: Coordinated with reporter

## Security Considerations

ahocorasick is a multi-pattern string matching library. This introduces security risks that users should be aware of.

### 1. Memory Exhaustion

**Risk**: Large number of patterns can consume significant memory.

**Attack Vectors**:
- Thousands of long patterns
- Patterns with many overlapping prefixes
- Deliberate trie explosion

**Mitigation**:
- ✅ ByteClasses reduce memory for transition tables
- ✅ Shared prefix storage in trie
- ⚠️ User should validate pattern count and size

**User Recommendations**:
```go
// ❌ BAD - Unbounded pattern input
patterns := getUserPatterns() // Could be millions of patterns
ac, _ := ahocorasick.NewBuilder().AddStrings(patterns).Build()

// ✅ GOOD - Validate pattern count and size
const maxPatterns = 10000
const maxPatternLen = 1000

if len(patterns) > maxPatterns {
    return errors.New("too many patterns")
}
for _, p := range patterns {
    if len(p) > maxPatternLen {
        return errors.New("pattern too long")
    }
}
```

### 2. Input Size Limits

**Risk**: Very large inputs can cause performance issues.

**Mitigation**:
- ✅ O(n) time complexity regardless of pattern count
- ⚠️ User should limit input size for untrusted data

**User Recommendations**:
```go
// ❌ BAD - Unbounded input
matches := ac.FindAll(hugeInput, -1)

// ✅ GOOD - Limit input size and match count
const maxInputSize = 10 * 1024 * 1024 // 10MB
if len(input) > maxInputSize {
    return errors.New("input too large")
}
matches := ac.FindAll(input, 1000) // Max 1000 matches
```

### 3. Integer Overflow

**Risk**: Very large pattern counts could cause integer overflow.

**Mitigation**:
- ✅ Go's built-in overflow protection
- ✅ Reasonable limits on pattern count in practice
- ✅ #nolint:gosec annotations with safety justification

## Security Best Practices

### Validate Inputs

```go
// Validate before building automaton
func validatePatterns(patterns []string) error {
    if len(patterns) == 0 {
        return errors.New("no patterns")
    }
    if len(patterns) > 10000 {
        return errors.New("too many patterns")
    }
    for i, p := range patterns {
        if len(p) == 0 {
            return fmt.Errorf("empty pattern at index %d", i)
        }
        if len(p) > 1000 {
            return fmt.Errorf("pattern %d too long", i)
        }
    }
    return nil
}
```

### Resource Limits

```go
// Set limits for untrusted input
const (
    maxInputSize = 10 * 1024 * 1024 // 10MB
    maxMatches   = 10000
)

if len(input) > maxInputSize {
    return errors.New("input too large")
}

matches := ac.FindAll(input, maxMatches)
```

## Dependencies

ahocorasick has zero external dependencies:

- Pure Go implementation
- No CGO
- No external libraries

This minimizes the attack surface and supply chain risk.

## Security Testing

- ✅ Unit tests with edge cases
- ✅ Linter with security checks (gosec via golangci-lint)
- ✅ Race detector tests
- 🔄 Fuzz testing (planned)

## Security Contact

- **GitHub Security Advisory**: https://github.com/coregx/ahocorasick/security/advisories/new
- **Public Issues** (for non-sensitive bugs): https://github.com/coregx/ahocorasick/issues

---

**Thank you for helping keep ahocorasick secure!**
