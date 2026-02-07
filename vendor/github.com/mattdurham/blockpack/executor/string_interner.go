package executor

import "strings"

// StringInterner caches strings to reduce allocations
type StringInterner struct {
	cache map[string]string
}

func NewStringInterner() *StringInterner {
	return &StringInterner{
		cache: make(map[string]string),
	}
}

// Intern returns a cached copy of the string if it exists, otherwise caches and returns the input
func (si *StringInterner) Intern(s string) string {
	if cached, ok := si.cache[s]; ok {
		return cached
	}
	si.cache[s] = s
	return s
}

// InternConcat concatenates strings and interns the result
func (si *StringInterner) InternConcat(parts ...string) string {
	// Build key inline to avoid allocation
	totalLen := 0
	for _, p := range parts {
		totalLen += len(p)
	}

	var b strings.Builder
	b.Grow(totalLen)
	for _, p := range parts {
		b.WriteString(p)
	}

	key := b.String()
	return si.Intern(key)
}
