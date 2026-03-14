package ahocorasick

// Automaton is the compiled Aho-Corasick multi-pattern matcher.
// Uses optimized dense array transitions for high performance.
type Automaton struct {
	nfa       *OptimizedNFA
	patterns  [][]byte
	matchKind MatchKind
}

// Find returns the first match in haystack starting at or after position start.
// Returns nil if no match is found.
func (a *Automaton) Find(haystack []byte, start int) *Match {
	if start >= len(haystack) {
		return nil
	}

	state := a.nfa.startState
	var bestMatch *Match

	for i := start; i < len(haystack); i++ {
		b := haystack[i]
		state = a.nfa.nextState(state, b)

		if !a.nfa.isMatch(state) {
			continue
		}

		matches := a.nfa.getMatches(state)
		if len(matches) == 0 {
			continue
		}

		// For LeftmostFirst, take the first pattern that matches
		patternID := matches[0]
		pattern := a.patterns[patternID]
		matchEnd := i + 1
		matchStart := matchEnd - len(pattern)

		match := &Match{
			PatternID: int(patternID),
			Start:     matchStart,
			End:       matchEnd,
		}

		if a.matchKind == LeftmostFirst {
			// Return immediately for leftmost-first
			return match
		}

		// For LeftmostLongest, track the longest match
		if bestMatch == nil || match.Len() > bestMatch.Len() {
			bestMatch = match
		}
	}

	return bestMatch
}

// FindAt returns the first match starting exactly at position start.
// This is useful for anchored matching.
// Returns nil if no match starts at the given position.
func (a *Automaton) FindAt(haystack []byte, start int) *Match {
	if start >= len(haystack) {
		return nil
	}

	state := a.nfa.startState
	var bestMatch *Match

	for i := start; i < len(haystack); i++ {
		b := haystack[i]
		prevState := state
		state = a.nfa.nextState(state, b)

		// Check if we've moved past a potential match position
		if prevState == a.nfa.startState && i > start {
			// We're back at start state after position 'start'
			// No match can start at 'start'
			break
		}

		if !a.nfa.isMatch(state) {
			continue
		}

		for _, patternID := range a.nfa.getMatches(state) {
			pattern := a.patterns[patternID]
			matchEnd := i + 1
			matchStart := matchEnd - len(pattern)

			// Only accept if match starts at 'start'
			if matchStart != start {
				continue
			}

			match := &Match{
				PatternID: int(patternID),
				Start:     matchStart,
				End:       matchEnd,
			}

			if a.matchKind == LeftmostFirst {
				return match
			}

			if bestMatch == nil || match.Len() > bestMatch.Len() {
				bestMatch = match
			}
		}
	}

	return bestMatch
}

// IsMatch returns true if any pattern matches anywhere in the haystack.
// Optimized: inlined nextState for maximum performance.
func (a *Automaton) IsMatch(haystack []byte) bool {
	nfa := a.nfa
	state := nfa.startState
	bc := nfa.byteClasses
	states := nfa.states
	startState := nfa.startState

	for i := 0; i < len(haystack); i++ {
		class := bc.Get(haystack[i]) //nolint:gosec // G602: bounded by loop condition

		// Inlined nextState for performance
		state = a.advanceState(states, state, startState, class)

		if len(states[state].matches) > 0 {
			return true
		}
	}

	return false
}

// advanceState computes next state given current state and byte class.
// Inlined by compiler for hot path performance.
func (a *Automaton) advanceState(states []optState, state, startState StateID, class int) StateID {
	// Fast path for root state
	if state == startState {
		if next := states[state].trans[class]; next != 0 {
			return next
		}
		return startState
	}

	// Follow failure links for non-root states
	for {
		if next := states[state].trans[class]; next != 0 {
			return next
		}
		if state == startState {
			return startState
		}
		state = states[state].fail
	}
}

// FindAll returns all non-overlapping matches in the haystack.
// If n >= 0, at most n matches are returned.
func (a *Automaton) FindAll(haystack []byte, n int) []Match {
	var matches []Match
	pos := 0

	for pos < len(haystack) && (n < 0 || len(matches) < n) {
		match := a.Find(haystack, pos)
		if match == nil {
			break
		}

		matches = append(matches, *match)

		// Move past this match (non-overlapping)
		pos = match.End
		if pos <= match.Start {
			// Safety: ensure progress
			pos = match.Start + 1
		}
	}

	return matches
}

// FindAllOverlapping returns all overlapping matches in the haystack.
// This may return multiple matches at the same position.
func (a *Automaton) FindAllOverlapping(haystack []byte) []Match {
	var matches []Match
	state := a.nfa.startState

	for i, b := range haystack {
		state = a.nfa.nextState(state, b)

		if a.nfa.isMatch(state) {
			for _, patternID := range a.nfa.getMatches(state) {
				pattern := a.patterns[patternID]
				matchEnd := i + 1
				matchStart := matchEnd - len(pattern)

				matches = append(matches, Match{
					PatternID: int(patternID),
					Start:     matchStart,
					End:       matchEnd,
				})
			}
		}
	}

	return matches
}

// Count returns the number of non-overlapping matches in the haystack.
func (a *Automaton) Count(haystack []byte) int {
	count := 0
	pos := 0

	for pos < len(haystack) {
		match := a.Find(haystack, pos)
		if match == nil {
			break
		}
		count++
		pos = match.End
		if pos <= match.Start {
			pos = match.Start + 1
		}
	}

	return count
}

// PatternCount returns the number of patterns in the automaton.
func (a *Automaton) PatternCount() int {
	return len(a.patterns)
}

// Pattern returns the pattern bytes at the given index.
func (a *Automaton) Pattern(id int) []byte {
	if id < 0 || id >= len(a.patterns) {
		return nil
	}
	return a.patterns[id]
}

// StateCount returns the number of states in the underlying automaton.
// This is useful for debugging and performance analysis.
func (a *Automaton) StateCount() int {
	return a.nfa.stateCount()
}

// MatchKind returns the match semantics used by this automaton.
func (a *Automaton) MatchKind() MatchKind {
	return a.matchKind
}
