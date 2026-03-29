# Task: Evaluate whether executor should be a state machine

**Status:** Pending
**Created:** 2026-02-15

## Description

Investigate whether the query executor (`internal/executor/`) would benefit from being restructured as a state machine pattern.

## Context

The executor package is the 2nd largest in the codebase (100+ files) with multiple implementations (blockpack, folder, single-file). The current execution flow is: compile query -> extract predicates -> select blocks -> scan columns -> evaluate bytecode -> return results.

## Investigation Areas

1. Map the current execution stages and transitions in the executor
2. Identify if there are implicit states that would be clearer as explicit state machine states
3. Evaluate whether a state machine would simplify: error handling, early-exit logic, streaming results, cancellation
4. Consider impact on the existing TraceQL structural executor and column scanner
5. Assess complexity cost vs clarity benefit — is the current approach already sufficient?
6. Look at how the executor handles partial failures, retries, and block-level transitions
7. Consider whether Phase 4 streaming callbacks (REQ-013, REQ-014) would be easier with a state machine

## Expected Output

A recommendation (yes/no/partial) with rationale and, if yes, a sketch of the proposed states and transitions.
