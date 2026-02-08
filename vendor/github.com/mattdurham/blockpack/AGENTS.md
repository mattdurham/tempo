# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Development Workflow

**At the start of ANY work session (before touching code):**

1. **Create a git worktree for isolation** - Do this FIRST, before any implementation
   - Use the `using-git-worktrees` skill to set up isolated workspace
   - All work for the session goes in this worktree (features, tests, docs, improvements)
   - Prevents conflicts with other work
   - Allows easy review and cleanup
   - Keeps main branch clean

   **Examples of what goes in the same worktree:**
   - Feature implementation
   - Tests and benchmarks
   - Documentation updates
   - Related bug fixes discovered during work
   - AGENTS.md or other meta-documentation improvements

2. **Exception**: Only work directly on main for:
   - Emergency hotfixes that must go out immediately
   - Trivial typo fixes (1-2 character changes)

All changes should be committed in a single PR. One worktree to on PR. Subagents are allowed work in different worktrees but they need to be merged into one before PR creation.

**Remember:** Create the worktree at the START of your session, not separately for each ticket.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
```

## Test Data

**Location**: `~/source/files/raw.pb` (532MB Tempo protobuf traces)

**Regenerate test files**:

```bash
make test-data        # Regenerate all test files from raw.pb
make blockpack-data   # Regenerate blockpack file (cold path)
make blockpack-hot    # Generate blockpack with precomputed metric streams
make parquet-data     # Regenerate parquet file
```

**Check if test files exist**:

```bash
make check-test-data  # Verifies presence of required test files
```

Tests requiring real data will skip if files are missing. Generate them before running integration tests.

## Code Structure Analysis (blockgraph.dag)

**Purpose**: The blockgraph.dag file provides a visual representation of the codebase's call graph and dependencies, helping agents quickly understand system architecture and make informed decisions.

**Check if blockgraph needs regeneration**:

```bash
# Check if blockgraph.dag is up to date with current commit
git diff HEAD -- blockgraph.dag
```

**Regenerate blockgraph**:

```bash
# Generate fresh dependency graph from api.go
./codepath-dag -file=api.go -o blockpath.dag
```

**When to regenerate**:
- After checking out a new branch or pulling changes
- Before starting work on a new feature or bug fix
- If the blockgraph.dag file doesn't exist or is stale
- After making significant changes to api.go or core files

**Using the blockgraph for decision-making**:

1. **Load into memory at session start** - Parse the .dag file to understand:
   - Entry points and API surfaces
   - Function call chains and dependencies
   - Module boundaries and coupling
   - Critical paths and hot spots

2. **Quick decision-making** - Use the graph to:
   - Identify which files need modification for a feature
   - Understand impact radius of changes
   - Find related code that may need updates
   - Spot potential circular dependencies
   - Plan refactoring with minimal blast radius

3. **Integration with bd issues** - When claiming work:
   ```bash
   # 1. Regenerate graph if needed
   ./codepath-dag -file=api.go -o blockpath.dag

   # 2. Review the graph to understand affected areas
   cat blockpath.dag  # or use visualization tool

   # 3. Claim the issue with informed understanding
   bd update <id> --status in_progress
   ```

**Best practices**:
- Always check blockgraph currency before major refactoring
- Use the graph to validate test coverage (all paths exercised?)
- Reference the graph when writing architectural documentation
- Keep blockpath.dag in version control for team visibility

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Single command to verify everything:

   ```bash
   make precommit      # Runs fmt, lint, build, cyclo check, tests, and coverage
   ```

   **Quality requirements:**
   - All tests must pass (including unrelated tests)
   - Cyclomatic complexity for non-test Go code (`*.go` excluding `*_test.go`) must not exceed 40 for any function
   - No lint errors
   - Code must be properly formatted
   - Project must build successfully
3. **Update issue status** - Close finished work, update in-progress items
4. **Create PR and iterate on feedback** - When work is ready for review:

   ```bash
   gh pr create --title "..." --body "..."
   ```

   **After creating PR:**
   - Monitor the PR for feedback from reviewers (especially Copilot)
   - When feedback is received, address ALL comments
   - After addressing feedback, add a comment: `@copilot please review again`
   - Repeat this cycle until Copilot (and other reviewers) have no more feedback
   - Do NOT merge until all feedback is resolved
5. **PUSH TO REMOTE** - This is MANDATORY:

   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```

6. **Clean up** - Clear stashes, prune remote branches
7. **Verify** - All changes committed AND pushed
8. **Hand off** - Provide context for next session

**CRITICAL RULES:**

- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

Use 'bd' for task tracking

<!-- BEGIN BEADS INTEGRATION -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Auto-syncs to JSONL for version control
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update bd-42 --status in_progress --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task**: `bd update <id> --status in_progress`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Auto-Sync

bd automatically syncs with git:

- Exports to `.beads/issues.jsonl` after changes (5s debounce)
- Imports from JSONL when newer (e.g., after `git pull`)
- No manual export/import needed!

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

<!-- END BEADS INTEGRATION -->
