#!/bin/bash

# Automated Code Review and Fix Loop
# Reviews code as junior engineer, fixes issues, runs checks, repeats until clean

# Handle Ctrl+C gracefully - kill all child processes
cleanup() {
    echo -e "\n${YELLOW}⚠${NC}  Interrupted by user. Cleaning up..."
    # Kill all child processes
    pkill -P $$ 2>/dev/null || true
    exit 130
}

trap cleanup INT TERM

set -e

# ANSI colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
DIM='\033[2m'
NC='\033[0m'

MAX_ITERATIONS=${1:-5}
ITERATION=0
REPO_NAME=$(basename "$(git rev-parse --show-toplevel 2>/dev/null || echo 'repo')")
WORKTREE_BASE=~/source/${REPO_NAME}-worktrees
BRANCH_NAME="review-fix-$(date +%Y%m%d-%H%M%S)"

echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}> Code Review & Fix Loop${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""

# Check if we're already in a worktree
CURRENT_DIR=$(pwd)
if [[ "$CURRENT_DIR" =~ -worktrees/ ]]; then
    echo -e "${YELLOW}>${NC} Already in worktree: ${GREEN}$CURRENT_DIR${NC}"
    WORKTREE_PATH="$CURRENT_DIR"
else
    # Ask user: create new or use existing worktree?
    echo -e "${BLUE}>${NC} Worktree options:"
    echo "   1. Create new worktree"
    echo "   2. Use existing worktree"
    echo ""
    read -p "Choose (1 or 2): " choice

    case "$choice" in
        1)
            # Create new worktree
            echo -e "${BLUE}>${NC} Creating new worktree..."

            # Pull latest from main
            git checkout main 2>/dev/null || git checkout master 2>/dev/null
            git pull origin main 2>/dev/null || git pull origin master 2>/dev/null || true

            WORKTREE_PATH="${WORKTREE_BASE}/${BRANCH_NAME}"
            mkdir -p "$WORKTREE_BASE"

            git worktree add -b "$BRANCH_NAME" "$WORKTREE_PATH"
            echo -e "${GREEN}✓${NC} Created worktree: ${GREEN}$WORKTREE_PATH${NC}"
            ;;
        2)
            # Use existing worktree
            echo -e "${BLUE}>${NC} Available worktrees:"
            git worktree list
            echo ""
            read -p "Enter worktree path: " WORKTREE_PATH

            if [ ! -d "$WORKTREE_PATH" ]; then
                echo -e "${RED}x${NC} Worktree not found: $WORKTREE_PATH"
                exit 1
            fi
            echo -e "${GREEN}✓${NC} Using existing worktree: ${GREEN}$WORKTREE_PATH${NC}"
            ;;
        *)
            echo -e "${RED}x${NC} Invalid choice"
            exit 1
            ;;
    esac
fi

cd "$WORKTREE_PATH"
echo -e "${BLUE}>${NC} Working in: ${DIM}$WORKTREE_PATH${NC}"
echo ""

# Ensure bots directory exists
mkdir -p bots

# Main review-fix loop
while [ $ITERATION -lt $MAX_ITERATIONS ]; do
    ITERATION=$((ITERATION + 1))

    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}> Iteration $ITERATION of $MAX_ITERATIONS${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""

    # Step 1: Code Review
    echo -e "${BLUE}> Step 1: Code Review (as junior engineer)${NC}"
    stdbuf -o0 -e0 claude --permission-mode bypassPermissions --print "Review the code as a junior engineer looking for bugs, unclear logic, edge cases, and potential issues. Write your findings to bots/review.md, replacing any existing content. For each bug found, note that a test must be created or updated to cover it. If no findings, create an empty file. Do NOT check in or commit anything. Ignore any commit hooks." || \
    claude --permission-mode bypassPermissions --print "Review the code as a junior engineer looking for bugs, unclear logic, edge cases, and potential issues. Write your findings to bots/review.md, replacing any existing content. For each bug found, note that a test must be created or updated to cover it. If no findings, create an empty file. Do NOT check in or commit anything. Ignore any commit hooks."
    echo ""

    # Check if review.md exists and has content
    if [ ! -f bots/review.md ]; then
        echo -e "${RED}x${NC} bots/review.md not found - creating empty file"
        touch bots/review.md
    fi

    REVIEW_SIZE=$(wc -c < bots/review.md | tr -d ' ')

    if [ "$REVIEW_SIZE" -eq 0 ] || [ "$REVIEW_SIZE" -lt 10 ]; then
        echo -e "${GREEN}✓${NC} Review complete - no issues found!"
        echo ""
        break
    fi

    echo -e "${YELLOW}⚠${NC}  Found issues (${REVIEW_SIZE} bytes)"
    echo -e "${DIM}$(head -20 bots/review.md)${NC}"
    echo ""

    # Step 2: Fix Issues
    echo -e "${BLUE}> Step 2: Implementing Fixes${NC}"
    stdbuf -o0 -e0 claude --permission-mode bypassPermissions --print "Read bots/review.md and implement ALL the fixes mentioned. CRITICAL: For every bug you fix, you MUST create or edit an existing test to cover that bug. The test should fail before the fix and pass after the fix. Verify the test passes after implementing the fix. Do NOT check in or commit anything." || \
    claude --permission-mode bypassPermissions --print "Read bots/review.md and implement ALL the fixes mentioned. CRITICAL: For every bug you fix, you MUST create or edit an existing test to cover that bug. The test should fail before the fix and pass after the fix. Verify the test passes after implementing the fix. Do NOT check in or commit anything."
    echo ""

    # Step 3: Run Checks and Fix
    echo -e "${BLUE}> Step 3: Running Checks (fmt, lint, tests, complexity)${NC}"
    stdbuf -o0 -e0 claude --permission-mode bypassPermissions --print "Run the following checks and fix any issues found:
1. go fmt ./...
2. golangci-lint run
3. go test ./... (ALL tests must pass)
4. gocyclo -over 40 .

Fix any issues found. IMPORTANT: All tests must pass before proceeding. Do NOT check in or commit anything." || \
    claude --permission-mode bypassPermissions --print "Run the following checks and fix any issues found:
1. go fmt ./...
2. golangci-lint run
3. go test ./... (ALL tests must pass)
4. gocyclo -over 40 .

Fix any issues found. IMPORTANT: All tests must pass before proceeding. Do NOT check in or commit anything."
    echo ""

    sleep 2
done

echo ""
echo -e "${CYAN}========================================${NC}"
if [ $ITERATION -ge $MAX_ITERATIONS ]; then
    echo -e "${YELLOW}⚠${NC}  Max iterations reached"
    echo "   Review may still have issues"
else
    echo -e "${GREEN}done${NC} Review-fix loop complete!"
    echo "   All issues resolved"
fi
echo ""
echo -e "${BLUE}>${NC} Working directory: ${GREEN}$WORKTREE_PATH${NC}"
echo -e "${BLUE}>${NC} Review file: ${DIM}bots/review.md${NC}"
echo -e "${CYAN}========================================${NC}"
