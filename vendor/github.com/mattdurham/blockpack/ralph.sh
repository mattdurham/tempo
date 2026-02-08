#!/bin/bash

# Ralph Loop - Autonomous engineer relay
# Usage: ./ralph.sh [bead_id|description] [iterations]
# Examples:
#   ./ralph.sh                    # Auto-pick beads, max 10 iterations
#   ./ralph.sh 10                 # Auto-pick beads, max 10 iterations
#   ./ralph.sh BP-123             # Work on specific bead BP-123
#   ./ralph.sh BP-123 5           # Work on BP-123, max 5 iterations
#   ./ralph.sh "login bug"        # Search for bead matching "login bug"
#   ./ralph.sh "fix auth" 5       # Search for "fix auth", max 5 iterations

set -e

# ANSI colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
DIM='\033[2m'
NC='\033[0m' # No colour

# Parse arguments
BEAD_ID=""
BEAD_DESCRIPTION=""
MAX_ITERATIONS=10

if [ $# -eq 0 ]; then
    # No args: auto-pick, default iterations
    MAX_ITERATIONS=10
elif [ $# -eq 1 ]; then
    # One arg: could be bead ID, description, or iterations
    if [[ "$1" =~ ^[0-9]+$ ]]; then
        # Numeric: iterations count
        MAX_ITERATIONS=$1
    elif [[ "$1" =~ ^[a-zA-Z0-9_-]+-[a-zA-Z0-9]+$ ]]; then
        # Bead ID format (e.g., blockpack-jpr, BP-123)
        BEAD_ID="$1"
        MAX_ITERATIONS=10
    else
        # Text description
        BEAD_DESCRIPTION="$1"
        MAX_ITERATIONS=10
    fi
elif [ $# -eq 2 ]; then
    # Two args: (bead ID or description) and iterations
    if [[ "$2" =~ ^[0-9]+$ ]]; then
        # Second arg is iterations
        if [[ "$1" =~ ^[a-zA-Z0-9_-]+-[a-zA-Z0-9]+$ ]]; then
            BEAD_ID="$1"
        else
            BEAD_DESCRIPTION="$1"
        fi
        MAX_ITERATIONS=$2
    else
        # Treat as description with text iterations (shouldn't happen)
        BEAD_DESCRIPTION="$1"
        MAX_ITERATIONS=10
    fi
fi

ITERATION=0
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$PROJECT_DIR"

echo -e "${CYAN}>${NC} Starting Ralph Loop in $PROJECT_DIR"

# If description provided, search for matching bead
if [ -n "$BEAD_DESCRIPTION" ]; then
    echo -e "${YELLOW}>${NC} Searching for bead matching: ${DIM}$BEAD_DESCRIPTION${NC}"

    # Get all ready beads and search by description
    MATCHING_BEADS=$(bd list --status open 2>/dev/null | grep -i "$BEAD_DESCRIPTION" || echo "")

    if [ -z "$MATCHING_BEADS" ]; then
        echo -e "${RED}x${NC} No beads found matching '$BEAD_DESCRIPTION'"
        echo "   Try running: bd list --status open"
        exit 1
    fi

    # Get first matching bead ID (format: prefix-id where id can be alphanumeric)
    # Example: "○ blockpack-jpr [● P0]" -> extract "blockpack-jpr"
    BEAD_ID=$(echo "$MATCHING_BEADS" | head -1 | grep -oE '[a-zA-Z0-9_-]+-[a-zA-Z0-9]+' | head -1)

    if [ -z "$BEAD_ID" ]; then
        echo -e "${RED}x${NC} Could not extract bead ID from matches:"
        echo "$MATCHING_BEADS"
        exit 1
    fi

    echo -e "${GREEN}✓${NC} Found matching bead: ${GREEN}$BEAD_ID${NC}"
    echo ""
fi

if [ -n "$BEAD_ID" ]; then
    echo "   Target bead: ${GREEN}$BEAD_ID${NC}"
fi
echo "   Max iterations: $MAX_ITERATIONS"
echo ""

# Check beads are ready
if ! command -v bd &> /dev/null; then
    echo -e "${RED}x${NC} bd (beads) not found. Install it first."
    exit 1
fi

# Show initial state
echo -e "${BLUE}>${NC} Current beads status:"
bd ready 2>/dev/null || echo "   No beads ready or bd not initialised"
echo ""

while [ $ITERATION -lt $MAX_ITERATIONS ]; do
    # Check for dirty state - if dirty, skip fetch/pull (we're mid-work)
    if git diff --quiet && git diff --cached --quiet; then
        echo -e "${DIM}> Fetching latest changes...${NC}"
        git fetch --quiet
        git pull --rebase --quiet || true
        bd sync 2>/dev/null || true
    else
        echo -e "${YELLOW}> Dirty working tree detected - resuming previous work...${NC}"
    fi

    # Check if there are any beads to work on
    READY_COUNT=$(bd count --status open 2>/dev/null || echo "0")
    IN_PROGRESS=$(bd count --status in_progress 2>/dev/null || echo "0")

    if [ "$READY_COUNT" = "0" ] && [ "$IN_PROGRESS" = "0" ]; then
        echo -e "${DIM}o No beads available. Waiting 20s for new work...${NC}"
        sleep 20
        continue
    fi

    ITERATION=$((ITERATION + 1))
    echo -e "${CYAN}======================================================${NC}"
    echo -e "${CYAN}>${NC} Ralph iteration ${GREEN}$ITERATION${NC} of $MAX_ITERATIONS"
    echo "   Started: $(date '+%Y-%m-%d %H:%M:%S')"
    echo -e "${CYAN}======================================================${NC}"
    echo ""

    echo -e "${BLUE}> Spawning Claude engineer...${NC}"
    echo ""

    # Build Claude prompt
    if [ -n "$BEAD_ID" ]; then
        CLAUDE_PROMPT="Read @RALPH.md and follow the instructions. Work on bead ${BEAD_ID}. Complete ONE bead."
    else
        CLAUDE_PROMPT="Read @RALPH.md and follow the instructions. Pick up where the last engineer left off. Complete ONE bead."
    fi

    # Stream output with clean formatting
    # Note: Most of this complexity is parsing Claude's streaming JSON
    # to show what Ralph is doing. The -p flag doesn't give verbose output.
    claude --permission-mode acceptEdits --verbose --print "$CLAUDE_PROMPT" --output-format stream-json | while read -r line; do
        type=$(echo "$line" | jq -r '.type // empty' 2>/dev/null)
        if [ "$type" = "assistant" ]; then
            # Show text
            echo "$line" | jq -r '.message.content[]? | select(.type == "text") | .text' 2>/dev/null | while IFS= read -r text; do
                [ -z "$text" ] && continue
                echo -e "${BLUE}>${NC} $text"
            done
            # Show tool calls concisely: > tool_name { inputs }
            echo "$line" | jq -c '.message.content[]? | select(.type == "tool_use")' 2>/dev/null | while read -r tool; do
                [ -z "$tool" ] && continue
                name=$(echo "$tool" | jq -r '.name' 2>/dev/null)
                input=$(echo "$tool" | jq -c '.input' 2>/dev/null)
                echo -e "${YELLOW}>${NC} ${CYAN}$name${NC} ${DIM}$input${NC}"
            done
        elif [ "$type" = "user" ]; then
            # Show tool results cleanly
            echo "$line" | jq -c '.message.content[]? | select(.type == "tool_result")' 2>/dev/null | while read -r result; do
                [ -z "$result" ] && continue
                is_error=$(echo "$result" | jq -r '.is_error // false' 2>/dev/null)
                # Extract and clean content
                content=$(echo "$result" | jq -r '
                    .content |
                    if type == "array" then
                        map(select(.type == "text") | .text) | join("\n")
                    elif type == "string" then
                        .
                    else
                        "..."
                    end
                ' 2>/dev/null | tr -d '\r' | head -n 20)
                # Truncate if contains base64 image data
                if echo "$content" | grep -q '/9j/4AAQ\|data:image'; then
                    content="[image captured]"
                fi
                # Format line numbers
                formatted=$(echo "$content" | sed -E "s/^([[:space:]]*[0-9]+)>/\x1b[2m\1\x1b[0m  /")
                if [ "$is_error" = "true" ]; then
                    echo ""
                    echo -e "${RED}x${NC}"
                    echo -e "$formatted"
                else
                    echo ""
                    echo -e "${DIM}o${NC}"
                    echo -e "$formatted"
                fi
            done
        elif [ "$type" != "system" ]; then
            echo -e "${DIM}? $line${NC}"
        fi
    done

    echo ""
    echo -e "${GREEN}ok${NC} Iteration $ITERATION complete"
    echo ""
    sleep 2
done

echo ""
echo -e "${CYAN}======================================================${NC}"
echo -e "${GREEN}done${NC} Ralph loop finished"
echo "   Total iterations: $ITERATION"
echo "   Ended: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo -e "${BLUE}>${NC} Final beads status:"
bd ready 2>/dev/null || echo "   No beads ready"
echo -e "${CYAN}======================================================${NC}"
