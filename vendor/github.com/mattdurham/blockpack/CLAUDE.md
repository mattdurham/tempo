# Claude Code Configuration

## Behavioral Rules (Always Enforced)

- Do what has been asked; nothing more, nothing less
- NEVER create files unless they're absolutely necessary for achieving your goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (*.md) or README files unless explicitly requested
- NEVER save working files, text/mds, or tests to the root folder
- ALWAYS read a file before editing it
- NEVER commit secrets, credentials, or .env files

## Code Quality & Pre-Commit Checks (STRICT ENFORCEMENT)

- **ALWAYS** run `make precommit` before committing changes
- This runs all quality checks: gofumpt, golines, golangci-lint, nilaway, betteralign, gocyclo, tests, coverage
- **NEVER** commit if `make precommit` fails
- CI will enforce these same checks - precommit ensures you catch issues locally first
- All checks are blocking - zero tolerance for violations

## Quality Standards

- **Cyclomatic complexity:** < 40 (enforced by gocyclo and golangci-lint)
- **Test coverage:** > 70% (enforced in CI)
- **Formatting:** gofumpt (stricter than gofmt)
- **Line length:** 120 characters (enforced by golines)
- **Nil safety:** nilaway checks for nil pointer dereferences
- **Memory efficiency:** betteralign checks for struct field alignment
- **Linting:** 40+ linters via golangci-lint (.golangci.yml)

## Build & Test

```bash
# Build
make build

# Test
make test

# Lint
make lint

# Run all pre-commit checks (REQUIRED before committing)
make precommit

# Run full CI pipeline locally
make ci

# Auto-fix formatting issues
make format-all

# Individual checks
make lint              # golangci-lint
make nilaway           # Nil safety checks
make betteralign       # Struct alignment checks
make gofumpt-check     # Format check
make golines-check     # Line length check
```

## Tool Installation

```bash
# Install all code quality tools
go install go.uber.org/nilaway/cmd/nilaway@latest
go install github.com/dkorunic/betteralign/cmd/betteralign@latest
go install mvdan.cc/gofumpt@latest
go install github.com/segmentio/golines@latest
go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
```

## Project Architecture

- Follow Domain-Driven Design with bounded contexts
- Keep files under 500 lines
- Use typed interfaces for all public APIs
- Ensure input validation at system boundaries
- Keep functions small (< 40 cyclomatic complexity)

## Security Rules

- NEVER hardcode API keys, secrets, or credentials in source files
- NEVER commit .env files or any file containing secrets
- Always validate user input at system boundaries
- Always sanitize file paths to prevent directory traversal

## Concurrency Best Practices

- All operations should be concurrent/parallel when possible
- Batch related operations in a single message
- Batch all file reads/writes/edits together
- Batch all Bash commands together when they don't depend on each other
