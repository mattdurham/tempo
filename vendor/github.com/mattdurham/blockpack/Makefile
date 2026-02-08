.PHONY: test-data clean-test-data help bench bench-report serve-report cost-analysis cost-analysis-no-freetier build-cost-tool build test format lint clean format-benchmark precommit

# Source data file
RAW_PB ?= /home/matt/source/files/raw.pb

# Package list (excluding /bots) - lazy evaluation to avoid running on every make invocation
PACKAGES = $(shell go list ./... | grep -v '/bots')

# Generated test files
BLOCKPACK_FILE := traces.blockpack
BLOCKPACK_HOT := traces-hot.blockpack
BLOCKPACK_COLD := traces-cold.blockpack
PARQUET_DIR := traces.parquet

# Benchmark outputs
BENCH_OUTPUT := benchmark/unified.txt
BENCH_HTML := benchmark/unified_report.html
COMPARE_OUTPUT := benchmark/format_comparison.txt
COMPARE_HTML := benchmark/format_comparison.html
BLOCKPACK_ANALYSIS := benchmark/traces.blockpack.analysis.html

# Cost analysis tool
COST_TOOL := benchmark-cost-analysis
COST_OUTPUT := benchmark/cost_analysis.txt

help:
	@echo "Build targets:"
	@echo "  make build            - Build all binaries to ./bin/"
	@echo "  make clean            - Remove all build artifacts and test data"
	@echo ""
	@echo "Development targets:"
	@echo "  make test             - Run all unit tests"
	@echo "  make format           - Format all Go code with go fmt"
	@echo "  make lint             - Run golangci-lint"
	@echo "  make precommit        - Run build and all quality checks (build, fmt, lint, cyclo, tests, coverage)"
	@echo "  make format-benchmark - Run benchmarks and check for regressions"
	@echo ""
	@echo "Test data generation targets:"
	@echo "  make test-data        - Regenerate all test files from raw.pb"
	@echo "  make blockpack-data   - Regenerate blockpack file (cold path)"
	@echo "  make blockpack-hot    - Generate blockpack with precomputed metric streams"
	@echo "  make blockpack-cold   - Generate blockpack without precomputed metric streams"
	@echo "  make blockpack-both   - Generate both hot and cold blockpack files"
	@echo "  make parquet-data     - Regenerate parquet file"
	@echo "  make clean-test-data  - Remove all generated test files"
	@echo "  make check-test-data  - Check if test files are up to date"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  make bench            - Run all benchmarks (real-world + format comparison)"
	@echo "  make bench-report     - Generate unified HTML report from benchmarks"
	@echo "  make serve-report     - Start HTTP server to view benchmark reports (port 8000)"
	@echo ""
	@echo "Cost analysis targets:"
	@echo "  make cost-analysis          - Analyze benchmark costs (1000x scale, NO free tier)"
	@echo "  make cost-analysis-freetier - Analyze benchmark costs (1000x scale, with free tier)"
	@echo "  make build-cost-tool        - Build the cost analysis tool"

# Build all binaries
build:
	@echo "==> Building all binaries..."
	@mkdir -p bin
	@go build -o bin/analyze ./cmd/analyze
	@go build -o bin/analyze-pb ./cmd/analyze-pb
	@go build -o bin/ingest-sorted ./cmd/ingest-sorted
	@go build -o bin/temposerver ./cmd/temposerver
	@go build -o bin/convert-parquet-folder ./cmd/convert-parquet-folder
	@go build -o bin/deep-compare ./cmd/deep-compare
	@go build -o bin/generate-unified-report ./cmd/generate_unified_report
	@go build -o bin/inspect-parquet-schema ./cmd/inspect-parquet-schema
	@go build -o bin/inspect-web ./cmd/inspect-web
	@echo "==> Binaries built in ./bin/"

# Run all tests
test:
	@echo "==> Running unit tests..."
	@go test -v -race -timeout=10m $(PACKAGES)
	@echo "==> All tests passed!"

# Format all Go code
format:
	@echo "==> Formatting Go code..."
	@go fmt $(PACKAGES)
	@echo "==> Code formatted!"

# Run linters
lint:
	@echo "==> Running golangci-lint..."
	@golangci-lint run $$(echo "$(PACKAGES)" | sed 's|github.com/mattdurham/blockpack/||g')
	@echo "==> Linting complete!"

# Run all pre-commit quality checks
precommit:
	@echo "==> Running pre-commit quality checks..."
	@echo ""
	@echo "[1/6] Formatting code..."
	@$(MAKE) format
	@echo "✅ go fmt: clean"
	@echo ""
	@echo "[2/6] Running golangci-lint..."
	@$(MAKE) lint
	@echo "✅ golangci-lint: clean"
	@echo ""
	@echo "[3/6] Building project..."
	@go build $(PACKAGES)
	@echo "✅ build: successful"
	@echo ""
	@echo "[4/6] Checking cyclomatic complexity..."
	@output=$$(find . -name '*.go' -not -path './bots/*' ! -name '*_test.go' -print0 | xargs -0 go run github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0 -over 40 2>&1); \
	status=$$?; \
	if [ $$status -ne 0 ]; then \
		echo "❌ gocyclo failed (exit $$status)"; \
		echo "$$output"; \
		exit 1; \
	fi; \
	if echo "$$output" | grep -q -v '^$$'; then \
		echo "❌ Found functions with cyclomatic complexity > 40 (non-test code only)"; \
		echo "$$output"; \
		exit 1; \
	fi
	@echo "✅ cyclomatic complexity: all < 40 (non-test code)"
	@echo ""
	@echo "[5/6] Running tests..."
	@$(MAKE) test
	@echo "✅ tests: all passing"
	@echo ""
	@echo "[6/6] Checking code coverage..."
	@coverage_file=$$(mktemp); \
	go_test_output=$$(mktemp); \
	go test -cover $(PACKAGES) >$$go_test_output 2>&1; \
	go_test_status=$$?; \
	cat $$go_test_output | tee $$coverage_file | grep -E '^(ok|\?)' | awk '{if ($$1 == "ok") print $$2 ": " $$5; else print $$2 ": [no test files]"}'; \
	rm -f $$go_test_output; \
	if [ $$go_test_status -ne 0 ]; then \
		echo "❌ Coverage check failed (go test exited with $$go_test_status)"; \
		rm -f $$coverage_file; \
		exit 1; \
	fi; \
	if ! grep -q "^ok" $$coverage_file; then \
		echo "❌ Coverage check failed (no successful tests)"; \
		rm -f $$coverage_file; \
		exit 1; \
	fi; \
	rm -f $$coverage_file
	@echo ""
	@echo "✅ All pre-commit checks passed!"
	@echo "✅ Ready to commit"

# Clean all build artifacts
clean: clean-test-data
	@echo "==> Cleaning build artifacts..."
	@rm -rf bin/
	@rm -f $(BENCH_OUTPUT) $(BENCH_HTML) $(BLOCKPACK_ANALYSIS)
	@echo "==> Cleaned!"

# Run benchmarks and check for performance regressions
format-benchmark: bench
	@echo "==> Checking for performance regressions..."
	@echo "==> Review $(BENCH_OUTPUT) for any unexpected slowdowns"
	@echo "==> Note: This target runs benchmarks but does not auto-fail on regressions"
	@echo "==> Manual review required"

# Check if raw.pb exists
$(RAW_PB):
	@echo "Error: $(RAW_PB) not found. Please provide raw.pb file."
	@exit 1

# Generate blockpack file (default: without precomputed metric streams)
blockpack-data: $(RAW_PB)
	@echo "==> Generating blockpack file (cold path)..."
	@go run ./cmd/ingest-sorted/main.go $(RAW_PB) $(BLOCKPACK_FILE)
	@echo "==> Created $(BLOCKPACK_FILE) ($(shell ls -lh $(BLOCKPACK_FILE) | awk '{print $$5}'))"

# Generate blockpack file with precomputed metric streams (hot path)
blockpack-hot: $(RAW_PB)
	@echo "==> Generating blockpack file with precomputed metric streams (hot path)..."
	@go run ./cmd/ingest-sorted/main.go --precompute-metric-streams $(RAW_PB) $(BLOCKPACK_HOT)
	@echo "==> Created $(BLOCKPACK_HOT) ($(shell ls -lh $(BLOCKPACK_HOT) | awk '{print $$5}'))"

# Generate blockpack file without precomputed metric streams (cold path)
blockpack-cold: $(RAW_PB)
	@echo "==> Generating blockpack file without precomputed metric streams (cold path)..."
	@go run ./cmd/ingest-sorted/main.go $(RAW_PB) $(BLOCKPACK_COLD)
	@echo "==> Created $(BLOCKPACK_COLD) ($(shell ls -lh $(BLOCKPACK_COLD) | awk '{print $$5}'))"

# Generate both hot and cold blockpack files
blockpack-both: blockpack-hot blockpack-cold
	@echo "==> Generated both hot and cold blockpack files"

# Generate parquet file
parquet-data: $(RAW_PB)
	@echo "==> Generating parquet file..."
	@rm -rf $(PARQUET_DIR)
	@go run ./cmd/analyze-pb/main.go --input=$(RAW_PB) --parquet-output=$(PARQUET_DIR) --blockpack-output=/tmp/.temp_blockpack_$$$$.blockpack --md-output=/dev/null
	@rm -f /tmp/.temp_blockpack_*.blockpack 2>/dev/null || true
	@echo "==> Created $(PARQUET_DIR) ($(shell du -sh $(PARQUET_DIR) | cut -f1))"

# Generate all test data
test-data: $(RAW_PB) blockpack-data parquet-data
	@echo ""
	@echo "==> All test data regenerated successfully!"
	@echo ""
	@$(MAKE) check-test-data

# Clean generated test files
clean-test-data:
	@echo "==> Removing generated test files..."
	@rm -f $(BLOCKPACK_FILE) $(BLOCKPACK_HOT) $(BLOCKPACK_COLD)
	@rm -rf $(PARQUET_DIR)
	@echo "==> Cleaned test data"

# Check if test files are up to date
check-test-data:
	@echo "==> Checking test data files..."
	@if [ ! -f $(RAW_PB) ]; then \
		echo "  ❌ $(RAW_PB) not found"; \
		exit 1; \
	fi
	@echo "  ✓ $(RAW_PB) exists ($(shell ls -lh $(RAW_PB) | awk '{print $$5}'))"
	@if [ ! -f $(BLOCKPACK_FILE) ]; then \
		echo "  ❌ $(BLOCKPACK_FILE) not found - run 'make blockpack-data'"; \
	elif [ $(RAW_PB) -nt $(BLOCKPACK_FILE) ]; then \
		echo "  ⚠️  $(BLOCKPACK_FILE) is older than raw.pb - run 'make blockpack-data'"; \
	else \
		echo "  ✓ $(BLOCKPACK_FILE) exists ($(shell ls -lh $(BLOCKPACK_FILE) | awk '{print $$5}'))"; \
	fi
	@if [ ! -d $(PARQUET_DIR) ]; then \
		echo "  ❌ $(PARQUET_DIR) not found - run 'make parquet-data'"; \
	elif [ $(RAW_PB) -nt $(PARQUET_DIR) ]; then \
		echo "  ⚠️  $(PARQUET_DIR) is older than raw.pb - run 'make parquet-data'"; \
	else \
		echo "  ✓ $(PARQUET_DIR) exists ($(shell du -sh $(PARQUET_DIR) | cut -f1))"; \
	fi

# Run all benchmarks and generate report
bench-all: bench bench-report

# Run all benchmarks (real-world + format comparison)
bench:
	@echo "==> Running real-world benchmarks (blockpack only)..."
	@mkdir -p benchmark
	@go test -run=^$$ -bench=BenchmarkRealWorldQueries$$ -benchmem -timeout=5m \
		github.com/mattdurham/blockpack/benchmark 2>&1 | tee $(BENCH_OUTPUT)
	@echo ""
	@echo "==> Running limited query benchmarks..."
	@go test -run=^$$ -bench=BenchmarkRealWorldQueriesLimited -benchmem -timeout=5m \
		github.com/mattdurham/blockpack/benchmark 2>&1 | tee -a $(BENCH_OUTPUT)
	@echo ""
	@echo "==> Running trace-by-ID benchmark..."
	@go test -run=^$$ -bench=BenchmarkGetByTraceID -benchmem -timeout=5m \
		github.com/mattdurham/blockpack/benchmark 2>&1 | tee -a $(BENCH_OUTPUT)
	@echo ""
	@echo "==> Running aggregation comparison benchmarks (aggregation vs manual vs parquet)..."
	@go test -run=^$$ -bench=BenchmarkAggregationComparison -benchmem -timeout=30m \
		github.com/mattdurham/blockpack/benchmark 2>&1 | tee -a $(BENCH_OUTPUT)
	@echo ""
	@echo "==> Running format comparison benchmarks (blockpack vs parquet)..."
	@go test -run=^$$ -bench=BenchmarkFormatComparison -benchmem -timeout=30m \
		github.com/mattdurham/blockpack/benchmark 2>&1 | tee -a $(BENCH_OUTPUT)
	@echo ""
	@echo "==> All benchmark results saved to $(BENCH_OUTPUT)"
	@echo "==> Run 'make bench-report' to generate HTML report"

# Generate HTML report from benchmark results
bench-report: $(BENCH_OUTPUT)
	@echo "==> Generating blockpack structure analysis..."
	@if [ -f $(BLOCKPACK_FILE) ]; then \
		go run ./cmd/analyze/main.go --input=$(BLOCKPACK_FILE) --html --output=$(BLOCKPACK_ANALYSIS); \
		echo "==> Blockpack analysis: $(BLOCKPACK_ANALYSIS)"; \
	else \
		echo "==> Warning: $(BLOCKPACK_FILE) not found, skipping analysis"; \
	fi
	@echo "==> Generating unified HTML report..."
	@cat $(BENCH_OUTPUT) | go run ./cmd/generate_unified_report --blockpack-analysis=$(BLOCKPACK_ANALYSIS) > $(BENCH_HTML)
	@echo "==> HTML report generated: $(BENCH_HTML)"
	@echo "==> Open with: open $(BENCH_HTML) or xdg-open $(BENCH_HTML)"

$(BENCH_OUTPUT):
	@echo "Error: $(BENCH_OUTPUT) not found. Run 'make bench' first."
	@exit 1

# Serve benchmark reports via HTTP server
serve-report:
	@if [ ! -f $(BENCH_HTML) ]; then \
		echo "Error: $(BENCH_HTML) not found. Run 'make bench-report' first."; \
		exit 1; \
	fi
	@echo "==> Starting HTTP server on http://localhost:8000"
	@echo "==> Unified report: http://localhost:8000/unified_report.html"
	@echo "==> Blockpack analysis: http://localhost:8000/traces.blockpack.analysis.html"
	@echo "==> Press Ctrl+C to stop"
	@cd benchmark && python3 -m http.server 8000

# Build cost analysis tool
build-cost-tool:
	@echo "==> Building cost analysis tool..."
	@go build -o $(COST_TOOL) ./cmd/benchmark-cost-analysis
	@echo "==> Built $(COST_TOOL)"

# Analyze benchmark costs without free tier (default - more realistic)
cost-analysis: build-cost-tool $(BENCH_OUTPUT)
	@echo "==> Analyzing benchmark costs (1000x scale, NO free tier)..."
	@cat $(BENCH_OUTPUT) | ./$(COST_TOOL) -scale=1000 | tee $(COST_OUTPUT)
	@echo ""
	@echo "==> Cost analysis saved to $(COST_OUTPUT)"

# Analyze benchmark costs with free tier
cost-analysis-freetier: build-cost-tool $(BENCH_OUTPUT)
	@echo "==> Analyzing benchmark costs (1000x scale, with AWS free tier)..."
	@cat $(BENCH_OUTPUT) | ./$(COST_TOOL) -scale=1000 -free-tier=true | tee $(COST_OUTPUT)
	@echo ""
	@echo "==> Cost analysis saved to $(COST_OUTPUT)"

.DEFAULT_GOAL := help
