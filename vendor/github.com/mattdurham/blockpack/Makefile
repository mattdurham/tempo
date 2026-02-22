.PHONY: test-data clean-test-data help bench bench-report serve-report cost-analysis cost-analysis-no-freetier build-cost-tool build test format lint clean format-benchmark precommit ci install-tools nilaway betteralign betteralign-fix gofumpt-check gofumpt golines-check golines format-all

# Number of traces to generate (approximately 670K spans like original dataset)
NUM_TRACES ?= 191119

# Package list (excluding /bots) - lazy evaluation to avoid running on every make invocation
PACKAGES = $(shell go list -e -f '{{if .GoFiles}}{{.ImportPath}}{{end}}' ./... | grep -v '/bots')

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
	@echo "  make format           - Format all Go code with gofumpt"
	@echo "  make format-all       - Auto-fix all formatting issues (gofumpt, golines, betteralign)"
	@echo "  make lint             - Run golangci-lint (40+ linters)"
	@echo "  make nilaway          - Run nil safety checks"
	@echo "  make betteralign      - Check struct alignment"
	@echo "  make betteralign-fix  - Auto-fix struct alignment"
	@echo "  make gofumpt-check    - Check formatting (CI)"
	@echo "  make gofumpt          - Auto-fix formatting"
	@echo "  make golines-check    - Check line length (CI)"
	@echo "  make golines          - Auto-fix line length"
	@echo "  make install-tools    - Install all code quality tools"
	@echo "  make ci               - Run full CI pipeline locally (mirrors .github/workflows/ci.yml)"
	@echo "  make precommit        - Run build and all quality checks (REQUIRED before commit)"
	@echo "  make format-benchmark - Run benchmarks and check for regressions"
	@echo ""
	@echo "Test data generation targets:"
	@echo "  make test-data              - Generate all synthetic test data (NO customer data)"
	@echo "  make blockpack-data         - Generate blockpack file using synthetic data"
	@echo "  make blockpack-hot          - Generate blockpack (hot path - not yet fully implemented)"
	@echo "  make blockpack-cold         - Generate blockpack (cold path)"
	@echo "  make blockpack-both         - Generate both hot and cold blockpack files"
	@echo "  make parquet-data           - Generate parquet data using synthetic traces"
	@echo "  make generate-synthetic-data- Generate synthetic e-commerce traces (~670K spans)"
	@echo "  make clean-test-data        - Remove all generated test files"
	@echo "  make check-test-data        - Check if test files exist"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  make bench            - Run all benchmarks (real-world + format comparison)"
	@echo "  make bench-report     - Generate unified HTML report from benchmarks"
	@echo "  make bench-store      - Validate and store benchmark results with metadata"
	@echo "  make bench-full       - Complete pipeline: data + benchmarks + validation + storage"
	@echo "  make serve-report     - Start HTTP server to view benchmark reports (port 10001)"
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
	@go build -o bin/temposerver ./cmd/temposerver
	@go build -o bin/generate-unified-report ./cmd/generate_unified_report
	@echo "==> Binaries built in ./bin/"

# Run all tests
test:
	@echo "==> Running unit tests..."
	@go test -v -race -gcflags="all=-d=checkptr=0" -timeout=10m $(PACKAGES)
	@echo "==> All tests passed!"

# Install all code quality tools
install-tools:
	@echo "==> Installing code quality tools..."
	@go install go.uber.org/nilaway/cmd/nilaway@latest
	@go install github.com/dkorunic/betteralign/cmd/betteralign@latest
	@go install mvdan.cc/gofumpt@latest
	@go install github.com/segmentio/golines@latest
	@go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v2.8.0
	@echo "==> All tools installed!"

# Nil safety checks
nilaway:
	@echo "==> Running nil safety checks (nilaway)..."
	@nilaway -exclude-pkgs "github.com/mattdurham/blockpack/benchmark" ./...
	@echo "==> Nil safety checks passed!"

# Struct alignment checks
betteralign:
	@echo "==> Checking struct alignment..."
	@betteralign -exclude_files "internal/xunsafe/any.go,internal/blockio/compaction/compaction.go" ./...
	@echo "==> Struct alignment check passed!"

# Auto-fix struct alignment
betteralign-fix:
	@echo "==> Auto-fixing struct alignment..."
	@betteralign -apply ./... || true
	@echo "==> Struct alignment fixed!"

# Check formatting with gofumpt (CI mode)
gofumpt-check:
	@echo "==> Checking formatting (gofumpt)..."
	@output=$$(gofumpt -l . 2>&1); \
	if [ -n "$$output" ]; then \
		echo "❌ Code is not formatted. Run 'make gofumpt' to fix:"; \
		echo "$$output"; \
		exit 1; \
	fi
	@echo "==> Formatting check passed!"

# Auto-fix formatting with gofumpt
gofumpt:
	@echo "==> Formatting Go code (gofumpt)..."
	@gofumpt -w .
	@echo "==> Code formatted!"

# Check line length (CI mode)
golines-check:
	@echo "==> Checking line length (120 chars)..."
	@output=$$(golines --dry-run --max-len=120 --base-formatter=gofumpt . 2>&1 | grep -v "^$$"); \
	if [ -n "$$output" ]; then \
		echo "❌ Line length violations found. Run 'make golines' to fix:"; \
		echo "$$output"; \
		exit 1; \
	fi
	@echo "==> Line length check passed!"

# Auto-fix line length
golines:
	@echo "==> Fixing line length violations..."
	@golines -w --max-len=120 --base-formatter=gofumpt .
	@echo "==> Line length fixed!"

# Auto-fix all formatting issues
format-all: gofumpt golines betteralign-fix
	@echo "==> All auto-fixable issues resolved!"

# Format all Go code (updated to use gofumpt)
format:
	@echo "==> Formatting Go code (gofumpt)..."
	@gofumpt -w .
	@echo "==> Code formatted!"

# Run linters (updated to use .golangci.yml)
lint:
	@echo "==> Running golangci-lint (40+ linters)..."
	@golangci-lint run
	@echo "==> Linting complete!"

# Run full CI pipeline locally (UPDATED)
# Note: benchmark job is intentionally excluded from local CI (too resource-intensive,
# requires git operations to compare main vs PR branch). Run `make format-benchmark` manually if needed.
ci:
	@echo "🔄 Running CI pipeline locally (mirrors .github/workflows/ci.yml)..."
	@echo ""
	@PASS=0; FAIL=0; \
	echo "── [1/7] Format check (gofumpt)"; \
	if $(MAKE) gofumpt-check > /tmp/blockpack-ci-gofumpt.log 2>&1; then \
		echo "   ✅ PASS"; PASS=$$((PASS + 1)); \
	else \
		echo "   ❌ FAIL"; tail -20 /tmp/blockpack-ci-gofumpt.log | sed 's/^/   /'; FAIL=$$((FAIL + 1)); \
	fi; \
	echo "── [2/7] Line length check (golines)"; \
	if $(MAKE) golines-check > /tmp/blockpack-ci-golines.log 2>&1; then \
		echo "   ✅ PASS"; PASS=$$((PASS + 1)); \
	else \
		echo "   ❌ FAIL"; tail -20 /tmp/blockpack-ci-golines.log | sed 's/^/   /'; FAIL=$$((FAIL + 1)); \
	fi; \
	echo "── [3/7] Lint (golangci-lint - 40+ linters)"; \
	if $(MAKE) lint > /tmp/blockpack-ci-lint.log 2>&1; then \
		echo "   ✅ PASS"; PASS=$$((PASS + 1)); \
	else \
		echo "   ❌ FAIL"; tail -30 /tmp/blockpack-ci-lint.log | sed 's/^/   /'; FAIL=$$((FAIL + 1)); \
	fi; \
	echo "── [4/7] Nil safety (nilaway)"; \
	if $(MAKE) nilaway > /tmp/blockpack-ci-nilaway.log 2>&1; then \
		echo "   ✅ PASS"; PASS=$$((PASS + 1)); \
	else \
		echo "   ❌ FAIL"; tail -20 /tmp/blockpack-ci-nilaway.log | sed 's/^/   /'; FAIL=$$((FAIL + 1)); \
	fi; \
	echo "── [5/7] Struct alignment (betteralign)"; \
	if $(MAKE) betteralign > /tmp/blockpack-ci-betteralign.log 2>&1; then \
		echo "   ✅ PASS"; PASS=$$((PASS + 1)); \
	else \
		echo "   ❌ FAIL"; tail -20 /tmp/blockpack-ci-betteralign.log | sed 's/^/   /'; FAIL=$$((FAIL + 1)); \
	fi; \
	echo "── [6/7] Tests with coverage"; \
	PKGS=$$(go list ./... | grep -v -E '/benchmark|/real_world_comparison|/encodings|/parquet-util' \
		| grep -v -E '^github.com/mattdurham/blockpack/cmd/(analyze$$|analyze-pb|benchsummary|codepath-dag|convert-parquet-folder|deep-compare|generate-synthetic-traces|generate-testdata|ingest-sorted|inspect-parquet-schema|inspect-web|measure_actual_distribution|pr-copilot-loop|temposerver|test_normalize|test_parse|trace_plan)$$' \
		| grep -v -E '/executor/testutil$$'); \
	if go test $$PKGS -coverprofile=/tmp/blockpack-coverage.out -covermode=atomic -timeout=10m > /tmp/blockpack-ci.log 2>&1; then \
		COVERAGE=$$(go tool cover -func=/tmp/blockpack-coverage.out | grep total | awk '{print $$3}' | sed 's/%//'); \
		if [ $$(echo "$$COVERAGE < 70" | bc) -eq 1 ]; then \
			echo "   ❌ FAIL — coverage $${COVERAGE}% < 70%"; FAIL=$$((FAIL + 1)); \
		else \
			echo "   ✅ PASS — coverage $${COVERAGE}%"; PASS=$$((PASS + 1)); \
		fi; \
	else \
		echo "   ❌ FAIL"; tail -30 /tmp/blockpack-ci.log | sed 's/^/   /'; FAIL=$$((FAIL + 1)); \
	fi; \
	echo "── [7/7] Parity smoke test"; \
	if go test ./tempo_parquet_comparison -run TestParitySmokeTest -v > /tmp/blockpack-ci.log 2>&1; then \
		echo "   ✅ PASS"; PASS=$$((PASS + 1)); \
	else \
		echo "   ❌ FAIL"; tail -20 /tmp/blockpack-ci.log | sed 's/^/   /'; FAIL=$$((FAIL + 1)); \
	fi; \
	rm -f /tmp/blockpack-ci*.log /tmp/blockpack-coverage.out; \
	echo ""; \
	echo "── Summary: $$PASS passed, $$FAIL failed"; \
	if [ "$$FAIL" -gt 0 ]; then \
		echo "❌ CI pipeline FAILED"; exit 1; \
	else \
		echo "✅ CI pipeline PASSED"; \
	fi

# Run all pre-commit quality checks (UPDATED)
precommit:
	@echo "==> Running pre-commit quality checks..."
	@echo ""
	@echo "[1/9] Auto-formatting code (gofumpt)..."
	@$(MAKE) gofumpt
	@echo "✅ gofumpt: formatted"
	@echo ""
	@echo "[2/9] Auto-fixing line length (golines)..."
	@$(MAKE) golines
	@echo "✅ golines: fixed"
	@echo ""
	@echo "[3/9] Running golangci-lint (40+ linters)..."
	@$(MAKE) lint
	@echo "✅ golangci-lint: clean"
	@echo ""
	@echo "[4/9] Running nil safety checks (nilaway)..."
	@$(MAKE) nilaway
	@echo "✅ nilaway: clean"
	@echo ""
	@echo "[5/9] Checking struct alignment (betteralign)..."
	@$(MAKE) betteralign
	@echo "✅ betteralign: clean"
	@echo ""
	@echo "[6/9] Building project..."
	@go build $(PACKAGES)
	@echo "✅ build: successful"
	@echo ""
	@echo "[7/9] Checking cyclomatic complexity..."
	@golangci-lint run --enable-only=gocyclo ./... 2>&1 || (echo "❌ gocyclo found functions with complexity > 40"; exit 1)
	@echo "✅ cyclomatic complexity: all < 40 (non-test code)"
	@echo ""
	@echo "[8/9] Running tests..."
	@$(MAKE) test
	@echo "✅ tests: all passing"
	@echo ""
	@echo "[9/9] Checking code coverage..."
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

# Generate synthetic test data using OTEL generator (NO customer data)
# Generates ~670K spans across 191K traces with realistic e-commerce microservices
generate-synthetic-data:
	@echo "==> Generating synthetic e-commerce test data..."
	@echo "    Traces: $(NUM_TRACES)"
	@echo "    Services: frontend, cart, checkout, payment, etc."
	@go run ./cmd/generate-testdata/main.go -traces=$(NUM_TRACES) -blockpack=$(BLOCKPACK_FILE) -parquet=$(PARQUET_DIR)
	@echo "==> Synthetic data generation complete!"

# Generate blockpack file (using synthetic data generator)
blockpack-data: generate-synthetic-data
	@echo "==> Blockpack file ready: $(BLOCKPACK_FILE)"

# Generate hot blockpack (with precomputed metric streams) - not yet supported
blockpack-hot:
	@echo "==> Generating hot blockpack with metric streams..."
	@TMP_PARQUET=$$(mktemp -d); \
	go run ./cmd/generate-testdata/main.go -traces=$(NUM_TRACES) -blockpack=$(BLOCKPACK_HOT) -parquet=$$TMP_PARQUET; \
	rm -rf $$TMP_PARQUET
	@echo "==> Created $(BLOCKPACK_HOT) ($(shell ls -lh $(BLOCKPACK_HOT) 2>/dev/null | awk '{print $$5}' || echo 'N/A'))"
	@echo "⚠️  Note: Metric streams not yet implemented in generate-testdata"

# Generate cold blockpack (without metric streams) - same as regular
blockpack-cold:
	@echo "==> Generating cold blockpack..."
	@TMP_PARQUET=$$(mktemp -d); \
	go run ./cmd/generate-testdata/main.go -traces=$(NUM_TRACES) -blockpack=$(BLOCKPACK_COLD) -parquet=$$TMP_PARQUET; \
	rm -rf $$TMP_PARQUET
	@echo "==> Created $(BLOCKPACK_COLD) ($(shell ls -lh $(BLOCKPACK_COLD) 2>/dev/null | awk '{print $$5}' || echo 'N/A'))"

# Generate both hot and cold blockpack files
blockpack-both: blockpack-hot blockpack-cold
	@echo "==> Generated both blockpack files"

# Generate parquet file (using synthetic data generator)
parquet-data: generate-synthetic-data
	@echo "==> Parquet data ready: $(PARQUET_DIR)"

# Generate all test data
test-data: generate-synthetic-data
	@echo ""
	@echo "==> All synthetic test data generated successfully!"
	@echo "==> Using OTEL generator (NO customer data)"
	@echo ""
	@$(MAKE) check-test-data

# Clean generated test files
clean-test-data:
	@echo "==> Removing generated test files..."
	@rm -f $(BLOCKPACK_FILE) $(BLOCKPACK_HOT) $(BLOCKPACK_COLD)
	@rm -rf $(PARQUET_DIR)
	@echo "==> Cleaned test data"

# Check if test files exist
check-test-data:
	@echo "==> Checking test data files..."
	@if [ ! -f $(BLOCKPACK_FILE) ]; then \
		echo "  ❌ $(BLOCKPACK_FILE) not found - run 'make test-data'"; \
	else \
		echo "  ✓ $(BLOCKPACK_FILE) exists ($(shell ls -lh $(BLOCKPACK_FILE) | awk '{print $$5}'))"; \
	fi
	@if [ ! -d $(PARQUET_DIR) ]; then \
		echo "  ❌ $(PARQUET_DIR) not found - run 'make test-data'"; \
	else \
		echo "  ✓ $(PARQUET_DIR) exists ($(shell du -sh $(PARQUET_DIR) | cut -f1))"; \
	fi
	@echo ""
	@echo "  ℹ️  All test data generated synthetically using OTEL generator"
	@echo "  ℹ️  NO customer data (raw.pb) used"

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

# Validate and store benchmark results with metadata
bench-store: $(BENCH_OUTPUT)
	@echo "==> Validating and storing benchmark results..."
	@mkdir -p benchmark_results
	@cat $(BENCH_OUTPUT) | go run ./cmd/generate_unified_report \
		--validate \
		--output-dir=benchmark_results \
		--blockpack-analysis=$(BLOCKPACK_ANALYSIS) || \
	(echo "❌ Benchmark validation failed - no results saved" && exit 1)

# Run complete benchmark pipeline: generate data, run benchmarks, generate report, validate and store
bench-full: test-data bench bench-report bench-store
	@echo ""
	@echo "==> Full benchmark pipeline complete!"
	@echo "==> HTML report: $(BENCH_HTML)"
	@echo "==> Stored results: benchmark_results/latest"
	@echo "==> View: open $(BENCH_HTML) or xdg-open $(BENCH_HTML)"

$(BENCH_OUTPUT):
	@echo "Error: $(BENCH_OUTPUT) not found. Run 'make bench' first."
	@exit 1

# Serve benchmark reports via HTTP server
serve-report:
	@if [ ! -f $(BENCH_HTML) ]; then \
		echo "Error: $(BENCH_HTML) not found. Run 'make bench-report' first."; \
		exit 1; \
	fi
	@echo "==> Starting HTTP server on http://0.0.0.0:10001"
	@echo "==> Unified report: http://0.0.0.0:10001/unified_report.html"
	@echo "==> Blockpack analysis: http://0.0.0.0:10001/traces.blockpack.analysis.html"
	@echo "==> Press Ctrl+C to stop"
	@cd benchmark && python3 -m http.server 10001

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
