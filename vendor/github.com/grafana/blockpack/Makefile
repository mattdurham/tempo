.PHONY: test-data clean-test-data help bench bench-report serve-report cost-analysis cost-analysis-no-freetier build-cost-tool build build-embed-server download-model docker-embed-server test format lint clean format-benchmark precommit ci install-tools nilaway betteralign betteralign-fix gofumpt-check gofumpt golines-check golines format-all bench-profile-writes deadcode staticcheck bench-logs bench-logs-html bench-logs-full bench-logs-full-html clean-bench-logs-full docs

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
BENCH_BINARY := benchmark/bench.test
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
	@echo "  make build-embed-server - Build embed-server binary (downloads model)"
	@echo "  make docker-embed-server - Build embed-server Docker image"
	@echo "  make download-model   - Download GGUF embedding model (~260MB)"
	@echo "  make clean            - Remove all build artifacts and test data"
	@echo ""
	@echo "Development targets:"
	@echo "  make test             - Run all unit tests"
	@echo "  make format           - Format all Go code with gofumpt"
	@echo "  make format-all       - Auto-fix all formatting issues (gofumpt, golines, betteralign)"
	@echo "  make lint             - Run golangci-lint (40+ linters)"
	@echo "  make nilaway          - Run nil safety checks (also runs on GHA)"
	@echo "  make betteralign      - Check struct alignment"
	@echo "  make betteralign-fix  - Auto-fix struct alignment"
	@echo "  make gofumpt-check    - Check formatting (CI)"
	@echo "  make gofumpt          - Auto-fix formatting"
	@echo "  make golines-check    - Check line length (CI)"
	@echo "  make golines          - Auto-fix line length"
	@echo "  make deadcode         - Check for unreachable code in public APIs"
	@echo "  make staticcheck      - Run staticcheck (additional static analysis)"
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
	@echo "  make bench-logs       - Run synthetic log benchmark (blockpack vs Loki chunks, ~50 MB)"
	@echo "  make bench-logs-html  - Run synthetic log benchmark and open HTML report"
	@echo "  make bench-logs-full  - Run synthetic + large-scale (1 GB) log benchmarks"
	@echo "  make bench-logs-full-html - Run full log benchmarks and open HTML reports"
	@echo "  make serve-report     - Start HTTP server to view benchmark reports (port 10001)"
	@echo "  make bench-profile-writes - Run write path benchmark with CPU+mem pprof (profiles in benchmark/profiles/)"
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

# --- Embed-server ---

# Model config
EMBED_MODEL_URL  ?= https://huggingface.co/nomic-ai/nomic-embed-text-v1.5-GGUF/resolve/main/nomic-embed-text-v1.5.Q8_0.gguf
EMBED_MODEL_DIR  ?= models
EMBED_MODEL_FILE ?= $(EMBED_MODEL_DIR)/nomic-embed-text-v1.5.Q8_0.gguf

# Download the GGUF model (skip if already present)
download-model:
	@mkdir -p $(EMBED_MODEL_DIR)
	@if [ -f "$(EMBED_MODEL_FILE)" ]; then \
		echo "==> Model already exists at $(EMBED_MODEL_FILE)"; \
	else \
		echo "==> Downloading model to $(EMBED_MODEL_FILE) (~260MB)..."; \
		curl -L --progress-bar -o "$(EMBED_MODEL_FILE).tmp" "$(EMBED_MODEL_URL)" && \
		mv "$(EMBED_MODEL_FILE).tmp" "$(EMBED_MODEL_FILE)"; \
		echo "==> Model downloaded."; \
	fi

# Build the embed-server binary
build-embed-server: download-model
	@echo "==> Building embed-server..."
	@mkdir -p bin
	@go build -o bin/embed-server ./cmd/embed-server
	@echo "==> bin/embed-server built (model at $(EMBED_MODEL_FILE))"

# Build embed-server Docker image (downloads model during build)
EMBED_SERVER_IMAGE ?= blockpack/embed-server
docker-embed-server:
	@echo "==> Building Docker image $(EMBED_SERVER_IMAGE)..."
	docker build -t $(EMBED_SERVER_IMAGE) -f cmd/embed-server/Dockerfile .
	@echo "==> Image built: $(EMBED_SERVER_IMAGE)"
	@echo "    Run: docker run -p 8765:8765 $(EMBED_SERVER_IMAGE)"

# Rebuild docs site from wiki Markdown sources
docs:
	@echo "==> Rebuilding docs site..."
	@go run ./cmd/docs-build
	@echo "==> Done — commit docs/ and push to deploy"

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
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install golang.org/x/tools/cmd/deadcode@latest
	@go install honnef.co/go/tools/cmd/staticcheck@latest
	@echo "==> All tools installed!"

# Nil safety checks
nilaway:
	@echo "==> Running nil safety checks (nilaway)..."
	@nilaway -include-pkgs "github.com/grafana/blockpack/..." -exclude-pkgs "github.com/grafana/blockpack/benchmark" ./...
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

# Run deadcode check (exercises public APIs via cmd/deadcode first)
# Excludes: internal/xunsafe (unsafe ops), internal/debug (build tags), benchmark (external utility package)
# Also excludes: internal/tempoapi (SearchTags/SearchTagValues/GetBlockMeta removed from public API)
# Also excludes: internal/blockio sub-packages (old format reader/writer; executor removed leaving query-path methods dead)
deadcode:
	@echo "==> Running deadcode analysis..."
	@echo "    Building cmd/deadcode to anchor public APIs..."
	@go build -o /dev/null ./cmd/deadcode
	@echo "    Running deadcode tool..."
	@output=$$(deadcode -test ./... | grep -v "internal/xunsafe\|internal/debug\|benchmark/\|Writer.allocBytes\|internal/modules/blockio\|internal/tempoapi\|internal/blockio\|cmd/embed-server"); \
	if [ -n "$$output" ]; then \
		echo "❌ Deadcode found (excluding internal/xunsafe, internal/debug, benchmark, internal/modules/blockio, internal/tempoapi, internal/blockio, cmd/embed-server):"; \
		echo "$$output"; \
		exit 1; \
	fi
	@echo "==> Deadcode check passed!"

# Run staticcheck (excludes internal/xunsafe - uses unsafe for performance)
staticcheck:
	@echo "==> Running staticcheck..."
	@staticcheck -f stylish $$(go list ./... | grep -v '/internal/xunsafe$$')
	@echo "==> Staticcheck passed!"

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
	echo "── [4/7] Struct alignment (betteralign)"; \
	if $(MAKE) betteralign > /tmp/blockpack-ci-betteralign.log 2>&1; then \
		echo "   ✅ PASS"; PASS=$$((PASS + 1)); \
	else \
		echo "   ❌ FAIL"; tail -20 /tmp/blockpack-ci-betteralign.log | sed 's/^/   /'; FAIL=$$((FAIL + 1)); \
	fi; \
	echo "── [5/7] Tests with coverage"; \
	PKGS=$$(go list ./... | grep -v -E '/benchmark|/real_world_comparison|/encodings|/parquet-util' \
		| grep -v -E '^github.com/grafana/blockpack/cmd/(analyze$$|analyze-pb|benchsummary|codepath-dag|convert-parquet-folder|deep-compare|generate-synthetic-traces|generate-testdata|ingest-sorted|inspect-parquet-schema|inspect-web|measure_actual_distribution|pr-copilot-loop|temposerver|test_normalize|test_parse|trace_plan)$$' \
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
	echo "── [6/7] Parity smoke test"; \
	if go test ./internal/parity -run TestParitySmokeTest -v > /tmp/blockpack-ci.log 2>&1; then \
		echo "   ✅ PASS"; PASS=$$((PASS + 1)); \
	else \
		echo "   ❌ FAIL"; tail -20 /tmp/blockpack-ci.log | sed 's/^/   /'; FAIL=$$((FAIL + 1)); \
	fi; \
	echo "── [7/7] Format comparison deep compare"; \
	if go test ./benchmark -run TestFormatComparisonDeepCompare -v -timeout 120s > /tmp/blockpack-ci.log 2>&1; then \
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

# Run all pre-commit quality checks
# Mirrors .github/workflows/ci.yml — every check here must also run in GHA and vice versa.
# Exception: golines runs here but is disabled in CI due to CI-only failures (see ci.yml comment).
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
	@echo "[3/9] Running golangci-lint (40+ linters, includes gocyclo + staticcheck)..."
	@$(MAKE) lint
	@echo "✅ golangci-lint: clean"
	@echo ""
	@echo "[4/9] Checking struct alignment (betteralign)..."
	@$(MAKE) betteralign
	@echo "✅ betteralign: clean"
	@echo ""
	@echo "[5/9] Running nil safety checks (nilaway)..."
	@$(MAKE) nilaway
	@echo "✅ nilaway: clean"
	@echo ""
	@echo "[6/9] Building project..."
	@go build $(PACKAGES)
	@echo "✅ build: successful"
	@echo ""
	@echo "[7/9] Running tests..."
	@$(MAKE) test
	@echo "✅ tests: all passing"
	@echo ""
	@echo "[8/9] Running deadcode check..."
	@$(MAKE) deadcode
	@echo "✅ deadcode: no unreachable code"
	@echo ""
	@echo "[9/9] Running staticcheck..."
	@$(MAKE) staticcheck
	@echo "✅ staticcheck: clean"
	@echo ""
	@echo "✅ All pre-commit checks passed!"
	@echo "✅ Ready to commit"

# Clean all build artifacts
clean: clean-test-data
	@echo "==> Cleaning build artifacts..."
	@rm -rf bin/
	@rm -f $(BENCH_OUTPUT) $(BENCH_HTML) $(BLOCKPACK_ANALYSIS) $(BENCH_BINARY)
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
	@echo "==> Compiling benchmark binary (once)..."
	@mkdir -p benchmark
	@go test -c -o $(BENCH_BINARY) github.com/grafana/blockpack/benchmark
	@echo ""
	@echo "==> Running real-world benchmarks (blockpack only)..."
	@cd benchmark && ./bench.test -test.run=^$$ -test.bench='^BenchmarkRealWorldQueries$$' \
		-test.benchmem -test.timeout=5m 2>&1 | tee unified.txt
	@echo ""
	@echo "==> Running limited query benchmarks..."
	@cd benchmark && ./bench.test -test.run=^$$ -test.bench=BenchmarkRealWorldQueriesLimited \
		-test.benchmem -test.timeout=5m 2>&1 | tee -a unified.txt
	@echo ""
	@echo "==> Running trace-by-ID benchmark..."
	@cd benchmark && ./bench.test -test.run=^$$ -test.bench=BenchmarkGetByTraceID \
		-test.benchmem -test.timeout=5m 2>&1 | tee -a unified.txt
	@echo ""
	@echo "==> Running aggregation comparison benchmarks (aggregation vs manual vs parquet)..."
	@cd benchmark && ./bench.test -test.run=^$$ -test.bench=BenchmarkAggregationComparison \
		-test.benchmem -test.timeout=30m 2>&1 | tee -a unified.txt
	@echo ""
	@echo "==> Running format comparison benchmarks (blockpack vs parquet)..."
	@cd benchmark && ./bench.test -test.run=^$$ -test.bench=BenchmarkFormatComparison \
		-test.benchmem -test.timeout=30m 2>&1 | tee -a unified.txt
	@echo ""
	@echo "==> Running write path benchmarks (blockpack vs parquet)..."
	@cd benchmark && ./bench.test -test.run=^$$ -test.bench=BenchmarkWritePathComparison \
		-test.benchmem -test.timeout=10m 2>&1 | tee -a unified.txt
	@echo ""
	@echo "==> All benchmark results saved to $(BENCH_OUTPUT)"
	@echo "==> Run 'make bench-report' to generate HTML report"

# Run write path benchmark with CPU and memory pprof profiling.
# Profiles are written to benchmark/profiles/ as write_path.cpu.pprof and write_path.mem.pprof.
# Analyse with: go tool pprof -http=:8080 benchmark/profiles/write_path.cpu.pprof
bench-profile-writes:
	@echo "==> Running write path benchmark with CPU + memory profiling..."
	@mkdir -p benchmark/profiles
	@BENCH_PROFILE_CPU=true BENCH_PROFILE_MEM=true BENCH_PROFILE_DIR=./benchmark/profiles \
		go test -run=^$$ -bench=BenchmarkWritePathComparison -benchmem -benchtime=10s -count=1 -timeout=10m \
		github.com/grafana/blockpack/benchmark
	@echo ""
	@echo "==> Profiles written to benchmark/profiles/"
	@ls -lh benchmark/profiles/write_path.*.pprof 2>/dev/null || echo "  (no profiles found — check BENCH_PROFILE_CPU/MEM env vars)"
	@echo ""
	@echo "==> Analyse CPU profile:    go tool pprof -http=:8080 benchmark/profiles/write_path.cpu.pprof"
	@echo "==> Analyse memory profile: go tool pprof -http=:8080 benchmark/profiles/write_path.mem.pprof"
	@echo "  Use -focus=blockpack or -focus=parquet to isolate each implementation"

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

# Synthetic log benchmark: Loki chunks vs blockpack, no external data required.
# Generates synthetic log data in-process (~50 MB, 50 streams, 1 hour window).
# Writes HTML report to /tmp/lokibench-synthetic-report.html.
LOKIBENCH_HTML ?= benchmark/lokibench-bench.html

bench-logs:
	@echo "==> Running synthetic log benchmark (blockpack vs Loki chunks)..."
	@echo "==> Generating ~50 MB synthetic data in-process (no external files required)"
	@cd benchmark/lokibench && \
		go test -run '^$$' \
		-bench BenchmarkSynthetic \
		-benchtime=1x \
		-timeout 600s \
		-count=1 \
		-v \
		-benchmem \
		./... 2>&1
	@echo "==> HTML report: $(LOKIBENCH_HTML)"

bench-logs-html: bench-logs
	@echo "==> Opening HTML report..."
	@if command -v xdg-open >/dev/null 2>&1; then \
		xdg-open $(LOKIBENCH_HTML); \
	elif command -v open >/dev/null 2>&1; then \
		open $(LOKIBENCH_HTML); \
	else \
		echo "==> Open manually: $(LOKIBENCH_HTML)"; \
	fi

LOKIBENCH_LARGESCALE_HTML ?= benchmark/lokibench-logs.html

bench-logs-full:
	@echo "==> Running unified log benchmark (50 queries × 100 MB + 1 GB OTLP datasets)..."
	@echo "==> Data generated in benchmark/lokibench/testdata/ (bench-logs-full-html cleans this first)"
	@cd benchmark/lokibench && \
		LOGFILTER_REPORT=../lokibench-logs.html \
		LOGFILTER_IO_LATENCY=10ms \
		go test -run '^$$' \
		-bench BenchmarkLogsFull \
		-benchtime=1x \
		-timeout 3600s \
		-count=1 \
		-v \
		-benchmem \
		./... 2>&1
	@echo "==> HTML report: $(LOKIBENCH_LARGESCALE_HTML)"

clean-bench-logs-full:
	@echo "==> Cleaning benchmark/lokibench/testdata/ for a fresh run..."
	@rm -rf benchmark/lokibench/testdata/

bench-logs-full-html: clean-bench-logs-full bench-logs-full
	@echo "==> Opening HTML report..."
	@if command -v xdg-open >/dev/null 2>&1; then \
		xdg-open $(LOKIBENCH_LARGESCALE_HTML); \
	elif command -v open >/dev/null 2>&1; then \
		open $(LOKIBENCH_LARGESCALE_HTML); \
	else \
		echo "==> Open manually: $(LOKIBENCH_LARGESCALE_HTML)"; \
	fi

# Serve benchmark reports via Go HTTP server
serve-report:
	@go run ./cmd/serve-report

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

.PHONY: lokibench-smoke
lokibench-smoke: ## Run blockpack LogQL bench smoke test (fast, ~30s)
	cd benchmark/lokibench && go test -v -run 'TestBlockpackSmoke' -timeout 180s ./...

.PHONY: lokibench-test
lokibench-test: ## Run full blockpack LogQL storage equality test (~10min)
	cd benchmark/lokibench && go test -v -run 'TestStorageEquality' -timeout 600s ./...

.PHONY: lokibench-verify
lokibench-verify: ## Run chunk-store vs blockpack mismatch verification tests (~2min)
	cd benchmark/lokibench && go test \
		-run 'TestSyntheticDeepCompare|TestSMLeakMismatch|TestLokiConverter_SelectLogs_MultiLabelEquality' \
		-timeout 120s \
		-v \
		./...

.DEFAULT_GOAL := help
