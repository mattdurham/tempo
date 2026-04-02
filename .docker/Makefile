.PHONY: up down build clean reload rebuild reset

# Full reset: stop, wipe data, rebuild, start
up: down build clean
	docker compose up -d

# Rebuild and restart without wiping data
rebuild:
	docker compose build s3fs tempo-parquet tempo-blockpack
	docker compose up -d

down:
	docker compose down --volumes

build:
	docker compose build tempo-parquet tempo-blockpack

clean:
	rm -rf s3-data/tempo-parquet s3-data/tempo-blockpack

# Full reset + load 10GB of fresh trace data with rebased timestamps
reset: up
	cd .. && go build -o cmd/parquet-otlp-forwarder/parquet-otlp-forwarder ./cmd/parquet-otlp-forwarder/ && \
	until curl -sf http://localhost:13200/ready && curl -sf http://localhost:13201/ready; do sleep 2; done && \
	until cmd/parquet-otlp-forwarder/parquet-otlp-forwarder \
		-dir ~/utils/dev_traces/3/ \
		-endpoint localhost:14317 \
		-endpoint localhost:14319 \
		-insecure \
		-max-input-gb 85 \
		-rebase-timestamps; do echo "distributor not ready, retrying in 5s..."; sleep 5; done

# Reload 10GB of trace data into both Tempo instances (original timestamps, no rebase)
reload:
	cd .. && go build -o cmd/parquet-otlp-forwarder/parquet-otlp-forwarder ./cmd/parquet-otlp-forwarder/ && \
	cmd/parquet-otlp-forwarder/parquet-otlp-forwarder \
		-dir ~/utils/dev_traces/3/ \
		-endpoint localhost:14317 \
		-endpoint localhost:14319 \
		-insecure \
		-max-input-gb 85
