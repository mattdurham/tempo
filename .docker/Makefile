.PHONY: up down build clean

up: down build clean
	docker compose up -d

down:
	docker compose down --volumes

build:
	docker compose build tempo-parquet tempo-blockpack

clean:
	rm -rf s3-data/tempo-parquet s3-data/tempo-blockpack
