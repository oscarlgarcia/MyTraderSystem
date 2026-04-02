SHELL := /bin/sh

.PHONY: install lint test run-dev

install:
	poetry install

lint:
	poetry run python -m compileall app

test:
	poetry run pytest

run-dev:
	poetry run python -m app.main

run-live:
	poetry run python -m app --env dev --mode live --duration 60 --max-events 200

ingest-dev:
	poetry run python -m app.ingestion.runner --env dev --duration 600

inspect-dev:
	poetry run python -m app.ingestion.inspect --env dev --limit 20

SYMBOL ?= BTCUSDT
START ?= 2024-01-01T00:00:00+00:00
END ?= 2024-01-01T01:00:00+00:00
INTERVAL ?= 1m
BATCH ?= 500
FEED_TYPE ?= kline

backfill-dev:
	poetry run python -m app.ingestion.backfill --env dev --symbol $(SYMBOL) --feed-type $(FEED_TYPE) --start $(START) --end $(END) --interval $(INTERVAL) --batch $(BATCH) --dry-run

backfill-dev-write:
	poetry run python -m app.ingestion.backfill --env dev --symbol $(SYMBOL) --feed-type $(FEED_TYPE) --start $(START) --end $(END) --interval $(INTERVAL) --batch $(BATCH)

# Docker helpers
.PHONY: docker-build docker-up docker-down docker-shell docker-exec docker-test docker-test-all docker-test-slow docker-test-ingestion-readiness docker-ingestion-soak docker-ingestion-canary

docker-build:
	docker compose build

docker-up:
	docker compose up -d app

docker-down:
	docker compose down

docker-shell:
	docker compose run --rm app bash

docker-exec:
	docker compose exec app bash

docker-test:
	docker compose run --rm app sh -c "poetry install && poetry run pytest"

docker-test-all:
	docker compose exec app poetry run pytest

docker-test-slow:
	docker compose exec app poetry run pytest -m slow -q

docker-test-ingestion-readiness:
	docker compose exec app poetry run pytest tests/slow/test_ingestion_readiness.py -m slow -q

docker-ingestion-soak:
	docker compose exec app poetry run python scripts/ingestion_soak.py

docker-ingestion-canary:
	docker compose exec app poetry run python scripts/ingestion_canary.py

demo-ingest:
	poetry run python -m app.ingestion.demo --env dev --duration 30 --max-events 200
