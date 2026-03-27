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

ingest-dev:
	poetry run python -m app.ingestion.runner --env dev --duration 600

inspect-dev:
	poetry run python -m app.ingestion.inspect --env dev --limit 20

backfill-dev:
	poetry run python -m app.ingestion.backfill --env dev --symbol BTCUSDT --start 2024-01-01T00:00:00+00:00 --end 2024-01-01T01:00:00+00:00 --interval 1m --batch 500 --dry-run

backfill-dev-write:
	poetry run python -m app.ingestion.backfill --env dev --symbol BTCUSDT --start 2024-01-01T00:00:00+00:00 --end 2024-01-01T01:00:00+00:00 --interval 1m --batch 500

# Docker helpers
.PHONY: docker-build docker-shell docker-test

docker-build:
	docker compose build

docker-shell:
	docker compose run --rm app bash

docker-test:
	docker compose run --rm app sh -c "poetry install && poetry run pytest"
