SHELL := /bin/sh

POETRY := $(shell command -v poetry 2>/dev/null)
LOCAL_VENV_PY := $(firstword $(wildcard .venv/bin/python .venv/Scripts/python.exe venv/bin/python venv/Scripts/python.exe))

ifeq ($(strip $(POETRY)),)
ifneq ($(strip $(LOCAL_VENV_PY)),)
PYTHON_CMD := $(LOCAL_VENV_PY)
PYTEST_CMD := $(LOCAL_VENV_PY) -m pytest
INSTALL_CMD := $(LOCAL_VENV_PY) -m pip install --upgrade pip && $(LOCAL_VENV_PY) -m pip install websockets httpx pyarrow pytest
else
$(error Neither poetry nor a local virtualenv python was found. Create .venv first or install poetry.)
endif
else
PYTHON_CMD := $(POETRY) run python
PYTEST_CMD := $(POETRY) run pytest
INSTALL_CMD := $(POETRY) install
endif

.PHONY: install lint test run-dev

install:
	$(INSTALL_CMD)

lint:
	$(PYTHON_CMD) -m compileall app

test:
	$(PYTEST_CMD)

run-dev:
	$(PYTHON_CMD) -m app.main

controlplane-web:
	$(PYTHON_CMD) -m app.controlplane.api --env dev --host 127.0.0.1 --port 8001

controlplane-worker:
	$(PYTHON_CMD) -m app.controlplane.worker --env dev

run-live:
	$(PYTHON_CMD) -m app --env dev --mode live --duration 60 --max-events 200

ingest-dev:
	$(PYTHON_CMD) -m app.ingestion.runner --env dev --duration 600

inspect-dev:
	$(PYTHON_CMD) -m app.ingestion.inspect --env dev --limit 20

SYMBOL ?= BTCUSDT
START ?= 2024-01-01T00:00:00+00:00
END ?= 2024-01-01T01:00:00+00:00
INTERVAL ?= 1m
BATCH ?= 500
FEED_TYPE ?= kline

backfill-dev:
	$(PYTHON_CMD) -m app.ingestion.backfill --env dev --symbol $(SYMBOL) --feed-type $(FEED_TYPE) --start $(START) --end $(END) --interval $(INTERVAL) --batch $(BATCH) --dry-run

backfill-dev-write:
	$(PYTHON_CMD) -m app.ingestion.backfill --env dev --symbol $(SYMBOL) --feed-type $(FEED_TYPE) --start $(START) --end $(END) --interval $(INTERVAL) --batch $(BATCH)

# Docker helpers
.PHONY: docker-build docker-up docker-down docker-shell docker-exec docker-test docker-test-all docker-test-slow docker-test-ingestion-readiness docker-ingestion-soak docker-ingestion-canary docker-test-ingestion-strict

docker-build:
	docker compose build

docker-up:
	docker compose up -d app control-plane-web control-plane-worker

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

docker-test-ingestion-strict:
	docker compose exec app poetry run pytest tests/ops/test_readiness_orchestrator.py tests/ops/test_ingestion_validation.py tests/ops/test_release_gates.py tests/ops/test_live_cutover.py tests/ops/test_operational_claims.py -q

docker-ingestion-soak:
	docker compose exec app poetry run python scripts/ingestion_soak.py

docker-ingestion-canary:
	docker compose exec app poetry run python scripts/ingestion_canary.py

demo-ingest:
	$(PYTHON_CMD) -m app.ingestion.demo --env dev --duration 30 --max-events 200
