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

PYTEST_FAST_ARGS := tests/controlplane tests/execution tests/features tests/ingestion tests/marketdata tests/portfolio tests/risk tests/strategy tests/test_*.py --ignore=tests/ops --ignore=tests/performance --ignore=tests/network --ignore=tests/slow

.PHONY: install lint test test-all run-dev

install:
	$(INSTALL_CMD)

lint:
	$(PYTHON_CMD) -m compileall app

test:
	$(PYTEST_CMD) $(PYTEST_FAST_ARGS)

test-all:
	$(PYTEST_CMD)

run-dev:
	$(PYTHON_CMD) -m app.main

controlplane-web:
	$(PYTHON_CMD) -m app.controlplane.api --env dev --host 127.0.0.1 --port 8000

webui:
	$(PYTHON_CMD) -m app.controlplane.api --env dev --host 127.0.0.1 --port 8000

controlplane-worker:
	$(PYTHON_CMD) -m app.controlplane.worker --env dev

run-live:
	$(PYTHON_CMD) -m app --env dev --mode live --duration 60 --max-events 200 --feature-audit-path $(FEATURE_AUDIT_PATH)

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
FEATURE_AUDIT_PATH ?= docs/validation/feature_decision_audit.jsonl
FEATURE_SET_NAME ?= legacy
FEATURE_SET_VERSION ?= legacy
FEATURE_STORE_PATH ?= data/dev/dev/feature-store/online.sqlite
FEATURE_STORE_HOST ?= 127.0.0.1
FEATURE_STORE_PORT ?= 8011
FEATURE_STORE_URL ?= http://$(FEATURE_STORE_HOST):$(FEATURE_STORE_PORT)
FEATURE_SHADOW_URL ?=
FEATURE_SYMBOL ?= BTCUSDT
FEATURE_EVIDENCE_DIR ?= docs/validation
FEATURE_RELEASE_REGISTRY ?= docs/validation/feature_releases.json

backfill-dev:
	$(PYTHON_CMD) -m app.ingestion.backfill --env dev --symbol $(SYMBOL) --feed-type $(FEED_TYPE) --start $(START) --end $(END) --interval $(INTERVAL) --batch $(BATCH) --dry-run

backfill-dev-write:
	$(PYTHON_CMD) -m app.ingestion.backfill --env dev --symbol $(SYMBOL) --feed-type $(FEED_TYPE) --start $(START) --end $(END) --interval $(INTERVAL) --batch $(BATCH)

feature-store-server:
	$(PYTHON_CMD) scripts/feature_store_server.py --backend local_sqlite --path $(FEATURE_STORE_PATH) --host $(FEATURE_STORE_HOST) --port $(FEATURE_STORE_PORT)

feature-release-evidence:
	$(PYTHON_CMD) scripts/feature_release_evidence.py --primary-url $(FEATURE_STORE_URL) --feature-set-name $(FEATURE_SET_NAME) --feature-set-version $(FEATURE_SET_VERSION) --symbol $(FEATURE_SYMBOL) --output-dir $(FEATURE_EVIDENCE_DIR) $(if $(FEATURE_SHADOW_URL),--shadow-url $(FEATURE_SHADOW_URL),)

feature-release-gates:
	$(PYTHON_CMD) scripts/feature_release_gates.py --target paper --parity-path docs/validation/feature_parity_report.json --benchmark-path docs/validation/feature_benchmark_report.json --observability-path docs/validation/feature_observability.json --contract-path docs/validation/feature_contract_validation.json --online-backend local_sqlite --observability-sink http --output docs/validation/feature_release_gates.json

feature-live-go-no-go:
	$(PYTHON_CMD) scripts/feature_live_go_no_go.py --target live --registry-path $(FEATURE_RELEASE_REGISTRY) --feature-set-name $(FEATURE_SET_NAME) --feature-set-version $(FEATURE_SET_VERSION) --parity-path docs/validation/feature_parity_report.json --benchmark-path docs/validation/feature_benchmark_report.json --observability-path docs/validation/feature_observability.json --contract-path docs/validation/feature_contract_validation.json --online-backend http --observability-sink http --shadow-path docs/validation/feature_shadow_summary.json --soak-path docs/validation/feature_serving_soak.json --concurrency-path docs/validation/feature_serving_concurrency.json --rollout-audit-path docs/validation/feature_rollout_audit.json --gates-output docs/validation/feature_release_gates.json

# Docker helpers
.PHONY: docker-build docker-up docker-down docker-shell docker-exec docker-test docker-test-all docker-test-slow docker-test-ingestion-readiness docker-ingestion-soak docker-ingestion-canary docker-test-ingestion-strict

docker-build:
	docker compose build

docker-up:
	docker compose up -d app webUI control-plane-worker

docker-down:
	docker compose down

docker-shell:
	docker compose run --rm app bash

docker-exec:
	docker compose exec app bash

docker-test:
	docker compose run --rm app sh -c "/opt/poetry/bin/poetry install && /opt/poetry/bin/poetry run pytest $(PYTEST_FAST_ARGS)"

docker-test-all:
	docker compose exec app /opt/poetry/bin/poetry run pytest

docker-test-slow:
	docker compose exec app /opt/poetry/bin/poetry run pytest -m slow -q

docker-test-ingestion-readiness:
	docker compose exec app /opt/poetry/bin/poetry run pytest tests/slow/test_ingestion_readiness.py -m slow -q

docker-test-ingestion-strict:
	docker compose exec app /opt/poetry/bin/poetry run pytest tests/ops/test_readiness_orchestrator.py tests/ops/test_ingestion_validation.py tests/ops/test_release_gates.py tests/ops/test_live_cutover.py tests/ops/test_operational_claims.py -q

docker-ingestion-soak:
	docker compose exec app /opt/poetry/bin/poetry run python scripts/ingestion_soak.py

docker-ingestion-canary:
	docker compose exec app /opt/poetry/bin/poetry run python scripts/ingestion_canary.py

demo-ingest:
	$(PYTHON_CMD) -m app.ingestion.demo --env dev --duration 30 --max-events 200
