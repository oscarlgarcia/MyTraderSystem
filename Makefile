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

# Docker helpers
.PHONY: docker-build docker-shell docker-test

docker-build:
	docker compose build

docker-shell:
	docker compose run --rm app bash

docker-test:
	docker compose run --rm app sh -c "poetry install --no-root && poetry run pytest"
