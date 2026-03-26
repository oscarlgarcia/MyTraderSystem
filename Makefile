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

