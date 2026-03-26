# syntax=docker/dockerfile:1.6
FROM python:3.11-slim AS base

ENV POETRY_VERSION=1.7.1 \
    POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PATH="/opt/poetry/bin:/workspace/.venv/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 - \
    && poetry --version

WORKDIR /workspace

# Only copy lock/pyproject for dependency layer; repo is bind-mounted in compose.
COPY pyproject.toml README.md ./

RUN poetry install --no-root

CMD ["bash"]
