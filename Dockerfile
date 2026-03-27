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

# Install poetry explicitly (curl installer) and ensure it is on PATH
RUN curl -sSL https://install.python-poetry.org | python3 - \
    || pip install poetry==1.7.1
RUN /opt/poetry/bin/poetry --version || poetry --version

WORKDIR /workspace

# Only copy lock/pyproject for dependency layer; repo is bind-mounted in compose.
COPY pyproject.toml poetry.lock README.md ./

RUN poetry install --no-root

CMD ["bash"]
