# Based on the Python image with uv pre-installed.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS base

ENV UV_LOCKED=1

COPY pyproject.toml uv.lock /app/
WORKDIR /app


FROM base AS producer

COPY ./src/producer /app
RUN uv sync --only-group producer

ENTRYPOINT ["uv", "run", "generate_data.py"]
