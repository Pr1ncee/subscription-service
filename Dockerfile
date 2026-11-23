# syntax=docker/dockerfile:1.7

# ---- builder: resolve dependencies into /app/.venv ---------------------------------
FROM python:3.12.15-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:0.9.13 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# Dependencies first: this layer is reused until pyproject.toml/uv.lock change.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY README.md alembic.ini ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# ---- runtime: no build tools, no dev dependencies, non-root -------------------------
FROM python:3.12.15-slim-bookworm AS runtime
RUN groupadd --system --gid 1001 app \
    && useradd --system --uid 1001 --gid 1001 --no-create-home app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chown=app:app src ./src
COPY --chown=app:app pyproject.toml alembic.ini ./
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER 1001:1001
EXPOSE 8000
# One image, four roles: api (default), worker, consumers, migrate — see docker-compose.yml.
# No HEALTHCHECK here: only the api serves HTTP; compose defines per-service checks.
CMD ["fastapi", "run", "--host", "0.0.0.0", "--port", "8000"]
