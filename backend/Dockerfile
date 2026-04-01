FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --no-dev

COPY . .
RUN uv sync --locked --no-dev

ENV PORT=8000
EXPOSE ${PORT}

CMD uv run uvicorn main:app --host 0.0.0.0 --port $PORT
