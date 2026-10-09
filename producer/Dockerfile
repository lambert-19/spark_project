# Image du producer Mastodon -> Kafka. Construite depuis la racine du projet
# (contexte « . » dans docker-compose.yml) pour réutiliser pyproject.toml et uv.lock.
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

# Uniquement les dépendances du producer (pas de pyspark/pandas) : image légère.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-default-groups

COPY producer/ producer/

CMD ["python", "producer/mastodon_producer.py"]
