FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /uvx /bin/

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Install system dependencies for confluent-kafka and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m marketpulse

# 1. Copy dependency specification and lockfile
COPY pyproject.toml uv.lock ./

# 2. Install locked Python dependencies
RUN uv sync --locked --no-dev --no-install-project

# 3. Copy source code
COPY --chown=marketpulse:marketpulse consumer/ ./consumer/
COPY --chown=marketpulse:marketpulse producer/schema_validator.py ./producer/schema_validator.py

# Set ownership
RUN chown marketpulse:marketpulse /app

# Switch to non-root user
USER marketpulse

# CMD is overridden in docker-compose for each consumer type
CMD ["python", "consumer/tick_consumer.py"]
