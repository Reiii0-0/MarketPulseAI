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

# Install system dependencies for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m marketpulse

# 1. Copy dependency specification and lockfile
COPY pyproject.toml uv.lock ./

# 2. Install locked Python dependencies
RUN uv sync --locked --no-dev --no-install-project

# 3. Copy source code and data placeholders
COPY --chown=marketpulse:marketpulse config/ ./config/
COPY --chown=marketpulse:marketpulse pipeline/ ./pipeline/
COPY --chown=marketpulse:marketpulse consumer/db.py ./consumer/db.py
COPY --chown=marketpulse:marketpulse producer/ ./producer/
# data/ is usually mounted, but we copy directory structure
RUN mkdir -p /app/data/raw /app/data/manifests /app/data/fixtures

# Set ownership
RUN chown marketpulse:marketpulse /app

# Switch to non-root user
USER marketpulse

# Run the replayer
CMD ["python", "producer/replay_producer.py"]
