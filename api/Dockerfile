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

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m marketpulse

# 1. Copy dependency specification and lockfile to leverage Docker cache
COPY pyproject.toml uv.lock ./

# 2. Install locked Python dependencies before copying source code
RUN uv sync --locked --extra dev --no-install-project

# 3. Copy the rest of the source code (changes here won't bust the uv cache)
COPY --chown=marketpulse:marketpulse api/ ./api/
COPY --chown=marketpulse:marketpulse config/ ./config/
COPY --chown=marketpulse:marketpulse consumer/ ./consumer/
COPY --chown=marketpulse:marketpulse model/ ./model/
COPY --chown=marketpulse:marketpulse pipeline/ ./pipeline/
COPY --chown=marketpulse:marketpulse producer/ ./producer/
COPY --chown=marketpulse:marketpulse tests/ ./tests/
COPY --chown=marketpulse:marketpulse scripts/ ./scripts/
COPY --chown=marketpulse:marketpulse data/fixtures/smoke_1m_v1.csv ./data/fixtures/
COPY --chown=marketpulse:marketpulse data/manifests/fixture_smoke_v1.json ./data/manifests/

COPY --chown=marketpulse:marketpulse docker-compose.yml ./
COPY --chown=marketpulse:marketpulse deploy/ ./deploy/
COPY --chown=marketpulse:marketpulse monitoring/ ./monitoring/
COPY --chown=marketpulse:marketpulse dbt_project/models/ ./dbt_project/models/
COPY --chown=marketpulse:marketpulse dbt_project/tests/ ./dbt_project/tests/
COPY --chown=marketpulse:marketpulse dbt_project/macros/ ./dbt_project/macros/

# Create directory for model artefacts (mounted at runtime)
RUN mkdir -p /app/model/artefacts

# Set ownership
RUN chown marketpulse:marketpulse /app

# Switch to non-root user
USER marketpulse

# Expose API port
EXPOSE 8000

# Start Uvicorn
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
