FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m marketpulse

# 1. Copy only dependency specification to leverage Docker cache
COPY pyproject.toml .

# 2. Install Python dependencies before copying source code
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir ".[dev]"

# 3. Copy the rest of the source code (changes here won't bust the pip cache)
COPY api/ ./api/
COPY consumer/ ./consumer/
COPY model/ ./model/
COPY producer/ ./producer/
COPY tests/ ./tests/

# Create directory for model artefacts (mounted at runtime)
RUN mkdir -p /app/model/artefacts

# Set ownership
RUN chown -R marketpulse:marketpulse /app

# Switch to non-root user
USER marketpulse

# Expose API port
EXPOSE 8000

# Start Uvicorn
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
