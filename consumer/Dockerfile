FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

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

# 1. Copy only dependency specification
COPY pyproject.toml .

# 2. Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# 3. Copy source code
COPY consumer/ ./consumer/
COPY producer/schema_validator.py ./producer/schema_validator.py

# Set ownership
RUN chown -R marketpulse:marketpulse /app

# Switch to non-root user
USER marketpulse

# CMD is overridden in docker-compose for each consumer type
CMD ["python", "consumer/tick_consumer.py"]
