FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install system dependencies for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m marketpulse

# 1. Copy only dependency specification
COPY pyproject.toml .

# 2. Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# 3. Copy source code and data placeholders
COPY producer/ ./producer/
# data/ is usually mounted, but we copy directory structure
RUN mkdir -p /app/data/raw

# Set ownership
RUN chown -R marketpulse:marketpulse /app

# Switch to non-root user
USER marketpulse

# Run the replayer
CMD ["python", "producer/replay_producer.py"]
