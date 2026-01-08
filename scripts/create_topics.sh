#!/bin/bash
# project/scripts/create_topics.sh
# Kafka topic initialisation script with finite retry logic

set -e

BOOTSTRAP_SERVER="kafka:9092"
MAX_RETRIES=30
RETRY_COUNT=0

echo "🔍 Waiting for Kafka to be ready at $BOOTSTRAP_SERVER..."

# Use the absolute path discovered in previous debugging
KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"

until $KAFKA_BIN --bootstrap-server $BOOTSTRAP_SERVER --list > /dev/null 2>&1; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "❌ Error: Kafka was not ready after $MAX_RETRIES retries. Exiting."
    exit 1
  fi
  echo "⏳ Kafka is unavailable (attempt $RETRY_COUNT/$MAX_RETRIES) - sleeping 2s"
  sleep 2
done

echo "🚀 Kafka is up. Creating topics..."

# Create raw.market.ticks
$KAFKA_BIN --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw.market.ticks \
  --partitions 3 \
  --replication-factor 1

# Create raw.sentiment.events
$KAFKA_BIN --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw.sentiment.events \
  --partitions 3 \
  --replication-factor 1

echo "✅ Topics created successfully:"
$KAFKA_BIN --bootstrap-server $BOOTSTRAP_SERVER --list

exit 0
