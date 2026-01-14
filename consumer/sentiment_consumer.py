from __future__ import annotations

import json
import logging
import signal
from typing import Any

from confluent_kafka import Consumer, Message
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from consumer.db import DatabaseConfig, DatabaseManager
from producer.schema_validator import SentimentEventMessage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


class SentimentConsumerConfig(BaseSettings):
    """Configuration for Sentiment Kafka consumer service."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_sentiment_events: str = "raw.sentiment.events"
    kafka_consumer_group_sentiment: str = "sentiment-consumer-group-v1"
    kafka_auto_offset_reset: str = "earliest"
    kafka_enable_auto_commit: bool = False
    kafka_consumer_poll_timeout_seconds: float = 1.0


class SentimentConsumer:
    """Consumer for raw.sentiment.events topic. Persists to raw.sentiment_events table."""

    UPSERT_QUERY = """
        INSERT INTO raw.sentiment_events (
            ticker, event_timestamp, source, headline, sentiment_score, 
            sentiment_label, confidence, kafka_offset, kafka_partition
        ) VALUES (
            %(ticker)s, %(event_timestamp)s, %(source)s, %(headline)s, 
            %(sentiment_score)s, %(sentiment_label)s, %(confidence)s, 
            %(kafka_offset)s, %(kafka_partition)s
        ) ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING;
    """

    def __init__(self, config: SentimentConsumerConfig, db_manager: DatabaseManager) -> None:
        """Initialise the consumer and database manager.

        Args:
            config (SentimentConsumerConfig): Kafka consumer settings.
            db_manager (DatabaseManager): Initialised database manager.
        """
        self.config = config
        self.db = db_manager
        self.consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "group.id": config.kafka_consumer_group_sentiment,
                "auto.offset.reset": config.kafka_auto_offset_reset,
                "enable.auto.commit": config.kafka_enable_auto_commit,
            }
        )
        self.running = True
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    def _handle_exit(self, signum: int, frame: Any) -> None:
        """Graceful shutdown handler."""
        logger.info("Interrupt received (%d), shutting down...", signum)
        self.running = False

    def run(self) -> None:
        """Main consumption loop."""
        try:
            self.consumer.subscribe([self.config.kafka_topic_sentiment_events])
            logger.info("Subscribed to topic: %s", self.config.kafka_topic_sentiment_events)

            while self.running:
                msg = self.consumer.poll(timeout=self.config.kafka_consumer_poll_timeout_seconds)

                if msg is None:
                    continue
                if msg.error():
                    logger.error("Kafka error: %s", msg.error())
                    continue

                self._process_message(msg)

        finally:
            self.consumer.close()
            self.db.close()

    def _process_message(self, msg: Message) -> None:
        """Validate JSON and persist to database.

        Args:
            msg (Message): Kafka message object.
        """
        try:
            payload = json.loads(msg.value().decode("utf-8"))
            event = SentimentEventMessage(**payload)

            # Map Pydantic model + Kafka metadata to DB query params
            params = event.model_dump()
            params["kafka_offset"] = msg.offset()
            params["kafka_partition"] = msg.partition()

            # Execute idempotent upsert
            self.db.execute_upsert(self.UPSERT_QUERY, params)

            # Commit offset only after successful DB write
            self.consumer.commit(message=msg, asynchronous=False)

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error("Failed to decode JSON from Kafka: %s", e)
        except ValidationError as e:
            logger.warning("Validation failure for sentiment event: %s", e)
        except Exception as e:
            logger.error("Unexpected error processing message: %s", e)


if __name__ == "__main__":
    db_config = DatabaseConfig()
    db_mgr = DatabaseManager(db_config)
    consumer_config = SentimentConsumerConfig()
    app = SentimentConsumer(consumer_config, db_mgr)
    app.run()
