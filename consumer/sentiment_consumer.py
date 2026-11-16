from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

from confluent_kafka import Consumer
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from consumer.base import (
    DEFAULT_RUN_ID,
    BusinessUpsertSpec,
    KafkaMessageMeta,
    ReliableKafkaConsumer,
    compute_fallback_source_event_id,
)
from consumer.db import DatabaseConfig, DatabaseManager
from producer.schema_validator import SentimentEventMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


class SentimentConsumerConfig(BaseSettings):
    """Configuration for Sentiment Kafka consumer service."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_sentiment_events: str = "raw.sentiment.events"
    kafka_consumer_group_sentiment: str = "sentiment-consumer-group-v1"
    kafka_auto_offset_reset: str = "earliest"
    kafka_enable_auto_commit: bool = False
    kafka_consumer_poll_timeout_seconds: float = 1.0
    max_db_retries: int = 3
    retry_backoff_seconds: float = 0.5


class SentimentConsumer(ReliableKafkaConsumer):
    """Consumer for raw.sentiment.events topic. Persists to raw.sentiment_events by business identity."""

    UPSERT_QUERY = """
        INSERT INTO raw.sentiment_events (
            dataset_id, source_event_id, run_id, ticker, event_timestamp,
            event_at, available_at, source, headline, sentiment_score,
            sentiment_label, confidence, observation_type, provenance,
            kafka_offset, kafka_partition
        ) VALUES (
            %(dataset_id)s, %(source_event_id)s, %(run_id)s, %(ticker)s, %(event_timestamp)s,
            %(event_at)s, %(available_at)s, %(source)s, %(headline)s, %(sentiment_score)s,
            %(sentiment_label)s, %(confidence)s, %(observation_type)s, %(provenance)s,
            %(kafka_offset)s, %(kafka_partition)s
        ) ON CONFLICT (dataset_id, source_event_id) DO NOTHING;
    """

    def __init__(
        self,
        config: SentimentConsumerConfig,
        db_manager: DatabaseManager,
        *,
        max_db_retries: int | None = None,
        retry_backoff_seconds: float | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        super().__init__(
            config,
            db_manager,
            topic=getattr(config, "kafka_topic_sentiment_events", "raw.sentiment.events"),
            group_id=config.kafka_consumer_group_sentiment,
            max_db_retries=max_db_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            consumer_cls=Consumer,
            **({"sleep_fn": sleep_fn} if sleep_fn is not None else {}),
        )

    def parse_record(self, payload: dict[str, Any]) -> SentimentEventMessage:
        """Validate decoded JSON into SentimentEventMessage."""
        return SentimentEventMessage(**payload)

    def build_upsert_spec(self, model: BaseModel, meta: KafkaMessageMeta) -> BusinessUpsertSpec:
        """Build business-identity upsert parameters for raw.sentiment_events."""
        event = cast(SentimentEventMessage, model)
        event_at = event.event_at or event.event_timestamp
        available_at = event.available_at or event_at
        dataset_id = event.dataset_id or f"live:{meta.topic}"
        source_event_id = event.source_event_id or compute_fallback_source_event_id(
            "sentiment",
            (
                event.ticker,
                event_at.isoformat(),
                event.source,
                event.headline or "",
                event.sentiment_score,
            ),
        )
        run_id = event.run_id or DEFAULT_RUN_ID
        params: dict[str, Any] = {
            "dataset_id": dataset_id,
            "source_event_id": source_event_id,
            "run_id": run_id,
            "ticker": event.ticker,
            "event_timestamp": event.event_timestamp,
            "event_at": event_at,
            "available_at": available_at,
            "source": event.source,
            "headline": event.headline,
            "sentiment_score": event.sentiment_score,
            "sentiment_label": event.sentiment_label,
            "confidence": event.confidence,
            "observation_type": event.observation_type,
            "provenance": event.provenance,
            "kafka_offset": meta.offset,
            "kafka_partition": meta.partition,
        }
        return BusinessUpsertSpec(
            query=self.UPSERT_QUERY,
            params=params,
            run_id=run_id,
            source_event_id=source_event_id,
            available_at=available_at,
        )


if __name__ == "__main__":
    db_config = DatabaseConfig()  # type: ignore[call-arg]
    db_mgr = DatabaseManager(db_config)
    consumer_config = SentimentConsumerConfig()
    app = SentimentConsumer(consumer_config, db_mgr)
    app.run()
