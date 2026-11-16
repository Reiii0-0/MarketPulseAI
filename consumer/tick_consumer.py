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
from producer.schema_validator import MarketTickMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


class ConsumerConfig(BaseSettings):
    """Configuration for Market Tick Kafka consumer service."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_market_ticks: str = "raw.market.ticks"
    kafka_consumer_group_ticks: str = "tick-consumer-group-v1"
    kafka_auto_offset_reset: str = "earliest"
    kafka_enable_auto_commit: bool = False
    kafka_consumer_poll_timeout_seconds: float = 1.0
    max_db_retries: int = 3
    retry_backoff_seconds: float = 0.5


class TickConsumer(ReliableKafkaConsumer):
    """Consumer for raw.market.ticks topic. Persists to raw.market_ticks by business identity."""

    UPSERT_QUERY = """
        INSERT INTO raw.market_ticks (
            dataset_id, source_event_id, run_id, ticker, event_timestamp,
            event_at, available_at, price, bid, ask, volume, trade_condition,
            observation_type, provenance, kafka_offset, kafka_partition
        ) VALUES (
            %(dataset_id)s, %(source_event_id)s, %(run_id)s, %(ticker)s, %(event_timestamp)s,
            %(event_at)s, %(available_at)s, %(price)s, %(bid)s, %(ask)s, %(volume)s,
            %(trade_condition)s, %(observation_type)s, %(provenance)s,
            %(kafka_offset)s, %(kafka_partition)s
        ) ON CONFLICT (dataset_id, source_event_id) DO NOTHING;
    """

    def __init__(
        self,
        config: ConsumerConfig,
        db_manager: DatabaseManager,
        *,
        max_db_retries: int | None = None,
        retry_backoff_seconds: float | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        super().__init__(
            config,
            db_manager,
            topic=getattr(config, "kafka_topic_market_ticks", "raw.market.ticks"),
            group_id=config.kafka_consumer_group_ticks,
            max_db_retries=max_db_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            consumer_cls=Consumer,
            **({"sleep_fn": sleep_fn} if sleep_fn is not None else {}),
        )

    def parse_record(self, payload: dict[str, Any]) -> MarketTickMessage:
        """Validate decoded JSON into MarketTickMessage."""
        return MarketTickMessage(**payload)

    def build_upsert_spec(self, model: BaseModel, meta: KafkaMessageMeta) -> BusinessUpsertSpec:
        """Build business-identity upsert parameters for raw.market_ticks."""
        tick = cast(MarketTickMessage, model)
        event_at = tick.event_at or tick.event_timestamp
        available_at = tick.available_at or event_at
        dataset_id = tick.dataset_id or f"live:{meta.topic}"
        source_event_id = tick.source_event_id or compute_fallback_source_event_id(
            "tick",
            (tick.ticker, event_at.isoformat(), tick.price, tick.volume, tick.replay_sequence),
        )
        run_id = tick.run_id or DEFAULT_RUN_ID
        params: dict[str, Any] = {
            "dataset_id": dataset_id,
            "source_event_id": source_event_id,
            "run_id": run_id,
            "ticker": tick.ticker,
            "event_timestamp": tick.event_timestamp,
            "event_at": event_at,
            "available_at": available_at,
            "price": tick.price,
            "bid": tick.bid,
            "ask": tick.ask,
            "volume": tick.volume,
            "trade_condition": tick.trade_condition,
            "observation_type": tick.observation_type,
            "provenance": tick.provenance,
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
    consumer_config = ConsumerConfig()
    app = TickConsumer(consumer_config, db_mgr)
    app.run()
