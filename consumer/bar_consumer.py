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
)
from consumer.db import DatabaseConfig, DatabaseManager
from producer.schema_validator import MarketBarMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


class BarConsumerConfig(BaseSettings):
    """Configuration for 1-minute OHLCV Bar Kafka consumer service."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_market_bars: str = "raw.market.bars"
    kafka_consumer_group_bars: str = "bar-consumer-group-v1"
    kafka_auto_offset_reset: str = "earliest"
    kafka_enable_auto_commit: bool = False
    kafka_consumer_poll_timeout_seconds: float = 1.0
    max_db_retries: int = 3
    retry_backoff_seconds: float = 0.5


class BarConsumer(ReliableKafkaConsumer):
    """Consumer for raw.market.bars topic. Persists canonical OHLCV bars into raw.market_bars."""

    UPSERT_QUERY = """
        INSERT INTO raw.market_bars (
            dataset_id, source_event_id, run_id, ticker, bar_start, bar_end,
            event_at, available_at, open, high, low, close, volume,
            trade_count, vwap, observation_type, provenance, availability_assumed,
            kafka_offset, kafka_partition
        ) VALUES (
            %(dataset_id)s, %(source_event_id)s, %(run_id)s, %(ticker)s, %(bar_start)s,
            %(bar_end)s, %(event_at)s, %(available_at)s, %(open)s, %(high)s,
            %(low)s, %(close)s, %(volume)s, %(trade_count)s, %(vwap)s,
            %(observation_type)s, %(provenance)s, %(availability_assumed)s,
            %(kafka_offset)s, %(kafka_partition)s
        ) ON CONFLICT (dataset_id, source_event_id) DO NOTHING;
    """

    def __init__(
        self,
        config: BarConsumerConfig,
        db_manager: DatabaseManager,
        *,
        max_db_retries: int | None = None,
        retry_backoff_seconds: float | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        super().__init__(
            config,
            db_manager,
            topic=getattr(config, "kafka_topic_market_bars", "raw.market.bars"),
            group_id=config.kafka_consumer_group_bars,
            max_db_retries=max_db_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            consumer_cls=Consumer,
            **({"sleep_fn": sleep_fn} if sleep_fn is not None else {}),
        )

    def parse_record(self, payload: dict[str, Any]) -> MarketBarMessage:
        """Validate decoded JSON into MarketBarMessage."""
        return MarketBarMessage(**payload)

    def build_upsert_spec(self, model: BaseModel, meta: KafkaMessageMeta) -> BusinessUpsertSpec:
        """Build business-identity upsert parameters for raw.market_bars."""
        bar = cast(MarketBarMessage, model)
        event_at = bar.event_at or bar.bar_end
        run_id = bar.run_id or DEFAULT_RUN_ID
        params: dict[str, Any] = {
            "dataset_id": bar.dataset_id,
            "source_event_id": bar.source_event_id,
            "run_id": run_id,
            "ticker": bar.ticker,
            "bar_start": bar.bar_start,
            "bar_end": bar.bar_end,
            "event_at": event_at,
            "available_at": bar.available_at,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "trade_count": bar.trade_count,
            "vwap": bar.vwap,
            "observation_type": bar.observation_type,
            "provenance": bar.provenance,
            "availability_assumed": bar.availability_assumed,
            "kafka_offset": meta.offset,
            "kafka_partition": meta.partition,
        }
        return BusinessUpsertSpec(
            query=self.UPSERT_QUERY,
            params=params,
            run_id=run_id,
            source_event_id=bar.source_event_id,
            available_at=bar.available_at,
        )


if __name__ == "__main__":
    db_config = DatabaseConfig()  # type: ignore[call-arg]
    db_mgr = DatabaseManager(db_config)
    consumer_config = BarConsumerConfig()
    app = BarConsumer(consumer_config, db_mgr)
    app.run()
