from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProducerConfig(BaseSettings):
    """Configuration for the Data Replay Producer service.

    Loads settings from environment variables using pydantic-settings.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Kafka Settings
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_market_ticks: str = "raw.market.ticks"
    kafka_topic_sentiment_events: str = "raw.sentiment.events"
    kafka_producer_acks: str = "all"
    kafka_producer_retries: int = 3
    kafka_producer_max_block_ms: int = 60000

    # Replay Settings
    replay_tick_data_path: str = "/app/data/raw/tick_data.csv"
    replay_sentiment_data_path: str = "/app/data/raw/sentiment_scores.csv"
    replay_speed_multiplier: float = Field(default=100.0, ge=0.0)
    replay_batch_size: int = Field(default=500, ge=1)
    replay_ticker_filter: str | None = None  # Comma-separated tickers
    replay_loop: bool = False
    replay_loop_delay_seconds: int = Field(default=5, ge=0)

    # Live Data APIs
    finnhub_api_key: str | None = None
    marketaux_api_key: str | None = None
