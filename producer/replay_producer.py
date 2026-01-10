from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
from confluent_kafka import Producer
from pydantic import ValidationError

from producer.config import ProducerConfig
from producer.schema_validator import MarketTickMessage, SentimentEventMessage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


class ReplayProducer:
    """Core engine for replaying historical CSV data into Kafka topics.

    Reads, validates, and streams tick and sentiment data with timing control.
    """

    def __init__(self, config: ProducerConfig) -> None:
        """Initialise the replayer with configuration and Kafka producer.

        Args:
            config (ProducerConfig): Configuration object for the replayer.
        """
        self.config = config
        self.producer = Producer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "acks": config.kafka_producer_acks,
                "retries": config.kafka_producer_retries,
            }
        )
        self.replay_sequence = 0
        self.ticker_filter = (
            [t.strip() for t in config.replay_ticker_filter.split(",")]
            if config.replay_ticker_filter
            else None
        )

    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Callback for Kafka message delivery results.

        Args:
            err (Any): Error object if delivery failed.
            msg (Any): The message that was (not) delivered.
        """
        if err is not None:
            logger.error("Message delivery failed: %s", err)
        else:
            logger.debug(
                "Message delivered to %s [%d] at offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def _load_data(self) -> pd.DataFrame:
        """Load and merge tick and sentiment datasets, sorted by timestamp.

        Returns:
            pd.DataFrame: A unified DataFrame containing all events to replay.
        """
        logger.info("Loading datasets...")

        # Load Ticks
        ticks_df = pd.DataFrame()
        if Path(self.config.replay_tick_data_path).exists():
            ticks_df = pd.read_csv(self.config.replay_tick_data_path)
            ticks_df["event_type"] = "TICK"
        else:
            logger.warning("Tick data file not found at %s", self.config.replay_tick_data_path)

        # Load Sentiment
        sentiment_df = pd.DataFrame()
        if Path(self.config.replay_sentiment_data_path).exists():
            sentiment_df = pd.read_csv(self.config.replay_sentiment_data_path)
            sentiment_df["event_type"] = "SENTIMENT"
        else:
            logger.warning(
                "Sentiment data file not found at %s", self.config.replay_sentiment_data_path
            )

        # Combine
        if ticks_df.empty and sentiment_df.empty:
            return pd.DataFrame()

        combined_df = pd.concat([ticks_df, sentiment_df], ignore_index=True)

        # Apply ticker filter
        if self.ticker_filter:
            combined_df = combined_df[combined_df["ticker"].isin(self.ticker_filter)]

        # Convert and sort by timestamp
        combined_df["event_timestamp"] = pd.to_datetime(combined_df["event_timestamp"])
        combined_df = combined_df.sort_values(by="event_timestamp").reset_index(drop=True)

        return combined_df

    def run(self) -> None:
        """Execute the replay loop until exhausted or interrupted."""
        while True:
            df = self._load_data()
            if df.empty:
                logger.error("No data found to replay. Exiting.")
                break

            logger.info("Starting replay of %d events...", len(df))
            start_time_real = time.time()
            start_time_data = df.iloc[0]["event_timestamp"].timestamp()

            for i, row in df.iterrows():
                # Timing Control
                if self.config.replay_speed_multiplier > 0:
                    current_event_time = row["event_timestamp"].timestamp()
                    data_elapsed = current_event_time - start_time_data
                    target_real_elapsed = data_elapsed / self.config.replay_speed_multiplier
                    actual_real_elapsed = time.time() - start_time_real
                    sleep_time = target_real_elapsed - actual_real_elapsed

                    if sleep_time > 0:
                        time.sleep(sleep_time)

                # Validation and Publishing
                self._process_row(row)

                # Batch Flush
                if (i + 1) % self.config.replay_batch_size == 0:
                    self.producer.flush()
                    logger.info("Processed %d events...", i + 1)

            self.producer.flush()
            logger.info("Replay cycle complete.")

            if not self.config.replay_loop:
                break

            logger.info("Looping enabled. Waiting %d seconds...", self.config.replay_loop_delay_seconds)
            time.sleep(self.config.replay_loop_delay_seconds)

    def _process_row(self, row: pd.Series) -> None:
        """Validate a single row and publish to the appropriate Kafka topic.

        Args:
            row (pd.Series): The data row to process.
        """
        self.replay_sequence += 1
        event_type = row["event_type"]
        ticker = row["ticker"]
        row_dict = row.to_dict()
        row_dict["replay_sequence"] = self.replay_sequence

        # Clean up pandas specific types (NaN, Timestamps) for Pydantic
        row_dict = {
            k: (None if pd.isna(v) else (v.isoformat() if isinstance(v, pd.Timestamp) else v))
            for k, v in row_dict.items()
        }

        try:
            msg: MarketTickMessage | SentimentEventMessage
            if event_type == "TICK":
                msg = MarketTickMessage(**row_dict)
                topic = self.config.kafka_topic_market_ticks
            else:
                msg = SentimentEventMessage(**row_dict)
                topic = self.config.kafka_topic_sentiment_events

            # Publish
            self.producer.produce(
                topic=topic,
                key=ticker,
                value=msg.model_dump_json(),
                on_delivery=self._delivery_report,
            )
            self.producer.poll(0)

        except ValidationError as e:
            logger.warning(
                "Skipping invalid %s row (sequence %d, ticker %s): %s",
                event_type,
                self.replay_sequence,
                ticker,
                e.json(),
            )


if __name__ == "__main__":
    config = ProducerConfig()
    replayer = ReplayProducer(config)
    replayer.run()
