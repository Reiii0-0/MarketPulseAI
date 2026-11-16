from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd
from confluent_kafka import Producer
from pydantic import BaseModel, ValidationError

from pipeline.dataset_manifest import (
    DatasetManifest,
    compute_sha256,
    load_manifest,
    load_ohlcv_bars_from_csv,
)
from pipeline.replay import (
    DeliveredKafkaOffset,
    ReplayCoordinator,
    ReplayEvent,
    ReplayStateStore,
    ReplaySummary,
    generate_run_id,
)
from producer.config import ProducerConfig
from producer.schema_validator import MarketTickMessage, ProvenanceType, SentimentEventMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _clean_row_dict(row: pd.Series) -> dict[str, Any]:
    return {
        k: (None if pd.isna(v) else (v.isoformat() if isinstance(v, pd.Timestamp) else v))
        for k, v in row.to_dict().items()
    }


def _to_utc_datetime(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw.replace(tzinfo=UTC) if raw.tzinfo is None else raw.astimezone(UTC)
    parsed = cast(datetime, pd.Timestamp(raw).to_pydatetime())
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class ReplayProducer:
    """Core engine for deterministic availability-ordered replay into Kafka topics."""

    def __init__(
        self,
        config: ProducerConfig,
        *,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.config = config
        self.sleep_fn = sleep_fn
        self.producer = Producer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "acks": config.kafka_producer_acks,
                "retries": config.kafka_producer_retries,
            }
        )
        self.replay_sequence = 0
        self.ticker_filter = (
            [t.strip() for t in config.replay_ticker_filter.split(",") if t.strip()]
            if config.replay_ticker_filter
            else None
        )
        self._batch_deliveries: list[DeliveredKafkaOffset] = []

    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Callback recording Kafka delivery metadata for partition barrier verification."""
        if err is not None:
            logger.error("Message delivery failed: %s", err)
            return
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        logger.debug("Message delivered to %s [%s] at offset %s", topic, partition, offset)
        if (
            isinstance(topic, str)
            and isinstance(partition, int)
            and not isinstance(partition, bool)
            and isinstance(offset, int)
            and not isinstance(offset, bool)
        ):
            self._batch_deliveries.append(
                DeliveredKafkaOffset(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    available_at=getattr(self, "_current_batch_available_at", datetime.now(UTC)),
                )
            )

    def _load_data(self) -> pd.DataFrame:
        """Load and merge tick and sentiment CSV datasets, sorted by availability/event time."""
        logger.info("Loading datasets...")

        ticks_df = pd.DataFrame()
        if Path(self.config.replay_tick_data_path).exists():
            ticks_df = pd.read_csv(self.config.replay_tick_data_path)
            ticks_df["event_type"] = "TICK"
        else:
            logger.warning("Tick data file not found at %s", self.config.replay_tick_data_path)

        sentiment_df = pd.DataFrame()
        if Path(self.config.replay_sentiment_data_path).exists():
            sentiment_df = pd.read_csv(self.config.replay_sentiment_data_path)
            sentiment_df["event_type"] = "SENTIMENT"
        else:
            logger.warning(
                "Sentiment data file not found at %s", self.config.replay_sentiment_data_path
            )

        if ticks_df.empty and sentiment_df.empty:
            return pd.DataFrame()

        combined_df = pd.concat([ticks_df, sentiment_df], ignore_index=True)

        if self.ticker_filter:
            combined_df = combined_df[combined_df["ticker"].isin(self.ticker_filter)]

        if combined_df.empty:
            return combined_df

        combined_df["event_timestamp"] = pd.to_datetime(combined_df["event_timestamp"], utc=True)
        if "available_at" in combined_df.columns:
            avail_series = pd.to_datetime(combined_df["available_at"], utc=True, errors="coerce")
            combined_df["available_at"] = avail_series.fillna(combined_df["event_timestamp"])
        else:
            combined_df["available_at"] = combined_df["event_timestamp"]

        combined_df = combined_df.sort_values(by=["available_at", "event_timestamp"]).reset_index(
            drop=True
        )
        return combined_df

    def _build_csv_event(
        self,
        row: pd.Series,
        *,
        run_id: str | None = None,
    ) -> ReplayEvent | None:
        self.replay_sequence += 1
        event_type = str(row["event_type"])
        ticker = str(row["ticker"])
        row_dict = _clean_row_dict(row)
        row_dict["replay_sequence"] = self.replay_sequence
        row_dict.setdefault("dataset_id", self.config.replay_dataset_id)
        row_dict.setdefault("provenance", self.config.replay_provenance)
        if run_id is not None:
            row_dict["run_id"] = run_id

        event_ts = _to_utc_datetime(row_dict["event_timestamp"])
        avail_raw = row_dict.get("available_at") or event_ts
        avail_ts = _to_utc_datetime(avail_raw)
        row_dict.setdefault("event_at", event_ts)
        row_dict["available_at"] = avail_ts
        row_dict.setdefault(
            "source_event_id",
            f"{row_dict['dataset_id']}:{event_type.lower()}:{ticker}:{event_ts.isoformat()}:{self.replay_sequence}",
        )

        try:
            msg: MarketTickMessage | SentimentEventMessage
            if event_type == "TICK":
                msg = MarketTickMessage(**row_dict)
                topic = self.config.kafka_topic_market_ticks
            else:
                msg = SentimentEventMessage(**row_dict)
                topic = self.config.kafka_topic_sentiment_events
        except ValidationError as exc:
            logger.warning(
                "Skipping invalid %s row (sequence %d, ticker %s): %s",
                event_type,
                self.replay_sequence,
                ticker,
                exc.json(),
            )
            return None

        return ReplayEvent(
            topic=topic,
            key=ticker,
            payload=msg,
            available_at=msg.available_at or msg.event_timestamp,
            source_event_id=msg.source_event_id or str(self.replay_sequence),
        )

    def _process_row(self, row: pd.Series, *, run_id: str | None = None) -> ReplayEvent | None:
        """Validate a single row and publish to the appropriate Kafka topic."""
        event = self._build_csv_event(row, run_id=run_id)
        if event is None:
            return None
        self.producer.produce(
            topic=event.topic,
            key=event.key,
            value=event.payload.model_dump_json(),
            on_delivery=self._delivery_report,
        )
        self.producer.poll(0)
        return event

    def load_manifest_events(
        self,
        manifest_path: str | Path,
        *,
        run_id: str,
    ) -> tuple[list[ReplayEvent], DatasetManifest]:
        """Load validated canonical records described by a DatasetManifest."""
        manifest = load_manifest(manifest_path)
        csv_path = Path(manifest.file_path)
        if not csv_path.exists():
            # Support relative manifest file_path regardless of working directory
            csv_path = Path(manifest_path).resolve().parents[2] / manifest.file_path

        if compute_sha256(csv_path) != manifest.checksum_sha256:
            raise ValueError("Dataset checksum does not match manifest")
        if manifest.schema_type != "ohlcv_1m_bars":
            raise ValueError("Manifest replay currently requires ohlcv_1m_bars")

        bars = load_ohlcv_bars_from_csv(
            csv_path,
            dataset_id=manifest.dataset_id,
            provenance=manifest.provenance,
            availability_assumed=manifest.availability_assumed,
        )
        if len(bars) != manifest.record_count:
            raise ValueError("Dataset record count does not match manifest")
        events: list[ReplayEvent] = []
        for bar in bars:
            if self.ticker_filter and bar.ticker not in self.ticker_filter:
                continue
            bar_with_run = bar.model_copy(update={"run_id": run_id})
            events.append(
                ReplayEvent(
                    topic=self.config.kafka_topic_market_bars,
                    key=bar_with_run.ticker,
                    payload=bar_with_run,
                    available_at=bar_with_run.available_at,
                    source_event_id=bar_with_run.source_event_id,
                )
            )
        return events, manifest

    def _publish_batch(
        self,
        run_id: str,
        batch_available_at: datetime,
        batch_events: list[ReplayEvent],
    ) -> list[DeliveredKafkaOffset]:
        self._batch_deliveries = []
        self._current_batch_available_at = batch_available_at
        batch_size = self.config.replay_batch_size
        for idx, event in enumerate(batch_events, start=1):
            payload: BaseModel = event.payload
            if getattr(payload, "run_id", None) != run_id:
                payload = payload.model_copy(update={"run_id": run_id})
            self.producer.produce(
                topic=event.topic,
                key=event.key,
                value=payload.model_dump_json(),
                on_delivery=self._delivery_report,
            )
            self.producer.poll(0)
            if idx % batch_size == 0:
                self.producer.flush()
        self.producer.flush()
        return list(self._batch_deliveries)

    def run(self, *, state_store: ReplayStateStore | None = None) -> ReplaySummary | None:
        """Execute deterministic availability-ordered replay cycles."""
        last_summary: ReplaySummary | None = None
        while True:
            run_id = self.config.replay_run_id or generate_run_id("replay")
            dataset_id = self.config.replay_dataset_id
            provenance: ProvenanceType = self.config.replay_provenance
            events: list[ReplayEvent] = []

            if self.config.replay_manifest_path and Path(self.config.replay_manifest_path).exists():
                events, manifest = self.load_manifest_events(
                    self.config.replay_manifest_path,
                    run_id=run_id,
                )
                dataset_id = manifest.dataset_id
                provenance = manifest.provenance
            else:
                df = self._load_data()
                if not df.empty:
                    for _, row in df.iterrows():
                        ev = self._build_csv_event(row, run_id=run_id)
                        if ev is not None:
                            events.append(ev)

            if not events:
                logger.error("No data found to replay. Exiting.")
                break

            logger.info(
                "Starting deterministic replay run_id=%s dataset_id=%s provenance=%s events=%d",
                run_id,
                dataset_id,
                provenance,
                len(events),
            )
            sleep = self.sleep_fn or (lambda s: time.sleep(s))
            coordinator = ReplayCoordinator(
                state_store=state_store,
                barrier_timeout_seconds=self.config.replay_barrier_timeout_seconds,
                sleep_fn=sleep,
            )
            last_summary = coordinator.execute(
                events,
                publish_batch_fn=self._publish_batch,
                run_id=run_id,
                dataset_id=dataset_id,
                provenance=provenance,
                speed_multiplier=self.config.replay_speed_multiplier,
            )
            logger.info(
                "Replay cycle complete run_id=%s batches=%d logical_as_of=%s dataset_end_at=%s",
                last_summary.run_id,
                last_summary.batches_completed,
                last_summary.logical_as_of,
                last_summary.dataset_end_at,
            )

            if not self.config.replay_loop:
                break

            logger.info(
                "Looping enabled. Waiting %d seconds...",
                self.config.replay_loop_delay_seconds,
            )
            sleep(self.config.replay_loop_delay_seconds)

        return last_summary


if __name__ == "__main__":
    from consumer.db import DatabaseConfig, DatabaseManager
    from pipeline.replay import PostgresReplayStateStore

    config = ProducerConfig()
    db_mgr: DatabaseManager | None = None
    store: PostgresReplayStateStore | None = None
    try:
        db_mgr = DatabaseManager(DatabaseConfig())  # type: ignore[call-arg]
        store = PostgresReplayStateStore(db_mgr)
    except Exception as exc:
        raise RuntimeError("Replay requires PostgreSQL delivery barriers") from exc
    try:
        ReplayProducer(config).run(state_store=store)
    finally:
        if db_mgr is not None:
            db_mgr.close()
