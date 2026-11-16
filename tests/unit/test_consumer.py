from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

from consumer.bar_consumer import BarConsumer, BarConsumerConfig
from consumer.base import PROGRESS_UPSERT_QUERY, QUARANTINE_INSERT_QUERY
from consumer.db import DatabaseManager
from consumer.sentiment_consumer import SentimentConsumer, SentimentConsumerConfig
from consumer.tick_consumer import ConsumerConfig, TickConsumer


def _make_mock_db() -> tuple[MagicMock, MagicMock]:
    db_mgr = MagicMock(spec=DatabaseManager)
    cur = MagicMock()

    @contextmanager
    def _tx():
        yield cur

    db_mgr.transaction.side_effect = _tx
    return db_mgr, cur


def _make_kafka_msg(
    payload: bytes | None,
    *,
    topic: str = "raw.market.ticks",
    partition: int = 0,
    offset: int = 123,
) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = payload
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


def test_tick_consumer_inserts_valid_message_and_progress_in_one_transaction() -> None:
    """Valid tick persists business row + delivery progress atomically before Kafka commit."""
    with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        db_mgr, cur = _make_mock_db()

        consumer = TickConsumer(
            ConsumerConfig(kafka_bootstrap_servers="localhost:9092"),
            db_mgr,
        )
        payload: dict[str, Any] = {
            "ticker": "AAPL",
            "event_timestamp": "2026-01-01T10:00:00+00:00",
            "price": 182.55,
            "volume": 200,
            "replay_sequence": 1,
            "dataset_id": "ds_test",
            "source_event_id": "ds_test:tick:AAPL:1",
            "run_id": "run_001",
            "provenance": "real_historical",
        }
        msg = _make_kafka_msg(json.dumps(payload).encode("utf-8"))
        consumer._process_message(msg)

        assert db_mgr.transaction.call_count == 1
        assert cur.execute.call_count == 2
        assert cur.execute.call_args_list[0].args[0] == TickConsumer.UPSERT_QUERY
        assert cur.execute.call_args_list[1].args[0] == PROGRESS_UPSERT_QUERY
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)


def test_tick_consumer_quarantines_invalid_json_and_advances_offset() -> None:
    """Invalid JSON poison payload is quarantined with sanitized payload before offset commit."""
    with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        db_mgr, cur = _make_mock_db()

        consumer = TickConsumer(
            ConsumerConfig(kafka_bootstrap_servers="localhost:9092"),
            db_mgr,
        )
        msg = _make_kafka_msg(b"not-valid-json api_key=supersecret123")
        consumer._process_message(msg)

        assert db_mgr.transaction.call_count == 1
        assert cur.execute.call_count == 2
        assert cur.execute.call_args_list[0].args[0] == QUARANTINE_INSERT_QUERY
        quarantine_params = cur.execute.call_args_list[0].args[1]
        assert "supersecret123" not in quarantine_params[8]
        assert "[REDACTED]" in quarantine_params[8]
        assert cur.execute.call_args_list[1].args[0] == PROGRESS_UPSERT_QUERY
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)


def test_tick_consumer_quarantines_validation_failure_message() -> None:
    """Pydantic ValidationError quarantines event with trace metadata and commits offset."""
    with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        db_mgr, cur = _make_mock_db()

        consumer = TickConsumer(
            ConsumerConfig(kafka_bootstrap_servers="localhost:9092"),
            db_mgr,
        )
        bad_payload = {
            "ticker": "AAPL",
            "event_timestamp": "2026-01-01T10:00:00+00:00",
            "price": -10.0,
            "volume": 10,
            "replay_sequence": 1,
            "dataset_id": "ds_bad",
            "source_event_id": "ds_bad:tick:1",
            "run_id": "run_bad",
        }
        msg = _make_kafka_msg(json.dumps(bad_payload).encode("utf-8"))
        consumer._process_message(msg)

        assert db_mgr.transaction.call_count == 1
        assert cur.execute.call_args_list[0].args[0] == QUARANTINE_INSERT_QUERY
        q_params = cur.execute.call_args_list[0].args[1]
        assert q_params[0] == "run_bad"
        assert q_params[1] == "ds_bad"
        assert q_params[2] == "ds_bad:tick:1"
        assert q_params[6] == "ValidationError"
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)


def test_consumers_use_business_identity_for_deduplication() -> None:
    """All consumers deduplicate on business identity (dataset_id, source_event_id), not Kafka offset."""
    for query in (
        TickConsumer.UPSERT_QUERY,
        SentimentConsumer.UPSERT_QUERY,
        BarConsumer.UPSERT_QUERY,
    ):
        assert "ON CONFLICT (dataset_id, source_event_id) DO NOTHING" in query
        assert "ON CONFLICT (kafka_partition, kafka_offset)" not in query


def test_sentiment_consumer_inserts_valid_event_to_postgres() -> None:
    """Valid sentiment event persists business row + delivery progress and commits offset."""
    with patch("consumer.sentiment_consumer.Consumer") as mock_consumer_cls:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        db_mgr, cur = _make_mock_db()

        consumer = SentimentConsumer(
            SentimentConsumerConfig(kafka_bootstrap_servers="localhost:9092"),
            db_mgr,
        )
        payload = {
            "ticker": "MSFT",
            "event_timestamp": "2026-01-01T10:05:00+00:00",
            "available_at": "2026-01-01T10:05:02+00:00",
            "source": "MARKETAUX",
            "headline": "Microsoft beats earnings",
            "sentiment_score": 0.72,
            "sentiment_label": "POSITIVE",
            "confidence": 0.95,
            "replay_sequence": 2,
            "dataset_id": "ds_news",
            "source_event_id": "ds_news:msft:2",
            "provenance": "real_historical",
        }
        msg = _make_kafka_msg(
            json.dumps(payload).encode("utf-8"),
            topic="raw.sentiment.events",
            partition=1,
            offset=50,
        )
        consumer._process_message(msg)

        assert db_mgr.transaction.call_count == 1
        assert cur.execute.call_args_list[0].args[0] == SentimentConsumer.UPSERT_QUERY
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)


def test_bar_consumer_inserts_canonical_bar_record() -> None:
    """Valid 1-minute OHLCV bar persists to raw.market_bars inside one transaction."""
    with patch("consumer.bar_consumer.Consumer") as mock_consumer_cls:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        db_mgr, cur = _make_mock_db()

        consumer = BarConsumer(
            BarConsumerConfig(kafka_bootstrap_servers="localhost:9092"),
            db_mgr,
        )
        payload = {
            "ticker": "AAPL",
            "bar_start": "2026-01-02T14:30:00+00:00",
            "bar_end": "2026-01-02T14:31:00+00:00",
            "available_at": "2026-01-02T14:31:05+00:00",
            "open": 185.0,
            "high": 185.5,
            "low": 184.9,
            "close": 185.2,
            "volume": 12000,
            "dataset_id": "fixture_smoke_v1",
            "source_event_id": "fixture_smoke_v1:AAPL:2026-01-02T14:31:00+00:00",
            "run_id": "replay_001",
            "provenance": "synthetic_mock",
            "availability_assumed": True,
        }
        msg = _make_kafka_msg(
            json.dumps(payload).encode("utf-8"),
            topic="raw.market.bars",
            partition=2,
            offset=9,
        )
        consumer._process_message(msg)

        assert db_mgr.transaction.call_count == 1
        assert cur.execute.call_args_list[0].args[0] == BarConsumer.UPSERT_QUERY
        mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)
