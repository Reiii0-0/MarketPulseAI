from __future__ import annotations

import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from consumer.base import QUARANTINE_INSERT_QUERY
from consumer.db import DatabaseManager
from consumer.sentiment_consumer import SentimentConsumer
from consumer.tick_consumer import TickConsumer


@pytest.fixture
def mock_db_manager() -> tuple[MagicMock, MagicMock]:
    db = MagicMock(spec=DatabaseManager)
    cur = MagicMock()

    @contextmanager
    def _tx():
        yield cur

    db.transaction.side_effect = _tx
    return db, cur


def test_sentiment_consumer_process_message(mock_db_manager: tuple[MagicMock, MagicMock]) -> None:
    """Verifies sentiment consumer processes valid event transactionally and commits Kafka offset."""
    db_mgr, cur = mock_db_manager
    config = MagicMock()
    config.kafka_bootstrap_servers = "localhost"
    config.kafka_topic_sentiment_events = "raw.sentiment.events"
    config.kafka_consumer_group_sentiment = "test-group"
    config.kafka_auto_offset_reset = "earliest"
    config.kafka_enable_auto_commit = False

    with patch("consumer.sentiment_consumer.Consumer") as mock_consumer_cls:
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        consumer = SentimentConsumer(config, db_mgr)

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.topic.return_value = "raw.sentiment.events"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 10
        mock_msg.value.return_value = json.dumps(
            {
                "ticker": "AAPL",
                "event_timestamp": "2024-01-01T10:00:00Z",
                "source": "MarketAux",
                "headline": "Good news",
                "sentiment_score": 0.8,
                "sentiment_label": "POSITIVE",
                "confidence": 0.9,
                "replay_sequence": 1,
            }
        ).encode("utf-8")

        consumer._process_message(mock_msg)
        assert db_mgr.transaction.called
        assert cur.execute.call_args_list[0].args[0] == SentimentConsumer.UPSERT_QUERY
        mock_kafka.commit.assert_called_once_with(message=mock_msg, asynchronous=False)


def test_tick_consumer_process_message(mock_db_manager: tuple[MagicMock, MagicMock]) -> None:
    """Verifies tick consumer computes fallback business identity when omitted and commits."""
    db_mgr, cur = mock_db_manager
    config = MagicMock()
    config.kafka_bootstrap_servers = "localhost"
    config.kafka_topic_market_ticks = "raw.market.ticks"
    config.kafka_consumer_group_ticks = "test-group"
    config.kafka_auto_offset_reset = "earliest"
    config.kafka_enable_auto_commit = False

    with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        consumer = TickConsumer(config, db_mgr)

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.topic.return_value = "raw.market.ticks"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 20
        mock_msg.value.return_value = json.dumps(
            {
                "ticker": "MSFT",
                "event_timestamp": "2024-01-01T10:00:00Z",
                "price": 300.5,
                "volume": 100,
                "replay_sequence": 1,
            }
        ).encode("utf-8")

        consumer._process_message(mock_msg)
        assert db_mgr.transaction.called
        params = cur.execute.call_args_list[0].args[1]
        assert params["dataset_id"] == "live:raw.market.ticks"
        assert params["source_event_id"].startswith("tick:")
        mock_kafka.commit.assert_called_once_with(message=mock_msg, asynchronous=False)


def test_sentiment_consumer_error(mock_db_manager: tuple[MagicMock, MagicMock]) -> None:
    """Verifies sentiment consumer quarantines malformed payloads without calling business upsert."""
    db_mgr, cur = mock_db_manager
    config = MagicMock()
    config.kafka_bootstrap_servers = "localhost"
    config.kafka_topic_sentiment_events = "raw.sentiment.events"
    config.kafka_consumer_group_sentiment = "test-group"
    config.kafka_auto_offset_reset = "earliest"
    config.kafka_enable_auto_commit = False

    with patch("consumer.sentiment_consumer.Consumer") as mock_consumer_cls:
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        consumer = SentimentConsumer(config, db_mgr)

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.topic.return_value = "raw.sentiment.events"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 30
        mock_msg.value.return_value = b"not-json"

        consumer._process_message(mock_msg)
        assert cur.execute.call_args_list[0].args[0] == QUARANTINE_INSERT_QUERY
        mock_kafka.commit.assert_called_once_with(message=mock_msg, asynchronous=False)


def test_tick_consumer_validation_fail(mock_db_manager: tuple[MagicMock, MagicMock]) -> None:
    """Verifies tick consumer quarantines invalid payloads rather than dropping or crashing."""
    db_mgr, cur = mock_db_manager
    config = MagicMock()
    config.kafka_bootstrap_servers = "localhost"
    config.kafka_topic_market_ticks = "raw.market.ticks"
    config.kafka_consumer_group_ticks = "test-group"
    config.kafka_auto_offset_reset = "earliest"
    config.kafka_enable_auto_commit = False

    with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        consumer = TickConsumer(config, db_mgr)

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.topic.return_value = "raw.market.ticks"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 40
        mock_msg.value.return_value = b"invalid-json"

        consumer._process_message(mock_msg)
        assert cur.execute.call_args_list[0].args[0] == QUARANTINE_INSERT_QUERY
        mock_kafka.commit.assert_called_once_with(message=mock_msg, asynchronous=False)
