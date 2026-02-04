from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import Message

from consumer.db import DatabaseManager
from consumer.sentiment_consumer import SentimentConsumer
from consumer.tick_consumer import TickConsumer


@pytest.fixture
def mock_msg():
    """Create a mock Kafka message."""
    msg = MagicMock(spec=Message)
    msg.error.return_value = None
    msg.offset.return_value = 123
    msg.partition.return_value = 0
    return msg


@pytest.mark.unit
def test_tick_consumer_inserts_valid_message_to_postgres(fake_market_tick, mock_msg, mocker):
    """T-024a: Verify valid tick causes DB insert and Kafka commit."""
    mock_msg.value.return_value = json.dumps(fake_market_tick).encode("utf-8")

    db_mgr = MagicMock(spec=DatabaseManager)
    config = ConsumerConfig(kafka_bootstrap_servers="localhost:9092")

    with patch("consumer.tick_consumer.Consumer"):
        consumer = TickConsumer(config, db_mgr)
        consumer.consumer = MagicMock() # Mock the actual confluent-kafka consumer

        consumer._process_message(mock_msg)

        # Verify DB insert
        assert db_mgr.execute_upsert.call_count == 1
        query, params = db_mgr.execute_upsert.call_args[0]
        assert "INSERT INTO raw.market_ticks" in query
        assert params["ticker"] == "AAPL"
        assert params["kafka_offset"] == 123

        # Verify Kafka commit
        consumer.consumer.commit.assert_called_once_with(message=mock_msg, asynchronous=False)


@pytest.mark.unit
def test_tick_consumer_skips_invalid_json_message(mock_msg, caplog):
    """T-024b: Verify invalid JSON is caught and logged."""
    mock_msg.value.return_value = b"invalid json {"

    db_mgr = MagicMock(spec=DatabaseManager)
    config = ConsumerConfig(kafka_bootstrap_servers="localhost:9092")

    with patch("consumer.tick_consumer.Consumer"):
        consumer = TickConsumer(config, db_mgr)
        consumer.consumer = MagicMock()

        consumer._process_message(mock_msg)

        assert db_mgr.execute_upsert.call_count == 0
        assert consumer.consumer.commit.call_count == 0
        assert "Failed to decode JSON from Kafka" in caplog.text


@pytest.mark.unit
def test_tick_consumer_skips_validation_failure_message(fake_market_tick, mock_msg, caplog):
    """T-024c: Verify validation failure skips DB write."""
    fake_market_tick["price"] = -10.0 # Invalid
    mock_msg.value.return_value = json.dumps(fake_market_tick).encode("utf-8")

    db_mgr = MagicMock(spec=DatabaseManager)
    config = ConsumerConfig(kafka_bootstrap_servers="localhost:9092")

    with patch("consumer.tick_consumer.Consumer"):
        consumer = TickConsumer(config, db_mgr)
        consumer.consumer = MagicMock()

        consumer._process_message(mock_msg)

        assert db_mgr.execute_upsert.call_count == 0
        assert "Validation failure for tick message" in caplog.text


@pytest.mark.unit
def test_consumer_uses_kafka_offset_for_deduplication():
    """T-024d: Verify SQL query contains ON CONFLICT."""
    assert "ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING" in TickConsumer.UPSERT_QUERY
    assert "ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING" in SentimentConsumer.UPSERT_QUERY


@pytest.mark.unit
def test_sentiment_consumer_inserts_valid_event_to_postgres(fake_sentiment_event, mock_msg):
    """T-024e: Verify valid sentiment event causes DB insert."""
    mock_msg.value.return_value = json.dumps(fake_sentiment_event).encode("utf-8")

    db_mgr = MagicMock(spec=DatabaseManager)
    config = SentimentConsumerConfig(kafka_bootstrap_servers="localhost:9092")

    with patch("consumer.sentiment_consumer.Consumer"):
        consumer = SentimentConsumer(config, db_mgr)
        consumer.consumer = MagicMock()

        consumer._process_message(mock_msg)

        assert db_mgr.execute_upsert.call_count == 1
        query, params = db_mgr.execute_upsert.call_args[0]
        assert "INSERT INTO raw.sentiment_events" in query
        assert params["ticker"] == "AAPL"
        assert params["sentiment_label"] == "POSITIVE"

        consumer.consumer.commit.assert_called_once_with(message=mock_msg, asynchronous=False)
