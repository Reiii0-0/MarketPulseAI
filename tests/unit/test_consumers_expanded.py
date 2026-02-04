from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from consumer.sentiment_consumer import SentimentConsumer
from consumer.tick_consumer import TickConsumer

@pytest.fixture
def mock_db_manager():
    return MagicMock()

@pytest.fixture
def mock_consumer_base():
    with patch("confluent_kafka.Consumer") as mock:
        yield mock

def test_sentiment_consumer_process_message(mock_db_manager, mock_consumer_base):
    """Verifies sentiment consumer correctly parses and saves messages."""
    config = MagicMock()
    config.kafka_bootstrap_servers = "localhost"
    config.kafka_consumer_group_sentiment = "test-group"
    config.kafka_auto_offset_reset = "earliest"
    config.kafka_enable_auto_commit = True
    
    consumer = SentimentConsumer(config, mock_db_manager)
    
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = json.dumps({
        "ticker": "AAPL",
        "event_timestamp": "2023-01-01T00:00:00Z",
        "source": "news",
        "sentiment_score": 0.5,
        "sentiment_label": "POSITIVE",
        "confidence": 0.9,
        "replay_sequence": 1
    }).encode("utf-8")
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 100
    
    consumer._process_message(mock_msg)
    
    assert mock_db_manager.execute_upsert.called
    args = mock_db_manager.execute_upsert.call_args[0]
    assert "raw.sentiment_events" in args[0]
    assert args[1]["ticker"] == "AAPL"

def test_tick_consumer_process_message(mock_db_manager, mock_consumer_base):
    """Verifies tick consumer correctly parses and saves messages."""
    config = MagicMock()
    config.kafka_bootstrap_servers = "localhost"
    config.kafka_consumer_group_ticks = "test-group"
    config.kafka_auto_offset_reset = "earliest"
    config.kafka_enable_auto_commit = True
    
    consumer = TickConsumer(config, mock_db_manager)
    
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = json.dumps({
        "ticker": "AAPL",
        "event_timestamp": "2023-01-01T00:00:00Z",
        "price": 150.0,
        "volume": 1000,
        "bid": 149.9,
        "ask": 150.1,
        "replay_sequence": 1
    }).encode("utf-8")
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 200
    
    consumer._process_message(mock_msg)
    
    assert mock_db_manager.execute_upsert.called
    args = mock_db_manager.execute_upsert.call_args[0]
    assert "raw.market_ticks" in args[0]
    assert args[1]["price"] == 150.0

def test_sentiment_consumer_error(mock_db_manager, mock_consumer_base):
    """Verifies sentiment consumer handles Kafka errors."""
    config = MagicMock()
    config.kafka_bootstrap_servers = "localhost"
    config.kafka_consumer_group_sentiment = "test-group"
    config.kafka_auto_offset_reset = "earliest"
    config.kafka_enable_auto_commit = True
    
    consumer = SentimentConsumer(config, mock_db_manager)
    
    mock_msg = MagicMock()
    mock_msg.error.return_value = MagicMock(code=lambda: "Error")
    
    # Should not raise exception
    consumer._process_message(mock_msg)
    assert not mock_db_manager.execute_upsert.called

def test_tick_consumer_validation_fail(mock_db_manager, mock_consumer_base):
    """Verifies tick consumer handles invalid data."""
    config = MagicMock()
    config.kafka_bootstrap_servers = "localhost"
    config.kafka_consumer_group_ticks = "test-group"
    config.kafka_auto_offset_reset = "earliest"
    config.kafka_enable_auto_commit = True
    
    consumer = TickConsumer(config, mock_db_manager)
    
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b"invalid-json"
    
    consumer._process_message(mock_msg)
    assert not mock_db_manager.execute_upsert.called
