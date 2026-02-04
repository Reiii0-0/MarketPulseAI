from __future__ import annotations

import logging
from unittest.mock import patch

import pandas as pd
import pytest

from producer.config import ProducerConfig
from producer.replay_producer import ReplayProducer


@pytest.mark.unit
def test_producer_reads_csv_rows_in_timestamp_order(tmp_path, mock_kafka_producer):
    """T-015a: Verify messages are published in event_timestamp ascending order."""
    # Create mock CSV files
    tick_file = tmp_path / "ticks.csv"
    sentiment_file = tmp_path / "sentiment.csv"

    # Write out of order
    tick_data = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "event_timestamp": "2024-01-15T10:00:00Z",
                "price": 180.0,
                "volume": 100,
            },
            {
                "ticker": "AAPL",
                "event_timestamp": "2024-01-15T09:00:00Z",
                "price": 179.0,
                "volume": 100,
            },
        ]
    )
    tick_data.to_csv(tick_file, index=False)
    sentiment_data = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "event_timestamp": "2024-01-15T09:30:00Z",
                "source": "x",
                "sentiment_score": 0.5,
                "sentiment_label": "POSITIVE",
                "confidence": 0.9,
            }
        ]
    )
    sentiment_data.to_csv(sentiment_file, index=False)

    config = ProducerConfig(
        replay_tick_data_path=str(tick_file),
        replay_sentiment_data_path=str(sentiment_file),
        replay_speed_multiplier=0,  # No sleep
    )

    with patch("producer.replay_producer.Producer", return_value=mock_kafka_producer):
        replayer = ReplayProducer(config)
        replayer.run()

    # Check calls to produce
    calls = mock_kafka_producer.produce.call_args_list
    assert len(calls) == 3

    # Extract timestamps from JSON values
    import json
    timestamps = [pd.to_datetime(json.loads(c.kwargs["value"])["event_timestamp"]) for c in calls]
    assert timestamps == sorted(timestamps)
    assert timestamps[0].hour == 9
    assert timestamps[1].minute == 30
    assert timestamps[2].hour == 10


@pytest.mark.unit
def test_producer_validates_each_row_before_publishing(tmp_path, mock_kafka_producer, caplog):
    """T-015b: Verify invalid rows are skipped and logged."""
    tick_file = tmp_path / "ticks.csv"
    # Invalid ticker (lowercase)
    tick_data = pd.DataFrame(
        [
            {"ticker": "aapl", "event_timestamp": "2024-01-15T09:00:00Z", "price": 180.0, "volume": 100}
        ]
    )
    tick_data.to_csv(tick_file, index=False)

    config = ProducerConfig(
        replay_tick_data_path=str(tick_file),
        replay_sentiment_data_path="nonexistent.csv",
        replay_speed_multiplier=0,
    )

    with caplog.at_level(logging.WARNING):
        with patch("producer.replay_producer.Producer", return_value=mock_kafka_producer):
            replayer = ReplayProducer(config)
            replayer.run()

    # Produce should NOT have been called
    assert mock_kafka_producer.produce.call_count == 0
    assert "Skipping invalid TICK row" in caplog.text


@pytest.mark.unit
def test_producer_applies_speed_multiplier_to_delay(tmp_path, mock_kafka_producer):
    """T-015c: Verify timing logic uses the speed multiplier."""
    tick_file = tmp_path / "ticks.csv"
    # 10 seconds apart
    tick_data = pd.DataFrame(
        [
            {"ticker": "AAPL", "event_timestamp": "2024-01-15T09:00:00Z", "price": 180.0, "volume": 100},
            {"ticker": "AAPL", "event_timestamp": "2024-01-15T09:00:10Z", "price": 181.0, "volume": 100},
        ]
    )
    tick_data.to_csv(tick_file, index=False)

    config = ProducerConfig(
        replay_tick_data_path=str(tick_file),
        replay_sentiment_data_path="nonexistent.csv",
        replay_speed_multiplier=10.0,
    )

    with patch("time.sleep") as mock_sleep:
        with patch("producer.replay_producer.Producer", return_value=mock_kafka_producer):
            replayer = ReplayProducer(config)
            replayer.run()

    # Second row should trigger sleep of (10s / 10) = 1s (minus small execution time)
    assert mock_sleep.call_count >= 1
    sleep_val = mock_sleep.call_args_list[0].args[0]
    assert 0 < sleep_val <= 1.0


@pytest.mark.unit
def test_producer_assigns_monotonically_increasing_replay_sequence(tmp_path, mock_kafka_producer):
    """T-015d: Verify replay_sequence increments."""
    tick_file = tmp_path / "ticks.csv"
    tick_data = pd.DataFrame(
        [
            {"ticker": "AAPL", "event_timestamp": "2024-01-15T09:00:00Z", "price": 180.0, "volume": 100},
            {"ticker": "MSFT", "event_timestamp": "2024-01-15T09:01:00Z", "price": 400.0, "volume": 50},
        ]
    )
    tick_data.to_csv(tick_file, index=False)

    config = ProducerConfig(
        replay_tick_data_path=str(tick_file),
        replay_sentiment_data_path="nonexistent.csv",
        replay_speed_multiplier=0,
    )

    with patch("producer.replay_producer.Producer", return_value=mock_kafka_producer):
        replayer = ReplayProducer(config)
        replayer.run()

    calls = mock_kafka_producer.produce.call_args_list
    import json
    seq1 = json.loads(calls[0].kwargs["value"])["replay_sequence"]
    seq2 = json.loads(calls[1].kwargs["value"])["replay_sequence"]
    assert seq2 == seq1 + 1


@pytest.mark.unit
def test_producer_flushes_after_batch(tmp_path, mock_kafka_producer):
    """T-015e: Verify flush is called."""
    tick_file = tmp_path / "ticks.csv"
    tick_data = pd.DataFrame(
        [{"ticker": "AAPL", "event_timestamp": "2024-01-15T09:00:00Z", "price": 180.0, "volume": 100}]
    )
    tick_data.to_csv(tick_file, index=False)

    config = ProducerConfig(
        replay_tick_data_path=str(tick_file),
        replay_sentiment_data_path="nonexistent.csv",
        replay_speed_multiplier=0,
        replay_batch_size=1,
    )

    with patch("producer.replay_producer.Producer", return_value=mock_kafka_producer):
        replayer = ReplayProducer(config)
        replayer.run()

    # Flush called once for batch and once at the end
    assert mock_kafka_producer.flush.call_count >= 2
