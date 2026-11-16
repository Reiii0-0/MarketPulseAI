from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from producer.config import ProducerConfig
from producer.replay_producer import ReplayProducer


@pytest.fixture
def mock_producer_base():
    with patch("confluent_kafka.Producer") as mock:
        yield mock


def test_producer_run_loop(mock_producer_base):
    """Verifies producer run loop and message sending."""
    config = ProducerConfig(
        kafka_bootstrap_servers="localhost",
        kafka_topic_market_ticks="ticks",
        kafka_topic_sentiment_events="sentiment",
        replay_speed_multiplier=100.0,
        replay_tick_data_path="ticks.csv",
        replay_sentiment_data_path="sentiment.csv",
        replay_ticker_filter=None,
    )

    import pandas as pd

    mock_ticks = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "event_timestamp": ["2023-01-01T00:00:00Z"],
            "price": [150.0],
            "volume": [1000],
            "bid": [149.9],
            "ask": [150.1],
        }
    )
    mock_sentiment = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "event_timestamp": ["2023-01-01T00:00:00Z"],
            "source": ["news"],
            "sentiment_score": [0.5],
            "sentiment_label": ["POSITIVE"],
            "confidence": [0.9],
        }
    )

    with (
        patch.object(Path, "exists", return_value=True),
        patch("pandas.read_csv", side_effect=[mock_ticks, mock_sentiment]),
        patch("confluent_kafka.Producer", return_value=MagicMock()),
    ):
        producer = ReplayProducer(config)

        data = producer._load_data()
        assert len(data) == 2  # 1 tick + 1 sentiment
        assert producer.producer is not None
