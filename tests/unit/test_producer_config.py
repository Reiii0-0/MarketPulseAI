from __future__ import annotations

import os
from unittest.mock import patch

from producer.config import ProducerConfig


def test_producer_config_env():
    """Verifies ProducerConfig loads from environment."""
    env = {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_TOPIC_TICKS": "ticks",
        "KAFKA_TOPIC_SENTIMENT": "sentiment",
        "REPLAY_SPEED_MULTIPLIER": "2.0",
    }
    with patch.dict(os.environ, env):
        config = ProducerConfig()
        assert config.kafka_bootstrap_servers == "localhost:9092"
        assert config.replay_speed_multiplier == 2.0
