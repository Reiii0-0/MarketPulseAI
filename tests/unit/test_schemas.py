from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from producer.schema_validator import MarketTickMessage, SentimentEventMessage


@pytest.mark.unit
def test_market_tick_message_valid_parses_correctly(fake_market_tick):
    """Verify a valid tick dict is accepted."""
    msg = MarketTickMessage(**fake_market_tick)
    assert msg.ticker == "AAPL"
    assert isinstance(msg.event_timestamp, datetime)
    assert msg.price == 185.2350


@pytest.mark.unit
def test_market_tick_message_rejects_invalid_ticker(fake_market_tick):
    """Verify lowercase ticker is rejected."""
    fake_market_tick["ticker"] = "aapl"
    with pytest.raises(ValidationError):
        MarketTickMessage(**fake_market_tick)


@pytest.mark.unit
def test_market_tick_message_rejects_zero_price(fake_market_tick):
    """Verify zero price is rejected."""
    fake_market_tick["price"] = 0.0
    with pytest.raises(ValidationError):
        MarketTickMessage(**fake_market_tick)


@pytest.mark.unit
def test_market_tick_message_rejects_negative_volume(fake_market_tick):
    """Verify negative volume is rejected."""
    fake_market_tick["volume"] = -1
    with pytest.raises(ValidationError):
        MarketTickMessage(**fake_market_tick)


@pytest.mark.unit
def test_sentiment_event_message_valid_parses_correctly(fake_sentiment_event):
    """Verify a valid sentiment dict is accepted."""
    msg = SentimentEventMessage(**fake_sentiment_event)
    assert msg.sentiment_label == "POSITIVE"
    assert msg.sentiment_score == 0.7821


@pytest.mark.unit
def test_sentiment_event_message_rejects_score_above_one(fake_sentiment_event):
    """Verify score > 1.0 is rejected."""
    fake_sentiment_event["sentiment_score"] = 1.1
    with pytest.raises(ValidationError):
        SentimentEventMessage(**fake_sentiment_event)


@pytest.mark.unit
def test_sentiment_event_message_rejects_invalid_label(fake_sentiment_event):
    """Verify invalid label is rejected."""
    fake_sentiment_event["sentiment_label"] = "ANGRY"
    with pytest.raises(ValidationError):
        SentimentEventMessage(**fake_sentiment_event)
