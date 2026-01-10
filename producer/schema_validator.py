from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, field_validator


class MarketTickMessage(BaseModel):
    """Pydantic model for validating market tick messages before publishing to Kafka.

    Attributes:
        ticker (str): 1–10 uppercase characters.
        event_timestamp (datetime): Original event time from the source dataset.
        price (float): Last traded price (> 0.0).
        bid (float | None): Best bid price (> 0.0).
        ask (float | None): Best ask price (> 0.0).
        volume (int): Shares traded in this tick (>= 0).
        trade_condition (str | None): Exchange condition code (max 20 chars).
        replay_sequence (int): Monotonically increasing per producer session.
    """

    ticker: Annotated[str, Field(pattern=r"^[A-Z]{1,10}$")]
    event_timestamp: datetime
    price: Annotated[float, Field(gt=0.0)]
    bid: Annotated[float | None, Field(gt=0.0)] = None
    ask: Annotated[float | None, Field(gt=0.0)] = None
    volume: Annotated[int, Field(ge=0)]
    trade_condition: Annotated[str | None, Field(max_length=20)] = None
    replay_sequence: int

    @field_validator("event_timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, value: str | datetime) -> datetime:
        """Ensure the timestamp is a timezone-aware datetime object.

        Args:
            value (str | datetime): The timestamp to parse.

        Returns:
            datetime: Timezone-aware datetime object.
        """
        if isinstance(value, str):
            # Handle ISO format strings from CSV/JSON
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt
        return value


class SentimentEventMessage(BaseModel):
    """Pydantic model for validating sentiment events before publishing to Kafka.

    Attributes:
        ticker (str): 1–10 uppercase characters.
        event_timestamp (datetime): Timestamp of the news/social event.
        source (str): Source of the sentiment event (max 50 chars).
        headline (str | None): Raw headline or text snippet (max 1000 chars).
        sentiment_score (float): Pre-computed NLP score in [-1.0, 1.0].
        sentiment_label (str): Sentiment classification (POSITIVE, NEGATIVE, NEUTRAL).
        confidence (float): Model confidence in [0.0, 1.0].
        replay_sequence (int): Monotonically increasing per producer session.
    """

    ticker: Annotated[str, Field(pattern=r"^[A-Z]{1,10}$")]
    event_timestamp: datetime
    source: Annotated[str, Field(max_length=50)]
    headline: Annotated[str | None, Field(max_length=1000)] = None
    sentiment_score: Annotated[float, Field(ge=-1.0, le=1.0)]
    sentiment_label: Annotated[str, Field(pattern=r"^(POSITIVE|NEGATIVE|NEUTRAL)$")]
    confidence: Annotated[float, Field(ge=0.0, le=1.0)]
    replay_sequence: int

    @field_validator("event_timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, value: str | datetime) -> datetime:
        """Ensure the timestamp is a timezone-aware datetime object.

        Args:
            value (str | datetime): The timestamp to parse.

        Returns:
            datetime: Timezone-aware datetime object.
        """
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt
        return value
