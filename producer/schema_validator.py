from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

ObservationType = Literal["trade_tick", "ohlcv_bar", "quote", "sentiment_event"]
ProvenanceType = Literal["real_historical", "real_live", "synthetic_fixture", "synthetic_mock"]


def _parse_utc_datetime(value: str | datetime) -> datetime:
    """Parse ISO string or datetime and normalize to timezone-aware UTC."""
    if isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        dt = value
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class MarketTickMessage(BaseModel):
    """Pydantic model for validating market tick messages before publishing to Kafka."""

    ticker: Annotated[str, Field(pattern=r"^[A-Z]{1,10}$")]
    event_timestamp: datetime
    event_at: datetime | None = None
    available_at: datetime | None = None
    ingested_at: datetime | None = None
    price: Annotated[float, Field(gt=0.0)]
    bid: Annotated[float | None, Field(gt=0.0)] = None
    ask: Annotated[float | None, Field(gt=0.0)] = None
    volume: Annotated[int, Field(ge=0)]
    trade_condition: Annotated[str | None, Field(max_length=20)] = None
    replay_sequence: int
    dataset_id: str | None = None
    source_event_id: str | None = None
    run_id: str | None = None
    observation_type: Literal["trade_tick"] = "trade_tick"
    provenance: ProvenanceType = "real_historical"

    @field_validator("event_timestamp", "event_at", "available_at", "ingested_at", mode="before")
    @classmethod
    def parse_timestamp(cls, value: str | datetime | None) -> datetime | None:
        """Ensure provided timestamps are timezone-aware UTC datetime objects."""
        if value is None:
            return None
        return _parse_utc_datetime(value)

    @model_validator(mode="after")
    def sync_event_timestamps(self) -> MarketTickMessage:
        """Keep legacy event_timestamp synchronized with canonical event_at."""
        if self.event_at is None:
            self.event_at = self.event_timestamp
        elif self.event_at != self.event_timestamp:
            raise ValueError("event_at must equal event_timestamp")
        if self.available_at is not None and self.available_at < self.event_at:
            raise ValueError("available_at cannot precede event_at")
        return self


class MarketBarMessage(BaseModel):
    """Canonical 1-minute OHLCV bar schema that preserves OHLCV instead of faking ticks."""

    ticker: Annotated[str, Field(pattern=r"^[A-Z]{1,10}$")]
    bar_start: datetime
    bar_end: datetime
    open: Annotated[float, Field(gt=0.0)]
    high: Annotated[float, Field(gt=0.0)]
    low: Annotated[float, Field(gt=0.0)]
    close: Annotated[float, Field(gt=0.0)]
    volume: Annotated[int, Field(ge=0)]
    trade_count: Annotated[int | None, Field(ge=0)] = None
    vwap: Annotated[float | None, Field(gt=0.0)] = None
    event_at: datetime | None = None
    available_at: datetime
    ingested_at: datetime | None = None
    availability_assumed: bool = False
    dataset_id: Annotated[str, Field(min_length=1, max_length=100)]
    source_event_id: Annotated[str, Field(min_length=1, max_length=200)]
    run_id: str | None = None
    observation_type: Literal["ohlcv_bar"] = "ohlcv_bar"
    provenance: ProvenanceType

    @field_validator(
        "bar_start", "bar_end", "event_at", "available_at", "ingested_at", mode="before"
    )
    @classmethod
    def parse_bar_timestamps(cls, value: str | datetime | None) -> datetime | None:
        """Normalize bar timestamps to UTC."""
        if value is None:
            return None
        return _parse_utc_datetime(value)

    @model_validator(mode="after")
    def validate_bar_invariants(self) -> MarketBarMessage:
        """Enforce OHLC ordering and temporal ordering invariants."""
        if self.bar_end <= self.bar_start:
            raise ValueError("bar_end must be strictly after bar_start")
        if self.event_at is None:
            self.event_at = self.bar_end
        elif self.event_at != self.bar_end:
            raise ValueError("OHLCV event_at must equal bar_end")
        if (self.bar_end - self.bar_start).total_seconds() != 60:
            raise ValueError("OHLCV bars must span exactly 60 seconds")
        if self.available_at < self.bar_end:
            raise ValueError("available_at cannot precede bar_end")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be >= max(open, close)")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be <= min(open, close)")
        if self.low > self.high:
            raise ValueError("low must be <= high")
        return self


class QuoteMessage(BaseModel):
    """Top-of-book quote schema explicitly separated from executed trades (no synthetic volume)."""

    ticker: Annotated[str, Field(pattern=r"^[A-Z]{1,10}$")]
    event_at: datetime
    available_at: datetime
    ingested_at: datetime | None = None
    bid: Annotated[float, Field(gt=0.0)]
    ask: Annotated[float, Field(gt=0.0)]
    bid_size: Annotated[int | None, Field(ge=0)] = None
    ask_size: Annotated[int | None, Field(ge=0)] = None
    dataset_id: Annotated[str, Field(min_length=1, max_length=100)]
    source_event_id: Annotated[str, Field(min_length=1, max_length=200)]
    run_id: str | None = None
    observation_type: Literal["quote"] = "quote"
    provenance: ProvenanceType

    @field_validator("event_at", "available_at", "ingested_at", mode="before")
    @classmethod
    def parse_quote_timestamps(cls, value: str | datetime | None) -> datetime | None:
        """Normalize quote timestamps to UTC."""
        if value is None:
            return None
        return _parse_utc_datetime(value)

    @model_validator(mode="after")
    def validate_quote_spread(self) -> QuoteMessage:
        """Ensure ask >= bid and available_at >= event_at."""
        if self.ask < self.bid:
            raise ValueError("ask must be >= bid")
        if self.available_at < self.event_at:
            raise ValueError("available_at cannot precede event_at")
        return self


class SentimentEventMessage(BaseModel):
    """Pydantic model for validating sentiment events before publishing to Kafka."""

    ticker: Annotated[str, Field(pattern=r"^[A-Z]{1,10}$")]
    event_timestamp: datetime
    event_at: datetime | None = None
    available_at: datetime | None = None
    ingested_at: datetime | None = None
    source: Annotated[str, Field(max_length=50)]
    headline: Annotated[str | None, Field(max_length=1000)] = None
    sentiment_score: Annotated[float, Field(ge=-1.0, le=1.0)]
    sentiment_label: Annotated[str, Field(pattern=r"^(POSITIVE|NEGATIVE|NEUTRAL)$")]
    confidence: Annotated[float, Field(ge=0.0, le=1.0)]
    replay_sequence: int
    dataset_id: str | None = None
    source_event_id: str | None = None
    run_id: str | None = None
    observation_type: Literal["sentiment_event"] = "sentiment_event"
    provenance: ProvenanceType = "real_historical"

    @field_validator("event_timestamp", "event_at", "available_at", "ingested_at", mode="before")
    @classmethod
    def parse_timestamp(cls, value: str | datetime | None) -> datetime | None:
        """Ensure provided timestamps are timezone-aware UTC datetime objects."""
        if value is None:
            return None
        return _parse_utc_datetime(value)

    @model_validator(mode="after")
    def sync_event_timestamps(self) -> SentimentEventMessage:
        """Keep legacy event_timestamp synchronized with canonical event_at."""
        if self.event_at is None:
            self.event_at = self.event_timestamp
        elif self.event_at != self.event_timestamp:
            raise ValueError("event_at must equal event_timestamp")
        if self.available_at is not None and self.available_at < self.event_at:
            raise ValueError("available_at cannot precede event_at")
        return self

    @property
    def is_eligible_for_primary_experiment(self) -> bool:
        """Only sentiment with explicit availability time and real/audited provenance is eligible."""
        return self.available_at is not None and self.provenance in (
            "real_historical",
            "real_live",
        )
