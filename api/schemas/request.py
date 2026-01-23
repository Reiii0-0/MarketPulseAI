from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, model_validator


class PredictionRequest(BaseModel):
    """Request schema for predicting price movement of a 5-minute window.

    Includes OHLCV data and optional technical/sentiment indicators.
    """

    ticker: Annotated[str, Field(pattern=r"^[A-Z]{1,10}$")]
    window_start: datetime
    open: Annotated[float, Field(gt=0.0)]
    high: Annotated[float, Field(gt=0.0)]
    low: Annotated[float, Field(gt=0.0)]
    close: Annotated[float, Field(gt=0.0)]
    volume: Annotated[int, Field(ge=0)]
    sma_20: Annotated[float | None, Field(gt=0.0)] = None
    ema_12: Annotated[float | None, Field(gt=0.0)] = None
    ema_26: Annotated[float | None, Field(gt=0.0)] = None
    rsi_14: Annotated[float | None, Field(ge=0.0, le=100.0)] = None
    macd_line: float | None = None
    macd_signal: float | None = None
    macd_histogram: float | None = None
    avg_sentiment_score: Annotated[float | None, Field(ge=-1.0, le=1.0)] = None
    sentiment_event_count: Annotated[int | None, Field(ge=0)] = None
    sentiment_polarity_shift: Annotated[float | None, Field(ge=-2.0, le=2.0)] = None

    @model_validator(mode="after")
    def validate_ohlcv(self) -> PredictionRequest:
        """Cross-field validation for OHLCV logic.

        Ensures high is the maximum and low is the minimum.
        """
        if self.high < self.low:
            raise ValueError("high price cannot be less than low price")
        if self.high < self.open:
            raise ValueError("high price cannot be less than open price")
        if self.high < self.close:
            raise ValueError("high price cannot be less than close price")
        if self.low > self.open:
            raise ValueError("low price cannot be greater than open price")
        if self.low > self.close:
            raise ValueError("low price cannot be greater than close price")
        return self


class BatchPredictionRequest(BaseModel):
    """Batch request schema for multiple predictions (max 100)."""

    requests: Annotated[list[PredictionRequest], Field(min_length=1, max_length=100)]
