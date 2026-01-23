from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel


class SHAPFactor(BaseModel):
    """SHAP contribution factor for a specific feature."""
    feature: str
    shap_value: float
    direction: Literal["positive", "negative"]


class PredictionResponse(BaseModel):
    """Response schema for a single movement prediction."""
    request_id: UUID
    ticker: str
    window_start: datetime
    predicted_direction: Literal["UP", "DOWN"]
    probability_up: float
    probability_down: float
    confidence_tier: Literal["HIGH", "MEDIUM", "LOW"]
    factors: list[SHAPFactor]
    model_version: str
    predicted_at: datetime


class BatchPredictionResponse(BaseModel):
    """Response schema for a batch of predictions."""
    batch_id: UUID
    count: int
    predictions: list[PredictionResponse]
    processed_at: datetime


class HealthResponse(BaseModel):
    """Response schema for system health check."""
    status: Literal["healthy", "degraded", "unhealthy"]
    model_loaded: bool
    db_connected: bool
    uptime_seconds: int
    version: str


class ModelInfoResponse(BaseModel):
    """Response schema for current model metadata."""
    model_version: str
    algorithm: str
    training_date: datetime
    feature_count: int
    feature_names: list[str]
    training_accuracy: float
    training_samples: int
    hyperparameters: dict[str, float | int]


class RecentPredictionItem(BaseModel):
    """Simplified prediction record for history history."""
    request_id: UUID
    ticker: str
    window_start: datetime
    predicted_direction: Literal["UP", "DOWN"]
    probability_up: float
    confidence_tier: Literal["HIGH", "MEDIUM", "LOW"]
    actual_direction: Literal["UP", "DOWN"] | None = None
    was_correct: bool | None = None
    predicted_at: datetime


class RecentPredictionsResponse(BaseModel):
    """Response schema for recent predictions history."""
    total: int
    limit: int
    offset: int
    predictions: list[RecentPredictionItem]


class ErrorDetail(BaseModel):
    """Standardized error envelope for API failures."""
    error: str
    message: str
    details: dict[str, str] | None = None
