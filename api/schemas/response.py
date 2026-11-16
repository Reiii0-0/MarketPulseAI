from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

DirectionLabel = Literal["UP", "NOT_UP"]
SignalLabel = Literal["UP", "NOT_UP", "NO_SIGNAL"]


class SHAPFactor(BaseModel):
    """Feature attribution factor reported in the predictor's native score space."""

    feature: str
    shap_value: float
    direction: Literal["positive", "negative"]


class PredictionResponse(BaseModel):
    """Response schema for a single movement prediction."""

    request_id: UUID
    ticker: str
    window_start: datetime
    predicted_direction: DirectionLabel
    signal_label: SignalLabel = "UP"
    probability_up: float
    probability_down: float
    probability_not_up: float | None = None
    confidence_tier: Literal["HIGH", "MEDIUM", "LOW"]
    factors: list[SHAPFactor]
    attribution_space: str = "base_model_log_odds"
    model_version: str
    predicted_at: datetime


class BatchPredictionResponse(BaseModel):
    """Response schema for a batch of predictions."""

    batch_id: UUID
    count: int
    predictions: list[PredictionResponse]
    processed_at: datetime


class HealthResponse(BaseModel):
    """Response schema for process liveness check."""

    status: Literal["healthy", "degraded", "unhealthy"]
    model_loaded: bool
    db_connected: bool
    contract_loaded: bool = True
    readiness_reasons: list[str] = Field(default_factory=list)
    uptime_seconds: int
    version: str


class ReadinessResponse(BaseModel):
    """Response schema for readiness gate (DB + verified bundle + contract)."""

    ready: bool
    status: Literal["ready", "not_ready"]
    db_connected: bool
    model_loaded: bool
    contract_loaded: bool
    reasons: list[str] = Field(default_factory=list)


class ModelInfoResponse(BaseModel):
    """Response schema for current model metadata and contract lineage."""

    model_version: str
    algorithm: str
    champion_model: str = "xgboost"
    training_date: datetime
    feature_count: int
    feature_names: list[str]
    training_accuracy: float
    training_samples: int
    hyperparameters: dict[str, Any]
    contract_id: str | None = None
    contract_version: str | None = None
    fit_information_end: datetime | None = None
    is_real_market_data: bool | None = None


class RecentPredictionItem(BaseModel):
    """Simplified prediction record for recent predictions history."""

    request_id: UUID
    ticker: str
    window_start: datetime
    predicted_direction: DirectionLabel
    probability_up: float
    confidence_tier: Literal["HIGH", "MEDIUM", "LOW"]
    actual_direction: DirectionLabel | None = None
    was_correct: bool | None = None
    predicted_at: datetime


class RecentPredictionsResponse(BaseModel):
    """Response schema for recent predictions history."""

    total: int
    limit: int
    offset: int
    predictions: list[RecentPredictionItem]


class RunSummaryItem(BaseModel):
    """Read-only summary of an ingestion/prediction run."""

    run_id: str
    dataset_id: str
    mode: str
    provenance: str
    status: str
    logical_as_of: datetime | None = None
    dataset_end_at: datetime | None = None
    worker_status: str | None = None
    last_prepared_bar_end: datetime | None = None
    last_predicted_bar_end: datetime | None = None
    predictions_written: int = 0
    expired_backlog_count: int = 0
    last_compute_duration_ms: float | None = None


class RunListResponse(BaseModel):
    """Read-only list of recorded runs."""

    total: int
    runs: list[RunSummaryItem]


class RunDetailResponse(BaseModel):
    """Detailed read-only status of a run and its delivery partition progress."""

    run: RunSummaryItem
    delivery_progress: list[dict[str, Any]]
    fit_information_end: datetime | None = None
    serves_post_fit_window: bool | None = None


class FinalBarItem(BaseModel):
    """Read-only canonical finalized 1-minute bar."""

    contract_id: str
    dataset_id: str
    ticker: str
    bar_start: datetime
    bar_end: datetime
    available_at: datetime
    finalized_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    provenance: str


class BarsListResponse(BaseModel):
    """Paginated read-only finalized bars."""

    total: int
    limit: int
    offset: int
    bars: list[FinalBarItem]


class WorkerPredictionItem(BaseModel):
    """Read-only worker prediction joined with non-mutating realized outcome status."""

    prediction_id: UUID
    contract_id: str
    dataset_id: str
    run_id: str | None = None
    ticker: str
    bar_end: datetime
    decision_at: datetime
    target_at: datetime
    feature_hash: str
    bundle_id: str | None = None
    model_version: str
    champion_model: str
    signal_label: SignalLabel
    predicted_direction: DirectionLabel
    probability_up: float
    probability_not_up: float
    confidence_tier: Literal["HIGH", "MEDIUM", "LOW"]
    attribution_space: str
    factors: list[SHAPFactor]
    run_mode: str
    timeliness_status: Literal["timely", "expired_backlog"]
    logical_as_of: datetime
    predicted_at: datetime
    compute_duration_ms: float
    outcome_status: Literal["pending", "realized", "unavailable", "expired"]
    outcome_reason: str | None = None
    actual_direction: DirectionLabel | None = None
    return_5m: float | None = None
    was_correct: bool | None = None


class WorkerPredictionsResponse(BaseModel):
    """Paginated list of worker predictions and their realized outcomes."""

    total: int
    limit: int
    offset: int
    predictions: list[WorkerPredictionItem]


class WorkerPredictionDetailResponse(BaseModel):
    """Full immutable prediction detail with frozen snapshot features and lineage."""

    prediction: WorkerPredictionItem
    snapshot_features: dict[str, Any]
    contract_version: str
    fit_information_end: datetime | None = None


class OutcomesSummaryResponse(BaseModel):
    """Aggregated non-mutating outcome counts and timely evaluation metrics."""

    total_predictions: int
    realized_count: int
    pending_count: int
    unavailable_count: int
    expired_count: int
    no_signal_count: int
    timely_signaled_realized_count: int
    timely_correct_count: int
    timely_accuracy: float | None = None
    outcomes: list[WorkerPredictionItem]


class EvidenceResponse(BaseModel):
    """Read-only evaluation evidence and provenance summary (never mixes fixture vs real market)."""

    real_market_validated: bool
    real_market_status: Literal["validated", "unavailable"]
    real_market_report: dict[str, Any] | None = None
    synthetic_smoke_reports: list[dict[str, Any]] = Field(default_factory=list)
    active_bundle_manifest: dict[str, Any] | None = None
    notes: str


class TickersResponse(BaseModel):
    """Read-only distinct tickers available in finalized canonical bars."""

    dataset_id: str | None = None
    tickers: list[str] = Field(default_factory=list)


class ErrorDetail(BaseModel):
    """Standardized error envelope for API failures."""

    error: str
    message: str
    details: dict[str, str] | None = None
