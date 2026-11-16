from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from api.dependencies import get_db, get_predictor
from api.schemas.request import BatchPredictionRequest, PredictionRequest
from api.schemas.response import (
    BatchPredictionResponse,
    DirectionLabel,
    PredictionResponse,
    RecentPredictionItem,
    RecentPredictionsResponse,
)
from consumer.db import DatabaseManager
from model.predictor import Predictor

logger = logging.getLogger(__name__)
router = APIRouter(tags=["predict"])
_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


class PredictRouterConfig(BaseSettings):
    """Configuration for the Predict Router."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    db_schema_predictions: str = "raw"
    prediction_write_to_db: bool = True

    @field_validator("db_schema_predictions")
    @classmethod
    def validate_schema_identifier(cls, v: str) -> str:
        if not _SCHEMA_NAME_RE.fullmatch(v):
            raise ValueError(f"Invalid SQL schema identifier: {v!r}")
        return v


config = PredictRouterConfig()


def _direction_label(val: int) -> DirectionLabel:
    return "UP" if val == 1 else "NOT_UP"


@router.post("/predict", response_model=PredictionResponse)
def predict(
    request: PredictionRequest,
    predictor: Predictor = Depends(get_predictor),
    db: DatabaseManager = Depends(get_db),
) -> PredictionResponse:
    """Predict price movement for a single bar horizon using verified model bundle."""
    if not predictor:
        raise HTTPException(
            status_code=503,
            detail={"error": "MODEL_NOT_LOADED", "message": "Model is not currently loaded."},
        )

    try:
        features = request.model_dump()
        result = predictor.predict(features)

        request_id = uuid4()
        if config.prediction_write_to_db:
            db.execute_upsert(
                f"INSERT INTO {config.db_schema_predictions}.predictions "
                "(request_id, ticker, window_start, predicted_direction, probability_up, "
                "probability_down, confidence_tier, shap_factors, model_version) "
                "VALUES (%(req_id)s, %(ticker)s, %(window)s, %(dir)s, %(p_up)s, "
                "%(p_down)s, %(conf)s, %(shap)s, %(ver)s)",
                {
                    "req_id": str(request_id),
                    "ticker": result["ticker"],
                    "window": result["window_start"],
                    "dir": result["predicted_direction"],
                    "p_up": result["probability_up"],
                    "p_down": result["probability_down"],
                    "conf": result["confidence_tier"],
                    "shap": json.dumps(result["factors"]),
                    "ver": result["model_version"],
                },
            )

        dir_label = _direction_label(int(result["predicted_direction"]))
        sig_label = result.get("signal_label", dir_label)
        prob_not_up = float(result["probability_down"])
        return PredictionResponse(
            request_id=request_id,
            ticker=result["ticker"],
            window_start=result["window_start"],
            predicted_direction=dir_label,
            signal_label=sig_label,
            probability_up=result["probability_up"],
            probability_down=prob_not_up,
            probability_not_up=prob_not_up,
            confidence_tier=result["confidence_tier"],
            factors=result["factors"],
            attribution_space=str(result.get("attribution_space", "base_model_log_odds")),
            model_version=result["model_version"],
            predicted_at=datetime.now(UTC),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Prediction inference/persistence error")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "INTERNAL_SERVER_ERROR",
                "message": "Prediction request could not be processed.",
            },
        ) from exc


@router.post("/predict/batch", response_model=BatchPredictionResponse)
def predict_batch(
    batch_request: BatchPredictionRequest,
    predictor: Predictor = Depends(get_predictor),
    db: DatabaseManager = Depends(get_db),
) -> BatchPredictionResponse:
    """Predict price movements for a batch of windows (up to 100)."""
    if not predictor:
        raise HTTPException(
            status_code=503,
            detail={"error": "MODEL_NOT_LOADED", "message": "Model is not currently loaded."},
        )

    predictions = []
    try:
        for req in batch_request.requests:
            res = predict(req, predictor, db)
            predictions.append(res)

        return BatchPredictionResponse(
            batch_id=uuid4(),
            count=len(predictions),
            processed_at=datetime.now(UTC),
            predictions=predictions,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Batch prediction error")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "INTERNAL_SERVER_ERROR",
                "message": "Batch prediction request could not be processed.",
            },
        ) from exc


@router.get("/predictions/recent", response_model=RecentPredictionsResponse)
def get_recent_predictions(
    ticker: str | None = Query(None, min_length=1, max_length=10, pattern=r"^[A-Za-z0-9._-]+$"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: DatabaseManager = Depends(get_db),
) -> RecentPredictionsResponse:
    """Retrieve recent predictions history from the database."""
    query = f"""
        SELECT request_id, ticker, window_start, predicted_direction,
               probability_up, confidence_tier, actual_direction, was_correct, predicted_at
        FROM {config.db_schema_predictions}.predictions
    """
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if ticker:
        query += " WHERE ticker = %(ticker)s"
        params["ticker"] = ticker.upper()

    query += " ORDER BY predicted_at DESC LIMIT %(limit)s OFFSET %(offset)s"

    try:
        predictions = []
        total_count = 0

        with db.get_connection() as conn:
            with conn.cursor() as cur:
                count_query = f"SELECT COUNT(*) FROM {config.db_schema_predictions}.predictions"
                if ticker:
                    count_query += " WHERE ticker = %(ticker)s"
                cur.execute(count_query, params)
                row = cur.fetchone()
                if row:
                    total_count = int(row[0])

                cur.execute(query, params)
                for row in cur.fetchall():
                    actual_val: DirectionLabel | None = None
                    if row[6] == 1:
                        actual_val = "UP"
                    elif row[6] == 0:
                        actual_val = "NOT_UP"

                    predictions.append(
                        RecentPredictionItem(
                            request_id=row[0],
                            ticker=row[1],
                            window_start=row[2],
                            predicted_direction=_direction_label(int(row[3])),
                            probability_up=row[4],
                            confidence_tier=row[5],
                            actual_direction=actual_val,
                            was_correct=row[7],
                            predicted_at=row[8],
                        )
                    )

        return RecentPredictionsResponse(
            total=total_count,
            limit=limit,
            offset=offset,
            predictions=predictions,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Recent predictions query failed")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "INTERNAL_SERVER_ERROR",
                "message": "Could not retrieve prediction history.",
            },
        ) from exc
