from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic_settings import BaseSettings, SettingsConfigDict

from api.dependencies import get_db, get_predictor
from api.schemas.request import BatchPredictionRequest, PredictionRequest
from api.schemas.response import (
    BatchPredictionResponse,
    PredictionResponse,
    RecentPredictionItem,
    RecentPredictionsResponse,
)
from consumer.db import DatabaseManager
from model.predictor import Predictor

router = APIRouter(tags=["predict"])


class PredictRouterConfig(BaseSettings):
    """Configuration for the Predict Router."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    db_schema_marts: str = "marts"


config = PredictRouterConfig()


@router.post("/predict", response_model=PredictionResponse)
def predict(
    request: PredictionRequest,
    predictor: Predictor = Depends(get_predictor),
    db: DatabaseManager = Depends(get_db),
) -> PredictionResponse:
    """Predict price movement for a single 5-minute window."""
    if not predictor:
        raise HTTPException(
            status_code=503,
            detail={"error": "MODEL_NOT_LOADED", "message": "Model is not currently loaded."},
        )

    try:
        # 1. Generate Prediction
        features = request.model_dump()
        result = predictor.predict(features)

        # 2. Persist to Database for monitoring (Fire and forget in MVP)
        request_id = uuid4()
        db.execute_upsert(
            f"INSERT INTO {config.db_schema_marts}.predictions "
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

        return PredictionResponse(
            request_id=request_id,
            ticker=result["ticker"],
            window_start=result["window_start"],
            predicted_direction="UP" if result["predicted_direction"] == 1 else "DOWN",
            probability_up=result["probability_up"],
            probability_down=result["probability_down"],
            confidence_tier=result["confidence_tier"],
            factors=result["factors"],
            model_version=result["model_version"],
            predicted_at=datetime.now(UTC),
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "INTERNAL_SERVER_ERROR", "message": str(e)},
        ) from e


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
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "INTERNAL_SERVER_ERROR", "message": str(e)},
        ) from e


@router.get("/predictions/recent", response_model=RecentPredictionsResponse)
def get_recent_predictions(
    ticker: str | None = Query(None, min_length=1, max_length=10),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: DatabaseManager = Depends(get_db),
) -> RecentPredictionsResponse:
    """Retrieve recent predictions history from the database."""
    query = f"""
        SELECT request_id, ticker, window_start, predicted_direction,
               probability_up, confidence_tier, actual_direction, was_correct, predicted_at
        FROM {config.db_schema_marts}.predictions
    """
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if ticker:
        query += " WHERE ticker = %(ticker)s"
        params["ticker"] = ticker

    query += " ORDER BY predicted_at DESC LIMIT %(limit)s OFFSET %(offset)s"

    try:
        predictions = []
        total_count = 0

        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # Get total count for metadata
                count_query = f"SELECT COUNT(*) FROM {config.db_schema_marts}.predictions"
                if ticker:
                    count_query += " WHERE ticker = %(ticker)s"
                cur.execute(count_query, params)
                row = cur.fetchone()
                if row:
                    total_count = row[0]

                # Get rows
                cur.execute(query, params)
                for row in cur.fetchall():
                    predictions.append(
                        RecentPredictionItem(
                            request_id=row[0],
                            ticker=row[1],
                            window_start=row[2],
                            predicted_direction="UP" if row[3] == 1 else "DOWN",
                            probability_up=row[4],
                            confidence_tier=row[5],
                            actual_direction="UP"
                            if row[6] == 1
                            else ("DOWN" if row[6] == 0 else None),
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
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "INTERNAL_SERVER_ERROR", "message": str(e)},
        ) from e
