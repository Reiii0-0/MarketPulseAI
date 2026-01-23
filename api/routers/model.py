from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_predictor
from api.schemas.response import ModelInfoResponse
from model.predictor import Predictor

router = APIRouter(tags=["model"])


@router.get("/model/info", response_model=ModelInfoResponse)
def get_model_info(predictor: Predictor = Depends(get_predictor)) -> ModelInfoResponse:
    """Retrieve technical metadata about the current ML model."""
    if not predictor:
        raise HTTPException(
            status_code=503,
            detail={"error": "MODEL_NOT_LOADED", "message": "Model is not currently loaded."}
        )

    m = predictor.metadata
    return ModelInfoResponse(
        model_version=m["model_version"],
        algorithm="XGBoostClassifier",
        training_date=m["training_date"],
        feature_count=len(m["feature_names"]),
        feature_names=m["feature_names"],
        training_accuracy=m["accuracy"],
        training_samples=m.get("training_samples", 0),
        hyperparameters={
            "n_estimators": 1000,
            "max_depth": 6,
            "learning_rate": 0.01,
            "subsample": 0.8,
            "colsample_bytree": 0.8
        }
    )
