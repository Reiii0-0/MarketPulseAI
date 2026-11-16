from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_predictor
from api.schemas.response import ModelInfoResponse
from model.predictor import Predictor

router = APIRouter(tags=["model"])

_ALGORITHM_NAMES = {
    "prior_baseline": "EmpiricalPriorBaseline",
    "logistic_regression": "LogisticRegression",
    "xgboost": "XGBoostClassifier",
}


@router.get("/model/info", response_model=ModelInfoResponse)
def get_model_info(predictor: Predictor = Depends(get_predictor)) -> ModelInfoResponse:
    """Retrieve technical metadata and contract provenance about the current ML bundle."""
    if not predictor:
        raise HTTPException(
            status_code=503,
            detail={"error": "MODEL_NOT_LOADED", "message": "Model is not currently loaded."},
        )

    m: dict[str, Any] = predictor.metadata
    champion = str(m.get("champion_model", getattr(predictor, "champion_model", "xgboost")))
    algorithm = _ALGORITHM_NAMES.get(champion, "XGBoostClassifier")

    training_date = m.get("training_date") or m.get("created_at") or datetime.now(UTC)

    # Support both legacy metadata ('accuracy') and Stage 4 BundleManifest metrics
    training_accuracy = m.get("accuracy")
    if training_accuracy is None:
        holdout_metrics = m.get("metrics", {}).get("holdout", {}).get(champion, {})
        training_accuracy = holdout_metrics.get("balanced_accuracy", 0.0)

    training_samples = m.get("training_samples")
    if training_samples is None:
        training_samples = m.get("cutoffs", {}).get("pre_holdout_samples", 0)

    hyperparameters = m.get(
        "hyperparameters",
        {
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.03,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
        },
    )

    return ModelInfoResponse(
        model_version=str(m["model_version"]),
        algorithm=algorithm,
        champion_model=champion,
        training_date=training_date,
        feature_count=len(m["feature_names"]),
        feature_names=list(m["feature_names"]),
        training_accuracy=float(training_accuracy),
        training_samples=int(training_samples),
        hyperparameters=dict(hyperparameters),
        contract_id=m.get("contract_id"),
        contract_version=m.get("contract_version"),
        fit_information_end=m.get("fit_information_end"),
        is_real_market_data=m.get("is_real_market_data"),
    )
