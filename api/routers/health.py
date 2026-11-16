from __future__ import annotations

import time
from typing import Any, Literal

from fastapi import APIRouter, Request, Response

from api.schemas.response import HealthResponse, ReadinessResponse
from pipeline.features import validate_contract_compatibility

router = APIRouter(tags=["health"])


def _evaluate_dependency_state(request: Request) -> tuple[bool, bool, bool, list[str]]:
    app = request.app
    reasons: list[str] = []

    predictor = getattr(app.state, "predictor", None)
    model_loaded = bool(getattr(app.state, "model_loaded", False) and predictor is not None)
    if not model_loaded:
        reasons.append("model_bundle_unavailable")

    contract_loaded = False
    active_contract = getattr(app.state, "contract", None)
    if active_contract is not None:
        if predictor is not None:
            try:
                validate_contract_compatibility(
                    active_contract,
                    contract_id=predictor.metadata.get("contract_id"),
                    contract_version=predictor.metadata.get("contract_version"),
                    feature_mode=predictor.metadata.get("feature_mode"),
                    feature_names=predictor.metadata.get("feature_names"),
                )
                contract_loaded = True
            except Exception:
                reasons.append("contract_mismatch")
        else:
            contract_loaded = True
    elif predictor is not None:
        contract_loaded = True
    else:
        reasons.append("contract_unloaded")

    db_connected = False
    db_mgr: Any = getattr(app.state, "db_manager", None)
    if db_mgr is not None:
        try:
            with db_mgr.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            db_connected = True
        except Exception:
            reasons.append("database_unreachable")
    else:
        reasons.append("database_unconfigured")

    return db_connected, model_loaded, contract_loaded, reasons


@router.get("/health", response_model=HealthResponse)
def get_health(request: Request, response: Response) -> HealthResponse:
    """Process liveness endpoint: always returns 200 OK while reporting dependency state."""
    uptime = int(time.time() - getattr(request.app.state, "start_time", time.time()))
    db_connected, model_loaded, contract_loaded, reasons = _evaluate_dependency_state(request)

    status_val: Literal["healthy", "degraded", "unhealthy"] = (
        "healthy" if (db_connected and model_loaded and contract_loaded) else "degraded"
    )
    response.status_code = 200

    return HealthResponse(
        status=status_val,
        model_loaded=model_loaded,
        db_connected=db_connected,
        contract_loaded=contract_loaded,
        readiness_reasons=reasons,
        uptime_seconds=uptime,
        version="1.0.0",
    )


@router.get("/health/ready", response_model=ReadinessResponse)
def get_readiness(request: Request, response: Response) -> ReadinessResponse:
    """Strict readiness gate: returns 503 if DB, verified model bundle, or contract is not ready."""
    db_connected, model_loaded, contract_loaded, reasons = _evaluate_dependency_state(request)
    is_ready = db_connected and model_loaded and contract_loaded and not reasons
    if not is_ready:
        response.status_code = 503
    return ReadinessResponse(
        ready=is_ready,
        status="ready" if is_ready else "not_ready",
        db_connected=db_connected,
        model_loaded=model_loaded,
        contract_loaded=contract_loaded,
        reasons=reasons,
    )
