"""Read-only product API endpoints for runs, final bars, immutable predictions, outcomes, and evidence.

Implements Stage 5 requirements:
- Pure read-only endpoints parameterized against marts/raw tables and verified artifacts
- Never accepts free-form dashboard features for ad-hoc prediction
- Never leaks internal exceptions, SQL queries, or server filesystem paths
- Strictly distinguishes synthetic smoke fixture evidence from real-market validation
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic_settings import BaseSettings, SettingsConfigDict

from api.dependencies import get_db
from api.schemas.response import (
    BarsListResponse,
    DirectionLabel,
    EvidenceResponse,
    FinalBarItem,
    OutcomesSummaryResponse,
    RunDetailResponse,
    RunListResponse,
    RunSummaryItem,
    TickersResponse,
    WorkerPredictionDetailResponse,
    WorkerPredictionItem,
    WorkerPredictionsResponse,
)
from consumer.db import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter(tags=["pipeline-read"])


class ReadApiConfig(BaseSettings):
    """Paths for read-only evaluation reports and bundle manifests."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    evaluation_reports_dir: str = "reports/evaluation"
    bundle_manifest_path: str = "model/artefacts/manifest.json"


read_config = ReadApiConfig()


def _parse_json_field(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _ensure_utc_opt(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.replace(tzinfo=UTC) if val.tzinfo is None else val.astimezone(UTC)
    parsed = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _row_to_worker_prediction(row: tuple[Any, ...]) -> WorkerPredictionItem:
    pred_dir: DirectionLabel = "UP" if int(row[12]) == 1 else "NOT_UP"
    actual_dir: DirectionLabel | None = None
    if row[23] in ("UP", "NOT_UP"):
        actual_dir = row[23]
    elif row[23] in (0, 1):
        actual_dir = "UP" if int(row[23]) == 1 else "NOT_UP"

    timeliness: Literal["timely", "expired_backlog"] = (
        "expired_backlog" if str(row[18]) == "expired_backlog" else "timely"
    )
    out_status: Literal["pending", "realized", "unavailable", "expired"]
    raw_status = str(row[21])
    if raw_status in ("pending", "realized", "unavailable", "expired"):
        out_status = raw_status  # type: ignore[assignment]
    else:
        out_status = "unavailable"

    return WorkerPredictionItem(
        prediction_id=row[0],
        contract_id=str(row[1]),
        dataset_id=str(row[2]),
        run_id=row[3],
        ticker=str(row[4]),
        bar_end=row[5],
        decision_at=row[6],
        target_at=row[7],
        feature_hash=str(row[8]),
        bundle_id=row[9],
        model_version=str(row[10]),
        champion_model=str(row[11]),
        predicted_direction=pred_dir,
        signal_label=row[13],
        probability_up=float(row[14]),
        probability_not_up=float(row[15]),
        confidence_tier=row[16],
        attribution_space=str(row[17]),
        timeliness_status=timeliness,
        run_mode=str(row[19]),
        logical_as_of=row[20],
        outcome_status=out_status,
        outcome_reason=row[22],
        actual_direction=actual_dir,
        return_5m=float(row[24]) if row[24] is not None else None,
        was_correct=row[25],
        factors=_parse_json_field(row[26], []),
        predicted_at=row[27],
        compute_duration_ms=float(row[28] or 0.0),
    )


_REALIZED_OUTCOMES_SELECT = """
    SELECT
        prediction_id, contract_id, dataset_id, run_id, ticker,
        bar_end, decision_at, target_at, feature_hash, bundle_id,
        model_version, champion_model, predicted_direction, signal_label,
        probability_up, probability_not_up, confidence_tier, attribution_space,
        timeliness_status, run_mode, logical_as_of,
        outcome_status, outcome_reason, actual_direction, return_5m, was_correct,
        shap_factors, predicted_at, compute_duration_ms
    FROM marts.realized_outcomes_v
"""


@router.get("/runs", response_model=RunListResponse)
def list_runs(
    limit: int = Query(20, ge=1, le=100),
    db: DatabaseManager = Depends(get_db),
) -> RunListResponse:
    """List recorded ingestion/replay runs alongside worker checkpoint progress."""
    try:
        runs: list[RunSummaryItem] = []
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        ir.run_id, ir.dataset_id, ir.mode, ir.provenance, ir.status,
                        ir.logical_as_of, ir.dataset_end_at,
                        ws.status AS worker_status, ws.last_prepared_bar_end,
                        ws.last_predicted_bar_end,
                        COALESCE(ws.predictions_written, 0),
                        COALESCE(ws.expired_backlog_count, 0),
                        ws.last_compute_duration_ms
                    FROM raw.ingestion_runs ir
                    LEFT JOIN marts.prediction_worker_state ws
                        ON ws.run_id = ir.run_id
                    ORDER BY ir.started_at DESC
                    LIMIT %(limit)s
                    """,
                    {"limit": limit},
                )
                for row in cur.fetchall():
                    runs.append(
                        RunSummaryItem(
                            run_id=str(row[0]),
                            dataset_id=str(row[1]),
                            mode=str(row[2]),
                            provenance=str(row[3]),
                            status=str(row[4]),
                            logical_as_of=row[5],
                            dataset_end_at=row[6],
                            worker_status=row[7],
                            last_prepared_bar_end=row[8],
                            last_predicted_bar_end=row[9],
                            predictions_written=int(row[10] or 0),
                            expired_backlog_count=int(row[11] or 0),
                            last_compute_duration_ms=row[12],
                        )
                    )
        return RunListResponse(total=len(runs), runs=runs)
    except Exception as exc:
        logger.exception("Failed to list runs")
        raise HTTPException(
            status_code=500,
            detail={"error": "INTERNAL_SERVER_ERROR", "message": "Could not load runs."},
        ) from exc


@router.get("/runs/{run_id}", response_model=RunDetailResponse)
def get_run_detail(
    run_id: str,
    db: DatabaseManager = Depends(get_db),
) -> RunDetailResponse:
    """Retrieve run detail, partition delivery barriers, and post-fit cutoff check."""
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        ir.run_id, ir.dataset_id, ir.mode, ir.provenance, ir.status,
                        ir.logical_as_of, ir.dataset_end_at,
                        ws.status AS worker_status, ws.last_prepared_bar_end,
                        ws.last_predicted_bar_end,
                        COALESCE(ws.predictions_written, 0),
                        COALESCE(ws.expired_backlog_count, 0),
                        ws.last_compute_duration_ms
                    FROM raw.ingestion_runs ir
                    LEFT JOIN marts.prediction_worker_state ws
                        ON ws.run_id = ir.run_id
                    WHERE ir.run_id = %(run_id)s
                    """,
                    {"run_id": run_id},
                )
                row = cur.fetchone()
                if row is None:
                    raise HTTPException(
                        status_code=404,
                        detail={"error": "RUN_NOT_FOUND", "message": "Requested run not found."},
                    )

                run_item = RunSummaryItem(
                    run_id=str(row[0]),
                    dataset_id=str(row[1]),
                    mode=str(row[2]),
                    provenance=str(row[3]),
                    status=str(row[4]),
                    logical_as_of=row[5],
                    dataset_end_at=row[6],
                    worker_status=row[7],
                    last_prepared_bar_end=row[8],
                    last_predicted_bar_end=row[9],
                    predictions_written=int(row[10] or 0),
                    expired_backlog_count=int(row[11] or 0),
                    last_compute_duration_ms=row[12],
                )

                cur.execute(
                    """
                    SELECT topic, kafka_partition, last_kafka_offset, last_available_at,
                           processed_count, quarantined_count, status
                    FROM raw.run_delivery_progress
                    WHERE run_id = %(run_id)s
                    ORDER BY topic ASC, kafka_partition ASC
                    """,
                    {"run_id": run_id},
                )
                delivery = [
                    {
                        "topic": r[0],
                        "kafka_partition": int(r[1]),
                        "last_kafka_offset": int(r[2]),
                        "last_available_at": r[3],
                        "processed_count": int(r[4]),
                        "quarantined_count": int(r[5]),
                        "status": r[6],
                    }
                    for r in cur.fetchall()
                ]

        fit_end: datetime | None = None
        from model.evaluate import resolve_bundle_dir

        configured_manifest = Path(read_config.bundle_manifest_path)
        manifest_file = resolve_bundle_dir(configured_manifest.parent) / configured_manifest.name
        if manifest_file.exists():
            try:
                manifest_data = json.loads(manifest_file.read_text(encoding="utf-8"))
                fit_end = _ensure_utc_opt(manifest_data.get("fit_information_end"))
            except Exception:
                fit_end = None

        serves_post_fit: bool | None = None
        if fit_end is not None and run_item.logical_as_of is not None:
            serves_post_fit = _ensure_utc_opt(run_item.logical_as_of) >= fit_end  # type: ignore[operator]

        return RunDetailResponse(
            run=run_item,
            delivery_progress=delivery,
            fit_information_end=fit_end,
            serves_post_fit_window=serves_post_fit,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to inspect run detail")
        raise HTTPException(
            status_code=500,
            detail={"error": "INTERNAL_SERVER_ERROR", "message": "Could not load run detail."},
        ) from exc


@router.get("/bars", response_model=BarsListResponse)
def list_final_bars(
    run_id: str | None = Query(None, min_length=1, max_length=120),
    dataset_id: str | None = Query(None, min_length=1, max_length=120),
    ticker: str | None = Query(None, min_length=1, max_length=10, pattern=r"^[A-Za-z0-9._-]+$"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: DatabaseManager = Depends(get_db),
) -> BarsListResponse:
    """Read immutable finalized 1-minute UTC OHLCV bars."""
    clauses: list[str] = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if run_id:
        clauses.append("run_id = %(run_id)s")
        params["run_id"] = run_id
    if dataset_id:
        clauses.append("dataset_id = %(dataset_id)s")
        params["dataset_id"] = dataset_id
    if ticker:
        clauses.append("ticker = %(ticker)s")
        params["ticker"] = ticker.upper()
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    try:
        bars: list[FinalBarItem] = []
        total = 0
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM marts.final_bars {where_sql}", params)
                cnt = cur.fetchone()
                total = int(cnt[0]) if cnt else 0

                cur.execute(
                    f"""
                    SELECT contract_id, dataset_id, ticker, bar_start, bar_end,
                           available_at, finalized_at, open, high, low, close, volume, provenance
                    FROM marts.final_bars
                    {where_sql}
                    ORDER BY bar_end DESC, ticker ASC
                    LIMIT %(limit)s OFFSET %(offset)s
                    """,
                    params,
                )
                for row in cur.fetchall():
                    bars.append(
                        FinalBarItem(
                            contract_id=str(row[0]),
                            dataset_id=str(row[1]),
                            ticker=str(row[2]),
                            bar_start=row[3],
                            bar_end=row[4],
                            available_at=row[5],
                            finalized_at=row[6],
                            open=float(row[7]),
                            high=float(row[8]),
                            low=float(row[9]),
                            close=float(row[10]),
                            volume=int(row[11]),
                            provenance=str(row[12]),
                        )
                    )
        return BarsListResponse(total=total, limit=limit, offset=offset, bars=bars)
    except Exception as exc:
        logger.exception("Failed to list finalized bars")
        raise HTTPException(
            status_code=500,
            detail={"error": "INTERNAL_SERVER_ERROR", "message": "Could not load final bars."},
        ) from exc


@router.get("/predictions", response_model=WorkerPredictionsResponse)
def list_worker_predictions(
    run_id: str | None = Query(None, min_length=1, max_length=120),
    dataset_id: str | None = Query(None, min_length=1, max_length=120),
    ticker: str | None = Query(None, min_length=1, max_length=10, pattern=r"^[A-Za-z0-9._-]+$"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: DatabaseManager = Depends(get_db),
) -> WorkerPredictionsResponse:
    """Read immutable worker predictions joined with their non-mutating realized outcomes."""
    clauses: list[str] = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if run_id:
        clauses.append("run_id = %(run_id)s")
        params["run_id"] = run_id
    if dataset_id:
        clauses.append("dataset_id = %(dataset_id)s")
        params["dataset_id"] = dataset_id
    if ticker:
        clauses.append("ticker = %(ticker)s")
        params["ticker"] = ticker.upper()
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    try:
        total = 0
        items: list[WorkerPredictionItem] = []
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM marts.realized_outcomes_v {where_sql}", params)
                cnt = cur.fetchone()
                total = int(cnt[0]) if cnt else 0

                cur.execute(
                    f"""
                    {_REALIZED_OUTCOMES_SELECT}
                    {where_sql}
                    ORDER BY bar_end DESC, ticker ASC
                    LIMIT %(limit)s OFFSET %(offset)s
                    """,
                    params,
                )
                items = [_row_to_worker_prediction(r) for r in cur.fetchall()]
        return WorkerPredictionsResponse(
            total=total,
            limit=limit,
            offset=offset,
            predictions=items,
        )
    except Exception as exc:
        logger.exception("Failed to list worker predictions")
        raise HTTPException(
            status_code=500,
            detail={"error": "INTERNAL_SERVER_ERROR", "message": "Could not load predictions."},
        ) from exc


@router.get("/predictions/{prediction_id}", response_model=WorkerPredictionDetailResponse)
def get_worker_prediction_detail(
    prediction_id: UUID,
    db: DatabaseManager = Depends(get_db),
) -> WorkerPredictionDetailResponse:
    """Inspect full immutable lineage for one stored prediction and its frozen feature snapshot."""
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.prediction_id, r.contract_id, r.dataset_id, r.run_id, r.ticker,
                        r.bar_end, r.decision_at, r.target_at, r.feature_hash, r.bundle_id,
                        r.model_version, r.champion_model, r.predicted_direction, r.signal_label,
                        r.probability_up, r.probability_not_up, r.confidence_tier,
                        r.attribution_space, r.timeliness_status, r.run_mode, r.logical_as_of,
                        r.outcome_status, r.outcome_reason, r.actual_direction,
                        r.return_5m, r.was_correct, r.shap_factors, r.predicted_at,
                        r.compute_duration_ms,
                        r.contract_version, r.fit_information_end, fs.features
                    FROM marts.realized_outcomes_v r
                    LEFT JOIN marts.feature_snapshots fs
                        ON fs.contract_id = r.contract_id
                       AND fs.dataset_id = r.dataset_id
                       AND fs.run_id IS NOT DISTINCT FROM r.run_id
                       AND fs.ticker = r.ticker
                       AND fs.bar_end = r.bar_end
                    WHERE r.prediction_id = %(pid)s
                    """,
                    {"pid": str(prediction_id)},
                )
                row = cur.fetchone()
                if row is None:
                    raise HTTPException(
                        status_code=404,
                        detail={
                            "error": "PREDICTION_NOT_FOUND",
                            "message": "Requested prediction not found.",
                        },
                    )
                item = _row_to_worker_prediction(row[:29])
                feats = _parse_json_field(row[31], {})
                return WorkerPredictionDetailResponse(
                    prediction=item,
                    snapshot_features=feats,
                    contract_version=str(row[29]),
                    fit_information_end=row[30],
                )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to retrieve prediction detail")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "INTERNAL_SERVER_ERROR",
                "message": "Could not retrieve prediction detail.",
            },
        ) from exc


@router.get("/outcomes", response_model=OutcomesSummaryResponse)
def get_outcomes_summary(
    run_id: str | None = Query(None, min_length=1, max_length=120),
    dataset_id: str | None = Query(None, min_length=1, max_length=120),
    ticker: str | None = Query(None, min_length=1, max_length=10, pattern=r"^[A-Za-z0-9._-]+$"),
    limit: int = Query(100, ge=1, le=500),
    db: DatabaseManager = Depends(get_db),
) -> OutcomesSummaryResponse:
    """Summarize non-mutating realized/pending/unavailable/expired outcomes."""
    clauses: list[str] = []
    params: dict[str, Any] = {"limit": limit}
    if run_id:
        clauses.append("run_id = %(run_id)s")
        params["run_id"] = run_id
    if dataset_id:
        clauses.append("dataset_id = %(dataset_id)s")
        params["dataset_id"] = dataset_id
    if ticker:
        clauses.append("ticker = %(ticker)s")
        params["ticker"] = ticker.upper()
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    try:
        items: list[WorkerPredictionItem] = []
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    {_REALIZED_OUTCOMES_SELECT}
                    {where_sql}
                    ORDER BY bar_end DESC, ticker ASC
                    LIMIT %(limit)s
                    """,
                    params,
                )
                items = [_row_to_worker_prediction(r) for r in cur.fetchall()]

        realized_cnt = sum(1 for i in items if i.outcome_status == "realized")
        pending_cnt = sum(1 for i in items if i.outcome_status == "pending")
        unavail_cnt = sum(1 for i in items if i.outcome_status == "unavailable")
        expired_cnt = sum(1 for i in items if i.outcome_status == "expired")
        no_sig_cnt = sum(1 for i in items if i.signal_label == "NO_SIGNAL")

        timely_signaled = [
            i
            for i in items
            if i.outcome_status == "realized"
            and i.timeliness_status == "timely"
            and i.signal_label in ("UP", "NOT_UP")
        ]
        correct_cnt = sum(1 for i in timely_signaled if i.was_correct is True)
        acc = (correct_cnt / len(timely_signaled)) if timely_signaled else None

        return OutcomesSummaryResponse(
            total_predictions=len(items),
            realized_count=realized_cnt,
            pending_count=pending_cnt,
            unavailable_count=unavail_cnt,
            expired_count=expired_cnt,
            no_signal_count=no_sig_cnt,
            timely_signaled_realized_count=len(timely_signaled),
            timely_correct_count=correct_cnt,
            timely_accuracy=round(acc, 6) if acc is not None else None,
            outcomes=items,
        )
    except Exception as exc:
        logger.exception("Failed to summarize outcomes")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "INTERNAL_SERVER_ERROR",
                "message": "Could not load prediction outcomes.",
            },
        ) from exc


@router.get("/evidence", response_model=EvidenceResponse)
def get_evaluation_evidence() -> EvidenceResponse:
    """Return read-only offline evaluation reports and active bundle manifest provenance."""
    reports_dir = Path(read_config.evaluation_reports_dir)
    real_report: dict[str, Any] | None = None
    smoke_reports: list[dict[str, Any]] = []

    if reports_dir.exists():
        for path in sorted(reports_dir.glob("*_evaluation.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if payload.get("is_real_market_data") and payload.get("real_market_validated"):
                real_report = payload
            else:
                smoke_reports.append(payload)

    bundle_manifest: dict[str, Any] | None = None
    from model.evaluate import resolve_bundle_dir

    configured_manifest = Path(read_config.bundle_manifest_path)
    manifest_file = resolve_bundle_dir(configured_manifest.parent) / configured_manifest.name
    if manifest_file.exists():
        try:
            bundle_manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        except Exception:
            bundle_manifest = None

    real_validated = real_report is not None
    notes = (
        "Real-market walk-forward evaluation verified."
        if real_validated
        else "Real-market evaluation unavailable: only synthetic/fixture smoke reports are present and are never reported as real market performance."
    )
    return EvidenceResponse(
        real_market_validated=real_validated,
        real_market_status="validated" if real_validated else "unavailable",
        real_market_report=real_report,
        synthetic_smoke_reports=smoke_reports,
        active_bundle_manifest=bundle_manifest,
        notes=notes,
    )


@router.get("/tickers", response_model=TickersResponse)
def list_available_tickers(
    run_id: str | None = Query(None, min_length=1, max_length=120),
    dataset_id: str | None = Query(None, min_length=1, max_length=120),
    db: DatabaseManager = Depends(get_db),
) -> TickersResponse:
    """List distinct tickers available in canonical finalized bars."""
    params: dict[str, Any] = {}
    clauses = []
    if dataset_id:
        clauses.append("dataset_id = %(dataset_id)s")
        params["dataset_id"] = dataset_id
    if run_id:
        clauses.append("run_id = %(run_id)s")
        params["run_id"] = run_id
    where_sql = "WHERE " + " AND ".join(clauses) if clauses else ""

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT ticker
                    FROM marts.final_bars
                    {where_sql}
                    ORDER BY ticker ASC
                    LIMIT 200
                    """,
                    params,
                )
                tickers = [str(row[0]) for row in cur.fetchall() if row and row[0]]
        return TickersResponse(dataset_id=dataset_id, tickers=tickers)
    except Exception as exc:
        logger.exception("Failed to list available tickers")
        raise HTTPException(
            status_code=500,
            detail={"error": "INTERNAL_SERVER_ERROR", "message": "Could not load tickers."},
        ) from exc
