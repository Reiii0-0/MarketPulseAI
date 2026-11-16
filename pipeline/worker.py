"""Single-worker prediction lifecycle: snapshot preparation, idempotent serving, and outcomes.

Implements Stage 5 requirements:
- ``prepare_snapshots`` finalizes bars & freezes canonical snapshots WITHOUT requiring an ML model
- ``advance_run`` serves verified model bundles idempotently (<= 1 prediction per snapshot/model),
  enforces ``fit_information_end`` cutoff and per-run single-worker lease, distinguishes replay
  logical cutoffs vs wall-clock compute duration, and marks live backlog past target as ``expired``
- ``resolve_outcomes`` derives ``pending`` / ``realized`` / ``unavailable`` / ``expired`` outcomes
  without mutating immutable stored predictions, snapshot inputs, or attributions
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from consumer.db import DatabaseConfig, DatabaseManager
from model.predictor import Predictor
from pipeline.contract import (
    ContractBar,
    ForecastContract,
    evaluate_target_outcome,
    load_forecast_contract,
)
from pipeline.features import (
    BarFinalizationService,
    FeatureSnapshotRecord,
    FinalBarRecord,
)
from pipeline.replay import PartitionDeliveryProgress

logger = logging.getLogger(__name__)

MODEL_PREDICTION_INSERT_SQL = """
INSERT INTO marts.model_predictions (
    prediction_id, contract_id, contract_version, dataset_id, run_id,
    ticker, bar_start, bar_end, decision_at, target_at,
    feature_hash, bundle_id, model_version, champion_model, fit_information_end,
    predicted_direction, signal_label, probability_up, probability_not_up, raw_probability_up,
    confidence_tier, attribution_space, shap_factors,
    run_mode, timeliness_status, logical_as_of, predicted_at, compute_duration_ms
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s::jsonb,
    %s, %s, %s, %s, %s
)
ON CONFLICT (run_id, contract_id, dataset_id, ticker, bar_end, bundle_id) DO NOTHING
"""


class WorkerLeaseError(RuntimeError):
    """Raised when another active worker already holds the exclusive lease for a run."""


def _ensure_utc(dt: datetime | str) -> datetime:
    if isinstance(dt, str):
        parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _optional_utc(dt: datetime | str | None) -> datetime | None:
    if dt is None:
        return None
    return _ensure_utc(dt)


@dataclass(frozen=True, slots=True)
class StoredPredictionRecord:
    """Immutable prediction persisted once per (contract_id, dataset_id, ticker, bar_end, model_version)."""

    prediction_id: str
    contract_id: str
    contract_version: str
    dataset_id: str
    run_id: str | None
    ticker: str
    bar_start: datetime
    bar_end: datetime
    decision_at: datetime
    target_at: datetime
    feature_hash: str
    bundle_id: str | None
    model_version: str
    champion_model: str
    fit_information_end: datetime | None
    predicted_direction: int
    signal_label: str
    probability_up: float
    probability_not_up: float
    raw_probability_up: float | None
    confidence_tier: str
    attribution_space: str
    shap_factors: list[dict[str, Any]]
    run_mode: str
    timeliness_status: str
    logical_as_of: datetime
    predicted_at: datetime
    compute_duration_ms: float


@dataclass(frozen=True, slots=True)
class WorkerPreparationResult:
    """Summary returned by ``prepare_snapshots`` (model-free preparation stage)."""

    run_id: str | None
    dataset_id: str
    logical_as_of: datetime
    finalized_bars_count: int
    late_corrections_count: int
    snapshots_count: int
    last_bar_end: datetime | None


@dataclass(frozen=True, slots=True)
class WorkerAdvanceResult:
    """Summary returned by ``advance_run`` (idempotent prediction stage)."""

    run_id: str
    dataset_id: str
    run_mode: str
    model_version: str
    logical_as_of: datetime
    wall_clock_at: datetime
    snapshots_eligible: int
    predictions_written: int
    duplicates_skipped: int
    expired_backlog_count: int
    skipped_pre_fit_end: int
    compute_duration_ms: float
    last_predicted_bar_end: datetime | None


@dataclass(frozen=True, slots=True)
class RealizedOutcomeRecord:
    """Derived non-mutating outcome view record for a single stored prediction."""

    prediction_id: str
    contract_id: str
    dataset_id: str
    run_id: str | None
    ticker: str
    bar_start: datetime
    bar_end: datetime
    decision_at: datetime
    target_at: datetime
    feature_hash: str
    bundle_id: str | None
    model_version: str
    champion_model: str
    signal_label: str
    predicted_direction: int
    probability_up: float
    probability_not_up: float
    confidence_tier: str
    attribution_space: str
    factors: list[dict[str, Any]]
    run_mode: str
    timeliness_status: str
    logical_as_of: datetime
    predicted_at: datetime
    compute_duration_ms: float
    outcome_status: str
    outcome_reason: str | None
    actual_direction: str | None
    return_5m: float | None
    was_correct: bool | None


@dataclass
class InMemoryWorkerState:
    """Stateful in-memory backend enabling deterministic tests without PostgreSQL."""

    finalizer: BarFinalizationService
    predictions: dict[tuple[str | None, str, str, str, datetime, str], StoredPredictionRecord] = (
        field(default_factory=dict)
    )
    leases: dict[str, tuple[str, datetime]] = field(default_factory=dict)
    checkpoints: dict[str, dict[str, Any]] = field(default_factory=dict)
    run_modes: dict[str, str] = field(default_factory=dict)
    logical_clocks: dict[str, datetime] = field(default_factory=dict)
    dataset_ends: dict[str, datetime | None] = field(default_factory=dict)


def acquire_run_lease(
    run_id: str,
    *,
    dataset_id: str,
    worker_owner: str,
    lease_ttl_seconds: int = 120,
    now_utc: datetime | None = None,
    db_manager: DatabaseManager | None = None,
    memory_state: InMemoryWorkerState | None = None,
) -> None:
    """Acquire an exclusive single-worker lease for ``run_id`` or raise ``WorkerLeaseError``."""
    now = _ensure_utc(now_utc or datetime.now(UTC))
    expires = now + timedelta(seconds=lease_ttl_seconds)

    if db_manager is not None:
        with db_manager.transaction() as cur:
            cur.execute(
                """
                INSERT INTO marts.prediction_worker_state (
                    run_id, dataset_id, worker_owner, lease_expires_at, status, updated_at
                ) VALUES (%s, %s, %s, %s, 'running', NOW())
                ON CONFLICT (run_id) DO UPDATE SET
                    worker_owner = EXCLUDED.worker_owner,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    status = 'running',
                    updated_at = NOW()
                WHERE marts.prediction_worker_state.lease_expires_at IS NULL
                   OR marts.prediction_worker_state.lease_expires_at < %s
                   OR marts.prediction_worker_state.worker_owner = EXCLUDED.worker_owner
                RETURNING worker_owner
                """,
                (run_id, dataset_id, worker_owner, expires, now),
            )
            row = cur.fetchone()
            if row is None or str(row[0]) != worker_owner:
                raise WorkerLeaseError(
                    f"Run {run_id!r} already has an active lease held by another worker."
                )
        return

    if memory_state is None:
        return
    existing = memory_state.leases.get(run_id)
    if existing is not None:
        holder, holder_exp = existing
        if holder != worker_owner and holder_exp >= now:
            raise WorkerLeaseError(
                f"Run {run_id!r} already has an active lease held by {holder!r}."
            )
    memory_state.leases[run_id] = (worker_owner, expires)


def release_run_lease(
    run_id: str,
    *,
    worker_owner: str,
    last_predicted_bar_end: datetime | None = None,
    predictions_added: int = 0,
    expired_added: int = 0,
    compute_duration_ms: float | None = None,
    error_message: str | None = None,
    db_manager: DatabaseManager | None = None,
    memory_state: InMemoryWorkerState | None = None,
) -> None:
    """Release exclusive run lease and checkpoint the worker's latest progress."""
    status_val = "error" if error_message else "idle"
    if db_manager is not None:
        with db_manager.transaction() as cur:
            cur.execute(
                """
                UPDATE marts.prediction_worker_state
                SET
                    worker_owner = NULL,
                    lease_expires_at = NULL,
                    last_predicted_bar_end = COALESCE(
                        GREATEST(last_predicted_bar_end, %s),
                        COALESCE(%s, last_predicted_bar_end)
                    ),
                    predictions_written = predictions_written + %s,
                    expired_backlog_count = expired_backlog_count + %s,
                    last_compute_duration_ms = COALESCE(%s, last_compute_duration_ms),
                    status = %s,
                    last_error = %s,
                    updated_at = NOW()
                WHERE run_id = %s AND (worker_owner = %s OR worker_owner IS NULL)
                """,
                (
                    last_predicted_bar_end,
                    last_predicted_bar_end,
                    predictions_added,
                    expired_added,
                    compute_duration_ms,
                    status_val,
                    error_message,
                    run_id,
                    worker_owner,
                ),
            )
        return

    if memory_state is None:
        return
    existing = memory_state.leases.get(run_id)
    if existing and existing[0] == worker_owner:
        memory_state.leases.pop(run_id, None)
    cp = memory_state.checkpoints.setdefault(
        run_id,
        {
            "last_prepared_bar_end": None,
            "last_predicted_bar_end": None,
            "predictions_written": 0,
            "expired_backlog_count": 0,
            "status": "idle",
        },
    )
    if last_predicted_bar_end is not None:
        prev = cp["last_predicted_bar_end"]
        cp["last_predicted_bar_end"] = (
            last_predicted_bar_end if prev is None else max(prev, last_predicted_bar_end)
        )
    cp["predictions_written"] += predictions_added
    cp["expired_backlog_count"] += expired_added
    cp["last_compute_duration_ms"] = compute_duration_ms
    cp["status"] = status_val
    cp["last_error"] = error_message


def prepare_snapshots(
    *,
    dataset_id: str,
    run_id: str | None = None,
    logical_as_of: datetime | None = None,
    events: Sequence[Mapping[str, Any]] | None = None,
    delivery_progress: Sequence[PartitionDeliveryProgress] = (),
    run_status: str = "running",
    provenance: str = "fixture_replay",
    contract: ForecastContract | None = None,
    finalizer: BarFinalizationService | None = None,
    db_manager: DatabaseManager | None = None,
    memory_state: InMemoryWorkerState | None = None,
) -> WorkerPreparationResult:
    """Finalize eligible 1m bars and freeze canonical feature snapshots WITHOUT requiring any ML model."""
    active_contract = contract or load_forecast_contract()
    service = (
        finalizer
        or (memory_state.finalizer if memory_state is not None else None)
        or BarFinalizationService(contract=active_contract, db_manager=db_manager)
    )
    effective_as_of = _ensure_utc(logical_as_of or datetime.now(UTC))

    loaded_events: list[Mapping[str, Any]] = list(events or [])
    effective_progress: list[PartitionDeliveryProgress] = list(delivery_progress)
    effective_run_status = run_status
    barrier_gated_replay = False

    if db_manager is not None and events is None:
        with db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                if run_id is not None:
                    cur.execute(
                        """
                        SELECT logical_as_of, status, provenance, dataset_id, mode
                        FROM raw.ingestion_runs
                        WHERE run_id = %s
                        """,
                        (run_id,),
                    )
                    run_row = cur.fetchone()
                    if run_row is None:
                        raise ValueError("Run must exist before snapshot preparation")
                    if str(run_row[3]) != dataset_id:
                        raise ValueError("Run dataset does not match requested dataset")
                    barrier_gated_replay = run_row[4] == "replay"
                    if run_row[0] is None and run_row[4] == "replay":
                        raise ValueError("Replay has not passed its first delivery barrier")
                    if run_row is not None:
                        if run_row[0] is not None and logical_as_of is None:
                            effective_as_of = _ensure_utc(run_row[0])
                        effective_run_status = str(run_row[1])
                        provenance = str(run_row[2])

                    cur.execute(
                        """
                        SELECT run_id, topic, kafka_partition, last_kafka_offset,
                               last_available_at, status
                        FROM raw.run_delivery_progress
                        WHERE run_id = %s
                        """,
                        (run_id,),
                    )
                    effective_progress = [
                        PartitionDeliveryProgress(
                            topic=str(r[1]),
                            partition=int(r[2]),
                            last_kafka_offset=int(r[3]),
                            last_available_at=_optional_utc(r[4]),
                            status=str(r[5]),
                        )
                        for r in cur.fetchall()
                    ]

                cur.execute(
                    """
                    SELECT source_event_id, ticker, bar_start, bar_end, event_at,
                           available_at, open, high, low, close, volume
                    FROM raw.market_bars
                    WHERE dataset_id = %s
                      AND available_at <= %s
                    ORDER BY bar_end ASC, ticker ASC
                    """,
                    (dataset_id, effective_as_of),
                )
                for row in cur.fetchall():
                    loaded_events.append(
                        {
                            "source_event_id": row[0],
                            "ticker": row[1],
                            "bar_start": row[2],
                            "bar_end": row[3],
                            "event_at": row[4],
                            "available_at": row[5],
                            "open": float(row[6]),
                            "high": float(row[7]),
                            "low": float(row[8]),
                            "close": float(row[9]),
                            "volume": int(row[10]),
                        }
                    )

                if not loaded_events:
                    cur.execute(
                        """
                        SELECT source_event_id, ticker, event_at, available_at, price, volume
                        FROM raw.market_ticks
                        WHERE dataset_id = %s
                          AND available_at <= %s
                        ORDER BY event_at ASC, ticker ASC
                        """,
                        (dataset_id, effective_as_of),
                    )
                    for row in cur.fetchall():
                        loaded_events.append(
                            {
                                "source_event_id": row[0],
                                "ticker": row[1],
                                "event_at": row[2],
                                "available_at": row[3],
                                "price": float(row[4]),
                                "volume": int(row[5]),
                            }
                        )

    if db_manager is not None:
        service.restore_run(dataset_id, run_id)
        if barrier_gated_replay:
            effective_progress = []

    newly_finalized, new_corrections = service.finalize_events(
        loaded_events,
        dataset_id=dataset_id,
        run_id=run_id,
        logical_as_of=effective_as_of,
        delivery_progress=effective_progress,
        run_status=effective_run_status,
        provenance=provenance,
    )
    newly_frozen = service.freeze_feature_snapshots(dataset_id=dataset_id, run_id=run_id)

    all_bar_ends = [b.bar_end for b in newly_finalized] + [s.bar_end for s in newly_frozen]
    last_bar_end = max(all_bar_ends) if all_bar_ends else None

    if run_id is not None and db_manager is not None:
        with db_manager.transaction() as cur:
            cur.execute(
                """
                INSERT INTO marts.prediction_worker_state (
                    run_id, dataset_id, last_prepared_bar_end, status, updated_at
                ) VALUES (%s, %s, %s, 'prepared', NOW())
                ON CONFLICT (run_id) DO UPDATE SET
                    last_prepared_bar_end = COALESCE(
                        GREATEST(marts.prediction_worker_state.last_prepared_bar_end, EXCLUDED.last_prepared_bar_end),
                        COALESCE(EXCLUDED.last_prepared_bar_end, marts.prediction_worker_state.last_prepared_bar_end)
                    ),
                    updated_at = NOW()
                """,
                (run_id, dataset_id, last_bar_end),
            )
    elif run_id is not None and memory_state is not None:
        memory_state.logical_clocks[run_id] = effective_as_of
        cp = memory_state.checkpoints.setdefault(
            run_id,
            {
                "last_prepared_bar_end": None,
                "last_predicted_bar_end": None,
                "predictions_written": 0,
                "expired_backlog_count": 0,
                "status": "prepared",
            },
        )
        if last_bar_end is not None:
            prev = cp["last_prepared_bar_end"]
            cp["last_prepared_bar_end"] = last_bar_end if prev is None else max(prev, last_bar_end)

    return WorkerPreparationResult(
        run_id=run_id,
        dataset_id=dataset_id,
        logical_as_of=effective_as_of,
        finalized_bars_count=len(newly_finalized),
        late_corrections_count=len(new_corrections),
        snapshots_count=len(newly_frozen),
        last_bar_end=last_bar_end,
    )


def _fetch_snapshots_from_db(
    db_manager: DatabaseManager,
    *,
    contract_id: str,
    dataset_id: str,
    run_id: str,
    logical_as_of: datetime,
) -> list[FeatureSnapshotRecord]:
    snapshots: list[FeatureSnapshotRecord] = []
    with db_manager.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT contract_id, contract_version, dataset_id, run_id, ticker,
                       bar_start, bar_end, decision_at, target_at, feature_mode,
                       feature_hash, warmup_complete, contiguous_bar_count, features
                FROM marts.feature_snapshots
                WHERE contract_id = %s
                  AND dataset_id = %s
                  AND run_id = %s
                  AND warmup_complete = TRUE
                  AND decision_at <= %s
                ORDER BY bar_end ASC, ticker ASC
                """,
                (contract_id, dataset_id, run_id, logical_as_of),
            )
            for r in cur.fetchall():
                raw_feats = r[13]
                feats = json.loads(raw_feats) if isinstance(raw_feats, str) else dict(raw_feats)
                snapshots.append(
                    FeatureSnapshotRecord(
                        contract_id=str(r[0]),
                        contract_version=str(r[1]),
                        dataset_id=str(r[2]),
                        run_id=str(r[3]) if r[3] is not None else None,
                        ticker=str(r[4]),
                        bar_start=_ensure_utc(r[5]),
                        bar_end=_ensure_utc(r[6]),
                        decision_at=_ensure_utc(r[7]),
                        target_at=_ensure_utc(r[8]),
                        feature_mode=str(r[9]),
                        feature_hash=str(r[10]),
                        warmup_complete=bool(r[11]),
                        contiguous_bar_count=int(r[12]),
                        features={k: float(v) if v is not None else None for k, v in feats.items()},
                    )
                )
    return snapshots


def advance_run(
    *,
    run_id: str,
    dataset_id: str,
    predictor: Predictor | None,
    logical_as_of: datetime | None = None,
    wall_clock_now: datetime | None = None,
    run_mode: str = "replay_holdout_demo",
    worker_owner: str | None = None,
    require_post_fit_end: bool = True,
    db_manager: DatabaseManager | None = None,
    memory_state: InMemoryWorkerState | None = None,
) -> WorkerAdvanceResult:
    """Advance inference idempotently for eligible snapshots using a verified Predictor bundle."""
    if predictor is None:
        raise RuntimeError("Serving requires a loaded & verified model bundle (Predictor).")

    owner = worker_owner or f"{socket.gethostname()}:{os.getpid()}"
    now_wall = _ensure_utc(wall_clock_now or datetime.now(UTC))
    effective_as_of = _ensure_utc(logical_as_of or now_wall)
    is_live = run_mode in {"live", "real_live"}
    fit_end = _optional_utc(predictor.metadata.get("fit_information_end"))
    model_version = str(predictor.metadata["model_version"])
    bundle_id = (
        str(predictor.metadata["bundle_id"])
        if predictor.metadata.get("bundle_id") is not None
        else None
    )

    acquire_run_lease(
        run_id,
        dataset_id=dataset_id,
        worker_owner=owner,
        now_utc=now_wall,
        db_manager=db_manager,
        memory_state=memory_state,
    )

    t0 = time.perf_counter()
    predictions_written = 0
    duplicates_skipped = 0
    expired_backlog_count = 0
    skipped_pre_fit_end = 0
    snapshots_eligible = 0
    last_predicted_bar_end: datetime | None = None
    err_msg: str | None = None

    existing_keys: set[tuple[str | None, str, str, str, datetime, str]]
    pred_key: tuple[str | None, str, str, str, datetime, str]
    try:
        if db_manager is not None:
            snapshots = _fetch_snapshots_from_db(
                db_manager,
                contract_id=predictor.contract.contract_id,
                dataset_id=dataset_id,
                run_id=run_id,
                logical_as_of=effective_as_of,
            )
            with db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT run_id, contract_id, dataset_id, ticker, bar_end, bundle_id
                        FROM marts.model_predictions
                        WHERE run_id = %s AND contract_id = %s AND dataset_id = %s AND bundle_id = %s
                        """,
                        (run_id, predictor.contract.contract_id, dataset_id, bundle_id),
                    )
                    existing_keys = {
                        (str(r[0]), str(r[1]), str(r[2]), str(r[3]), _ensure_utc(r[4]), str(r[5]))
                        for r in cur.fetchall()
                    }
        elif memory_state is not None:
            memory_state.run_modes[run_id] = run_mode
            memory_state.logical_clocks[run_id] = effective_as_of
            snapshots = sorted(
                (
                    s
                    for s in memory_state.finalizer.snapshots.values()
                    if s.contract_id == predictor.contract.contract_id
                    and s.dataset_id == dataset_id
                    and s.run_id == run_id
                    and s.warmup_complete
                    and s.decision_at <= effective_as_of
                ),
                key=lambda s: (s.bar_end, s.ticker),
            )
            existing_keys = set(memory_state.predictions.keys())
        else:
            snapshots = []
            existing_keys = set()

        new_records: list[StoredPredictionRecord] = []
        for snap in snapshots:
            if require_post_fit_end and fit_end is not None and snap.decision_at < fit_end:
                skipped_pre_fit_end += 1
                continue

            snapshots_eligible += 1
            pred_key = (
                run_id,
                snap.contract_id,
                snap.dataset_id,
                snap.ticker,
                snap.bar_end,
                bundle_id or model_version,
            )
            if pred_key in existing_keys:
                duplicates_skipped += 1
                continue

            # Live backlog rule: if wall-clock now already reached/passed target_at, do not backdate;
            # record as expired_backlog and exclude from timely forecast evaluation.
            if is_live and now_wall >= snap.target_at:
                timeliness_status = "expired_backlog"
                decision_clock = now_wall
                expired_backlog_count += 1
            else:
                timeliness_status = "timely"
                decision_clock = now_wall if is_live else snap.decision_at

            infer_t0 = time.perf_counter()
            pred_out = predictor.predict(snap)
            infer_ms = max(0.0, (time.perf_counter() - infer_t0) * 1000.0)

            rec = StoredPredictionRecord(
                prediction_id=str(uuid.uuid4()),
                contract_id=snap.contract_id,
                contract_version=snap.contract_version,
                dataset_id=snap.dataset_id,
                run_id=run_id,
                ticker=snap.ticker,
                bar_start=snap.bar_start,
                bar_end=snap.bar_end,
                decision_at=snap.decision_at,
                target_at=snap.target_at,
                feature_hash=snap.feature_hash,
                bundle_id=bundle_id,
                model_version=model_version,
                champion_model=str(pred_out["champion_model"]),
                fit_information_end=fit_end,
                predicted_direction=int(pred_out["predicted_direction"]),
                signal_label=str(pred_out["signal_label"]),
                probability_up=round(float(pred_out["probability_up"]), 6),
                probability_not_up=round(float(pred_out["probability_down"]), 6),
                raw_probability_up=round(float(pred_out["raw_probability_up"]), 6),
                confidence_tier=str(pred_out["confidence_tier"]),
                attribution_space=str(pred_out["attribution_space"]),
                shap_factors=list(pred_out["factors"]),
                run_mode=run_mode,
                timeliness_status=timeliness_status,
                logical_as_of=decision_clock,
                predicted_at=now_wall,
                compute_duration_ms=round(infer_ms, 4),
            )
            new_records.append(rec)
            existing_keys.add(pred_key)
            last_predicted_bar_end = (
                snap.bar_end
                if last_predicted_bar_end is None
                else max(last_predicted_bar_end, snap.bar_end)
            )

        if db_manager is not None and new_records:
            with db_manager.transaction() as cur:
                for rec in new_records:
                    cur.execute(
                        MODEL_PREDICTION_INSERT_SQL,
                        (
                            rec.prediction_id,
                            rec.contract_id,
                            rec.contract_version,
                            rec.dataset_id,
                            rec.run_id,
                            rec.ticker,
                            rec.bar_start,
                            rec.bar_end,
                            rec.decision_at,
                            rec.target_at,
                            rec.feature_hash,
                            rec.bundle_id,
                            rec.model_version,
                            rec.champion_model,
                            rec.fit_information_end,
                            rec.predicted_direction,
                            rec.signal_label,
                            rec.probability_up,
                            rec.probability_not_up,
                            rec.raw_probability_up,
                            rec.confidence_tier,
                            rec.attribution_space,
                            json.dumps(rec.shap_factors),
                            rec.run_mode,
                            rec.timeliness_status,
                            rec.logical_as_of,
                            rec.predicted_at,
                            rec.compute_duration_ms,
                        ),
                    )
                    if cur.rowcount == 1:
                        predictions_written += 1
                    else:
                        duplicates_skipped += 1
        elif memory_state is not None and new_records:
            for rec in new_records:
                pred_key = (
                    rec.run_id,
                    rec.contract_id,
                    rec.dataset_id,
                    rec.ticker,
                    rec.bar_end,
                    rec.bundle_id or rec.model_version,
                )
                if pred_key not in memory_state.predictions:
                    memory_state.predictions[pred_key] = rec
                    predictions_written += 1
                else:
                    duplicates_skipped += 1

        total_ms = max(0.0, (time.perf_counter() - t0) * 1000.0)
        return WorkerAdvanceResult(
            run_id=run_id,
            dataset_id=dataset_id,
            run_mode=run_mode,
            model_version=model_version,
            logical_as_of=effective_as_of,
            wall_clock_at=now_wall,
            snapshots_eligible=snapshots_eligible,
            predictions_written=predictions_written,
            duplicates_skipped=duplicates_skipped,
            expired_backlog_count=expired_backlog_count,
            skipped_pre_fit_end=skipped_pre_fit_end,
            compute_duration_ms=round(total_ms, 4),
            last_predicted_bar_end=last_predicted_bar_end,
        )
    except Exception as exc:
        err_msg = type(exc).__name__
        raise
    finally:
        total_ms_final = max(0.0, (time.perf_counter() - t0) * 1000.0)
        release_run_lease(
            run_id,
            worker_owner=owner,
            last_predicted_bar_end=last_predicted_bar_end,
            predictions_added=predictions_written,
            expired_added=expired_backlog_count,
            compute_duration_ms=round(total_ms_final, 4),
            error_message=err_msg,
            db_manager=db_manager,
            memory_state=memory_state,
        )


def resolve_outcomes(
    *,
    current_cutoff_at: datetime,
    contract: ForecastContract | None = None,
    predictions: Sequence[StoredPredictionRecord] | None = None,
    final_bars: Mapping[tuple[str | None, str, str, str, datetime], FinalBarRecord] | None = None,
    dataset_end_at: datetime | None = None,
    memory_state: InMemoryWorkerState | None = None,
    run_id: str | None = None,
) -> list[RealizedOutcomeRecord]:
    """Derive non-mutating outcome status (pending/realized/unavailable/expired) for stored predictions."""
    active_contract = contract or load_forecast_contract()
    cutoff_utc = _ensure_utc(current_cutoff_at)
    grace_td = timedelta(seconds=active_contract.grace_seconds)

    if memory_state is not None:
        pred_list = [
            p for p in memory_state.predictions.values() if run_id is None or p.run_id == run_id
        ]
        bars_map = memory_state.finalizer.final_bars
        effective_dataset_end = (
            dataset_end_at
            if dataset_end_at is not None
            else (memory_state.dataset_ends.get(run_id) if run_id else None)
        )
    else:
        pred_list = list(predictions or [])
        bars_map = dict(final_bars or {})
        effective_dataset_end = dataset_end_at

    contract_bars: dict[
        tuple[str | None, str, str, str], dict[tuple[str, datetime], ContractBar]
    ] = {}
    for b in bars_map.values():
        contract_bars.setdefault((b.run_id, b.contract_id, b.dataset_id, b.ticker), {})[
            (b.ticker, b.bar_end)
        ] = ContractBar(
            ticker=b.ticker,
            bar_start=b.bar_start,
            bar_end=b.bar_end,
            open=b.open,
            high=b.high,
            low=b.low,
            close=b.close,
            volume=b.volume,
            event_count=b.event_count,
            late_event_count=b.late_event_count,
            duplicate_event_count=0,
        )

    outcomes: list[RealizedOutcomeRecord] = []
    for p in sorted(pred_list, key=lambda x: (x.bar_end, x.ticker)):
        if p.timeliness_status == "expired_backlog":
            status_val = "expired"
            reason_val: str | None = "live_backlog_past_target"
            actual_dir: str | None = None
            ret_5m: float | None = None
            was_correct: bool | None = None
        elif (
            effective_dataset_end is not None
            and cutoff_utc >= effective_dataset_end
            and p.target_at > effective_dataset_end
        ):
            status_val = "unavailable"
            reason_val = "dataset_end"
            actual_dir = None
            ret_5m = None
            was_correct = None
        elif cutoff_utc < p.target_at + grace_td:
            status_val = "pending"
            reason_val = "horizon_not_matured"
            actual_dir = None
            ret_5m = None
            was_correct = None
        else:
            t_bars = contract_bars.get((p.run_id, p.contract_id, p.dataset_id, p.ticker), {})
            eval_res = evaluate_target_outcome(
                bars=t_bars,
                ticker=p.ticker,
                anchor_bar_end=p.bar_end,
                contract=active_contract,
                dataset_end_utc=effective_dataset_end,
            )
            status_val = eval_res.status
            reason_val = eval_res.reason
            actual_dir = eval_res.target_direction
            ret_5m = eval_res.return_5m
            if (
                status_val == "realized"
                and actual_dir is not None
                and p.signal_label in ("UP", "NOT_UP")
            ):
                was_correct = p.signal_label == actual_dir
            else:
                was_correct = None

        outcomes.append(
            RealizedOutcomeRecord(
                prediction_id=p.prediction_id,
                contract_id=p.contract_id,
                dataset_id=p.dataset_id,
                run_id=p.run_id,
                ticker=p.ticker,
                bar_start=p.bar_start,
                bar_end=p.bar_end,
                decision_at=p.decision_at,
                target_at=p.target_at,
                feature_hash=p.feature_hash,
                bundle_id=p.bundle_id,
                model_version=p.model_version,
                champion_model=p.champion_model,
                signal_label=p.signal_label,
                predicted_direction=p.predicted_direction,
                probability_up=p.probability_up,
                probability_not_up=p.probability_not_up,
                confidence_tier=p.confidence_tier,
                attribution_space=p.attribution_space,
                factors=p.shap_factors,
                run_mode=p.run_mode,
                timeliness_status=p.timeliness_status,
                logical_as_of=p.logical_as_of,
                predicted_at=p.predicted_at,
                compute_duration_ms=p.compute_duration_ms,
                outcome_status=status_val,
                outcome_reason=reason_val,
                actual_direction=actual_dir,
                return_5m=ret_5m,
                was_correct=was_correct,
            )
        )
    return outcomes


def _discover_latest_runs(db: DatabaseManager, limit: int = 5) -> list[tuple[str, str, str]]:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, dataset_id, mode
                FROM raw.ingestion_runs
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [(str(r[0]), str(r[1]), str(r[2])) for r in cur.fetchall()]


def _run_once(
    db: DatabaseManager,
    *,
    run_id: str,
    dataset_id: str,
    mode: str,
    bundle_dir: Path,
    prepare_only: bool,
    as_of: datetime | None,
) -> None:
    prep = prepare_snapshots(
        dataset_id=dataset_id,
        run_id=run_id,
        logical_as_of=as_of,
        db_manager=db,
    )
    logger.info("Prepared snapshots: %s", json.dumps(asdict(prep), default=str))
    if prepare_only:
        return
    if not (bundle_dir / "manifest.json").exists() and not (bundle_dir / "current").exists():
        logger.warning("Model manifest not found at %s; skipping inference.", bundle_dir)
        return
    predictor = Predictor.load(str(bundle_dir), enforce_contract=True)
    adv = advance_run(
        run_id=run_id,
        dataset_id=dataset_id,
        predictor=predictor,
        logical_as_of=prep.logical_as_of,
        run_mode=mode,
        db_manager=db,
    )
    logger.info("Advanced run: %s", json.dumps(asdict(adv), default=str))


def _cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Run MarketPulseAI single-worker snapshot preparation and prediction serving."
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--dataset-id", default=None)
    parser.add_argument("--mode", default="replay_holdout_demo")
    parser.add_argument("--bundle-dir", default="model/artefacts")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--as-of", default=None, help="Optional ISO UTC cutoff timestamp")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=10)
    args = parser.parse_args()

    db = DatabaseManager(DatabaseConfig())  # type: ignore[call-arg]
    bundle_path = Path(args.bundle_dir)
    as_of = _optional_utc(args.as_of)

    try:
        while True:
            if args.run_id and args.dataset_id:
                targets = [(args.run_id, args.dataset_id, args.mode)]
            else:
                targets = _discover_latest_runs(db)
            for r_id, ds_id, r_mode in targets:
                effective_mode = (
                    "replay_holdout_demo"
                    if r_mode == "replay"
                    else ("real_live" if r_mode == "live" else r_mode)
                )
                try:
                    _run_once(
                        db,
                        run_id=r_id,
                        dataset_id=ds_id,
                        mode=args.mode if args.run_id else effective_mode,
                        bundle_dir=bundle_path,
                        prepare_only=args.prepare_only,
                        as_of=as_of,
                    )
                except WorkerLeaseError:
                    logger.info("Skipping run %s: held by another worker.", r_id)
                except Exception as exc:
                    logger.warning("Worker iteration failed for %s: %s", r_id, type(exc).__name__)
            if not args.loop:
                break
            time.sleep(max(1, args.interval_seconds))
    finally:
        db.close()


if __name__ == "__main__":
    _cli()
