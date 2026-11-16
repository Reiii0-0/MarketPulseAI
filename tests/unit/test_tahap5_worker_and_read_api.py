from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.dependencies import get_db, get_predictor
from api.routers import health, model, pipeline_read, predict
from model.evaluate import publish_model_bundle, run_walk_forward_evaluation
from model.predictor import Predictor
from pipeline.contract import load_forecast_contract
from pipeline.features import BarFinalizationService
from pipeline.worker import (
    InMemoryWorkerState,
    WorkerLeaseError,
    acquire_run_lease,
    advance_run,
    prepare_snapshots,
    release_run_lease,
    resolve_outcomes,
)


def _generate_bars_events(n_minutes: int = 85, seed: int = 7) -> list[dict[str, Any]]:
    rng = np.random.default_rng(seed)
    start = pd.Timestamp("2025-07-14T13:30:00Z")
    events: list[dict[str, Any]] = []
    for ticker, base_p in (("AAPL", 180.0), ("MSFT", 420.0)):
        price = base_p
        for idx in range(n_minutes):
            t0 = start + timedelta(minutes=idx)
            t1 = t0 + timedelta(minutes=1)
            step = float(rng.normal(0.03, 0.22))
            o = round(price, 4)
            c = round(max(10.0, o + step), 4)
            h = round(max(o, c) + 0.15, 4)
            lo = round(min(o, c) - 0.15, 4)
            events.append(
                {
                    "source_event_id": f"{ticker}:{idx}",
                    "ticker": ticker,
                    "bar_start": t0.to_pydatetime(),
                    "bar_end": t1.to_pydatetime(),
                    "event_at": t1.to_pydatetime(),
                    "available_at": (t1 + timedelta(seconds=2)).to_pydatetime(),
                    "open": o,
                    "high": h,
                    "low": lo,
                    "close": c,
                    "volume": int(rng.integers(1000, 6000)),
                }
            )
            price = c
    return events


def _train_temp_predictor(
    tmp_path: Path, events: list[dict[str, Any]]
) -> tuple[Predictor, datetime]:
    contract = load_forecast_contract()
    bars_df = pd.DataFrame(events)
    bars_df["dataset_id"] = "stage5_demo_ds"
    report, bundle_objects = run_walk_forward_evaluation(
        bars_df,
        contract=contract,
        dataset_id="stage5_demo_ds",
        dataset_provenance="synthetic_smoke_fixture",
        data_checksum="abc123",
    )
    bundle_dir = tmp_path / "bundle"
    publish_model_bundle(bundle_dir, report, bundle_objects, contract=contract)
    predictor = Predictor.load(str(bundle_dir), enforce_contract=True, contract=contract)
    fit_end = datetime.fromisoformat(str(report["cutoffs"]["fit_information_end"]))
    return predictor, fit_end


@pytest.mark.unit
def test_prepare_snapshots_succeeds_without_model() -> None:
    """prepare_snapshots is a model-free pipeline preparation step."""
    contract = load_forecast_contract()
    events = _generate_bars_events(n_minutes=30)
    state = InMemoryWorkerState(finalizer=BarFinalizationService(contract=contract))

    prep = prepare_snapshots(
        dataset_id="stage5_demo_ds",
        run_id="run-prep-only",
        logical_as_of=datetime(2025, 7, 14, 14, 15, tzinfo=UTC),
        events=events,
        contract=contract,
        memory_state=state,
    )
    assert prep.finalized_bars_count == 60
    assert prep.snapshots_count == 60
    warm_snaps = [s for s in state.finalizer.snapshots.values() if s.warmup_complete]
    assert len(warm_snaps) > 0


@pytest.mark.unit
def test_worker_full_lifecycle_idempotency_and_outcomes(tmp_path: Path) -> None:
    """Single replay flow: prepare -> advance (post-fit) -> no duplicates -> pending/realized/unavailable."""
    contract = load_forecast_contract()
    events = _generate_bars_events(n_minutes=85)
    predictor, fit_end = _train_temp_predictor(tmp_path, events)
    state = InMemoryWorkerState(finalizer=BarFinalizationService(contract=contract))

    # Partial cutoff right after fit_information_end + 3 minutes: targets still pending
    early_cutoff = fit_end + timedelta(minutes=3)
    prepare_snapshots(
        dataset_id="stage5_demo_ds",
        run_id="run-e2e-1",
        logical_as_of=early_cutoff,
        events=events,
        contract=contract,
        memory_state=state,
    )

    adv1 = advance_run(
        run_id="run-e2e-1",
        dataset_id="stage5_demo_ds",
        predictor=predictor,
        logical_as_of=early_cutoff,
        run_mode="replay_holdout_demo",
        memory_state=state,
    )
    assert adv1.skipped_pre_fit_end > 0
    assert adv1.predictions_written > 0
    assert adv1.duplicates_skipped == 0
    assert adv1.compute_duration_ms >= 0.0
    assert all(p.fit_information_end == fit_end for p in state.predictions.values())

    # Worker restart at same cutoff: zero duplicate predictions written
    adv_restart = advance_run(
        run_id="run-e2e-1",
        dataset_id="stage5_demo_ds",
        predictor=predictor,
        logical_as_of=early_cutoff,
        run_mode="replay_holdout_demo",
        memory_state=state,
    )
    assert adv_restart.predictions_written == 0
    assert adv_restart.duplicates_skipped == adv1.predictions_written

    # Outcomes at early_cutoff must be pending (horizon +5m + 15s not yet matured)
    pending_outcomes = resolve_outcomes(
        current_cutoff_at=early_cutoff,
        contract=contract,
        memory_state=state,
        run_id="run-e2e-1",
    )
    assert all(o.outcome_status == "pending" for o in pending_outcomes)
    assert all(o.outcome_reason == "horizon_not_matured" for o in pending_outcomes)

    # Finalize remaining dataset events and move clock past end -> realized + dataset_end unavailable
    dataset_end = max(ev["bar_end"] for ev in events)
    late_cutoff = dataset_end + timedelta(minutes=6)
    prepare_snapshots(
        dataset_id="stage5_demo_ds",
        run_id="run-e2e-1",
        logical_as_of=late_cutoff,
        events=events,
        contract=contract,
        memory_state=state,
    )
    adv2 = advance_run(
        run_id="run-e2e-1",
        dataset_id="stage5_demo_ds",
        predictor=predictor,
        logical_as_of=late_cutoff,
        run_mode="replay_holdout_demo",
        memory_state=state,
    )
    assert adv2.predictions_written > 0

    resolved = resolve_outcomes(
        current_cutoff_at=late_cutoff,
        contract=contract,
        dataset_end_at=dataset_end,
        memory_state=state,
        run_id="run-e2e-1",
    )
    statuses = {o.outcome_status for o in resolved}
    assert "realized" in statuses
    assert "unavailable" in statuses
    unavail = [o for o in resolved if o.outcome_status == "unavailable"]
    assert all(o.outcome_reason == contract.target_policy.dataset_boundary_reason for o in unavail)
    realized = [o for o in resolved if o.outcome_status == "realized"]
    assert all(o.actual_direction in ("UP", "NOT_UP") for o in realized)


@pytest.mark.unit
def test_worker_lease_prevents_concurrent_workers_and_requires_model(tmp_path: Path) -> None:
    """Exclusive run lease blocks concurrent workers; serving fails cleanly when predictor is None."""
    contract = load_forecast_contract()
    state = InMemoryWorkerState(finalizer=BarFinalizationService(contract=contract))

    with pytest.raises(RuntimeError, match="Predictor"):
        advance_run(
            run_id="run-missing-model",
            dataset_id="stage5_demo_ds",
            predictor=None,
            memory_state=state,
        )

    now = datetime(2025, 7, 14, 15, 0, tzinfo=UTC)
    acquire_run_lease(
        "run-leased",
        dataset_id="stage5_demo_ds",
        worker_owner="worker-A",
        now_utc=now,
        memory_state=state,
    )
    with pytest.raises(WorkerLeaseError):
        acquire_run_lease(
            "run-leased",
            dataset_id="stage5_demo_ds",
            worker_owner="worker-B",
            now_utc=now + timedelta(seconds=10),
            memory_state=state,
        )
    release_run_lease("run-leased", worker_owner="worker-A", memory_state=state)
    acquire_run_lease(
        "run-leased",
        dataset_id="stage5_demo_ds",
        worker_owner="worker-B",
        now_utc=now + timedelta(seconds=15),
        memory_state=state,
    )


@pytest.mark.unit
def test_live_backlog_never_backdates_and_marks_expired(tmp_path: Path) -> None:
    """In real_live mode, snapshots processed after target_at are marked expired_backlog."""
    contract = load_forecast_contract()
    events = _generate_bars_events(n_minutes=85)
    predictor, _ = _train_temp_predictor(tmp_path, events)
    state = InMemoryWorkerState(finalizer=BarFinalizationService(contract=contract))

    far_future_wall_clock = datetime(2025, 7, 15, 10, 0, tzinfo=UTC)
    prepare_snapshots(
        dataset_id="stage5_demo_ds",
        run_id="run-live-backlog",
        logical_as_of=far_future_wall_clock,
        events=events,
        contract=contract,
        memory_state=state,
    )
    adv = advance_run(
        run_id="run-live-backlog",
        dataset_id="stage5_demo_ds",
        predictor=predictor,
        wall_clock_now=far_future_wall_clock,
        run_mode="real_live",
        memory_state=state,
    )
    assert adv.predictions_written > 0
    assert adv.expired_backlog_count == adv.predictions_written
    assert all(p.predicted_at == far_future_wall_clock for p in state.predictions.values())

    outcomes = resolve_outcomes(
        current_cutoff_at=far_future_wall_clock,
        contract=contract,
        memory_state=state,
        run_id="run-live-backlog",
    )
    assert all(o.outcome_status == "expired" for o in outcomes)
    assert all(o.outcome_reason == "live_backlog_past_target" for o in outcomes)


@pytest.mark.unit
def test_health_liveness_vs_readiness_endpoints() -> None:
    """/health always returns 200 (degraded when unready); /health/ready returns 503 unless all checks pass."""
    contract = load_forecast_contract()

    def _build_health_app(*, db_ok: bool, model_ok: bool, contract_match: bool) -> TestClient:
        app = FastAPI()
        app.include_router(health.router)
        db_mock = MagicMock()
        if db_ok:
            cur = db_mock.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
            cur.execute.return_value = None
        else:
            db_mock.get_connection.side_effect = RuntimeError("sensitive postgres password host")

        app.state.db_manager = db_mock
        app.state.contract = contract
        if model_ok:
            pred = MagicMock()
            pred.metadata = {
                "contract_id": contract.contract_id if contract_match else "wrong_contract",
                "contract_version": contract.version,
                "feature_mode": contract.feature_policy.mode,
                "feature_names": list(contract.feature_policy.features),
            }
            app.state.predictor = pred
            app.state.model_loaded = True
        else:
            app.state.predictor = None
            app.state.model_loaded = False
        return TestClient(app)

    # 1. All healthy
    c_ok = _build_health_app(db_ok=True, model_ok=True, contract_match=True)
    assert c_ok.get("/health").status_code == 200
    assert c_ok.get("/health").json()["status"] == "healthy"
    r_ok = c_ok.get("/health/ready")
    assert r_ok.status_code == 200
    assert r_ok.json()["ready"] is True

    # 2. Missing model -> liveness 200 degraded, readiness 503
    c_no_model = _build_health_app(db_ok=True, model_ok=False, contract_match=True)
    assert c_no_model.get("/health").status_code == 200
    assert c_no_model.get("/health").json()["status"] == "degraded"
    r_no_model = c_no_model.get("/health/ready")
    assert r_no_model.status_code == 503
    assert "model_bundle_unavailable" in r_no_model.json()["reasons"]

    # 3. DB unreachable -> readiness 503 without leaking exception string
    c_no_db = _build_health_app(db_ok=False, model_ok=True, contract_match=True)
    r_no_db = c_no_db.get("/health/ready")
    assert r_no_db.status_code == 503
    assert "sensitive postgres" not in r_no_db.text
    assert "database_unreachable" in r_no_db.json()["reasons"]

    # 4. Contract mismatch -> readiness 503
    c_bad_c = _build_health_app(db_ok=True, model_ok=True, contract_match=False)
    r_bad_c = c_bad_c.get("/health/ready")
    assert r_bad_c.status_code == 503
    assert "contract_mismatch" in r_bad_c.json()["reasons"]


@pytest.mark.unit
def test_read_only_api_endpoints_and_not_up_label_migration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify read-only API endpoints, /model/info bundle support, NOT_UP labels, and evidence separation."""
    contract = load_forecast_contract()
    events = _generate_bars_events(n_minutes=85)
    predictor, fit_end = _train_temp_predictor(tmp_path, events)

    # Write synthetic smoke report into tmp_path/reports and point ReadApiConfig at it
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    smoke_report = {
        "dataset_id": "fixture_smoke_v1",
        "is_real_market_data": False,
        "real_market_validated": False,
        "champion_model": "logistic_regression",
    }
    (reports_dir / "fixture_smoke_v1_evaluation.json").write_text(
        json.dumps(smoke_report), encoding="utf-8"
    )
    bundle_manifest = tmp_path / "bundle" / "manifest.json"
    monkeypatch.setattr(pipeline_read.read_config, "evaluation_reports_dir", str(reports_dir))
    monkeypatch.setattr(pipeline_read.read_config, "bundle_manifest_path", str(bundle_manifest))

    pred_uuid = uuid.uuid4()
    now = datetime(2025, 7, 14, 15, 10, tzinfo=UTC)
    db_mock = MagicMock()
    cur = db_mock.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value

    app = FastAPI()
    app.include_router(predict.router, prefix="/api/v1")
    app.include_router(model.router, prefix="/api/v1")
    app.include_router(pipeline_read.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = lambda: db_mock
    app.dependency_overrides[get_predictor] = lambda: predictor
    client = TestClient(app)

    # 1. /api/v1/model/info works cleanly on Stage 4 BundleManifest metadata
    info_resp = client.get("/api/v1/model/info")
    assert info_resp.status_code == 200
    assert info_resp.json()["contract_id"] == contract.contract_id
    assert info_resp.json()["is_real_market_data"] is False

    # 2. /api/v1/evidence reports real_market_validated=False while keeping smoke report separate
    ev_resp = client.get("/api/v1/evidence")
    assert ev_resp.status_code == 200
    ev_json = ev_resp.json()
    assert ev_json["real_market_validated"] is False
    assert ev_json["real_market_status"] == "unavailable"
    assert len(ev_json["synthetic_smoke_reports"]) == 1
    assert ev_json["active_bundle_manifest"]["contract_id"] == contract.contract_id

    # 3. /api/v1/runs
    cur.fetchall.return_value = [
        (
            "run-1",
            "stage5_demo_ds",
            "replay",
            "synthetic_fixture",
            "completed",
            now,
            now,
            "idle",
            now,
            now,
            12,
            0,
            4.2,
        )
    ]
    runs_resp = client.get("/api/v1/runs")
    assert runs_resp.status_code == 200
    assert runs_resp.json()["runs"][0]["run_id"] == "run-1"

    # 4. /api/v1/runs/{run_id} (including post-fit window check)
    cur.fetchone.return_value = (
        "run-1",
        "stage5_demo_ds",
        "replay",
        "synthetic_fixture",
        "completed",
        fit_end + timedelta(minutes=10),
        now,
        "idle",
        now,
        now,
        12,
        0,
        4.2,
    )
    cur.fetchall.return_value = [("market_bars", 0, 80, now, 80, 0, "completed")]
    run_det = client.get("/api/v1/runs/run-1")
    assert run_det.status_code == 200
    assert run_det.json()["serves_post_fit_window"] is True

    # 5. /api/v1/outcomes & /api/v1/predictions/{id} verify NOT_UP direction and attribution_space
    outcome_row = (
        pred_uuid,
        contract.contract_id,
        "stage5_demo_ds",
        "run-1",
        "AAPL",
        now - timedelta(minutes=5),
        now - timedelta(minutes=5),
        now,
        "a" * 64,
        "bundle-1",
        predictor.metadata["model_version"],
        predictor.champion_model,
        0,
        "NOT_UP",
        0.42,
        0.58,
        "LOW",
        "standardized_linear_log_odds",
        "timely",
        "replay_holdout_demo",
        now,
        "realized",
        None,
        "NOT_UP",
        -0.0012,
        True,
        json.dumps([{"feature": "return_1m", "shap_value": -0.15, "direction": "negative"}]),
        now,
        1.5,
    )
    cur.fetchall.return_value = [outcome_row]
    out_resp = client.get("/api/v1/outcomes")
    assert out_resp.status_code == 200
    out_json = out_resp.json()
    assert out_json["realized_count"] == 1
    assert out_json["timely_accuracy"] == pytest.approx(1.0)
    assert out_json["outcomes"][0]["predicted_direction"] == "NOT_UP"
    assert out_json["outcomes"][0]["actual_direction"] == "NOT_UP"
    assert out_json["outcomes"][0]["attribution_space"] == "standardized_linear_log_odds"

    # 6. /api/v1/predictions/recent also maps non-positive direction to NOT_UP
    cur.fetchone.return_value = [1]
    cur.fetchall.return_value = [[pred_uuid, "AAPL", now, 0, 0.35, "MEDIUM", 0, True, now]]
    rec_resp = client.get("/api/v1/predictions/recent")
    assert rec_resp.status_code == 200
    rec_item = rec_resp.json()["predictions"][0]
    assert rec_item["predicted_direction"] == "NOT_UP"
    assert rec_item["actual_direction"] == "NOT_UP"

    # 7. DB failure in read endpoint is sanitized (never leaks SQL/driver internals)
    db_mock.get_connection.side_effect = RuntimeError("SELECT password FROM secret_table")
    err_resp = client.get("/api/v1/runs")
    assert err_resp.status_code == 500
    assert "secret_table" not in err_resp.text
