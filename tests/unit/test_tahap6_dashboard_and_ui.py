from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient

from api.dependencies import get_db
from api.main import app as real_app
from api.routers import dashboard, health, pipeline_read
from pipeline.contract import load_forecast_contract


@pytest.mark.unit
def test_dashboard_routes_security_headers_and_static_assets() -> None:
    """Dashboard HTML, CSS, and JS must serve locally with hardened CSP headers and zero CDN refs."""
    client = TestClient(real_app)

    for path in ("/", "/dashboard"):
        resp = client.get(path)
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert resp.headers.get("x-content-type-options") == "nosniff"
        assert resp.headers.get("x-frame-options") == "DENY"
        csp = resp.headers.get("content-security-policy", "")
        assert "default-src 'self'" in csp
        assert "script-src 'self'" in csp
        assert "http://" not in resp.text and "https://" not in resp.text

        # Required Stage 6 three areas, provenance clock, playback & edge state hooks
        for required_id in (
            'id="datasetModeBadge"',
            'id="logicalClockValue"',
            'id="readinessBadge"',
            'id="overview-panel"',
            'id="detail-panel"',
            'id="evidence-panel"',
            'id="timelineSlider"',
            'id="statePreviewSelect"',
            'id="priceChartSvg"',
            'id="chartSummaryText"',
            'id="detAttributionSpace"',
            'id="realMarketGateCard"',
        ):
            assert required_id in resp.text

    css_resp = client.get("/static/app.css")
    assert css_resp.status_code == 200
    assert ":focus-visible" in css_resp.text
    assert "prefers-reduced-motion" in css_resp.text
    assert "@media (max-width: 640px)" in css_resp.text

    js_resp = client.get("/static/app.js")
    assert js_resp.status_code == 200
    assert ".innerHTML" not in js_resp.text
    # Ensure dashboard client only calls read-only GET endpoints and never triggers training/reset
    assert "POST" not in js_resp.text
    assert "DELETE" not in js_resp.text
    assert "/api/v1/evidence" in js_resp.text
    assert "/api/v1/health/ready" in js_resp.text

    node_bin = shutil.which("node")
    if node_bin is not None:
        js_file = Path("api/static/app.js")
        check = subprocess.run([node_bin, "--check", str(js_file)], capture_output=True, text=True)
        assert check.returncode == 0, f"node --check failed: {check.stderr}"


@pytest.mark.unit
def test_static_assets_bypass_api_rate_limit() -> None:
    """Repeated dashboard and static asset loads must not exhaust or hit 429 rate limits."""
    client = TestClient(real_app)
    for _ in range(70):
        r = client.get("/static/app.css")
        assert r.status_code == 200


@pytest.mark.unit
def test_tickers_endpoint_and_sanitized_errors() -> None:
    """GET /api/v1/tickers returns sorted unique tickers and sanitizes SQL/DB exceptions."""
    db_mock = MagicMock()
    cur = db_mock.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    cur.fetchall.return_value = [("AAPL",), ("MSFT",)]

    app = FastAPI()
    app.include_router(pipeline_read.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = lambda: db_mock
    client = TestClient(app)

    ok_resp = client.get("/api/v1/tickers?dataset_id=fixture_smoke_v1")
    assert ok_resp.status_code == 200
    assert ok_resp.json() == {"dataset_id": "fixture_smoke_v1", "tickers": ["AAPL", "MSFT"]}

    db_mock.get_connection.side_effect = RuntimeError("SELECT secret_token FROM raw.secrets")
    err_resp = client.get("/api/v1/tickers")
    assert err_resp.status_code == 500
    assert "secret_token" not in err_resp.text
    assert "INTERNAL_SERVER_ERROR" in err_resp.text


@pytest.mark.unit
def test_three_minute_demo_read_flow_and_no_signal_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulate full Stage 6 dashboard API flow: readiness -> runs -> tickers -> bars -> NO_SIGNAL predictions -> outcomes -> evidence."""
    contract = load_forecast_contract()
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    smoke_eval = {
        "dataset_id": "fixture_smoke_v1",
        "is_real_market_data": False,
        "real_market_validated": False,
        "warning_banner": "SYNTHETIC FIXTURE ONLY",
        "champion_model": "logistic_regression",
        "selection_metric": "brier_score",
        "abstain_band_delta": 0.15,
        "cutoffs": {
            "purge_seconds": 300,
            "walk_forward_folds": [],
            "holdout_start": "2025-01-15T15:07:00+00:00",
            "holdout_end": "2025-01-15T15:10:00+00:00",
            "pre_holdout_samples": 23,
            "holdout_samples": 4,
        },
        "validation_summary": {},
        "holdout_summary": {},
    }
    (reports_dir / "fixture_smoke_v1_evaluation.json").write_text(
        json.dumps(smoke_eval), encoding="utf-8"
    )
    bundle_manifest = tmp_path / "manifest.json"
    bundle_manifest.write_text(
        json.dumps({"bundle_id": "demo-bundle-1", "contract_id": contract.contract_id}),
        encoding="utf-8",
    )
    monkeypatch.setattr(pipeline_read.read_config, "evaluation_reports_dir", str(reports_dir))
    monkeypatch.setattr(pipeline_read.read_config, "bundle_manifest_path", str(bundle_manifest))

    db_mock = MagicMock()
    cur = db_mock.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    now = datetime(2025, 1, 15, 15, 10, tzinfo=UTC)
    pid_no_sig = uuid.uuid4()
    pid_up = uuid.uuid4()

    app = FastAPI()
    static_dir = Path("api/static")
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    app.include_router(dashboard.router)
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(pipeline_read.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = lambda: db_mock
    app.state.db_manager = db_mock
    app.state.contract = contract
    app.state.model_loaded = False
    app.state.predictor = None

    client = TestClient(app)

    # 1. Readiness reports degraded cleanly (503) when model is missing while UI still loads
    ready_resp = client.get("/api/v1/health/ready")
    assert ready_resp.status_code == 503
    assert "model_bundle_unavailable" in ready_resp.json()["reasons"]

    # 2. Runs & tickers
    cur.fetchall.return_value = [
        (
            "run-demo-1",
            "fixture_smoke_v1",
            "replay_holdout_demo",
            "synthetic_fixture",
            "completed",
            now,
            now,
            "idle",
            now,
            now,
            2,
            0,
            2.1,
        )
    ]
    assert client.get("/api/v1/runs").status_code == 200

    cur.fetchall.return_value = [("MSFT",)]
    assert client.get("/api/v1/tickers?dataset_id=fixture_smoke_v1").json()["tickers"] == ["MSFT"]

    # 3. Canonical finalized bars
    cur.fetchone.return_value = [2]
    cur.fetchall.return_value = [
        (
            contract.contract_id,
            "fixture_smoke_v1",
            "MSFT",
            now - timedelta(minutes=1),
            now,
            now,
            now,
            420.1,
            420.8,
            420.0,
            420.6,
            1500,
            "synthetic_fixture",
        ),
        (
            contract.contract_id,
            "fixture_smoke_v1",
            "MSFT",
            now - timedelta(minutes=2),
            now - timedelta(minutes=1),
            now - timedelta(minutes=1),
            now - timedelta(minutes=1),
            419.8,
            420.2,
            419.7,
            420.1,
            1400,
            "synthetic_fixture",
        ),
    ]
    bars_resp = client.get("/api/v1/bars?dataset_id=fixture_smoke_v1&ticker=MSFT")
    assert bars_resp.status_code == 200
    assert len(bars_resp.json()["bars"]) == 2

    # 4. Outcomes with NO_SIGNAL abstention + realized UP signal verifies coverage math
    row_no_sig = (
        pid_no_sig,
        contract.contract_id,
        "fixture_smoke_v1",
        "run-demo-1",
        "MSFT",
        now - timedelta(minutes=5),
        now - timedelta(minutes=5),
        now,
        "b" * 64,
        "demo-bundle-1",
        "logistic_regression_v1.0.0",
        "logistic_regression",
        0,
        "NO_SIGNAL",
        0.51,
        0.49,
        "LOW",
        "standardized_linear_log_odds",
        "timely",
        "replay_holdout_demo",
        now,
        "realized",
        None,
        "NOT_UP",
        -0.0004,
        True,
        json.dumps([{"feature": "return_1m", "shap_value": -0.02, "direction": "negative"}]),
        now,
        1.1,
    )
    row_up = (
        pid_up,
        contract.contract_id,
        "fixture_smoke_v1",
        "run-demo-1",
        "MSFT",
        now - timedelta(minutes=6),
        now - timedelta(minutes=6),
        now - timedelta(minutes=1),
        "c" * 64,
        "demo-bundle-1",
        "logistic_regression_v1.0.0",
        "logistic_regression",
        1,
        "UP",
        0.74,
        0.26,
        "HIGH",
        "standardized_linear_log_odds",
        "timely",
        "replay_holdout_demo",
        now,
        "realized",
        None,
        "UP",
        0.0021,
        True,
        json.dumps([{"feature": "return_5m_past", "shap_value": 0.42, "direction": "positive"}]),
        now,
        1.2,
    )
    cur.fetchall.return_value = [row_no_sig, row_up]
    out_resp = client.get("/api/v1/outcomes?run_id=run-demo-1&ticker=MSFT")
    assert out_resp.status_code == 200
    out_json = out_resp.json()
    assert out_json["total_predictions"] == 2
    assert out_json["no_signal_count"] == 1
    # Only the signaled (non-NO_SIGNAL) prediction counts toward timely_signaled_realized_count
    assert out_json["timely_signaled_realized_count"] == 1
    assert out_json["timely_correct_count"] == 1
    assert out_json["timely_accuracy"] == pytest.approx(1.0)

    # 5. Evidence stays cleanly honest when real market data is absent
    ev_json = client.get("/api/v1/evidence").json()
    assert ev_json["real_market_validated"] is False
    assert ev_json["real_market_status"] == "unavailable"
    assert len(ev_json["synthetic_smoke_reports"]) == 1
