from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from api.dependencies import get_db
from api.routers import dashboard, health, model, pipeline_read
from pipeline.contract import load_forecast_contract

playwright_sync = pytest.importorskip("playwright.sync_api")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _build_demo_app(tmp_path: Path) -> FastAPI:
    contract = load_forecast_contract()
    now = datetime(2025, 1, 15, 15, 12, tzinfo=UTC)
    fit_end = datetime(2025, 1, 15, 15, 7, tzinfo=UTC)

    bars_rows: list[tuple[Any, ...]] = []
    price = 420.0
    for idx in range(15):
        t_end = now - timedelta(minutes=idx)
        t_start = t_end - timedelta(minutes=1)
        c = round(price + ((14 - idx) * 0.12) * (1 if idx % 2 == 0 else -0.4), 4)
        o = round(c - 0.1, 4)
        bars_rows.append(
            (
                contract.contract_id,
                "fixture_smoke_v1",
                "MSFT",
                t_start,
                t_end,
                t_end,
                t_end,
                o,
                round(max(o, c) + 0.15, 4),
                round(min(o, c) - 0.12, 4),
                c,
                1500 + idx * 40,
                "synthetic_fixture",
            )
        )

    pid_latest = uuid.uuid4()
    pid_prev = uuid.uuid4()
    pred_rows = [
        (
            pid_latest,
            contract.contract_id,
            "fixture_smoke_v1",
            "replay-smoke-1",
            "MSFT",
            now - timedelta(minutes=1),
            now - timedelta(minutes=1),
            now + timedelta(minutes=4),
            "a" * 64,
            "mp_1m_5m_price_v1_fixture_smoke_v1_demo",
            "logistic_regression_v1.0.0",
            "logistic_regression",
            0,
            "NO_SIGNAL",
            0.52,
            0.48,
            "LOW",
            "standardized_linear_log_odds",
            "timely",
            "replay_holdout_demo",
            now,
            "pending",
            "horizon_not_matured",
            None,
            None,
            None,
            json.dumps(
                [
                    {"feature": "return_1m", "shap_value": -0.041, "direction": "negative"},
                    {"feature": "sma_spread_5_20", "shap_value": 0.029, "direction": "positive"},
                    {"feature": "volatility_20", "shap_value": -0.015, "direction": "negative"},
                ]
            ),
            now,
            1.4,
        ),
        (
            pid_prev,
            contract.contract_id,
            "fixture_smoke_v1",
            "replay-smoke-1",
            "MSFT",
            now - timedelta(minutes=6),
            now - timedelta(minutes=6),
            now - timedelta(minutes=1),
            "b" * 64,
            "mp_1m_5m_price_v1_fixture_smoke_v1_demo",
            "logistic_regression_v1.0.0",
            "logistic_regression",
            1,
            "UP",
            0.73,
            0.27,
            "HIGH",
            "standardized_linear_log_odds",
            "timely",
            "replay_holdout_demo",
            now,
            "realized",
            None,
            "UP",
            0.0024,
            True,
            json.dumps(
                [
                    {"feature": "return_5m_past", "shap_value": 0.34, "direction": "positive"},
                    {"feature": "volume_ratio_20", "shap_value": 0.18, "direction": "positive"},
                    {"feature": "bar_range_pct", "shap_value": -0.09, "direction": "negative"},
                ]
            ),
            now - timedelta(minutes=6),
            1.8,
        ),
    ]

    snapshot_feats = {
        name: round(0.01 * (i + 1), 6) for i, name in enumerate(contract.feature_policy.features)
    }

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(exist_ok=True)
    eval_src = Path("reports/evaluation/fixture_smoke_v1_evaluation.json")
    if eval_src.exists():
        (reports_dir / "fixture_smoke_v1_evaluation.json").write_text(
            eval_src.read_text(encoding="utf-8"), encoding="utf-8"
        )
    manifest_path = tmp_path / "manifest.json"
    manifest_src = Path("model/artefacts/manifest.json")
    if manifest_src.exists():
        manifest_path.write_text(manifest_src.read_text(encoding="utf-8"), encoding="utf-8")

    pipeline_read.read_config.evaluation_reports_dir = str(reports_dir)
    pipeline_read.read_config.bundle_manifest_path = str(manifest_path)

    class _FakeCursor:
        def __init__(self) -> None:
            self._one: Any = None
            self._many: list[Any] = []

        def __enter__(self) -> _FakeCursor:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def execute(self, sql: str, params: Any = None) -> None:
            q = " ".join(sql.split()).lower()
            if q == "select 1":
                self._one = (1,)
                self._many = [(1,)]
            elif "from raw.ingestion_runs" in q and "where ir.run_id" not in q:
                self._many = [
                    (
                        "replay-smoke-1",
                        "fixture_smoke_v1",
                        "replay",
                        "synthetic_fixture",
                        "completed",
                        now,
                        now,
                        "idle",
                        now,
                        now,
                        2,
                        0,
                        1.6,
                    )
                ]
            elif "from raw.ingestion_runs" in q and "where ir.run_id" in q:
                self._one = (
                    "replay-smoke-1",
                    "fixture_smoke_v1",
                    "replay",
                    "synthetic_fixture",
                    "completed",
                    now,
                    now,
                    "idle",
                    now,
                    now,
                    2,
                    0,
                    1.6,
                )
            elif "from raw.run_delivery_progress" in q:
                self._many = [("market_bars", 0, 89, now, 89, 0, "completed")]
            elif "select distinct ticker from marts.final_bars" in q:
                self._many = [("MSFT",), ("AAPL",)]
            elif "select count(*) from marts.final_bars" in q:
                self._one = (len(bars_rows),)
            elif "from marts.final_bars" in q:
                self._many = bars_rows
            elif "select count(*) from marts.realized_outcomes_v" in q:
                self._one = (len(pred_rows),)
            elif "left join marts.feature_snapshots" in q:
                pid_arg = str((params or {}).get("pid", ""))
                matched = next((r for r in pred_rows if str(r[0]) == pid_arg), pred_rows[0])
                self._one = (
                    *matched,
                    contract.version,
                    fit_end,
                    json.dumps(snapshot_feats),
                )
            elif "from marts.realized_outcomes_v" in q:
                self._many = pred_rows
            else:
                self._one = None
                self._many = []

        def fetchone(self) -> Any:
            return self._one

        def fetchall(self) -> list[Any]:
            return self._many

    db_mock = MagicMock()
    conn_ctx = db_mock.get_connection.return_value.__enter__.return_value
    conn_ctx.cursor.return_value = _FakeCursor()

    app = FastAPI()
    app.mount("/static", StaticFiles(directory="api/static"), name="static")
    app.include_router(dashboard.router)
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(model.router, prefix="/api/v1")
    app.include_router(pipeline_read.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = lambda: db_mock
    app.state.db_manager = db_mock
    app.state.contract = contract
    app.state.model_loaded = True
    app.state.predictor = MagicMock(
        metadata={
            "contract_id": contract.contract_id,
            "contract_version": contract.version,
            "feature_mode": contract.feature_policy.mode,
            "feature_names": list(contract.feature_policy.features),
            "bundle_id": "mp_1m_5m_price_v1_fixture_smoke_v1_demo",
            "fit_information_end": fit_end.isoformat(),
        }
    )
    return app


@pytest.fixture
def live_ui_url(tmp_path: Path) -> Iterator[str]:
    prev_reports = pipeline_read.read_config.evaluation_reports_dir
    prev_manifest = pipeline_read.read_config.bundle_manifest_path
    app = _build_demo_app(tmp_path)
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 5.0
    while not server.started and time.time() < deadline:
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}/"
    finally:
        server.should_exit = True
        thread.join(timeout=3.0)
        pipeline_read.read_config.evaluation_reports_dir = prev_reports
        pipeline_read.read_config.bundle_manifest_path = prev_manifest


def _browser_targets() -> list[tuple[str, str | None]]:
    targets: list[tuple[str, str | None]] = [("chromium", None)]
    brave_bin = "/opt/brave-bin/brave"
    if os.path.exists(brave_bin) and os.access(brave_bin, os.X_OK):
        targets.append(("brave", brave_bin))
    return targets


@pytest.mark.e2e
@pytest.mark.parametrize(("browser_name", "executable_path"), _browser_targets())
def test_dashboard_renders_cleanly_in_playwright_and_brave(
    live_ui_url: str,
    browser_name: str,
    executable_path: str | None,
) -> None:
    """Verify live rendered UI, SVG chart, lineage, Model Evidence, edge states, and mobile layout."""
    with playwright_sync.sync_playwright() as p:
        launch_kwargs: dict[str, Any] = {"headless": True}
        if executable_path is not None:
            launch_kwargs["executable_path"] = executable_path
        browser = p.chromium.launch(**launch_kwargs)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()

        console_errors: list[str] = []
        page_errors: list[str] = []
        page.on(
            "console",
            lambda msg: console_errors.append(msg.text) if msg.type == "error" else None,
        )
        page.on("pageerror", lambda err: page_errors.append(str(err)))

        page.goto(live_ui_url, wait_until="domcontentloaded")
        page.locator("#datasetModeBadge").wait_for(state="visible")
        # Wait for initial async fetch cycle to populate predictions & chart
        page.locator("#predictionsTableBody tr.pred-row").first.wait_for(state="visible")
        page.locator("#snapshotFeatureTableBody tr").nth(13).wait_for(state="visible")

        # 1. Sticky provenance & clock bar
        assert "REPLAY" in page.locator("#datasetModeBadge").inner_text()
        assert "Unavailable" in page.locator("#realMarketGateBadge").inner_text()
        assert "Ready" in page.locator("#readinessBadge").inner_text()
        assert page.locator("#syntheticBanner").is_visible()

        # 2. Chart SVG & accessible summary + data table toggle
        assert page.locator("#priceChartSvg polyline").count() == 1
        assert page.locator("#priceChartSvg circle").count() >= 1
        assert "MSFT" in page.locator("#chartSummaryText").inner_text()
        page.locator("#toggleChartTableBtn").click()
        assert page.locator("#chartDataTableWrap").is_visible()
        assert page.locator("#chartDataTableBody tr").count() == 15
        page.locator("#toggleChartTableBtn").click()
        assert not page.locator("#chartDataTableWrap").is_visible()

        # Selection values must survive stopPlayback re-rendering the controls.
        with page.expect_response(
            lambda response: "/api/v1/bars?" in response.url and "ticker=AAPL" in response.url
        ):
            page.locator("#tickerSelect").select_option("AAPL")
        assert page.locator("#tickerSelect").input_value() == "AAPL"
        with page.expect_response(
            lambda response: "/api/v1/bars?" in response.url and "ticker=MSFT" in response.url
        ):
            page.locator("#tickerSelect").select_option("MSFT")
        assert page.locator("#tickerSelect").input_value() == "MSFT"
        slider = page.locator("#timelineSlider")
        slider.focus()
        slider.press("Home")
        slider.press("ArrowRight")
        assert slider.input_value() == "1"
        slider.press("End")
        assert slider.input_value() == "14"

        # 3. Prediction detail & frozen 14 snapshot features
        assert "NO_SIGNAL" in page.locator("#kpiSignal").inner_text()
        page.locator("#predictionsTableBody tr.pred-row").nth(1).click()
        page.locator("#snapshotFeatureTableBody tr").nth(13).wait_for(state="visible")
        assert "realized" in page.locator("#detOutcomeStatus").inner_text().lower()
        assert "standardized_linear_log_odds" in page.locator("#detAttributionSpace").inner_text()
        assert page.locator("#detFactorList li").count() == 3
        assert page.locator("#snapshotFeatureTableBody tr").count() == 14

        # 4. Model Evidence explicitly separates smoke fixture from real-market unavailable
        assert "UNAVAILABLE" in page.locator("#realMarketGateHeadline").inner_text()
        assert page.locator("#evidenceModelTableBody tr").count() >= 3

        # 5. Inspect all edge-state UI modes cleanly
        for mode in ("loading", "empty", "model_unavailable", "stale", "error", "live"):
            page.locator("#statePreviewSelect").select_option(mode)
            if mode == "loading":
                assert page.locator("#overviewSkeleton").is_visible()
            elif mode == "empty":
                assert page.locator("#overviewEmpty").is_visible()
            elif mode in ("model_unavailable", "stale", "error"):
                assert page.locator("#globalAlertBanner").is_visible()
            elif mode == "live":
                assert not page.locator("#globalAlertBanner").is_visible()
                assert not page.locator("#overviewSkeleton").is_visible()
                assert not page.locator("#overviewEmpty").is_visible()

        # 6. Check desktop horizontal overflow == 0 (and capture optional desktop screenshot)
        page.evaluate("window.scrollTo(0, 0)")
        screenshot_dir = os.getenv("MARKETPULSE_E2E_SCREENSHOT_DIR")
        if screenshot_dir:
            out_dir = Path(screenshot_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            page.screenshot(
                path=str(out_dir / f"dashboard_{browser_name}_desktop.png"), full_page=True
            )
        desktop_overflow = page.evaluate("document.documentElement.scrollWidth > window.innerWidth")
        assert desktop_overflow is False, f"{browser_name} desktop horizontal overflow"

        # 7. Check mobile (390x844) layout has zero horizontal overflow
        page.set_viewport_size({"width": 390, "height": 844})
        page.evaluate("window.scrollTo(0, 0)")
        if screenshot_dir:
            page.screenshot(
                path=str(Path(screenshot_dir) / f"dashboard_{browser_name}_mobile.png"),
                full_page=True,
            )
        mobile_overflow = page.evaluate("document.documentElement.scrollWidth > window.innerWidth")
        assert mobile_overflow is False, f"{browser_name} mobile horizontal overflow"

        assert console_errors == [], f"{browser_name} console errors: {console_errors}"
        assert page_errors == [], f"{browser_name} page errors: {page_errors}"

        context.close()
        browser.close()
