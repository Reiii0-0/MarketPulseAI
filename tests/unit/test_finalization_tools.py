from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from model.evaluate import build_temporal_splits, market_validation_gate
from scripts.benchmark_read_api import run_benchmark, validate_response
from scripts.demo import free_port, has_ready_predictions
from scripts.import_kibot import import_samples


def test_demo_port_selection_skips_busy_and_reserved_ports() -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        chosen = free_port(port, {port + 1})
        assert chosen not in {port, port + 1}


def test_kibot_conversion_preserves_ohlcv_and_handles_dst(tmp_path: Path) -> None:
    src = tmp_path / "input.txt"
    src.write_text("10/30/2026,09:30,100,102,99,101,15\n11/02/2026,09:30,101,103,100,102,16\n")
    path = import_samples({"IBM": src}, tmp_path / "normalized", "test_kibot")
    meta = json.loads(path.read_text())
    frame = pd.read_csv(meta["file_path"])
    assert frame.bar_start.tolist() == ["2026-10-30T13:30:00+00:00", "2026-11-02T14:30:00+00:00"]
    assert frame.bar_end.iloc[0] == "2026-10-30T13:31:00+00:00"
    assert frame.available_at.iloc[0] == "2026-10-30T13:31:05+00:00"
    assert frame.volume.tolist() == [15, 16]
    assert meta["availability_assumed"] is True
    assert meta["session_counts"] == {"IBM": 2}
    with pytest.raises(FileExistsError):
        import_samples({"IBM": src}, tmp_path / "normalized", "test_kibot")


@pytest.mark.parametrize(
    "row",
    [
        "10/30/2026,16:00,100,102,99,101,15\n",
        "invalid,row\n",
        "10/30/2026,09:30,100,102,99,101,15\n" * 2,
    ],
)
def test_kibot_rejects_invalid_session_shape_and_duplicates(tmp_path: Path, row: str) -> None:
    src = tmp_path / "input.txt"
    src.write_text(row)
    with pytest.raises(ValueError):
        import_samples({"IBM": src}, tmp_path / "normalized", "bad")
    assert not (tmp_path / "normalized" / "bad.json").exists()


def test_real_validation_requires_session_coverage_in_addition_to_folds() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["IBM"] * 60,
            "bar_end": pd.bdate_range("2026-01-01 16:00", periods=60, tz="UTC"),
        }
    )
    assert market_validation_gate(frame, is_real_market=True, fold_count=3)["passed"]
    assert not market_validation_gate(frame.iloc[:59], is_real_market=True, fold_count=3)["passed"]
    assert not market_validation_gate(frame, is_real_market=False, fold_count=3)["passed"]
    assert not market_validation_gate(frame, is_real_market=True, fold_count=2)["passed"]
    short_ticker = pd.DataFrame({"ticker": ["OIH"], "bar_end": [frame.bar_end.iloc[0]]})
    assert not market_validation_gate(
        pd.concat([frame, short_ticker]), is_real_market=True, fold_count=3
    )["passed"]


def test_benchmark_rejects_empty_or_wrong_run_payloads() -> None:
    with pytest.raises(ValueError):
        validate_response("/api/v1/evidence", b'{"active_bundle_manifest": null}', "run-a", "MSFT")
    ep = "/api/v1/predictions?run_id=run-a&ticker=MSFT&limit=50"
    payload: dict[str, Any]
    for payload in ({"predictions": []}, {"predictions": [{"ticker": "MSFT", "run_id": "run-b"}]}):
        with pytest.raises(ValueError):
            validate_response(ep, json.dumps(payload).encode(), "run-a", "MSFT")
    validate_response(
        ep, b'{"predictions": [{"ticker": "MSFT", "run_id": "run-a"}]}', "run-a", "MSFT"
    )


@pytest.mark.parametrize(
    "duration,rps,concurrency", [(0, 10, 1), (1, 0, 1), (1, 10, 0), (float("nan"), 1, 1)]
)
def test_benchmark_invalid_workload_fails_before_network(
    duration: float, rps: float, concurrency: int
) -> None:
    with pytest.raises(ValueError):
        run_benchmark("http://invalid", "run", "MSFT", duration, rps, concurrency)


def test_large_walk_forward_reserves_initial_history_before_validation() -> None:
    frame = pd.DataFrame(
        {"bar_end": pd.date_range("2026-08-18 13:50", periods=1200, freq="min", tz="UTC")}
    )
    splits = build_temporal_splits(frame)
    assert len(splits.folds) == 3
    assert len(splits.folds[0].train_timestamps) >= len(splits.pre_holdout_timestamps) * 0.45
    assert all(
        fold.train_end + pd.Timedelta(seconds=315) <= fold.val_start for fold in splits.folds
    )
    assert splits.fit_information_end <= splits.holdout_start


def test_demo_readiness_reads_nested_api_run_summary() -> None:
    assert has_ready_predictions({"ready": True}, {"run": {"predictions_written": 10}})
    assert not has_ready_predictions({"ready": False}, {"run": {"predictions_written": 10}})
    assert not has_ready_predictions({"ready": True}, {"run": {"predictions_written": 0}})
    assert not has_ready_predictions({"ready": True}, {})
