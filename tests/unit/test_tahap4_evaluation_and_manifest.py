from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from model.evaluate import (
    PRIMARY_SELECTION_METRIC,
    build_temporal_splits,
    compute_binary_classification_metrics,
    load_and_verify_bundle_manifest,
    prepare_evaluation_dataframe,
    publish_model_bundle,
    run_walk_forward_evaluation,
    write_evaluation_report,
)
from model.predictor import Predictor
from pipeline.contract import load_forecast_contract
from pipeline.features import (
    ContractMismatchError,
    compute_canonical_features_from_bars,
    dataframe_to_snapshots,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _make_multi_ticker_bars(n_minutes: int = 120, seed: int = 42) -> pd.DataFrame:
    """Create contiguous 1m OHLCV bars for 2 tickers inside a single NY session (deterministic synthetic)."""
    rng = np.random.default_rng(seed)
    start = pd.Timestamp("2025-07-14T13:30:00Z")
    rows = []
    for ticker, base_p in (("AAPL", 180.0), ("MSFT", 420.0)):
        price = base_p
        for idx in range(n_minutes):
            t0 = start + pd.Timedelta(minutes=idx)
            t1 = t0 + pd.Timedelta(minutes=1)
            step = float(rng.normal(0.02, 0.25))
            o = price
            c = max(10.0, o + step)
            h = max(o, c) + 0.15
            lo = min(o, c) - 0.15
            rows.append(
                {
                    "dataset_id": "synthetic_multi_v1",
                    "ticker": ticker,
                    "bar_start": t0,
                    "bar_end": t1,
                    "available_at": t1 + pd.Timedelta(seconds=2),
                    "open": o,
                    "high": h,
                    "low": lo,
                    "close": c,
                    "volume": int(rng.integers(1000, 8000)),
                }
            )
            price = c
    return pd.DataFrame(rows)


@pytest.mark.unit
def test_temporal_splits_align_across_tickers_and_enforce_horizon_purge() -> None:
    """Splits share exact timestamps across tickers and purge training samples within +horizon."""
    contract = load_forecast_contract()
    bars = _make_multi_ticker_bars(n_minutes=120)
    eval_df = prepare_evaluation_dataframe(bars, contract=contract, dataset_id="synthetic_multi_v1")

    plan = build_temporal_splits(
        eval_df,
        horizon_seconds=contract.horizon_seconds,
        n_folds=3,
        holdout_fraction=0.20,
    )

    assert plan.sufficient_for_three_folds is True
    assert len(plan.folds) == 3
    horizon_td = timedelta(seconds=contract.horizon_seconds + contract.grace_seconds)

    # Every fold must purge all train timestamps whose target horizon reaches validation start
    for fold in plan.folds:
        assert fold.train_end + horizon_td <= fold.val_start
        assert set(fold.train_timestamps).isdisjoint(set(fold.val_timestamps))
        assert set(fold.val_timestamps).isdisjoint(set(plan.holdout_timestamps))

    # Final pre-holdout pool ends with matured labels before untouched holdout starts
    assert plan.fit_information_end <= plan.holdout_start

    # Timestamps are shared identically across tickers
    holdout_rows = eval_df[eval_df["bar_end"].isin(plan.holdout_timestamps)]
    assert set(holdout_rows["ticker"].unique()) == {"AAPL", "MSFT"}


@pytest.mark.unit
def test_prepare_evaluation_dataframe_rejects_availability_leakage() -> None:
    """Reject pre-computed snapshot dataframe if available_at exceeds decision_at (bar_end + grace)."""
    contract = load_forecast_contract()
    bars = _make_multi_ticker_bars(n_minutes=35)
    feats = compute_canonical_features_from_bars(bars, contract=contract)
    feats["available_at"] = pd.to_datetime(feats["bar_end"], utc=True) + pd.Timedelta(seconds=90)
    feats["target_direction"] = 1

    with pytest.raises(ContractMismatchError, match="Availability leakage"):
        prepare_evaluation_dataframe(feats, contract=contract)


@pytest.mark.unit
def test_walk_forward_evaluation_reports_all_required_metrics_and_provenance(
    tmp_path: Path,
) -> None:
    """Evaluates prior/logreg/xgboost, random-walk comparator, CI, calibration, per-fold & per-ticker."""
    contract = load_forecast_contract()
    bars = _make_multi_ticker_bars(n_minutes=120, seed=7)

    report, _ = run_walk_forward_evaluation(
        bars,
        contract=contract,
        dataset_id="synthetic_multi_v1",
        dataset_provenance="synthetic_smoke_fixture",
        random_seed=42,
        n_folds=3,
    )

    assert report["selection_metric"] == PRIMARY_SELECTION_METRIC
    assert report["champion_model"] in {"prior_baseline", "logistic_regression", "xgboost"}
    assert report["is_real_market_data"] is False
    assert report["real_market_validated"] is False
    assert "SYNTHETIC" in str(report["warning_banner"])
    assert len(report["per_fold"]) == 3

    for model_name in ("prior_baseline", "logistic_regression", "xgboost"):
        assert model_name in report["validation_summary"]
        hold = report["holdout_summary"][model_name]
        assert 0.0 <= hold["brier_score"] <= 1.0
        assert hold["log_loss"] >= 0.0
        assert 0.0 <= hold["balanced_accuracy"] <= 1.0
        assert 0.0 <= hold["ece"] <= 1.0
        assert len(hold["calibration_bins"]) == 5
        assert "block_bootstrap_ci" in hold
        assert hold["block_bootstrap_ci"]["ci_low_95"] <= hold["block_bootstrap_ci"]["ci_high_95"]
        assert set(hold["per_ticker"].keys()) == {"AAPL", "MSFT"}

    assert "random_walk_price" in report["comparators"]

    json_path, md_path = write_evaluation_report(report, output_dir=tmp_path)
    assert json_path.exists()
    assert md_path.exists()
    assert "SYNTHETIC" in md_path.read_text(encoding="utf-8")


@pytest.mark.unit
def test_atomic_bundle_publish_predictor_load_and_tamper_rejection(tmp_path: Path) -> None:
    """Published bundles enforce manifest/hash/contract verification and fail loudly on tampering."""
    contract = load_forecast_contract()
    bars = _make_multi_ticker_bars(n_minutes=120, seed=11)

    report, bundle_objects = run_walk_forward_evaluation(
        bars,
        contract=contract,
        dataset_id="bundle_test_v1",
        dataset_provenance="synthetic_smoke_fixture",
    )

    bundle_dir = tmp_path / "artefacts"
    dest, manifest = publish_model_bundle(bundle_dir, report, bundle_objects, contract=contract)

    assert dest == bundle_dir
    assert (bundle_dir / "current" / "manifest.json").exists()
    assert (bundle_dir / "current" / "metadata.json").exists()
    assert manifest.contract_id == contract.contract_id
    assert manifest.contract_version == contract.version
    assert manifest.fit_information_end <= datetime.fromisoformat(
        report["cutoffs"]["holdout_start"]
    )

    # Load and score a real snapshot past warmup cleanly
    predictor = Predictor.load(str(bundle_dir), enforce_contract=True, contract=contract)
    features_df = compute_canonical_features_from_bars(bars, contract=contract)
    snapshots = dataframe_to_snapshots(
        features_df[features_df["warmup_complete"]], contract=contract
    )
    res = predictor.predict(snapshots[-1])

    assert 0.0 <= res["probability_up"] <= 1.0
    assert res["probability_up"] + res["probability_down"] == pytest.approx(1.0)
    assert res["signal_label"] in {"UP", "NOT_UP", "NO_SIGNAL"}
    assert res["champion_model"] == manifest.champion_model

    # 1. Tamper scaler.pkl -> loader must reject hash mismatch
    (bundle_dir / "current" / "scaler.pkl").write_bytes(b"corrupted-pickle-payload")
    with pytest.raises(ContractMismatchError, match="hash mismatch"):
        Predictor.load(str(bundle_dir), enforce_contract=True, contract=contract)

    # Restore cleanly then tamper contract_id inside manifest.json -> rejected
    publish_model_bundle(bundle_dir, report, bundle_objects, contract=contract)
    verified = load_and_verify_bundle_manifest(bundle_dir, contract=contract)
    bad_manifest = verified.model_copy(update={"contract_id": "wrong_contract_v999"})
    (bundle_dir / "current" / "manifest.json").write_text(
        bad_manifest.model_dump_json(indent=2), encoding="utf-8"
    )
    with pytest.raises(ContractMismatchError, match="Contract ID mismatch"):
        Predictor.load(str(bundle_dir), enforce_contract=True, contract=contract)

    # Restore cleanly then remove manifest.json -> rejected
    publish_model_bundle(bundle_dir, report, bundle_objects, contract=contract)
    (bundle_dir / "current" / "manifest.json").unlink()
    with pytest.raises(ContractMismatchError, match="manifest missing"):
        Predictor.load(str(bundle_dir), enforce_contract=True, contract=contract)


@pytest.mark.unit
def test_compute_metrics_abstain_band_and_single_class_resilience() -> None:
    """Abstention threshold reports coverage & covered error cleanly even on skewed subsets."""
    y = np.array([1, 0, 1, 0, 1, 1])
    p = np.array([0.85, 0.15, 0.52, 0.49, 0.90, 0.51])
    metrics = compute_binary_classification_metrics(y, p, abstain_delta=0.10, n_bins=5)
    assert metrics["coverage"] == pytest.approx(0.5)
    assert metrics["covered_balanced_accuracy"] == pytest.approx(1.0)
    assert metrics["covered_error_rate"] == pytest.approx(0.0)


@pytest.mark.unit
def test_predictor_serves_all_three_model_types(tmp_path: Path) -> None:
    """Predictor supports prior_baseline, logistic_regression, and xgboost champion bundles uniformly."""
    contract = load_forecast_contract()
    bars = _make_multi_ticker_bars(n_minutes=120, seed=19)
    report, bundle_objects = run_walk_forward_evaluation(
        bars,
        contract=contract,
        dataset_id="multi_champ_test",
        dataset_provenance="synthetic_smoke_fixture",
    )
    snap = dataframe_to_snapshots(
        compute_canonical_features_from_bars(bars, contract=contract).query("warmup_complete"),
        contract=contract,
    )[-1]

    for champion in ("prior_baseline", "logistic_regression", "xgboost"):
        sub_dir = tmp_path / champion
        patched_objs = {**bundle_objects, "champion_model": champion, "calibrator": None}
        patched_report = {
            **report,
            "champion_model": champion,
            "challenger_models": [
                m for m in ("prior_baseline", "logistic_regression", "xgboost") if m != champion
            ],
        }
        publish_model_bundle(sub_dir, patched_report, patched_objs, contract=contract)
        pred = Predictor.load(str(sub_dir), enforce_contract=True, contract=contract)
        out = pred.predict(snap)
        assert out["champion_model"] == champion
        assert out["probability_up"] + out["probability_down"] == pytest.approx(1.0)


def test_model_trainer_train_and_evaluate_publishes_bundle_and_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ModelTrainer.train_and_evaluate must produce atomic verifiable bundle + evaluation report."""
    from unittest.mock import MagicMock

    from model.train import ModelTrainer

    contract = load_forecast_contract()
    bars = _make_multi_ticker_bars(n_minutes=95)
    mock_db = MagicMock()
    trainer = ModelTrainer(mock_db, contract=contract)
    trainer.artefact_dir = tmp_path / "artefacts"
    monkeypatch.setattr(trainer, "load_data", lambda **kwargs: bars)
    mock_db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "trainer_eval_smoke",
        "synthetic_fixture",
    )

    report_dir = tmp_path / "reports"
    report = trainer.train_and_evaluate(
        run_id="trainer-eval-run",
        report_dir=report_dir,
    )
    assert report["dataset_id"] == "trainer_eval_smoke"
    assert report["is_real_market_data"] is False
    assert (report_dir / "trainer_eval_smoke_evaluation.json").exists()
    assert (report_dir / "trainer_eval_smoke_evaluation.md").exists()

    manifest = load_and_verify_bundle_manifest(trainer.artefact_dir, contract=contract)
    assert manifest.dataset_id == "trainer_eval_smoke"
    pred = Predictor.load(str(trainer.artefact_dir), enforce_contract=True, contract=contract)
    snap = dataframe_to_snapshots(
        compute_canonical_features_from_bars(bars, contract=contract).query("warmup_complete"),
        contract=contract,
    )[-1]
    out = pred.predict(snap)
    assert out["champion_model"] == manifest.champion_model


def test_cli_evaluate_on_smoke_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end CLI smoke check verifies manifest checksum, non-real flag, and output files."""
    import sys

    from model.evaluate import _cli

    out_dir = tmp_path / "reports"
    bundle_dir = tmp_path / "bundle"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "model.evaluate",
            "--csv",
            str(REPO_ROOT / "data/fixtures/smoke_1m_v1.csv"),
            "--manifest",
            str(REPO_ROOT / "data/manifests/fixture_smoke_v1.json"),
            "--output-dir",
            str(out_dir),
            "--bundle-dir",
            str(bundle_dir),
        ],
    )
    _cli()
    assert (out_dir / "fixture_smoke_v1_evaluation.json").exists()
    assert (out_dir / "fixture_smoke_v1_evaluation.md").exists()
    import json

    report_payload = json.loads((out_dir / "fixture_smoke_v1_evaluation.json").read_text())
    assert report_payload["is_real_market_data"] is False
    assert report_payload["real_market_validated"] is False
    manifest = load_and_verify_bundle_manifest(bundle_dir)
    assert manifest.is_real_market_data is False
    assert manifest.dataset_provenance == "synthetic_fixture"
