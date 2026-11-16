from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from model.predictor import Predictor, validate_bundle_contract
from pipeline.contract import load_forecast_contract
from pipeline.features import (
    BarFinalizationService,
    ContractMismatchError,
    compute_canonical_features_from_bars,
    compute_feature_hash,
    verify_snapshot_integrity,
)
from pipeline.replay import PartitionDeliveryProgress

REPO_ROOT = Path(__file__).resolve().parents[2]


def _make_bar_event(
    *,
    ticker: str,
    bar_start: datetime,
    open_p: float,
    high_p: float,
    low_p: float,
    close_p: float,
    volume: int,
    available_offset_s: int = 1,
    source_event_id: str | None = None,
) -> dict[str, Any]:
    bar_end = bar_start + timedelta(minutes=1)
    return {
        "source_event_id": source_event_id or f"{ticker}:{bar_end.isoformat()}",
        "ticker": ticker,
        "event_at": bar_start,
        "event_timestamp": bar_start,
        "available_at": bar_end + timedelta(seconds=available_offset_s),
        "open": open_p,
        "high": high_p,
        "low": low_p,
        "close": close_p,
        "volume": volume,
    }


@pytest.mark.unit
def test_fixture_canonical_features_match_exact_reference() -> None:
    """Smoke fixture rows yield exact reference values matching contract feature definitions."""
    contract = load_forecast_contract()
    fixture_df = pd.read_csv(REPO_ROOT / "data/fixtures/smoke_1m_v1.csv")
    bars_df = fixture_df[fixture_df["ticker"] == "AAPL"].copy()

    features_df = compute_canonical_features_from_bars(bars_df, contract=contract)
    assert len(features_df) == len(bars_df)

    # First 19 contiguous bars cannot be warm-up complete; 20th completes warm-up; gaps reset it
    assert (features_df.iloc[:19]["warmup_complete"] == False).all()  # noqa: E712
    assert bool(features_df.iloc[19]["warmup_complete"]) is True
    assert (
        features_df["warmup_complete"]
        == (features_df["contiguous_bar_count"] >= contract.warmup_bars)
    ).all()
    assert features_df["segment_id"].nunique() >= 2

    # Check exact reference formulas at index 20 (21st bar, well after warm-up)
    row = features_df.iloc[20]
    closes = bars_df["close"].astype(float).reset_index(drop=True)
    vols = bars_df["volume"].astype(float).reset_index(drop=True)
    rets_1m = closes.pct_change()

    exp_sma5 = closes.iloc[16:21].mean()
    exp_sma20 = closes.iloc[1:21].mean()
    exp_vol20 = rets_1m.iloc[1:21].std(ddof=1)
    exp_vol_ratio = vols.iloc[20] / vols.iloc[1:21].mean()

    assert row["return_1m"] == pytest.approx((closes.iloc[20] - closes.iloc[19]) / closes.iloc[19])
    assert row["return_5m_past"] == pytest.approx(
        (closes.iloc[20] - closes.iloc[15]) / closes.iloc[15]
    )
    assert row["return_15m_past"] == pytest.approx(
        (closes.iloc[20] - closes.iloc[5]) / closes.iloc[5]
    )
    assert row["sma_5"] == pytest.approx(exp_sma5)
    assert row["sma_20"] == pytest.approx(exp_sma20)
    assert row["sma_spread_5_20"] == pytest.approx((exp_sma5 - exp_sma20) / exp_sma20)
    assert row["volatility_20"] == pytest.approx(exp_vol20)
    assert row["volume_ratio_20"] == pytest.approx(exp_vol_ratio)
    assert row["bar_range_pct"] == pytest.approx(
        (bars_df["high"].iloc[20] - bars_df["low"].iloc[20]) / closes.iloc[20]
    )


@pytest.mark.unit
def test_gap_and_overnight_session_reset_warmup_and_prevent_leakage() -> None:
    """Missing 1-min gap and overnight session boundary reset segment_id & contiguous warm-up."""
    contract = load_forecast_contract()
    day1_start = datetime(2025, 7, 14, 13, 30, tzinfo=UTC)  # 09:30 EDT

    # 21 contiguous bars, then skip 1 minute (bar 21 omitted), then 5 more bars, then next day
    bars = []
    for idx in range(21):
        ts = day1_start + timedelta(minutes=idx)
        bars.append(
            {
                "ticker": "AAPL",
                "bar_start": ts,
                "bar_end": ts + timedelta(minutes=1),
                "open": 100.0 + idx,
                "high": 101.0 + idx,
                "low": 99.5 + idx,
                "close": 100.5 + idx,
                "volume": 1000,
            }
        )
    # Gap of 1 missing minute at idx=21; resume at idx=22..26
    for idx in range(22, 27):
        ts = day1_start + timedelta(minutes=idx)
        bars.append(
            {
                "ticker": "AAPL",
                "bar_start": ts,
                "bar_end": ts + timedelta(minutes=1),
                "open": 200.0,
                "high": 201.0,
                "low": 199.0,
                "close": 200.0,
                "volume": 1000,
            }
        )
    # Overnight next day 09:30 EDT (Jul 15 13:30 UTC) even immediately following 15:59 on Jul 14
    day2_start = datetime(2025, 7, 15, 13, 30, tzinfo=UTC)
    bars.append(
        {
            "ticker": "AAPL",
            "bar_start": day2_start,
            "bar_end": day2_start + timedelta(minutes=1),
            "open": 300.0,
            "high": 301.0,
            "low": 299.0,
            "close": 300.0,
            "volume": 1000,
        }
    )

    features_df = compute_canonical_features_from_bars(pd.DataFrame(bars), contract=contract)

    # Bar idx=20 (row 20) completes warm-up
    assert bool(features_df.iloc[20]["warmup_complete"]) is True
    assert features_df.iloc[20]["contiguous_bar_count"] == 21

    # Post-gap bar (row 21) resets contiguous count to 1 and has NaN cross-gap features
    assert features_df.iloc[21]["contiguous_bar_count"] == 1
    assert bool(features_df.iloc[21]["warmup_complete"]) is False
    assert pd.isna(features_df.iloc[21]["return_1m"])
    assert pd.isna(features_df.iloc[21]["sma_20"])

    # Overnight bar (last row) also resets contiguous count to 1
    assert features_df.iloc[-1]["contiguous_bar_count"] == 1
    assert bool(features_df.iloc[-1]["warmup_complete"]) is False
    assert pd.isna(features_df.iloc[-1]["return_1m"])


@pytest.mark.unit
def test_bar_finalization_grace_barrier_and_late_arrival_immutability() -> None:
    """Bars respect grace + delivery barrier; late arrivals go to late_bar_corrections without mutation."""
    contract = load_forecast_contract()
    service = BarFinalizationService(contract=contract)

    start = datetime(2025, 7, 14, 13, 30, tzinfo=UTC)
    events = [
        _make_bar_event(
            ticker="AAPL",
            bar_start=start + timedelta(minutes=i),
            open_p=100.0 + i * 0.1,
            high_p=100.5 + i * 0.1,
            low_p=99.8 + i * 0.1,
            close_p=100.2 + i * 0.1,
            volume=1000,
            available_offset_s=2,
        )
        for i in range(26)
    ]

    first_bar_end = start + timedelta(minutes=1)
    # 1. Before grace (bar_end + 10s < bar_end + 15s), nothing may finalize
    bars_early, _ = service.finalize_events(
        events[:1],
        dataset_id="ds1",
        run_id="run1",
        logical_as_of=first_bar_end + timedelta(seconds=10),
    )
    assert bars_early == []

    # 2. Past grace cutoff in logical clock, but active Kafka partition delivery still behind grace_cutoff
    stalled_progress = [
        PartitionDeliveryProgress(
            topic="raw.market.bars",
            partition=0,
            last_kafka_offset=5,
            last_available_at=first_bar_end + timedelta(seconds=5),
            status="active",
        )
    ]
    bars_blocked, _ = service.finalize_events(
        events[:1],
        dataset_id="ds1",
        run_id="run1",
        logical_as_of=first_bar_end + timedelta(seconds=30),
        delivery_progress=stalled_progress,
    )
    assert bars_blocked == []

    # 3. Once logical clock AND delivery barrier pass bar_end + grace, bars finalize & freeze snapshots
    last_bar_end = start + timedelta(minutes=26)
    ready_progress = [
        PartitionDeliveryProgress(
            topic="raw.market.bars",
            partition=0,
            last_kafka_offset=25,
            last_available_at=last_bar_end + timedelta(seconds=16),
            status="completed",
        )
    ]
    bars_done, corr_initial = service.finalize_events(
        events,
        dataset_id="ds1",
        run_id="run1",
        logical_as_of=last_bar_end + timedelta(seconds=20),
        delivery_progress=ready_progress,
    )
    assert len(bars_done) == 26
    assert corr_initial == []

    snapshots_first = service.freeze_feature_snapshots(dataset_id="ds1", run_id="run1")
    assert len(snapshots_first) == 26
    target_key = ("run1", contract.contract_id, "ds1", "AAPL", start + timedelta(minutes=20))
    orig_bar_close = service.final_bars[target_key].close
    orig_snap_hash = service.snapshots[target_key].feature_hash

    # 4. Late tick arrives targeting finalized bar #19 (bar_end = start + 20m) AND a future bar appends
    late_event = _make_bar_event(
        ticker="AAPL",
        bar_start=start + timedelta(minutes=19),
        open_p=999.0,
        high_p=999.0,
        low_p=999.0,
        close_p=999.0,
        volume=999999,
        available_offset_s=120,
        source_event_id="late-evt-1",
    )
    future_event = _make_bar_event(
        ticker="AAPL",
        bar_start=start + timedelta(minutes=26),
        open_p=105.0,
        high_p=106.0,
        low_p=104.0,
        close_p=105.5,
        volume=2500,
    )
    new_bars, late_corrs = service.finalize_events(
        [late_event, future_event],
        dataset_id="ds1",
        run_id="run1",
        logical_as_of=last_bar_end + timedelta(minutes=5),
        run_status="completed",
    )
    service.freeze_feature_snapshots(dataset_id="ds1", run_id="run1")

    assert len(new_bars) == 1
    assert len(late_corrs) == 1
    assert late_corrs[0].source_event_id == "late-evt-1"
    # Final bar & saved snapshot MUST remain completely unchanged!
    assert service.final_bars[target_key].close == orig_bar_close
    assert service.snapshots[target_key].feature_hash == orig_snap_hash


@pytest.mark.unit
def test_target_tie_is_not_up_and_session_boundary_unavailable() -> None:
    """Flat +5m return yields NOT_UP (0); strictly positive yields UP (1); session boundary unavailable."""
    contract = load_forecast_contract()
    service = BarFinalizationService(contract=contract)

    start = datetime(2025, 7, 14, 19, 50, tzinfo=UTC)  # 15:50 EDT
    # i=0 & i=5 -> 100.0 (flat tie -> NOT_UP = 0); i=1 -> 101.0, i=6 -> 102.0 (UP = 1)
    events = [
        _make_bar_event(
            ticker="MSFT",
            bar_start=start + timedelta(minutes=i),
            open_p=100.0,
            high_p=102.5,
            low_p=99.5,
            close_p=100.0 if i in (0, 5) else (102.0 if i == 6 else 101.0),
            volume=500,
        )
        for i in range(10)
    ]
    service.finalize_events(
        events,
        dataset_id="ds_tie",
        run_id="r1",
        logical_as_of=start + timedelta(minutes=20),
        run_status="completed",
    )

    tie_res = service.evaluate_target(
        dataset_id="ds_tie", run_id="r1", ticker="MSFT", bar_end=start + timedelta(minutes=1)
    )
    assert tie_res["target_status"] == "realized"
    assert tie_res["return_5m"] == 0.0
    assert tie_res["target_direction"] == 0
    assert tie_res["target_label"] == "NOT_UP"

    up_res = service.evaluate_target(
        dataset_id="ds_tie", run_id="r1", ticker="MSFT", bar_end=start + timedelta(minutes=2)
    )
    assert up_res["target_status"] == "realized"
    assert up_res["return_5m"] > 0.0
    assert up_res["target_direction"] == 1
    assert up_res["target_label"] == "UP"

    boundary_res = service.evaluate_target(
        dataset_id="ds_tie", run_id="r1", ticker="MSFT", bar_end=start + timedelta(minutes=9)
    )
    assert boundary_res["target_status"] == "unavailable"
    assert boundary_res["reason"] == "session_end"
    assert boundary_res["target_direction"] is None


@pytest.mark.unit
def test_predictor_and_snapshots_reject_mismatched_contract_or_warmup(
    mock_xgb_model: Any, mock_shap_explainer: Any
) -> None:
    """Model bundle, predictor, and snapshots explicitly reject contract mismatch or warm-up rows."""
    contract = load_forecast_contract()
    scaler = MagicMock()
    scaler.transform.side_effect = lambda x: x.values

    # 1. Missing contract stamp with require_contract_stamp=True fails
    with pytest.raises(ContractMismatchError):
        validate_bundle_contract(
            {"model_version": "xgb_v1.0.0", "feature_names": contract.feature_policy.features},
            contract=contract,
            require_contract_stamp=True,
        )

    # 2. Wrong contract_id fails in Predictor.__init__
    bad_meta = {
        "model_version": "xgb_v1.0.0",
        "contract_id": "wrong_contract_v99",
        "contract_version": contract.version,
        "feature_mode": contract.feature_policy.mode,
        "feature_names": contract.feature_policy.features,
    }
    with pytest.raises(ContractMismatchError):
        Predictor(
            model=mock_xgb_model,
            scaler=scaler,
            metadata=bad_meta,
            explainer=mock_shap_explainer,
            contract=contract,
            enforce_contract=True,
        )

    # 3. Valid contract predictor rejects warm-up incomplete snapshot AND tampered feature_hash
    good_meta = {
        "model_version": "xgb_v1.0.0",
        "contract_id": contract.contract_id,
        "contract_version": contract.version,
        "feature_mode": contract.feature_policy.mode,
        "feature_names": contract.feature_policy.features,
    }
    predictor = Predictor(
        model=mock_xgb_model,
        scaler=scaler,
        metadata=good_meta,
        explainer=mock_shap_explainer,
        contract=contract,
        enforce_contract=True,
    )

    valid_feats = {
        name: float(idx + 1) for idx, name in enumerate(contract.feature_policy.features)
    }
    good_hash = compute_feature_hash(contract, valid_feats)
    mock_xgb_model.get_booster().predict.return_value = np.array([0.78])

    # Warm-up incomplete rejected
    with pytest.raises(ContractMismatchError, match="warm-up"):
        predictor.predict(
            {
                "ticker": "AAPL",
                "bar_end": datetime(2025, 7, 14, 13, 35, tzinfo=UTC),
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "feature_mode": contract.feature_policy.mode,
                "feature_hash": good_hash,
                "warmup_complete": False,
                **valid_feats,
            }
        )

    # Tampered feature_hash rejected
    with pytest.raises(ContractMismatchError, match="hash mismatch"):
        predictor.predict(
            {
                "ticker": "AAPL",
                "bar_end": datetime(2025, 7, 14, 13, 55, tzinfo=UTC),
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "feature_mode": contract.feature_policy.mode,
                "feature_hash": "0" * 64,
                "warmup_complete": True,
                **valid_feats,
            }
        )

    # Valid snapshot succeeds
    res = predictor.predict(
        {
            "ticker": "AAPL",
            "bar_end": datetime(2025, 7, 14, 13, 55, tzinfo=UTC),
            "contract_id": contract.contract_id,
            "contract_version": contract.version,
            "feature_mode": contract.feature_policy.mode,
            "feature_hash": good_hash,
            "warmup_complete": True,
            **valid_feats,
        }
    )
    assert res["predicted_direction"] == 1
    assert res["feature_hash"] == good_hash


@pytest.mark.unit
def test_snapshot_integrity_verification() -> None:
    """verify_snapshot_integrity rejects altered feature values."""
    contract = load_forecast_contract()
    service = BarFinalizationService(contract=contract)
    start = datetime(2025, 7, 14, 13, 30, tzinfo=UTC)
    events = [
        _make_bar_event(
            ticker="NVDA",
            bar_start=start + timedelta(minutes=i),
            open_p=120.0 + i * 0.2,
            high_p=120.5 + i * 0.2,
            low_p=119.5 + i * 0.2,
            close_p=120.1 + i * 0.2,
            volume=2000 + i * 50,
        )
        for i in range(21)
    ]
    service.finalize_events(
        events,
        dataset_id="ds_snap",
        run_id="r_snap",
        logical_as_of=start + timedelta(minutes=25),
        run_status="completed",
    )
    snaps = service.freeze_feature_snapshots(dataset_id="ds_snap", run_id="r_snap")
    snap = snaps[-1]
    verify_snapshot_integrity(snap, contract=contract)

    import dataclasses

    tampered = dataclasses.replace(snap, features={**snap.features, "close": 9999.0})
    with pytest.raises(ContractMismatchError):
        verify_snapshot_integrity(tampered, contract=contract)


@pytest.mark.unit
def test_dbt_models_aligned_to_1m_and_separated_targets() -> None:
    """Static verification of dbt alignment, schema macro, feature purity, and test removal of int_ohlcv_5min."""
    dbt_dir = REPO_ROOT / "dbt_project"
    assert not (dbt_dir / "models/intermediate/int_ohlcv_5min.sql").exists()
    assert (dbt_dir / "models/intermediate/int_ohlcv_1min.sql").exists()
    assert (dbt_dir / "macros/generate_schema_name.sql").exists()
    assert (dbt_dir / "models/marts/bar_targets.sql").exists()

    schema_yml = (dbt_dir / "models/schema.yml").read_text()
    assert "int_ohlcv_5min" not in schema_yml
    assert "marts.stock_features" not in schema_yml
    assert "name: stock_features" in schema_yml
    assert "name: bar_targets" in schema_yml

    stock_features_sql = (dbt_dir / "models/marts/stock_features.sql").read_text()
    assert "LEAD(" not in stock_features_sql.upper()
    assert "target_direction" not in stock_features_sql

    indicators_sql = (dbt_dir / "models/intermediate/int_technical_indicators.sql").read_text()
    for removed in ("ema_12", "ema_26", "macd_line", "macd_signal", "macd_histogram", "rsi_14"):
        assert removed not in indicators_sql
    assert "segment_id" in indicators_sql
    assert "warmup_complete" in indicators_sql

    # All 4 singular dbt tests exist
    for test_sql in (
        "assert_ohlcv_1min_ranges.sql",
        "assert_utc_timestamps_and_session_hours.sql",
        "assert_warmup_prevents_null_features.sql",
        "assert_target_availability_and_no_leakage.sql",
    ):
        assert (dbt_dir / "tests" / test_sql).exists()
