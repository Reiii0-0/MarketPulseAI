from __future__ import annotations

import pandas as pd
import pytest

from pipeline.contract import load_forecast_contract
from pipeline.features import compute_canonical_features_from_bars


@pytest.fixture
def sample_session_bars() -> pd.DataFrame:
    """Provides 30 contiguous 1-minute bars during regular NY market hours."""
    # 13:30 UTC == 09:30 America/New_York during EDT (July)
    bar_starts = pd.date_range("2025-07-14T13:30:00Z", periods=30, freq="1min")
    return pd.DataFrame(
        {
            "ticker": ["AAPL"] * 30,
            "bar_start": bar_starts,
            "bar_end": bar_starts + pd.Timedelta(minutes=1),
            "open": [100.0 + i for i in range(30)],
            "high": [101.0 + i for i in range(30)],
            "low": [99.0 + i for i in range(30)],
            "close": [100.5 + i for i in range(30)],
            "volume": [1000 + i * 10 for i in range(30)],
        }
    )


def test_canonical_features_output_columns(sample_session_bars: pd.DataFrame) -> None:
    """Verifies only canonical contract features and warm-up columns are computed."""
    contract = load_forecast_contract()
    result = compute_canonical_features_from_bars(sample_session_bars, contract=contract)

    assert len(result) == 30
    for col in contract.feature_policy.features:
        assert col in result.columns
    for removed_col in ("ema_12", "ema_26", "macd_line", "macd_signal", "macd_histogram", "rsi_14"):
        assert removed_col not in result.columns


def test_canonical_features_warmup_and_sma(sample_session_bars: pd.DataFrame) -> None:
    """Verifies explicit 20-bar warm-up gating and exact SMA-20 computation."""
    contract = load_forecast_contract()
    result = compute_canonical_features_from_bars(sample_session_bars, contract=contract)

    # First 19 rows must NOT be warmup_complete and sma_20 must remain NaN
    assert not result.iloc[:19]["warmup_complete"].any()
    assert result.iloc[:19]["sma_20"].isna().all()

    # Row 19 (20th contiguous bar) completes warm-up and matches mean of first 20 closes
    assert bool(result.iloc[19]["warmup_complete"]) is True
    expected_sma_20 = sample_session_bars.iloc[0:20]["close"].mean()
    assert result.iloc[19]["sma_20"] == pytest.approx(expected_sma_20)
    assert result.iloc[19]["contiguous_bar_count"] == 20
