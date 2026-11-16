from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from model.train import ModelTrainer
from pipeline.contract import load_forecast_contract


@pytest.fixture
def mock_db_manager():
    return MagicMock()


def _build_canonical_df(rows: int) -> pd.DataFrame:
    contract = load_forecast_contract()
    bar_starts = pd.date_range("2025-07-14T13:30:00Z", periods=rows, freq="1min")
    rng = np.random.default_rng(42)
    closes = 150.0 + rng.normal(0.0, 0.5, size=rows)
    data = {
        "contract_id": [contract.contract_id] * rows,
        "contract_version": [contract.version] * rows,
        "dataset_id": ["fixture_smoke_v1"] * rows,
        "ticker": ["AAPL"] * rows,
        "bar_start": bar_starts,
        "bar_end": bar_starts + pd.Timedelta(minutes=1),
        "warmup_complete": [True] * rows,
        "target_direction": [i % 2 for i in range(rows)],
        "open": closes - 0.1,
        "high": closes + 0.3,
        "low": closes - 0.3,
        "close": closes,
        "volume": rng.integers(1000, 5000, size=rows),
        "return_1m": rng.normal(0.0, 0.002, size=rows),
        "return_5m_past": rng.normal(0.0, 0.004, size=rows),
        "return_15m_past": rng.normal(0.0, 0.007, size=rows),
        "sma_5": closes,
        "sma_20": closes - 0.05,
        "sma_spread_5_20": rng.normal(0.0, 0.001, size=rows),
        "volatility_20": np.full(rows, 0.002),
        "volume_ratio_20": np.full(rows, 1.05),
        "bar_range_pct": np.full(rows, 0.004),
    }
    return pd.DataFrame(data)


def test_model_trainer_init(mock_db_manager):
    """Verifies ModelTrainer binds to active ForecastContract canonical features."""
    trainer = ModelTrainer(mock_db_manager)
    contract = load_forecast_contract()
    assert trainer.db == mock_db_manager
    assert trainer.features == contract.feature_policy.features
    assert trainer.artefact_dir.name == "artefacts"


@patch("pandas.read_sql")
def test_load_data(mock_read_sql, mock_db_manager):
    """Verifies snapshot loading drops un-warmed or unlabeled rows without zero fabrication."""
    mock_df = _build_canonical_df(3)
    mock_df.loc[1, "target_direction"] = None
    mock_df.loc[2, "warmup_complete"] = False
    mock_read_sql.return_value = mock_df

    trainer = ModelTrainer(mock_db_manager)
    df = trainer.load_data(run_id="test-run")

    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "AAPL"


def test_train_empty_df(mock_db_manager):
    mock_db_manager.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "fixture_smoke_v1",
        "synthetic_fixture",
    )
    with patch.object(ModelTrainer, "load_data", return_value=pd.DataFrame()):
        trainer = ModelTrainer(mock_db_manager)
        with pytest.raises(ValueError, match="No canonical snapshots"):
            trainer.train_and_evaluate(run_id="test-run")


def test_training_run_must_exist(mock_db_manager):
    mock_db_manager.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = None
    with pytest.raises(ValueError, match="Training run does not exist"):
        ModelTrainer(mock_db_manager).train_and_evaluate(run_id="missing")
