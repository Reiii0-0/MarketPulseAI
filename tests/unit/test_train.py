from __future__ import annotations

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from model.train import ModelTrainer

@pytest.fixture
def mock_db_manager():
    return MagicMock()

def test_model_trainer_init(mock_db_manager):
    """Verifies ModelTrainer initialisation."""
    trainer = ModelTrainer(mock_db_manager)
    assert trainer.db == mock_db_manager
    assert trainer.artefact_dir.name == "artefacts"

@patch("pandas.read_sql")
def test_load_data(mock_read_sql, mock_db_manager):
    """Verifies data loading and filtering logic."""
    import pandas as pd
    mock_df = pd.DataFrame({
        "ticker": ["AAPL", "AAPL"],
        "window_start": pd.date_range("2023-01-01", periods=2),
        "target_direction": [1.0, None], # One row with target, one without
        "open": [100.0, 101.0],
        "high": [101.0, 102.0],
        "low": [99.0, 100.0],
        "close": [100.5, 101.5],
        "volume": [1000, 1100],
        "sma_20": [100.0, 100.0],
        "ema_12": [100.0, 100.0],
        "ema_26": [100.0, 100.0],
        "rsi_14": [50.0, 50.0],
        "macd_line": [0.0, 0.0],
        "macd_signal": [0.0, 0.0],
        "macd_histogram": [0.0, 0.0],
        "avg_sentiment_score": [0.5, 0.5],
        "sentiment_event_count": [1, 1],
        "sentiment_polarity_shift": [0.0, 0.0]
    })
    mock_read_sql.return_value = mock_df
    
    trainer = ModelTrainer(mock_db_manager)
    df = trainer.load_data()
    
    # Should drop the row with target_direction = None
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "AAPL"

def test_train_empty_df(mock_db_manager):
    """Verifies trainer handles empty dataframe gracefully."""
    with patch.object(ModelTrainer, "load_data", return_value=pd.DataFrame()):
        trainer = ModelTrainer(mock_db_manager)
        # Should not raise exception
        trainer.train()

def test_train_full_loop(mock_db_manager):
    """Verifies the full training and saving loop."""
    import numpy as np
    # Create 30 rows of data to satisfy 80/10/10 split and training
    rows = 30
    mock_df = pd.DataFrame({
        "ticker": ["AAPL"] * rows,
        "window_start": pd.date_range("2023-01-01", periods=rows),
        "target_direction": [1, 0] * (rows // 2),
        "open": np.random.rand(rows) + 100,
        "high": np.random.rand(rows) + 101,
        "low": np.random.rand(rows) + 99,
        "close": np.random.rand(rows) + 100,
        "volume": np.random.randint(1000, 2000, rows),
        "sma_20": [100.0] * rows,
        "ema_12": [100.0] * rows,
        "ema_26": [100.0] * rows,
        "rsi_14": [50.0] * rows,
        "macd_line": [0.0] * rows,
        "macd_signal": [0.0] * rows,
        "macd_histogram": [0.0] * rows,
        "avg_sentiment_score": [0.5] * rows,
        "sentiment_event_count": [1] * rows,
        "sentiment_polarity_shift": [0.0] * rows
    })
    
    # Test/Val size will be 10% of 30 = 3 rows each.
    test_size = 3
    
    with patch.object(ModelTrainer, "load_data", return_value=mock_df), \
         patch("xgboost.XGBClassifier.fit"), \
         patch("xgboost.XGBClassifier.predict", return_value=np.array([1] * test_size)), \
         patch("xgboost.XGBClassifier.predict_proba", return_value=np.array([[0.2, 0.8]] * test_size)), \
         patch("xgboost.XGBClassifier.get_booster") as mock_get_booster, \
         patch("builtins.open", MagicMock()), \
         patch("pickle.dump"):
        
        mock_booster = MagicMock()
        mock_get_booster.return_value = mock_booster
        
        trainer = ModelTrainer(mock_db_manager)
        trainer.train()
        
        assert mock_booster.save_model.called
