from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime
from pathlib import Path

import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.preprocessing import StandardScaler

from consumer.db import DatabaseConfig, DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class ModelTrainer:
    """Trains the XGBoost classification model for price direction prediction."""

    FEATURES = [
        "open", "high", "low", "close", "volume",
        "sma_20", "ema_12", "ema_26", "rsi_14",
        "macd_line", "macd_signal", "macd_histogram",
        "avg_sentiment_score", "sentiment_event_count", "sentiment_polarity_shift",
    ]
    TARGET = "target_direction"

    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db = db_manager
        self.artefact_dir = Path("model/artefacts")
        self.artefact_dir.mkdir(parents=True, exist_ok=True)

    def load_data(self) -> pd.DataFrame:
        """Load features from marts.stock_features."""
        query = "SELECT * FROM marts.stock_features ORDER BY window_start ASC"
        with self.db.get_connection() as conn:
            df = pd.read_sql(query, conn)

        # Drop rows where target is NULL (the latest windows)
        df = df.dropna(subset=[self.TARGET])

        # Fill optional indicators with 0 or forward fill
        df[self.FEATURES] = df[self.FEATURES].fillna(0)

        return df

    def train(self) -> None:
        """Execute the full training pipeline."""
        df = self.load_data()
        if df.empty:
            logger.error("No training data found in marts.stock_features.")
            return

        logger.info("Loaded %d rows for training.", len(df))

        # 1. Chronological Split (80/10/10)
        df = df.sort_values("window_start")
        n = len(df)
        train_end = int(n * 0.8)
        val_end = int(n * 0.9)

        train_df = df.iloc[:train_end]
        val_df = df.iloc[train_end:val_end]
        test_df = df.iloc[val_end:]

        X_train, y_train = train_df[self.FEATURES], train_df[self.TARGET]
        X_val, y_val = val_df[self.FEATURES], val_df[self.TARGET]
        X_test, y_test = test_df[self.FEATURES], test_df[self.TARGET]

        # 2. Scaling
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        X_test_scaled = scaler.transform(X_test)

        # 3. Model Fit
        logger.info("Training XGBClassifier...")
        model = xgb.XGBClassifier(
            n_estimators=1000,
            max_depth=6,
            learning_rate=0.01,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="binary:logistic",
            random_state=42,
            early_stopping_rounds=50,
        )

        model.fit(
            X_train_scaled,
            y_train,
            eval_set=[(X_val_scaled, y_val)],
            verbose=False,
        )

        # 4. Evaluation
        y_pred = model.predict(X_test_scaled)
        y_prob = model.predict_proba(X_test_scaled)[:, 1]

        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred),
            "recall": recall_score(y_test, y_pred),
            "f1": f1_score(y_test, y_pred),
            "brier_score": brier_score_loss(y_test, y_prob),
            "training_date": datetime.now().isoformat(),
            "model_version": "xgb_v1.0.0",
            "feature_names": self.FEATURES,
        }

        logger.info("Test Metrics: %s", json.dumps(metrics, indent=2))

        # 5. Save Artefacts
        model.get_booster().save_model(str(self.artefact_dir / "xgb_model.json"))
        with open(self.artefact_dir / "scaler.pkl", "wb") as f:
            pickle.dump(scaler, f)
        with open(self.artefact_dir / "metadata.json", "w") as f:
            json.dump(metrics, f, indent=2)

        logger.info("Artefacts saved to %s", self.artefact_dir)


if __name__ == "__main__":
    db_config = DatabaseConfig()  # type: ignore
    db_mgr = DatabaseManager(db_config)
    trainer = ModelTrainer(db_mgr)
    trainer.train()
