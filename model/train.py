from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from consumer.db import DatabaseConfig, DatabaseManager
from pipeline.contract import ForecastContract, load_forecast_contract
from pipeline.features import (
    TRAINING_SNAPSHOT_JOIN_SQL,
    validate_contract_compatibility,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class ModelTrainer:
    """Trains XGBoost directional classifier strictly from canonical feature snapshots and bar targets."""

    FEATURES = list(load_forecast_contract().feature_policy.features)
    TARGET = "target_direction"
    HYPERPARAMETERS: dict[str, int | float] = {
        "n_estimators": 1000,
        "max_depth": 6,
        "learning_rate": 0.01,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "early_stopping_rounds": 50,
    }

    def __init__(
        self,
        db_manager: DatabaseManager,
        contract: ForecastContract | None = None,
    ) -> None:
        self.db = db_manager
        self.contract = contract or load_forecast_contract()
        self.features = list(self.contract.feature_policy.features)
        self.artefact_dir = Path("model/artefacts")
        self.artefact_dir.mkdir(parents=True, exist_ok=True)

    def load_data(self, *, run_id: str) -> pd.DataFrame:
        """Load matured, warmed-up snapshots joined with realized +5m bar targets."""
        validate_contract_compatibility(self.contract, feature_names=self.features)
        with self.db.get_connection() as conn:
            df = pd.read_sql(
                TRAINING_SNAPSHOT_JOIN_SQL,
                conn,
                params=(self.contract.contract_id, self.contract.version, run_id),
            )

        if df.empty:
            return df

        if "contract_id" in df.columns:
            unique_contracts = set(df["contract_id"].dropna().astype(str))
            for cid in unique_contracts:
                validate_contract_compatibility(self.contract, contract_id=cid)

        if "warmup_complete" in df.columns:
            df = df[df["warmup_complete"].astype(bool)]

        # Drop rows without realized target or missing canonical features (never fabricate zeros for warm-up)
        df = df.dropna(subset=[self.TARGET, *self.features])
        return df

    def train_and_evaluate(
        self,
        *,
        run_id: str,
        report_dir: str | Path = "reports/evaluation",
    ) -> dict[str, Any]:
        """Run purged walk-forward evaluation, select champion, publish atomic bundle, and save reports."""
        from model.evaluate import (
            publish_model_bundle,
            run_walk_forward_evaluation,
            write_evaluation_report,
        )

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT dataset_id, provenance FROM raw.ingestion_runs WHERE run_id = %s",
                    (run_id,),
                )
                run = cur.fetchone()
        if run is None:
            raise ValueError("Training run does not exist")
        dataset_id, dataset_provenance = run
        df = self.load_data(run_id=run_id)
        if df.empty:
            raise ValueError(
                "No canonical snapshots with realized targets available for evaluation."
            )

        report, bundle_objects = run_walk_forward_evaluation(
            df,
            contract=self.contract,
            dataset_id=dataset_id,
            dataset_provenance=dataset_provenance,
            xgb_hyperparams=dict(self.HYPERPARAMETERS),
        )
        write_evaluation_report(report, output_dir=report_dir)
        publish_model_bundle(self.artefact_dir, report, bundle_objects, contract=self.contract)
        return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evaluate and publish a model from one run's immutable snapshots"
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bundle-dir", default="model/artefacts")
    parser.add_argument("--report-dir", default="reports/evaluation")
    args = parser.parse_args()
    db_mgr = DatabaseManager(DatabaseConfig())  # type: ignore[call-arg]
    try:
        trainer = ModelTrainer(db_mgr)
        trainer.artefact_dir = Path(args.bundle_dir)
        trainer.train_and_evaluate(run_id=args.run_id, report_dir=args.report_dir)
    finally:
        db_mgr.close()
