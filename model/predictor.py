from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path
from typing import Any

import pandas as pd
import shap
import xgboost as xgb

logger = logging.getLogger(__name__)


class Predictor:
    """Inference engine for serving UP/DOWN predictions with SHAP explanations."""

    def __init__(
        self,
        model: xgb.XGBClassifier,
        scaler: Any,
        metadata: dict[str, Any],
        explainer: shap.Explainer
    ) -> None:
        self.model = model
        self.scaler = scaler
        self.metadata = metadata
        self.explainer = explainer
        self.feature_names = metadata["feature_names"]

    @classmethod
    def load(cls, artefact_dir: str = "model/artefacts") -> Predictor:
        """Factory method to load artefacts from disk.

        Args:
            artefact_dir (str): Directory containing model, scaler, and metadata.

        Returns:
            Predictor: Initialised inference engine.
        """
        path = Path(artefact_dir)

        model = xgb.XGBClassifier()
        model.load_model(path / "xgb_model.json")

        with open(path / "scaler.pkl", "rb") as f:
            scaler = pickle.load(f)

        with open(path / "metadata.json") as f:
            metadata = json.load(f)

        # Initialise SHAP explainer
        explainer = shap.TreeExplainer(model)

        return cls(model, scaler, metadata, explainer)

    def predict(self, features: dict[str, Any]) -> dict[str, Any]:
        """Perform inference on a single 5-minute window.

        Args:
            features (dict): Dictionary of technical and sentiment indicators.

        Returns:
            dict: Prediction result including probabilities and SHAP factors.
        """
        # 1. Prepare input vector
        X = pd.DataFrame([features])[self.feature_names]
        X_scaled = self.scaler.transform(X)

        # 2. Get probabilities via Booster to avoid sklearn wrapper errors
        dmatrix = xgb.DMatrix(X_scaled, feature_names=self.feature_names)
        prob_up = float(self.model.get_booster().predict(dmatrix)[0])
        prob_down = 1.0 - prob_up

        # 3. Determine direction and confidence
        predicted_direction = 1 if prob_up > 0.5 else 0
        max_prob = max(prob_up, prob_down)

        if max_prob > 0.75:
            confidence_tier = "HIGH"
        elif max_prob > 0.60:
            confidence_tier = "MEDIUM"
        else:
            confidence_tier = "LOW"

        # 4. Compute SHAP explanations
        shap_values = self.explainer(X_scaled)
        # Extract values for the positive class (UP)
        # TreeExplainer returns a list of arrays for binary classification in some versions,
        # or a single array where the last dim is classes.
        # For simplicity, we assume shap_values.values[0] matches features.
        sv = shap_values.values[0]

        factors = []
        for i, feat_name in enumerate(self.feature_names):
            val = float(sv[i])
            factors.append({
                "feature": feat_name,
                "shap_value": val,
                "direction": "positive" if val > 0 else "negative"
            })

        # Sort by absolute SHAP value descending and take top 3
        top_factors = sorted(factors, key=lambda x: abs(x["shap_value"]), reverse=True)[:3]

        return {
            "ticker": features["ticker"],
            "window_start": features["window_start"],
            "predicted_direction": predicted_direction,
            "probability_up": prob_up,
            "probability_down": prob_down,
            "confidence_tier": confidence_tier,
            "factors": top_factors,
            "model_version": self.metadata["model_version"]
        }
