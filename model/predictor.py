from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import shap
import xgboost as xgb
from numpy.typing import NDArray

from model.evaluate import _apply_calibrator, load_and_verify_bundle_manifest, resolve_bundle_dir
from pipeline.contract import ForecastContract, load_forecast_contract
from pipeline.features import (
    ContractMismatchError,
    FeatureSnapshotRecord,
    compute_feature_hash,
    validate_contract_compatibility,
    verify_snapshot_integrity,
)

logger = logging.getLogger(__name__)


def validate_bundle_contract(
    metadata: dict[str, Any],
    *,
    contract: ForecastContract | None = None,
    require_contract_stamp: bool = True,
) -> ForecastContract:
    """Validate that a model bundle metadata/manifest matches the active ForecastContract."""
    active = contract or load_forecast_contract()
    has_stamp = any(k in metadata for k in ("contract_id", "contract_version", "feature_mode"))
    if require_contract_stamp and not has_stamp:
        raise ContractMismatchError(
            "Model bundle metadata is missing required contract_id/contract_version stamp."
        )
    if has_stamp or require_contract_stamp:
        validate_contract_compatibility(
            active,
            contract_id=metadata.get("contract_id"),
            contract_version=metadata.get("contract_version"),
            feature_mode=metadata.get("feature_mode"),
            feature_names=metadata.get("feature_names"),
        )
    return active


class Predictor:
    """Contract-verified inference engine serving champion probabilities and honest attributions."""

    def __init__(
        self,
        model: Any,
        scaler: Any,
        metadata: dict[str, Any],
        explainer: Any | None = None,
        *,
        contract: ForecastContract | None = None,
        enforce_contract: bool = False,
        calibrator: Any | None = None,
        champion_model: str | None = None,
    ) -> None:
        self.contract = contract or load_forecast_contract()
        has_stamp = any(k in metadata for k in ("contract_id", "contract_version", "feature_mode"))
        if enforce_contract or has_stamp:
            validate_bundle_contract(
                metadata,
                contract=self.contract,
                require_contract_stamp=enforce_contract,
            )
        self.enforce_contract = enforce_contract or has_stamp
        self.model = model
        self.scaler = scaler
        self.metadata = metadata
        self.explainer = explainer
        self.calibrator = calibrator
        self.champion_model = champion_model or str(metadata.get("champion_model", "xgboost"))
        self.abstain_band_delta = float(metadata.get("abstain_band_delta", 0.0))
        self.feature_names = list(metadata["feature_names"])

    @classmethod
    def load(
        cls,
        model_path: str = "model/artefacts/xgb_model.json",
        scaler_path: str | None = None,
        *,
        contract: ForecastContract | None = None,
        enforce_contract: bool = True,
    ) -> Predictor:
        """Load bundle directory/path after verifying manifest contract and SHA-256 artifact hashes."""
        path = Path(model_path)
        bundle_dir = resolve_bundle_dir(path if path.is_dir() else path.parent)
        active_contract = contract or load_forecast_contract()

        manifest_file = bundle_dir / "manifest.json"
        if enforce_contract or manifest_file.exists():
            manifest = load_and_verify_bundle_manifest(bundle_dir, contract=active_contract)
            metadata: dict[str, Any] = json.loads(manifest.model_dump_json())
        else:
            metadata = json.loads((bundle_dir / "metadata.json").read_text(encoding="utf-8"))

        scaler_file = bundle_dir / "scaler.pkl"
        if scaler_path is not None and Path(scaler_path).resolve() != scaler_file.resolve():
            if enforce_contract:
                raise ContractMismatchError("Scaler must belong to the verified bundle")
            scaler_file = Path(scaler_path)
        with open(scaler_file, "rb") as f:
            scaler = pickle.load(f)

        calibrator = None
        cal_file = bundle_dir / "calibrator.pkl"
        if cal_file.exists():
            with open(cal_file, "rb") as f:
                calibrator = pickle.load(f)

        champion = str(metadata.get("champion_model", "xgboost"))
        explainer: Any | None = None
        model: Any

        if champion == "prior_baseline":
            prior_data = json.loads(
                (bundle_dir / "prior_baseline.json").read_text(encoding="utf-8")
            )
            model = float(prior_data["prior_prob_up"])
        elif champion == "logistic_regression":
            with open(bundle_dir / "logistic_regression.pkl", "rb") as f:
                model = pickle.load(f)
        else:
            xgb_file = bundle_dir / "xgb_model.json"
            if xgb_file.exists():
                xgb_clf = xgb.XGBClassifier()
                xgb_clf.load_model(xgb_file)
                model = xgb_clf
                explainer = shap.TreeExplainer(xgb_clf)
            else:
                with open(bundle_dir / "xgb_fallback.pkl", "rb") as f:
                    model = pickle.load(f)

        return cls(
            model,
            scaler,
            metadata,
            explainer,
            contract=active_contract,
            enforce_contract=enforce_contract,
            calibrator=calibrator,
            champion_model=champion,
        )

    def _compute_raw_prob_and_factors(
        self, X_scaled: NDArray[np.float64]
    ) -> tuple[float, list[dict[str, Any]], str]:
        if self.champion_model == "prior_baseline":
            raw_prob_up = float(self.model)
            return raw_prob_up, [], "empirical_prior"

        if self.champion_model == "logistic_regression":
            raw_prob_up = float(self.model.predict_proba(X_scaled)[:, 1][0])
            coefs = getattr(self.model, "coef_", None)
            factors: list[dict[str, Any]] = []
            if coefs is not None and len(coefs) > 0:
                row = np.asarray(X_scaled[0], dtype=float)
                w = np.asarray(coefs[0], dtype=float)
                for idx, feat_name in enumerate(self.feature_names):
                    contrib = float(w[idx] * row[idx])
                    factors.append(
                        {
                            "feature": feat_name,
                            "shap_value": contrib,
                            "direction": "positive" if contrib > 0 else "negative",
                        }
                    )
            top = sorted(factors, key=lambda x: abs(x["shap_value"]), reverse=True)[:3]
            return raw_prob_up, top, "linear_log_odds"

        # XGBoost (supports mocked or real XGBClassifier + TreeExplainer)
        if hasattr(self.model, "get_booster"):
            dmatrix = xgb.DMatrix(X_scaled, feature_names=self.feature_names)
            raw_prob_up = float(self.model.get_booster().predict(dmatrix)[0])
        else:
            raw_prob_up = float(self.model.predict_proba(X_scaled)[:, 1][0])

        factors = []
        if self.explainer is not None:
            shap_values = self.explainer(X_scaled)
            sv = shap_values.values[0]
            for i, feat_name in enumerate(self.feature_names):
                val = float(sv[i])
                factors.append(
                    {
                        "feature": feat_name,
                        "shap_value": val,
                        "direction": "positive" if val > 0 else "negative",
                    }
                )
        top_factors = sorted(factors, key=lambda x: abs(x["shap_value"]), reverse=True)[:3]
        return raw_prob_up, top_factors, "base_model_log_odds"

    def predict(self, features: dict[str, Any] | FeatureSnapshotRecord) -> dict[str, Any]:
        """Perform inference on a canonical feature snapshot or feature payload."""
        feature_hash: str | None
        if isinstance(features, FeatureSnapshotRecord):
            verify_snapshot_integrity(features, contract=self.contract)
            if not features.warmup_complete:
                raise ContractMismatchError(
                    f"Cannot predict before warm-up completes ({features.contiguous_bar_count}/"
                    f"{self.contract.warmup_bars} bars) for {features.ticker}"
                )
            feature_payload: dict[str, Any] = dict(features.features)
            ticker = features.ticker
            window_ref: Any = features.bar_end
            feature_hash = features.feature_hash
            contract_stamped = True
        else:
            feature_payload = features
            ticker = str(features["ticker"])
            window_ref = (
                features.get("bar_end") or features.get("decision_at") or features["window_start"]
            )
            contract_stamped = self.enforce_contract or any(
                k in features
                for k in ("contract_id", "contract_version", "feature_mode", "feature_hash")
            )
            raw_hash = features.get("feature_hash")
            feature_hash = str(raw_hash) if raw_hash is not None else None
            if contract_stamped:
                validate_contract_compatibility(
                    self.contract,
                    contract_id=features.get("contract_id"),
                    contract_version=features.get("contract_version"),
                    feature_mode=features.get("feature_mode"),
                    feature_names=self.feature_names,
                )
                if features.get("warmup_complete") is False:
                    raise ContractMismatchError(
                        f"Cannot predict before warm-up completes for {ticker}"
                    )
                if feature_hash is not None:
                    expected_hash = compute_feature_hash(self.contract, feature_payload)
                    if feature_hash != expected_hash and feature_hash != compute_feature_hash(
                        self.contract, feature_payload, preserve_signed_zero=True
                    ):
                        raise ContractMismatchError(
                            f"Feature hash mismatch for {ticker}: expected {expected_hash}, got {feature_hash}"
                        )

        X_raw = pd.DataFrame([feature_payload]).reindex(columns=self.feature_names)
        if contract_stamped:
            X_float = X_raw.astype(float)
            if X_float.isna().any().any() or not np.isfinite(X_float.to_numpy()).all():
                missing_cols = [c for c in self.feature_names if pd.isna(X_float.iloc[0][c])]
                raise ContractMismatchError(
                    f"Canonical snapshot missing/non-finite required features: {missing_cols}"
                )
            X = X_float
        else:
            X = X_raw.astype(float).fillna(0)

        X_scaled = self.scaler.transform(
            X if hasattr(self.scaler, "feature_names_in_") else X.to_numpy()
        )
        raw_prob_up, top_factors, attr_space = self._compute_raw_prob_and_factors(X_scaled)
        if self.calibrator is not None:
            prob_up = float(_apply_calibrator(self.calibrator, np.array([raw_prob_up]))[0])
        else:
            prob_up = float(raw_prob_up)
        prob_down = 1.0 - prob_up

        predicted_direction = 1 if prob_up > 0.5 else 0
        if abs(prob_up - 0.5) < self.abstain_band_delta:
            signal_label = "NO_SIGNAL"
        else:
            signal_label = "UP" if predicted_direction == 1 else "NOT_UP"

        max_prob = max(prob_up, prob_down)
        if max_prob > 0.75:
            confidence_tier = "HIGH"
        elif max_prob > 0.60:
            confidence_tier = "MEDIUM"
        else:
            confidence_tier = "LOW"

        return {
            "ticker": ticker,
            "window_start": window_ref,
            "predicted_direction": predicted_direction,
            "signal_label": signal_label,
            "probability_up": prob_up,
            "probability_down": prob_down,
            "raw_probability_up": raw_prob_up,
            "confidence_tier": confidence_tier,
            "factors": top_factors,
            "attribution_space": attr_space,
            "model_version": self.metadata["model_version"],
            "bundle_id": self.metadata.get("bundle_id"),
            "champion_model": self.champion_model,
            "fit_information_end": self.metadata.get("fit_information_end"),
            "contract_id": self.metadata.get("contract_id"),
            "contract_version": self.metadata.get("contract_version"),
            "feature_hash": feature_hash,
        }
