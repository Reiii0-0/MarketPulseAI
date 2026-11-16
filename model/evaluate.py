"""Walk-forward temporal ML evaluation, probability calibration, and immutable bundle publishing.

Implements Stage 4 requirements:
- Timestamp-aligned train/validation/final-holdout splits with label-maturity purge (>= horizon)
- Comparison of probabilistic prior baseline, Logistic Regression, and XGBoost (plus separate
  random-walk price-direction comparator)
- Pre-registered primary selection metric (Brier score), calibration reliability, balanced
  accuracy, log loss, coverage, per-ticker/per-fold breakdowns, and contiguous block-bootstrap CI
- Strict provenance stamping (never mixing synthetic fixture checks into real-market claims)
- Atomic bundle publishing with SHA-256 artifact manifests and ``fit_information_end`` boundary
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pickle
import platform
import shutil
import subprocess
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
import sklearn
import xgboost as xgb
from numpy.typing import NDArray
from pydantic import BaseModel, Field
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    brier_score_loss,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
)
from sklearn.preprocessing import StandardScaler

from pipeline.contract import ForecastContract, load_forecast_contract
from pipeline.dataset_manifest import compute_sha256, load_manifest
from pipeline.features import (
    ContractMismatchError,
    compute_canonical_features_from_bars,
    validate_contract_compatibility,
)

PRIMARY_SELECTION_METRIC = "brier_score"
TIE_BREAK_ORDER = ("prior_baseline", "logistic_regression", "xgboost")
SequenceLike = Any


class BundleManifest(BaseModel):
    """Immutable manifest binding serialized model artifacts to data, cutoffs, and contract."""

    bundle_id: str
    model_version: str
    champion_model: Literal["prior_baseline", "logistic_regression", "xgboost"]
    challenger_models: list[str]
    contract_id: str
    contract_version: str
    feature_mode: Literal["price_only", "price_and_sentiment"]
    feature_names: list[str] = Field(min_length=1)
    dataset_id: str
    dataset_provenance: str
    is_real_market_data: bool
    data_checksum: str
    fit_information_end: datetime
    cutoffs: dict[str, Any]
    random_seed: int
    selection_metric: str = PRIMARY_SELECTION_METRIC
    decision_threshold: float = 0.5
    abstain_band_delta: float = 0.0
    hyperparameters: dict[str, Any]
    git_commit: str
    environment: dict[str, str]
    metrics: dict[str, Any]
    artifact_hashes: dict[str, str]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class TemporalFold:
    """Single expanding walk-forward fold with explicit label-maturity purge before validation."""

    fold_index: int
    train_start: datetime
    train_end: datetime
    val_start: datetime
    val_end: datetime
    train_timestamps: tuple[datetime, ...]
    val_timestamps: tuple[datetime, ...]


@dataclass(frozen=True, slots=True)
class TemporalSplitPlan:
    """Purged walk-forward folds plus untouched final holdout and fit_information_end bound."""

    folds: list[TemporalFold]
    pre_holdout_timestamps: tuple[datetime, ...]
    holdout_timestamps: tuple[datetime, ...]
    holdout_start: datetime
    holdout_end: datetime
    fit_information_end: datetime
    purge_seconds: int
    sufficient_for_three_folds: bool


def _to_utc_datetime(val: Any) -> datetime:
    if isinstance(val, pd.Timestamp):
        dt_val = val.to_pydatetime()
        if isinstance(dt_val, datetime):
            return dt_val.replace(tzinfo=UTC) if dt_val.tzinfo is None else dt_val.astimezone(UTC)
    if isinstance(val, datetime):
        return val.replace(tzinfo=UTC) if val.tzinfo is None else val.astimezone(UTC)
    parsed = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _get_git_commit() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or "unknown"
    except Exception:
        return "unknown"


def _runtime_environment() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "xgboost": str(xgb.__version__),
        "scikit_learn": str(sklearn.__version__),
        "pandas": str(pd.__version__),
        "numpy": str(np.__version__),
    }


def _clip_probs(probs: NDArray[Any]) -> NDArray[np.float64]:
    return np.asarray(np.clip(probs, 1e-6, 1.0 - 1e-6), dtype=np.float64)


def prepare_evaluation_dataframe(
    raw_df: pd.DataFrame,
    *,
    contract: ForecastContract | None = None,
    dataset_id: str = "unknown",
) -> pd.DataFrame:
    """Prepare canonical matured snapshot+target rows with zero lookahead or warm-up leakage."""
    active = contract or load_forecast_contract()
    df = raw_df.copy()

    # Check point-in-time availability discipline if timestamps are present
    if "available_at" in df.columns and "bar_end" in df.columns:
        avail_ts = pd.to_datetime(df["available_at"], utc=True)
        decision_ts = pd.to_datetime(df["bar_end"], utc=True) + pd.Timedelta(
            seconds=active.grace_seconds
        )
        # Allow direct bar arrivals past grace only if explicitly filtered out before snapshot creation;
        # raise if pre-computed snapshot dataframe embeds future availability past decision_at
        if "warmup_complete" in df.columns and bool((avail_ts > decision_ts).any()):
            raise ContractMismatchError(
                "Availability leakage detected: available_at exceeds decision_at (bar_end + grace)"
            )

    if "warmup_complete" not in df.columns or "sma_20" not in df.columns:
        df["dataset_id"] = df.get("dataset_id", dataset_id)
        if "available_at" in df.columns:
            bar_ends = pd.to_datetime(df["bar_end"], utc=True)
            avail = pd.to_datetime(df["available_at"], utc=True)
            df = df.loc[avail <= bar_ends + pd.Timedelta(seconds=active.grace_seconds)].copy()
        features_df = compute_canonical_features_from_bars(df, contract=active)
    else:
        features_df = df

    features_df["bar_start"] = pd.to_datetime(features_df["bar_start"], utc=True)
    features_df["bar_end"] = pd.to_datetime(features_df["bar_end"], utc=True)
    if "decision_at" not in features_df.columns:
        features_df["decision_at"] = features_df["bar_end"] + pd.Timedelta(
            seconds=active.grace_seconds
        )
    else:
        features_df["decision_at"] = pd.to_datetime(features_df["decision_at"], utc=True)

    if "target_at" not in features_df.columns:
        features_df["target_at"] = features_df["bar_end"] + pd.Timedelta(
            seconds=active.horizon_seconds
        )
    else:
        features_df["target_at"] = pd.to_datetime(features_df["target_at"], utc=True)

    if "session_date_ny" not in features_df.columns:
        features_df["session_date_ny"] = (
            features_df["bar_start"].dt.tz_convert(active.session_rules.exchange_timezone).dt.date
        )

    # Derive exact +5m same-session targets from finalized canonical bars if not already supplied
    if "target_direction" not in features_df.columns:
        bar_close_lookup = (
            features_df[["ticker", "bar_end", "session_date_ny", "close"]]
            .drop_duplicates(subset=["ticker", "bar_end"])
            .rename(
                columns={
                    "bar_end": "target_at",
                    "session_date_ny": "target_session_date_ny",
                    "close": "target_close",
                }
            )
        )
        merged = features_df.merge(
            bar_close_lookup,
            on=["ticker", "target_at"],
            how="left",
        )
        same_session = merged["session_date_ny"] == merged["target_session_date_ny"]
        valid_target = same_session & merged["target_close"].notna() & (merged["close"] > 0)
        merged["return_5m"] = np.where(
            valid_target,
            (merged["target_close"] - merged["close"]) / merged["close"],
            np.nan,
        )
        merged["target_direction"] = np.where(
            valid_target,
            np.where(merged["return_5m"] > 0.0, 1.0, 0.0),
            np.nan,
        )
        merged["target_status"] = np.where(valid_target, "realized", "unavailable")
        features_df = merged

    if "contract_id" in features_df.columns:
        for cid in set(features_df["contract_id"].dropna().astype(str)):
            validate_contract_compatibility(active, contract_id=cid)

    # Filter strictly to warmed-up rows with matured realized targets and complete canonical features
    out = features_df[features_df["warmup_complete"].astype(bool)].copy()
    if "target_status" in out.columns:
        out = out[out["target_status"] == "realized"].copy()
    out = out.dropna(subset=["target_direction", *active.feature_policy.features]).copy()
    out["target_direction"] = out["target_direction"].astype(int)
    out = out.sort_values(["bar_end", "ticker"]).reset_index(drop=True)
    return out


def build_temporal_splits(
    df: pd.DataFrame,
    *,
    horizon_seconds: int = 300,
    grace_seconds: int = 15,
    n_folds: int = 3,
    holdout_fraction: float = 0.20,
    min_train_timestamps: int = 8,
    min_val_timestamps: int = 3,
) -> TemporalSplitPlan:
    """Create purged expanding walk-forward folds plus an untouched final holdout split.

    Guarantees:
    - Common unique ``bar_end`` timestamps across all tickers
    - Every training label obeys ``target_at + grace <= next_cutoff``
    """
    if df.empty:
        raise ValueError("Cannot build temporal splits on an empty dataframe")

    unique_ts = tuple(
        _to_utc_datetime(ts)
        for ts in sorted(pd.to_datetime(df["bar_end"], utc=True).unique().tolist())
    )
    total_ts = len(unique_ts)
    if total_ts < min_train_timestamps + min_val_timestamps + 2:
        raise ValueError(
            f"Insufficient unique timestamps ({total_ts}) for temporal walk-forward + holdout splits"
        )

    holdout_count = max(min_val_timestamps, int(round(total_ts * holdout_fraction)))
    holdout_count = min(holdout_count, total_ts - (min_train_timestamps + min_val_timestamps))
    holdout_start = unique_ts[-holdout_count]
    holdout_end = unique_ts[-1]
    holdout_timestamps = tuple(unique_ts[-holdout_count:])

    # Keep strictly pre-holdout timestamps whose +horizon label has fully matured at or before holdout_start
    horizon_td = timedelta(seconds=horizon_seconds + grace_seconds)
    pre_holdout_pool = tuple(
        ts for ts in unique_ts[:-holdout_count] if ts + horizon_td <= holdout_start
    )
    eff_min_train = min(min_train_timestamps, max(3, len(pre_holdout_pool) // 2))
    eff_min_val = min(min_val_timestamps, max(1, (len(pre_holdout_pool) - eff_min_train)))
    if len(pre_holdout_pool) < eff_min_train + eff_min_val:
        raise ValueError(
            f"Insufficient purged pre-holdout timestamps ({len(pre_holdout_pool)}) for walk-forward evaluation"
        )

    pre_n = len(pre_holdout_pool)
    # Reserve meaningful initial history on real-sized datasets; tiny fixtures retain
    # their reduced smoke splits. Purging below still removes immature labels.
    if pre_n >= 4 * min_train_timestamps:
        eff_min_train = max(eff_min_train, pre_n // 2)
    actual_folds = min(
        n_folds,
        max(1, (pre_n - eff_min_train) // max(1, eff_min_val)),
    )
    val_block = max(eff_min_val, (pre_n - eff_min_train) // max(1, actual_folds))

    folds: list[TemporalFold] = []
    for k in range(actual_folds):
        val_end_idx = pre_n - (actual_folds - 1 - k) * val_block
        val_start_idx = max(eff_min_train, val_end_idx - val_block)
        val_start = pre_holdout_pool[val_start_idx]
        val_ts = pre_holdout_pool[val_start_idx:val_end_idx]
        # Purge all training samples whose target horizon overlaps validation start
        train_ts = tuple(
            ts for ts in pre_holdout_pool[:val_start_idx] if ts + horizon_td <= val_start
        )
        if not train_ts or not val_ts:
            continue
        folds.append(
            TemporalFold(
                fold_index=len(folds) + 1,
                train_start=train_ts[0],
                train_end=train_ts[-1],
                val_start=val_ts[0],
                val_end=val_ts[-1],
                train_timestamps=train_ts,
                val_timestamps=val_ts,
            )
        )

    if not folds:
        raise ValueError("Unable to construct at least 1 purged walk-forward fold")

    # fit_information_end includes full label maturity of every pre-holdout row used in final training/calibration
    fit_information_end = pre_holdout_pool[-1] + horizon_td

    return TemporalSplitPlan(
        folds=folds,
        pre_holdout_timestamps=pre_holdout_pool,
        holdout_timestamps=holdout_timestamps,
        holdout_start=holdout_start,
        holdout_end=holdout_end,
        fit_information_end=fit_information_end,
        purge_seconds=horizon_seconds + grace_seconds,
        sufficient_for_three_folds=(len(folds) >= 3),
    )


def compute_binary_classification_metrics(
    y_true: NDArray[Any],
    y_prob: NDArray[Any],
    *,
    abstain_delta: float = 0.0,
    n_bins: int = 5,
) -> dict[str, Any]:
    """Compute Brier, log loss, balanced accuracy, calibration ECE/bins, and abstain coverage."""
    y = np.asarray(y_true, dtype=int)
    p = _clip_probs(np.asarray(y_prob, dtype=float))
    preds = (p > 0.5).astype(int)

    brier = float(brier_score_loss(y, p))
    ll = float(log_loss(y, p, labels=[0, 1]))
    bal_acc = float(balanced_accuracy_score(y, preds))
    acc = float(accuracy_score(y, preds))
    prec = float(precision_score(y, preds, zero_division=0))
    rec = float(recall_score(y, preds, zero_division=0))
    f1 = float(f1_score(y, preds, zero_division=0))

    # Abstention coverage metrics when NO_SIGNAL band |p - 0.5| < delta is applied
    active_mask = np.abs(p - 0.5) >= abstain_delta
    coverage = float(active_mask.mean()) if len(y) > 0 else 0.0
    if active_mask.any():
        cov_bal_acc = float(balanced_accuracy_score(y[active_mask], preds[active_mask]))
        cov_err = float(1.0 - accuracy_score(y[active_mask], preds[active_mask]))
    else:
        cov_bal_acc = 0.0
        cov_err = 1.0

    # Reliability calibration bins + ECE
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    cal_bins: list[dict[str, Any]] = []
    n_total = len(y)
    for b_idx in range(n_bins):
        lo, hi = float(edges[b_idx]), float(edges[b_idx + 1])
        if b_idx < n_bins - 1:
            mask = (p >= lo) & (p < hi)
        else:
            mask = (p >= lo) & (p <= hi)
        cnt = int(mask.sum())
        if cnt == 0:
            cal_bins.append(
                {
                    "bin_range": [round(lo, 2), round(hi, 2)],
                    "count": 0,
                    "mean_pred_prob": None,
                    "empirical_up_rate": None,
                }
            )
            continue
        mean_p = float(p[mask].mean())
        emp_rate = float(y[mask].mean())
        ece += (cnt / max(1, n_total)) * abs(emp_rate - mean_p)
        cal_bins.append(
            {
                "bin_range": [round(lo, 2), round(hi, 2)],
                "count": cnt,
                "mean_pred_prob": round(mean_p, 6),
                "empirical_up_rate": round(emp_rate, 6),
            }
        )

    return {
        "n_samples": int(n_total),
        "n_up": int((y == 1).sum()),
        "n_not_up": int((y == 0).sum()),
        "brier_score": round(brier, 6),
        "log_loss": round(ll, 6),
        "balanced_accuracy": round(bal_acc, 6),
        "accuracy": round(acc, 6),
        "precision": round(prec, 6),
        "recall": round(rec, 6),
        "f1": round(f1, 6),
        "ece": round(float(ece), 6),
        "calibration_bins": cal_bins,
        "abstain_band_delta": round(float(abstain_delta), 4),
        "coverage": round(coverage, 6),
        "covered_balanced_accuracy": round(cov_bal_acc, 6),
        "covered_error_rate": round(cov_err, 6),
    }


def block_bootstrap_brier_ci(
    timestamps: SequenceLike,
    y_true: NDArray[Any],
    y_prob: NDArray[Any],
    *,
    block_minutes: int = 5,
    n_bootstrap: int = 200,
    seed: int = 42,
) -> dict[str, Any]:
    """Estimate 95% confidence interval for Brier score by resampling contiguous time blocks."""
    y = np.asarray(y_true, dtype=int)
    p = _clip_probs(np.asarray(y_prob, dtype=float))
    ts = pd.to_datetime(timestamps, utc=True)
    sq_err = (p - y) ** 2
    block_keys = ts.floor(f"{block_minutes}min")
    tmp = pd.DataFrame({"block": block_keys, "sq_err": sq_err})
    block_groups = [grp["sq_err"].to_numpy() for _, grp in tmp.groupby("block", sort=True)]

    if len(block_groups) < 2:
        point = float(sq_err.mean())
        return {
            "point_brier": round(point, 6),
            "ci_low_95": round(point, 6),
            "ci_high_95": round(point, 6),
            "n_blocks": len(block_groups),
            "block_minutes": block_minutes,
            "n_bootstrap": 0,
        }

    rng = np.random.default_rng(seed)
    boot_scores = np.empty(n_bootstrap, dtype=float)
    n_blocks = len(block_groups)
    for idx in range(n_bootstrap):
        chosen = rng.integers(0, n_blocks, size=n_blocks)
        sampled = np.concatenate([block_groups[c] for c in chosen])
        boot_scores[idx] = float(sampled.mean())

    return {
        "point_brier": round(float(sq_err.mean()), 6),
        "ci_low_95": round(float(np.percentile(boot_scores, 2.5)), 6),
        "ci_high_95": round(float(np.percentile(boot_scores, 97.5)), 6),
        "n_blocks": n_blocks,
        "block_minutes": block_minutes,
        "n_bootstrap": n_bootstrap,
    }


class _ConstantProbabilityModel:
    """Fallback classifier when a small training fold contains a single target class."""

    def __init__(self, prior_prob: float) -> None:
        self.prior_prob = float(np.clip(prior_prob, 1e-6, 1.0 - 1e-6))

    def predict_proba(self, X: NDArray[Any]) -> NDArray[Any]:
        n = len(X)
        p_up = np.full(n, self.prior_prob, dtype=float)
        return np.column_stack([1.0 - p_up, p_up])


def _fit_logreg_model(
    X_train_scaled: NDArray[Any],
    y_train: NDArray[Any],
    *,
    seed: int,
) -> Any:
    if len(np.unique(y_train)) < 2:
        return _ConstantProbabilityModel(float(y_train.mean()))
    clf = LogisticRegression(max_iter=1000, C=1.0, random_state=seed)
    clf.fit(X_train_scaled, y_train)
    return clf


def _fit_xgb_model(
    X_train_scaled: NDArray[Any],
    y_train: NDArray[Any],
    *,
    hyperparams: dict[str, Any],
) -> Any:
    if len(np.unique(y_train)) < 2:
        return _ConstantProbabilityModel(float(y_train.mean()))

    fit_params = dict(hyperparams)
    # Remove outer early_stopping_rounds unless internal train tail split exists cleanly
    early_stop = fit_params.pop("early_stopping_rounds", None)
    if early_stop and len(y_train) >= 60:
        split_idx = int(len(y_train) * 0.85)
        X_tr, y_tr = X_train_scaled[:split_idx], y_train[:split_idx]
        X_es, y_es = X_train_scaled[split_idx:], y_train[split_idx:]
        if len(np.unique(y_tr)) >= 2 and len(np.unique(y_es)) >= 2:
            clf = xgb.XGBClassifier(
                **fit_params,
                early_stopping_rounds=int(early_stop),
                objective="binary:logistic",
                eval_metric="logloss",
            )
            clf.fit(X_tr, y_tr, eval_set=[(X_es, y_es)], verbose=False)
            return clf

    # Fixed iteration count (capped to avoid overfitting tiny folds without touching val_fold)
    fit_params["n_estimators"] = min(int(fit_params.get("n_estimators", 200)), 250)
    clf = xgb.XGBClassifier(
        **fit_params,
        objective="binary:logistic",
        eval_metric="logloss",
    )
    clf.fit(X_train_scaled, y_train, verbose=False)
    return clf


def _predict_up_prob(model: Any, X_scaled: NDArray[Any]) -> NDArray[Any]:
    probs = model.predict_proba(X_scaled)[:, 1]
    return _clip_probs(probs)


def _fit_platt_calibrator(
    raw_probs: NDArray[Any], y_true: NDArray[Any]
) -> LogisticRegression | None:
    if len(np.unique(y_true)) < 2 or len(y_true) < 10:
        return None
    p = _clip_probs(raw_probs)
    logits = np.log(p / (1.0 - p)).reshape(-1, 1)
    cal = LogisticRegression(C=1.0, max_iter=500, random_state=42)
    cal.fit(logits, y_true)
    return cal


def _apply_calibrator(
    calibrator: LogisticRegression | None, raw_probs: NDArray[Any]
) -> NDArray[Any]:
    p = _clip_probs(raw_probs)
    if calibrator is None:
        return p
    logits = np.log(p / (1.0 - p)).reshape(-1, 1)
    cal_p = calibrator.predict_proba(logits)[:, 1]
    return _clip_probs(cal_p)


def _select_abstain_delta_on_validation(
    y_val: NDArray[Any],
    cal_probs: NDArray[Any],
) -> float:
    """Select abstain half-band delta strictly on validation data; defaults to 0.0 (full coverage)."""
    best_delta = 0.0
    best_score = -1.0
    for delta in (0.0, 0.05, 0.10, 0.15):
        stats = compute_binary_classification_metrics(y_val, cal_probs, abstain_delta=delta)
        if stats["coverage"] < 0.40:
            continue
        score = float(stats["covered_balanced_accuracy"])
        if score > best_score + 1e-6:
            best_score = score
            best_delta = float(delta)
    return best_delta


def _per_ticker_breakdown(
    df_subset: pd.DataFrame,
    y_prob: NDArray[Any],
    *,
    abstain_delta: float,
) -> dict[str, Any]:
    breakdown: dict[str, Any] = {}
    tmp = df_subset[["ticker", "target_direction"]].copy()
    tmp["prob_up"] = y_prob
    for ticker, grp in tmp.groupby("ticker", sort=True):
        breakdown[str(ticker)] = compute_binary_classification_metrics(
            grp["target_direction"].to_numpy(dtype=int),
            grp["prob_up"].to_numpy(dtype=float),
            abstain_delta=abstain_delta,
        )
    return breakdown


def market_validation_gate(
    df: pd.DataFrame, *, is_real_market: bool, fold_count: int
) -> dict[str, Any]:
    """Evidence sufficiency gate; passing it does not establish predictive or trading skill."""
    dates = pd.to_datetime(df["bar_end"], utc=True).dt.tz_convert("America/New_York").dt.date
    sessions = df.assign(_session_date=dates).groupby("ticker")["_session_date"].nunique()
    reasons = []
    if not is_real_market:
        reasons.append("synthetic_data")
    if fold_count < 3:
        reasons.append("fewer_than_three_walk_forward_folds")
    if sessions.empty or int(sessions.min()) < 60:
        reasons.append("fewer_than_60_evaluated_sessions_per_ticker")
    return {
        "passed": not reasons,
        "reasons": reasons,
        "minimum_sessions_per_ticker": 60,
        "sessions_per_ticker": {str(t): int(n) for t, n in sessions.items()},
        "meaning": "Temporal evaluation completed with planned coverage; not evidence of market edge.",
    }


def run_walk_forward_evaluation(
    eval_df: pd.DataFrame,
    *,
    contract: ForecastContract | None = None,
    dataset_id: str = "unknown",
    dataset_provenance: str = "synthetic_smoke_fixture",
    data_checksum: str = "",
    random_seed: int = 42,
    n_folds: int = 3,
    holdout_fraction: float = 0.20,
    xgb_hyperparams: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run purged walk-forward comparison, fit validation calibration/threshold, and evaluate untouched holdout."""
    active = contract or load_forecast_contract()
    df = prepare_evaluation_dataframe(eval_df, contract=active, dataset_id=dataset_id)
    features = list(active.feature_policy.features)
    split_plan = build_temporal_splits(
        df,
        horizon_seconds=active.horizon_seconds,
        grace_seconds=active.grace_seconds,
        n_folds=n_folds,
        holdout_fraction=holdout_fraction,
    )

    hyperparams: dict[str, Any] = {
        "n_estimators": 200,
        "max_depth": 4,
        "learning_rate": 0.03,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": random_seed,
        "early_stopping_rounds": 30,
    }
    if xgb_hyperparams:
        hyperparams.update(xgb_hyperparams)

    model_names = ("prior_baseline", "logistic_regression", "xgboost")
    oof_probs: dict[str, list[NDArray[Any]]] = {m: [] for m in model_names}
    oof_y: list[NDArray[Any]] = []
    fold_reports: list[dict[str, Any]] = []

    for fold in split_plan.folds:
        train_mask = pd.to_datetime(df["bar_end"], utc=True).isin(fold.train_timestamps)
        val_mask = pd.to_datetime(df["bar_end"], utc=True).isin(fold.val_timestamps)
        tr_df = df.loc[train_mask]
        va_df = df.loc[val_mask]

        X_tr = tr_df[features].to_numpy(dtype=float)
        y_tr = tr_df["target_direction"].to_numpy(dtype=int)
        X_va = va_df[features].to_numpy(dtype=float)
        y_va = va_df["target_direction"].to_numpy(dtype=int)

        scaler = StandardScaler()
        X_tr_s = scaler.fit_transform(X_tr)
        X_va_s = scaler.transform(X_va)

        prior_clf = _ConstantProbabilityModel(float(y_tr.mean()))
        logreg_clf = _fit_logreg_model(X_tr_s, y_tr, seed=random_seed)
        xgb_clf = _fit_xgb_model(X_tr_s, y_tr, hyperparams=hyperparams)

        fold_probs = {
            "prior_baseline": _predict_up_prob(prior_clf, X_va_s),
            "logistic_regression": _predict_up_prob(logreg_clf, X_va_s),
            "xgboost": _predict_up_prob(xgb_clf, X_va_s),
        }
        oof_y.append(y_va)
        for m in model_names:
            oof_probs[m].append(fold_probs[m])

        fold_reports.append(
            {
                "fold_index": fold.fold_index,
                "train_start": fold.train_start.isoformat(),
                "train_end": fold.train_end.isoformat(),
                "val_start": fold.val_start.isoformat(),
                "val_end": fold.val_end.isoformat(),
                "train_samples": int(len(tr_df)),
                "val_samples": int(len(va_df)),
                "models": {
                    m: compute_binary_classification_metrics(y_va, fold_probs[m])
                    for m in model_names
                },
            }
        )

    all_oof_y = np.concatenate(oof_y)
    val_summary: dict[str, Any] = {}
    calibrators: dict[str, LogisticRegression | None] = {}
    abstain_deltas: dict[str, float] = {}

    for m in model_names:
        raw_oof = np.concatenate(oof_probs[m])
        raw_metrics = compute_binary_classification_metrics(all_oof_y, raw_oof)
        # Keep empirical prior baseline un-warped unless needed; fit Platt scaling strictly on OOF validation
        cal = None if m == "prior_baseline" else _fit_platt_calibrator(raw_oof, all_oof_y)
        cal_oof = _apply_calibrator(cal, raw_oof)
        delta = _select_abstain_delta_on_validation(all_oof_y, cal_oof)
        cal_metrics = compute_binary_classification_metrics(all_oof_y, cal_oof, abstain_delta=delta)
        calibrators[m] = cal
        abstain_deltas[m] = delta
        val_summary[m] = {
            "raw": raw_metrics,
            "calibrated": cal_metrics,
        }

    # Select champion strictly by lowest calibrated validation Brier score (simpler model breaks ties)
    champion_model = min(
        TIE_BREAK_ORDER,
        key=lambda m: float(val_summary[m]["calibrated"][PRIMARY_SELECTION_METRIC]),
    )
    challenger_models = [m for m in model_names if m != champion_model]

    # Refit pre-holdout models strictly on pre-holdout matured samples (target_at <= holdout_start)
    pre_mask = pd.to_datetime(df["bar_end"], utc=True).isin(split_plan.pre_holdout_timestamps)
    hold_mask = pd.to_datetime(df["bar_end"], utc=True).isin(split_plan.holdout_timestamps)
    pre_df = df.loc[pre_mask].copy()
    hold_df = df.loc[hold_mask].copy()

    X_pre = pre_df[features].to_numpy(dtype=float)
    y_pre = pre_df["target_direction"].to_numpy(dtype=int)
    X_hold = hold_df[features].to_numpy(dtype=float)
    y_hold = hold_df["target_direction"].to_numpy(dtype=int)

    final_scaler = StandardScaler()
    X_pre_s = final_scaler.fit_transform(X_pre)
    X_hold_s = final_scaler.transform(X_hold)

    final_prior = _ConstantProbabilityModel(float(y_pre.mean()))
    final_logreg = _fit_logreg_model(X_pre_s, y_pre, seed=random_seed)
    final_xgb = _fit_xgb_model(X_pre_s, y_pre, hyperparams=hyperparams)
    final_models = {
        "prior_baseline": final_prior,
        "logistic_regression": final_logreg,
        "xgboost": final_xgb,
    }

    # Evaluate untouched final holdout ONCE
    holdout_summary: dict[str, Any] = {}
    holdout_probs_by_model: dict[str, NDArray[Any]] = {}
    for m in model_names:
        raw_hold = _predict_up_prob(final_models[m], X_hold_s)
        cal_hold = _apply_calibrator(calibrators[m], raw_hold)
        holdout_probs_by_model[m] = cal_hold
        delta = abstain_deltas[champion_model if m == champion_model else m]
        metrics_m = compute_binary_classification_metrics(y_hold, cal_hold, abstain_delta=delta)
        metrics_m["block_bootstrap_ci"] = block_bootstrap_brier_ci(
            hold_df["bar_end"].tolist(),
            y_hold,
            cal_hold,
            seed=random_seed,
        )
        metrics_m["per_ticker"] = _per_ticker_breakdown(hold_df, cal_hold, abstain_delta=delta)
        holdout_summary[m] = metrics_m

    # Separate random-walk price/return comparator (NOT scored as probabilistic classifier)
    rw_dir = (hold_df["return_1m"].to_numpy(dtype=float) > 0.0).astype(int)
    rw_comparator = {
        "definition": "Previous 1m return persistence (sign(return_1m) > 0) vs realized 5m direction; reported separately from calibrated binary probability models.",
        "n_samples": int(len(y_hold)),
        "hit_rate": round(float(accuracy_score(y_hold, rw_dir)), 6),
        "balanced_accuracy": round(float(balanced_accuracy_score(y_hold, rw_dir)), 6),
    }

    is_real_market = dataset_provenance in {"real_historical", "real_live"}
    validation_gate = market_validation_gate(
        df, is_real_market=is_real_market, fold_count=len(split_plan.folds)
    )
    dataset_sha = (
        data_checksum
        or hashlib.sha256(
            pd.util.hash_pandas_object(df[["ticker", "bar_end", "close"]], index=False)
            .to_numpy()
            .tobytes()
        ).hexdigest()
    )

    report: dict[str, Any] = {
        "report_type": "walk_forward_evaluation",
        "contract_id": active.contract_id,
        "contract_version": active.version,
        "feature_mode": active.feature_policy.mode,
        "feature_names": features,
        "dataset_id": dataset_id,
        "dataset_provenance": dataset_provenance,
        "is_real_market_data": is_real_market,
        "real_market_validated": validation_gate["passed"],
        "validation_gate": validation_gate,
        "warning_banner": (
            None
            if is_real_market
            else "SYNTHETIC OR NON-PRODUCTION FIXTURE ONLY — DO NOT REPORT AS REAL MARKET PERFORMANCE."
        ),
        "data_checksum": dataset_sha,
        "selection_metric": PRIMARY_SELECTION_METRIC,
        "champion_model": champion_model,
        "challenger_models": challenger_models,
        "abstain_band_delta": abstain_deltas[champion_model],
        "sufficient_for_three_folds": split_plan.sufficient_for_three_folds,
        "cutoffs": {
            "purge_seconds": split_plan.purge_seconds,
            "walk_forward_folds": [
                {
                    "fold": f.fold_index,
                    "train_start": f.train_start.isoformat(),
                    "train_end": f.train_end.isoformat(),
                    "val_start": f.val_start.isoformat(),
                    "val_end": f.val_end.isoformat(),
                }
                for f in split_plan.folds
            ],
            "fit_information_end": split_plan.fit_information_end.isoformat(),
            "holdout_start": split_plan.holdout_start.isoformat(),
            "holdout_end": split_plan.holdout_end.isoformat(),
            "pre_holdout_samples": int(len(pre_df)),
            "holdout_samples": int(len(hold_df)),
        },
        "validation_summary": val_summary,
        "per_fold": fold_reports,
        "holdout_summary": holdout_summary,
        "comparators": {
            "random_walk_price": rw_comparator,
        },
        "random_seed": random_seed,
        "hyperparameters": hyperparams,
        "git_commit": _get_git_commit(),
        "environment": _runtime_environment(),
        "generated_at": datetime.now(UTC).isoformat(),
    }

    bundle_objects = {
        "scaler": final_scaler,
        "prior_baseline_prob": float(final_prior.prior_prob),
        "logistic_regression": final_logreg,
        "xgboost": final_xgb,
        "calibrator": calibrators[champion_model],
        "champion_model": champion_model,
        "abstain_band_delta": abstain_deltas[champion_model],
    }
    return report, bundle_objects


def publish_model_bundle(
    target_dir: str | Path,
    report: dict[str, Any],
    bundle_objects: dict[str, Any],
    *,
    contract: ForecastContract | None = None,
    model_version: str | None = None,
) -> tuple[Path, BundleManifest]:
    """Serialize artifacts, compute SHA-256 hashes, and atomically swap into target_dir."""
    active = contract or load_forecast_contract()
    dest = Path(target_dir)
    dest.parent.mkdir(parents=True, exist_ok=True)
    staging = dest.parent / f".{dest.name}.staging_{uuid.uuid4().hex[:8]}"
    staging.mkdir(parents=True, exist_ok=True)

    champion = str(bundle_objects["champion_model"])
    version_str = model_version or f"{champion}_{uuid.uuid4().hex[:12]}"
    bundle_id = f"{active.contract_id}_{report['dataset_id']}_{uuid.uuid4().hex[:8]}"

    try:
        # Always persist scaler + baseline JSON + logreg pickle + xgb JSON (when XGBClassifier)
        scaler_path = staging / "scaler.pkl"
        with scaler_path.open("wb") as f:
            pickle.dump(bundle_objects["scaler"], f)

        prior_path = staging / "prior_baseline.json"
        prior_path.write_text(
            json.dumps({"prior_prob_up": bundle_objects["prior_baseline_prob"]}, indent=2),
            encoding="utf-8",
        )

        logreg_path = staging / "logistic_regression.pkl"
        with logreg_path.open("wb") as f:
            pickle.dump(bundle_objects["logistic_regression"], f)

        xgb_obj = bundle_objects["xgboost"]
        if isinstance(xgb_obj, xgb.XGBClassifier):
            xgb_path = staging / "xgb_model.json"
            xgb_obj.get_booster().save_model(str(xgb_path))
        else:
            xgb_fallback_path = staging / "xgb_fallback.pkl"
            with xgb_fallback_path.open("wb") as f:
                pickle.dump(xgb_obj, f)

        if bundle_objects.get("calibrator") is not None:
            cal_path = staging / "calibrator.pkl"
            with cal_path.open("wb") as f:
                pickle.dump(bundle_objects["calibrator"], f)

        artifact_hashes = {
            item.name: compute_sha256(item)
            for item in sorted(staging.iterdir())
            if item.is_file() and item.name not in ("manifest.json", "metadata.json")
        }

        manifest = BundleManifest(
            bundle_id=bundle_id,
            model_version=version_str,
            champion_model=champion,  # type: ignore[arg-type]
            challenger_models=list(report["challenger_models"]),
            contract_id=active.contract_id,
            contract_version=active.version,
            feature_mode=active.feature_policy.mode,
            feature_names=list(active.feature_policy.features),
            dataset_id=str(report["dataset_id"]),
            dataset_provenance=str(report["dataset_provenance"]),
            is_real_market_data=bool(report["is_real_market_data"]),
            data_checksum=str(report["data_checksum"]),
            fit_information_end=_to_utc_datetime(report["cutoffs"]["fit_information_end"]),
            cutoffs=dict(report["cutoffs"]),
            random_seed=int(report["random_seed"]),
            selection_metric=str(report["selection_metric"]),
            decision_threshold=0.5,
            abstain_band_delta=float(report["abstain_band_delta"]),
            hyperparameters=dict(report["hyperparameters"]),
            git_commit=str(report["git_commit"]),
            environment=dict(report["environment"]),
            metrics={
                "validation": report["validation_summary"],
                "holdout": report["holdout_summary"],
                "per_fold": report["per_fold"],
                "comparators": report["comparators"],
            },
            artifact_hashes=artifact_hashes,
            created_at=datetime.now(UTC),
        )

        manifest_json = manifest.model_dump_json(indent=2)
        (staging / "manifest.json").write_text(manifest_json, encoding="utf-8")
        # Sync metadata.json with contract-stamped summary for existing loader tooling
        (staging / "metadata.json").write_text(manifest_json, encoding="utf-8")

        # Verify staging integrity before touching destination
        load_and_verify_bundle_manifest(staging, contract=active)

        # Keep immutable releases inside the mounted directory. Only the current pointer changes.
        dest.mkdir(parents=True, exist_ok=True)
        releases = dest / "releases"
        releases.mkdir(exist_ok=True)
        release = releases / bundle_id
        os.replace(staging, release)
        activate_bundle(dest, bundle_id, contract=active)
        return dest, manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def resolve_bundle_dir(bundle_dir: str | Path) -> Path:
    root = Path(bundle_dir)
    pointer = root / "current"
    if pointer.is_symlink():
        resolved = pointer.resolve(strict=True)
        if resolved.parent != (root / "releases").resolve():
            raise ContractMismatchError("Bundle pointer must reference a local release")
        return resolved
    return root


def activate_bundle(
    bundle_dir: str | Path, bundle_id: str, *, contract: ForecastContract | None = None
) -> Path:
    """Verify and atomically activate a retained release, also used for rollback."""
    root = Path(bundle_dir)
    if Path(bundle_id).name != bundle_id:
        raise ValueError("Invalid bundle id")
    release = root / "releases" / bundle_id
    load_and_verify_bundle_manifest(release, contract=contract)
    pending = root / f".current_{uuid.uuid4().hex}"
    try:
        pending.symlink_to(Path("releases") / bundle_id, target_is_directory=True)
        os.replace(pending, root / "current")
    finally:
        pending.unlink(missing_ok=True)
    return release


def load_and_verify_bundle_manifest(
    bundle_dir: str | Path,
    *,
    contract: ForecastContract | None = None,
) -> BundleManifest:
    """Validate bundle manifest contract metadata and verify SHA-256 checksums of all artifacts."""
    active = contract or load_forecast_contract()
    root = resolve_bundle_dir(bundle_dir)
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        raise ContractMismatchError(f"Bundle manifest missing at {manifest_path}")

    try:
        manifest = BundleManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContractMismatchError(f"Invalid bundle manifest JSON: {exc}") from exc

    validate_contract_compatibility(
        active,
        contract_id=manifest.contract_id,
        contract_version=manifest.contract_version,
        feature_mode=manifest.feature_mode,
        feature_names=manifest.feature_names,
    )

    if not manifest.artifact_hashes:
        raise ContractMismatchError("Bundle manifest artifact_hashes cannot be empty")

    artifacts = {
        item.name
        for item in root.iterdir()
        if item.is_file()
        and item.suffix in {".json", ".pkl"}
        and item.name not in {"manifest.json", "metadata.json"}
    }
    if artifacts != set(manifest.artifact_hashes):
        raise ContractMismatchError("Bundle artifact set does not match manifest")
    for filename, expected_hash in manifest.artifact_hashes.items():
        if Path(filename).name != filename:
            raise ContractMismatchError("Bundle artifact must be a local filename")
        target = root / filename
        if not target.exists():
            raise ContractMismatchError(f"Missing bundle artifact '{filename}' listed in manifest")
        actual_hash = compute_sha256(target)
        if actual_hash != expected_hash:
            raise ContractMismatchError(
                f"Artifact hash mismatch for '{filename}': expected {expected_hash}, got {actual_hash}"
            )

    return manifest


def write_evaluation_report(
    report: dict[str, Any],
    output_dir: str | Path = "reports/evaluation",
    *,
    prefix: str | None = None,
) -> tuple[Path, Path]:
    """Write atomic JSON + Markdown evaluation report files into output_dir."""
    out_root = Path(output_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    stem = prefix or f"{report['dataset_id']}_evaluation"
    json_path = out_root / f"{stem}.json"
    md_path = out_root / f"{stem}.md"

    tmp_json = out_root / f".{stem}.{uuid.uuid4().hex[:6]}.json.tmp"
    tmp_md = out_root / f".{stem}.{uuid.uuid4().hex[:6]}.md.tmp"
    tmp_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        f"# Evaluation Report — {report['dataset_id']}",
        "",
        f"> **Provenance:** `{report['dataset_provenance']}` | "
        f"**Real Market Data:** `{report['is_real_market_data']}`",
    ]
    if report.get("warning_banner"):
        lines.extend(["", f"> **WARNING:** {report['warning_banner']}"])
    lines.extend(
        [
            "",
            f"- **Contract:** `{report['contract_id']}` v`{report['contract_version']}` (`{report['feature_mode']}`)",
            f"- **Selection Metric:** `{report['selection_metric']}`",
            f"- **Champion Model (selected on validation):** `{report['champion_model']}`",
            f"- **Challenger Models:** `{', '.join(report['challenger_models'])}`",
            f"- **fit_information_end:** `{report['cutoffs']['fit_information_end']}`",
            f"- **Holdout Window:** `{report['cutoffs']['holdout_start']}` .. `{report['cutoffs']['holdout_end']}` "
            f"(samples={report['cutoffs']['holdout_samples']})",
            "",
            "## Holdout Results (Single Untouched Evaluation)",
            "",
            "| Model | Brier Score | Log Loss | Balanced Acc | ECE | Coverage |",
            "|---|---|---|---|---|---|",
        ]
    )
    for m, stats in report["holdout_summary"].items():
        lines.append(
            f"| `{m}` | {stats['brier_score']:.6f} | {stats['log_loss']:.6f} | "
            f"{stats['balanced_accuracy']:.4f} | {stats['ece']:.4f} | {stats['coverage']:.4f} |"
        )
    rw = report["comparators"]["random_walk_price"]
    lines.extend(
        [
            "",
            f"- **Random-Walk Comparator (price persistence, separate from binary probabilities):** "
            f"hit_rate={rw['hit_rate']:.4f}, balanced_accuracy={rw['balanced_accuracy']:.4f}",
            "",
        ]
    )
    tmp_md.write_text("\n".join(lines), encoding="utf-8")

    os.replace(tmp_json, json_path)
    os.replace(tmp_md, md_path)
    return json_path, md_path


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Run reproducible walk-forward ML evaluation.")
    parser.add_argument("--csv", required=True, help="Path to canonical 1m OHLCV bars CSV file")
    parser.add_argument("--manifest", required=False, help="Optional DatasetManifest JSON file")
    parser.add_argument("--dataset-id", default="fixture_smoke_v1")
    parser.add_argument("--provenance", default="synthetic_smoke_fixture")
    parser.add_argument("--output-dir", default="reports/evaluation")
    parser.add_argument("--bundle-dir", default="model/artefacts")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    csv_path = Path(args.csv)
    raw_df = pd.read_csv(csv_path)
    dataset_id = args.dataset_id
    provenance = args.provenance
    checksum = compute_sha256(csv_path)

    if args.manifest:
        ds_manifest = load_manifest(args.manifest)
        if checksum != ds_manifest.checksum_sha256:
            raise ValueError(
                f"CSV SHA-256 mismatch against manifest {args.manifest}: "
                f"{checksum} != {ds_manifest.checksum_sha256}"
            )
        if ds_manifest.schema_type != "ohlcv_1m_bars" or len(raw_df) != ds_manifest.record_count:
            raise ValueError("CSV schema/count does not match the manifest")
        if sorted(raw_df["ticker"].unique()) != sorted(ds_manifest.tickers):
            raise ValueError("CSV tickers do not match the manifest")
        dataset_id = ds_manifest.dataset_id
        provenance = ds_manifest.provenance

    report, bundle_objects = run_walk_forward_evaluation(
        raw_df,
        dataset_id=dataset_id,
        dataset_provenance=provenance,
        data_checksum=checksum,
        random_seed=args.seed,
    )
    write_evaluation_report(report, output_dir=args.output_dir)
    if args.bundle_dir:
        publish_model_bundle(args.bundle_dir, report, bundle_objects)


if __name__ == "__main__":
    _cli()
