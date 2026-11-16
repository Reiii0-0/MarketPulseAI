"""Single canonical authority for 1-minute bar finalization and feature snapshots.

Implements the canonical computation matching dbt's ``int_ohlcv_1min`` and
``int_technical_indicators`` SQL models so SQL and Python share one formula authority.
Finalized bars (``marts.final_bars``) and feature snapshots (``marts.feature_snapshots``)
are write-once/immutable; events arriving after ``bar_end + grace_seconds`` or after a bar
is already finalized are routed to ``marts.late_bar_corrections`` without mutating prior state.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd

from pipeline.contract import (
    ContractBar,
    ForecastContract,
    compute_anchor_timestamps,
    compute_bar_bounds,
    evaluate_target_outcome,
    load_forecast_contract,
)
from pipeline.replay import PartitionDeliveryProgress

NY_TIMEZONE = "America/New_York"


class ContractMismatchError(ValueError):
    """Raised when a model bundle, snapshot, or SQL input violates the active forecast contract."""


@dataclass(frozen=True, slots=True)
class FinalBarRecord:
    """Immutable finalized 1-minute UTC OHLCV bar record."""

    contract_id: str
    contract_version: str
    dataset_id: str
    run_id: str | None
    ticker: str
    bar_start: datetime
    bar_end: datetime
    available_at: datetime
    finalized_as_of: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    event_count: int
    late_event_count: int
    provenance: str


@dataclass(frozen=True, slots=True)
class LateBarCorrection:
    """Audit record for an event arriving after bar finalization grace cutoff."""

    contract_id: str
    dataset_id: str
    source_event_id: str
    run_id: str | None
    ticker: str
    bar_end: datetime
    event_at: datetime
    available_at: datetime
    grace_cutoff: datetime
    correction_reason: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FeatureSnapshotRecord:
    """Immutable canonical feature snapshot stamped with contract version and SHA-256 hash."""

    contract_id: str
    contract_version: str
    dataset_id: str
    run_id: str | None
    ticker: str
    bar_start: datetime
    bar_end: datetime
    decision_at: datetime
    target_at: datetime
    feature_mode: str
    feature_hash: str
    warmup_complete: bool
    contiguous_bar_count: int
    features: dict[str, float | None]


# Parameterized SQL templates mirroring dbt marts logic and enforcing immutability.

FINAL_BAR_INSERT_SQL = """
INSERT INTO marts.final_bars (
    contract_id,
    contract_version,
    dataset_id,
    run_id,
    ticker,
    bar_start,
    bar_end,
    available_at,
    finalized_as_of,
    open,
    high,
    low,
    close,
    volume,
    event_count,
    late_event_count,
    provenance
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (run_id, contract_id, dataset_id, ticker, bar_end) DO NOTHING
RETURNING id
"""

LATE_CORRECTION_INSERT_SQL = """
INSERT INTO marts.late_bar_corrections (
    contract_id,
    dataset_id,
    source_event_id,
    run_id,
    ticker,
    bar_end,
    event_at,
    available_at,
    grace_cutoff,
    correction_reason,
    payload
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
)
ON CONFLICT (run_id, contract_id, dataset_id, source_event_id) DO NOTHING
"""

FEATURE_SNAPSHOT_INSERT_SQL = """
INSERT INTO marts.feature_snapshots (
    contract_id,
    contract_version,
    dataset_id,
    run_id,
    ticker,
    bar_start,
    bar_end,
    decision_at,
    target_at,
    feature_mode,
    feature_hash,
    warmup_complete,
    contiguous_bar_count,
    features,
    open,
    high,
    low,
    close,
    volume,
    return_1m,
    return_5m_past,
    return_15m_past,
    sma_5,
    sma_20,
    sma_spread_5_20,
    volatility_20,
    volume_ratio_20,
    bar_range_pct
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (run_id, contract_id, dataset_id, ticker, bar_end) DO NOTHING
RETURNING id
"""

CANONICAL_FINAL_BAR_FEATURES_SQL = """
WITH bars AS (
    SELECT
        contract_id,
        contract_version,
        dataset_id,
        run_id,
        ticker,
        bar_start,
        bar_end,
        available_at,
        (bar_start AT TIME ZONE 'America/New_York')::date AS session_date_ny,
        open::DOUBLE PRECISION AS open,
        high::DOUBLE PRECISION AS high,
        low::DOUBLE PRECISION AS low,
        close::DOUBLE PRECISION AS close,
        volume::BIGINT AS volume,
        LAG(bar_end) OVER (
            PARTITION BY contract_id, dataset_id, ticker, (bar_start AT TIME ZONE 'America/New_York')::date
            ORDER BY bar_end ASC
        ) AS prev_bar_end
    FROM marts.final_bars
    WHERE contract_id = %s
      AND contract_version = %s
      AND dataset_id = %s
      AND run_id IS NOT DISTINCT FROM %s
),
segments AS (
    SELECT
        *,
        CASE
            WHEN prev_bar_end IS NOT NULL AND bar_end = prev_bar_end + INTERVAL '1 minute' THEN 0
            ELSE 1
        END AS is_new_segment
    FROM bars
),
segmented AS (
    SELECT
        *,
        SUM(is_new_segment) OVER (
            PARTITION BY contract_id, dataset_id, ticker, session_date_ny
            ORDER BY bar_end ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS segment_id
    FROM segments
),
with_lags AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY contract_id, dataset_id, ticker, session_date_ny, segment_id
            ORDER BY bar_end ASC
        )::INTEGER AS contiguous_bar_count,
        LAG(close, 1) OVER (
            PARTITION BY contract_id, dataset_id, ticker, session_date_ny, segment_id
            ORDER BY bar_end ASC
        ) AS close_lag_1,
        LAG(close, 5) OVER (
            PARTITION BY contract_id, dataset_id, ticker, session_date_ny, segment_id
            ORDER BY bar_end ASC
        ) AS close_lag_5,
        LAG(close, 15) OVER (
            PARTITION BY contract_id, dataset_id, ticker, session_date_ny, segment_id
            ORDER BY bar_end ASC
        ) AS close_lag_15
    FROM segmented
),
with_windows AS (
    SELECT
        *,
        CASE
            WHEN contiguous_bar_count >= 2 AND close_lag_1 IS NOT NULL AND close_lag_1 > 0
            THEN (close - close_lag_1) / close_lag_1
        END AS return_1m,
        CASE
            WHEN contiguous_bar_count >= 6 AND close_lag_5 IS NOT NULL AND close_lag_5 > 0
            THEN (close - close_lag_5) / close_lag_5
        END AS return_5m_past,
        CASE
            WHEN contiguous_bar_count >= 16 AND close_lag_15 IS NOT NULL AND close_lag_15 > 0
            THEN (close - close_lag_15) / close_lag_15
        END AS return_15m_past,
        CASE
            WHEN contiguous_bar_count >= 5
            THEN AVG(close) OVER (
                PARTITION BY contract_id, dataset_id, ticker, session_date_ny, segment_id
                ORDER BY bar_end ASC
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            )
        END AS sma_5,
        CASE
            WHEN contiguous_bar_count >= 20
            THEN AVG(close) OVER (
                PARTITION BY contract_id, dataset_id, ticker, session_date_ny, segment_id
                ORDER BY bar_end ASC
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
        END AS sma_20,
        CASE
            WHEN contiguous_bar_count >= 20
            THEN STDDEV_SAMP(
                CASE WHEN close_lag_1 IS NOT NULL AND close_lag_1 > 0 THEN (close - close_lag_1) / close_lag_1 END
            ) OVER (
                PARTITION BY contract_id, dataset_id, ticker, session_date_ny, segment_id
                ORDER BY bar_end ASC
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
        END AS volatility_20,
        CASE
            WHEN contiguous_bar_count >= 20
            THEN AVG(volume::DOUBLE PRECISION) OVER (
                PARTITION BY contract_id, dataset_id, ticker, session_date_ny, segment_id
                ORDER BY bar_end ASC
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
        END AS avg_volume_20
    FROM with_lags
)
SELECT
    contract_id,
    contract_version,
    dataset_id,
    run_id,
    ticker,
    bar_start,
    bar_end,
    bar_end + %s * INTERVAL '1 second' AS decision_at,
    bar_end + %s * INTERVAL '1 second' AS target_at,
    contiguous_bar_count,
    (contiguous_bar_count >= %s) AS warmup_complete,
    open,
    high,
    low,
    close,
    volume,
    return_1m,
    return_5m_past,
    return_15m_past,
    sma_5,
    sma_20,
    CASE
        WHEN sma_5 IS NOT NULL AND sma_20 IS NOT NULL AND sma_20 <> 0
        THEN (sma_5 - sma_20) / sma_20
    END AS sma_spread_5_20,
    volatility_20,
    CASE
        WHEN contiguous_bar_count >= 20 AND avg_volume_20 IS NOT NULL AND avg_volume_20 > 0
        THEN volume::DOUBLE PRECISION / avg_volume_20
    END AS volume_ratio_20,
    CASE
        WHEN close > 0 THEN (high - low) / close
    END AS bar_range_pct
FROM with_windows
ORDER BY bar_end ASC, ticker ASC
"""

TRAINING_SNAPSHOT_JOIN_SQL = """
SELECT
    s.contract_id,
    s.contract_version,
    s.dataset_id,
    s.ticker,
    s.bar_start,
    s.bar_end,
    s.decision_at,
    s.target_at,
    s.feature_mode,
    s.feature_hash,
    s.warmup_complete,
    s.contiguous_bar_count,
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume,
    s.return_1m,
    s.return_5m_past,
    s.return_15m_past,
    s.sma_5,
    s.sma_20,
    s.sma_spread_5_20,
    s.volatility_20,
    s.volume_ratio_20,
    s.bar_range_pct,
    (t.close - s.close)::DOUBLE PRECISION / s.close AS return_5m,
    CASE WHEN t.close > s.close THEN 1 ELSE 0 END AS target_direction
FROM marts.feature_snapshots s
INNER JOIN marts.final_bars t
   ON s.contract_id = t.contract_id
  AND s.dataset_id = t.dataset_id
  AND s.run_id IS NOT DISTINCT FROM t.run_id
  AND s.ticker = t.ticker
  AND s.target_at = t.bar_end
INNER JOIN raw.ingestion_runs r ON r.run_id = s.run_id
WHERE s.contract_id = %s
  AND s.contract_version = %s
  AND s.run_id = %s
  AND s.warmup_complete = TRUE
  AND s.close > 0
  AND s.target_at + INTERVAL '15 seconds' <= COALESCE(r.logical_as_of, NOW())
  AND (s.bar_end AT TIME ZONE 'America/New_York')::date = (t.bar_end AT TIME ZONE 'America/New_York')::date
  AND (s.bar_end AT TIME ZONE 'America/New_York')::time > TIME '09:30:00'
  AND (t.bar_end AT TIME ZONE 'America/New_York')::time <= TIME '16:00:00'
ORDER BY s.bar_end ASC, s.ticker ASC
"""


def _ensure_utc(dt: datetime | pd.Timestamp | str) -> datetime:
    if isinstance(dt, str):
        parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    elif isinstance(dt, pd.Timestamp):
        parsed = dt.to_pydatetime()
    else:
        parsed = dt
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        val = float(value)
    else:
        val = float(value)
    if np.isnan(val) or np.isinf(val):
        return None
    return val


def validate_contract_compatibility(
    contract: ForecastContract,
    *,
    contract_id: str | None = None,
    contract_version: str | None = None,
    feature_mode: str | None = None,
    feature_names: Sequence[str] | None = None,
) -> None:
    """Validate that metadata/SQL/snapshot aligns strictly with the active ForecastContract."""
    if contract_id is not None and contract_id != contract.contract_id:
        raise ContractMismatchError(
            f"Contract ID mismatch: expected '{contract.contract_id}', got '{contract_id}'"
        )
    if contract_version is not None and contract_version != contract.version:
        raise ContractMismatchError(
            f"Contract version mismatch: expected '{contract.version}', got '{contract_version}'"
        )
    if feature_mode is not None and feature_mode != contract.feature_policy.mode:
        raise ContractMismatchError(
            f"Feature mode mismatch: expected '{contract.feature_policy.mode}', got '{feature_mode}'"
        )
    if feature_names is not None and list(feature_names) != list(contract.feature_policy.features):
        raise ContractMismatchError(
            "Feature list mismatch with active contract: "
            f"expected {contract.feature_policy.features}, got {list(feature_names)}"
        )


def compute_feature_hash(
    contract: ForecastContract,
    features: Mapping[str, float | int | Decimal | None],
    *,
    preserve_signed_zero: bool = False,
) -> str:
    """Compute a deterministic SHA-256 hash over contract metadata and canonical feature values."""
    ordered_values: list[list[Any]] = []
    for name in contract.feature_policy.features:
        if name not in features:
            raise ContractMismatchError(
                f"Cannot hash snapshot missing canonical contract feature '{name}'"
            )
        norm = _normalize_numeric(features[name])
        rounded = round(norm, 10) if norm is not None else None
        if rounded == 0 and not preserve_signed_zero:
            rounded = 0.0
        ordered_values.append([name, rounded])

    payload = {
        "contract_id": contract.contract_id,
        "contract_version": contract.version,
        "feature_mode": contract.feature_policy.mode,
        "features": ordered_values,
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def verify_snapshot_integrity(
    snapshot: FeatureSnapshotRecord,
    *,
    contract: ForecastContract | None = None,
) -> None:
    """Reject a FeatureSnapshotRecord if its contract metadata or feature hash fails verification."""
    active = contract or load_forecast_contract()
    validate_contract_compatibility(
        active,
        contract_id=snapshot.contract_id,
        contract_version=snapshot.contract_version,
        feature_mode=snapshot.feature_mode,
        feature_names=[
            name for name in active.feature_policy.features if name in snapshot.features
        ],
    )
    if set(snapshot.features) != set(active.feature_policy.features):
        raise ContractMismatchError("Snapshot feature set does not match contract")
    expected_hash = compute_feature_hash(active, snapshot.features)
    if snapshot.feature_hash != expected_hash and snapshot.feature_hash != compute_feature_hash(
        active, snapshot.features, preserve_signed_zero=True
    ):
        raise ContractMismatchError(
            f"Snapshot feature_hash mismatch for {snapshot.ticker} @ {snapshot.bar_end.isoformat()}: "
            f"expected {expected_hash}, got {snapshot.feature_hash}"
        )


def compute_canonical_features_from_bars(
    bars_df: pd.DataFrame,
    *,
    contract: ForecastContract | None = None,
) -> pd.DataFrame:
    """Compute canonical mp_1m_5m_price_v1 features from finalized 1-minute OHLCV bars.

    Mirrors ``int_technical_indicators.sql`` and ``CANONICAL_FINAL_BAR_FEATURES_SQL`` exactly:
    - Enforces regular session hours in ``contract.session_rules``
    - Isolates contiguous 1-minute segments (gaps and overnight transitions start a new segment)
    - Enforces explicit warm-up (``contiguous_bar_count >= contract.warmup_bars``)
    """
    active = contract or load_forecast_contract()
    required = {"ticker", "bar_start", "bar_end", "open", "high", "low", "close", "volume"}
    missing = required - set(bars_df.columns)
    if missing:
        raise ValueError(f"Missing required canonical bar columns: {sorted(missing)}")

    if bars_df.empty:
        return pd.DataFrame(
            columns=[
                "contract_id",
                "contract_version",
                "dataset_id",
                "run_id",
                "ticker",
                "bar_start",
                "bar_end",
                "decision_at",
                "target_at",
                "session_date_ny",
                "segment_id",
                "contiguous_bar_count",
                "warmup_complete",
                "feature_mode",
                "feature_hash",
                *active.feature_policy.features,
            ]
        )

    df = bars_df.copy()
    if "dataset_id" not in df.columns:
        df["dataset_id"] = "default"
    if "run_id" not in df.columns:
        df["run_id"] = None

    df["bar_start"] = pd.to_datetime(df["bar_start"], utc=True)
    df["bar_end"] = pd.to_datetime(df["bar_end"], utc=True)
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

    # Keep only bars strictly within regular NY session and matching canonical interval
    bar_delta = df["bar_end"] - df["bar_start"]
    in_session_mask = (bar_delta == pd.Timedelta(seconds=active.bar_interval_seconds)) & df.apply(
        lambda row: (
            active.session_rules.is_bar_in_session(row["bar_end"].to_pydatetime())
            and row["bar_start"].tz_convert(NY_TIMEZONE).date()
            == (row["bar_end"] - pd.Timedelta(microseconds=1)).tz_convert(NY_TIMEZONE).date()
        ),
        axis=1,
    )
    df = df.loc[in_session_mask].copy()
    if df.empty:
        return compute_canonical_features_from_bars(df.iloc[:0], contract=active)

    df["session_date_ny"] = df["bar_start"].dt.tz_convert(NY_TIMEZONE).dt.date
    df = df.sort_values(["dataset_id", "ticker", "session_date_ny", "bar_end"]).reset_index(
        drop=True
    )

    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype(float)
    df["volume"] = df["volume"].astype(np.int64)

    prev_bar_end = df.groupby(["dataset_id", "ticker", "session_date_ny"])["bar_end"].shift(1)
    is_new_segment = (df["bar_end"] - prev_bar_end != pd.Timedelta(minutes=1)).astype(int)
    df["segment_id"] = is_new_segment.groupby(
        [df["dataset_id"], df["ticker"], df["session_date_ny"]]
    ).cumsum()

    seg_keys = ["dataset_id", "ticker", "session_date_ny", "segment_id"]
    df["contiguous_bar_count"] = df.groupby(seg_keys).cumcount() + 1
    df["warmup_complete"] = df["contiguous_bar_count"] >= active.warmup_bars

    seg_group = df.groupby(seg_keys, group_keys=False)

    lag_1 = seg_group["close"].shift(1)
    lag_5 = seg_group["close"].shift(5)
    lag_15 = seg_group["close"].shift(15)

    df["return_1m"] = np.where(lag_1 > 0, (df["close"] - lag_1) / lag_1, np.nan)
    df["return_5m_past"] = np.where(lag_5 > 0, (df["close"] - lag_5) / lag_5, np.nan)
    df["return_15m_past"] = np.where(lag_15 > 0, (df["close"] - lag_15) / lag_15, np.nan)

    df["sma_5"] = seg_group["close"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    df["sma_20"] = seg_group["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["sma_spread_5_20"] = np.where(
        (df["sma_5"].notna()) & (df["sma_20"].notna()) & (df["sma_20"] != 0),
        (df["sma_5"] - df["sma_20"]) / df["sma_20"],
        np.nan,
    )

    # Gate volatility_20 to contiguous_bar_count >= 20 so min 19 non-null 1m returns match PostgreSQL STDDEV_SAMP
    raw_vol_20 = seg_group["return_1m"].transform(
        lambda s: s.rolling(20, min_periods=2).std(ddof=1)
    )
    df["volatility_20"] = np.where(df["contiguous_bar_count"] >= 20, raw_vol_20, np.nan)

    avg_vol_20 = seg_group["volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["volume_ratio_20"] = np.where(
        (df["contiguous_bar_count"] >= 20) & (avg_vol_20.notna()) & (avg_vol_20 > 0),
        df["volume"] / avg_vol_20,
        np.nan,
    )
    df["bar_range_pct"] = np.where(
        df["close"] > 0,
        (df["high"] - df["low"]) / df["close"],
        np.nan,
    )

    df["contract_id"] = active.contract_id
    df["contract_version"] = active.version
    df["feature_mode"] = active.feature_policy.mode

    anchors = [compute_anchor_timestamps(ts.to_pydatetime(), active) for ts in df["bar_end"]]
    df["decision_at"] = pd.to_datetime([dec for dec, _ in anchors], utc=True)
    df["target_at"] = pd.to_datetime([tgt for _, tgt in anchors], utc=True)

    hashes: list[str] = []
    for row in df.to_dict(orient="records"):
        feat_map = {name: _normalize_numeric(row[name]) for name in active.feature_policy.features}
        hashes.append(compute_feature_hash(active, feat_map))
    df["feature_hash"] = hashes

    return df


def dataframe_to_snapshots(
    features_df: pd.DataFrame,
    *,
    contract: ForecastContract | None = None,
) -> list[FeatureSnapshotRecord]:
    """Convert canonical feature dataframe rows into immutable FeatureSnapshotRecord objects."""
    active = contract or load_forecast_contract()
    snapshots: list[FeatureSnapshotRecord] = []
    for row in features_df.to_dict(orient="records"):
        validate_contract_compatibility(
            active,
            contract_id=str(row["contract_id"]),
            contract_version=str(row["contract_version"]),
            feature_mode=str(row["feature_mode"]),
        )
        feat_map = {name: _normalize_numeric(row[name]) for name in active.feature_policy.features}
        snapshots.append(
            FeatureSnapshotRecord(
                contract_id=str(row["contract_id"]),
                contract_version=str(row["contract_version"]),
                dataset_id=str(row["dataset_id"]),
                run_id=row.get("run_id"),
                ticker=str(row["ticker"]),
                bar_start=_ensure_utc(row["bar_start"]),
                bar_end=_ensure_utc(row["bar_end"]),
                decision_at=_ensure_utc(row["decision_at"]),
                target_at=_ensure_utc(row["target_at"]),
                feature_mode=str(row["feature_mode"]),
                feature_hash=str(row["feature_hash"]),
                warmup_complete=bool(row["warmup_complete"]),
                contiguous_bar_count=int(row["contiguous_bar_count"]),
                features=feat_map,
            )
        )
    return snapshots


class BarFinalizationService:
    """Finalizes 1-minute bars after grace + delivery barriers and freezes feature snapshots.

    Once a bar ``(contract_id, dataset_id, ticker, bar_end)`` is finalized in
    ``marts.final_bars`` or a snapshot is saved in ``marts.feature_snapshots``, subsequent
    late arrivals or future appends cannot mutate them; post-grace arrivals are logged in
    ``marts.late_bar_corrections`` for auditability.
    """

    def __init__(
        self,
        contract: ForecastContract | None = None,
        db_manager: Any | None = None,
    ) -> None:
        self.contract = contract or load_forecast_contract()
        self.db_manager = db_manager
        self._final_bars: dict[tuple[str | None, str, str, str, datetime], FinalBarRecord] = {}
        self._snapshots: dict[
            tuple[str | None, str, str, str, datetime], FeatureSnapshotRecord
        ] = {}
        self._late_corrections: dict[tuple[str | None, str, str, str], LateBarCorrection] = {}

    @property
    def final_bars(self) -> dict[tuple[str | None, str, str, str, datetime], FinalBarRecord]:
        return dict(self._final_bars)

    @property
    def snapshots(self) -> dict[tuple[str | None, str, str, str, datetime], FeatureSnapshotRecord]:
        return dict(self._snapshots)

    @property
    def late_corrections(self) -> dict[tuple[str | None, str, str, str], LateBarCorrection]:
        return dict(self._late_corrections)

    def restore_run(self, dataset_id: str, run_id: str | None) -> None:
        """Reload immutable state so restart never recomputes already frozen bars."""
        if self.db_manager is None:
            return
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT contract_id, contract_version, dataset_id, run_id, ticker,
                           bar_start, bar_end, available_at, finalized_as_of,
                           open, high, low, close, volume, event_count, late_event_count, provenance
                    FROM marts.final_bars
                    WHERE contract_id = %s AND dataset_id = %s
                      AND run_id IS NOT DISTINCT FROM %s
                """,
                    (self.contract.contract_id, dataset_id, run_id),
                )
                names = [d[0] for d in cur.description]
                for row in cur.fetchall():
                    values = dict(zip(names, row, strict=True))
                    for name in ("open", "high", "low", "close"):
                        values[name] = float(values[name])
                    bar = FinalBarRecord(**values)
                    self._final_bars[
                        (run_id, bar.contract_id, dataset_id, bar.ticker, bar.bar_end)
                    ] = bar
                cur.execute(
                    """
                    SELECT contract_id, contract_version, dataset_id, run_id, ticker,
                           bar_start, bar_end, decision_at, target_at, feature_mode,
                           feature_hash, warmup_complete, contiguous_bar_count, features
                    FROM marts.feature_snapshots
                    WHERE contract_id = %s AND dataset_id = %s
                      AND run_id IS NOT DISTINCT FROM %s
                """,
                    (self.contract.contract_id, dataset_id, run_id),
                )
                names = [d[0] for d in cur.description]
                for row in cur.fetchall():
                    snap = FeatureSnapshotRecord(**dict(zip(names, row, strict=True)))
                    self._snapshots[
                        (run_id, snap.contract_id, dataset_id, snap.ticker, snap.bar_end)
                    ] = snap

    def can_finalize_bar(
        self,
        bar_end: datetime,
        *,
        logical_as_of: datetime,
        delivery_progress: Sequence[PartitionDeliveryProgress] = (),
        run_status: str = "running",
    ) -> bool:
        """Check grace cutoff AND partition delivery barrier before finalizing ``bar_end``."""
        bar_end_utc = _ensure_utc(bar_end)
        as_of_utc = _ensure_utc(logical_as_of)
        grace_cutoff = bar_end_utc + timedelta(seconds=self.contract.grace_seconds)

        if as_of_utc < grace_cutoff:
            return False

        if not delivery_progress or run_status == "completed":
            return True

        # Wait until every active partition with progress has delivered past grace_cutoff (or completed)
        for progress in delivery_progress:
            if progress.status == "completed":
                continue
            if progress.last_available_at is None:
                return False
            if _ensure_utc(progress.last_available_at) < grace_cutoff:
                return False
        return True

    def finalize_events(
        self,
        events: Sequence[Mapping[str, Any]],
        *,
        dataset_id: str,
        run_id: str | None,
        logical_as_of: datetime,
        delivery_progress: Sequence[PartitionDeliveryProgress] = (),
        run_status: str = "running",
        provenance: str = "fixture_replay",
    ) -> tuple[list[FinalBarRecord], list[LateBarCorrection]]:
        """Finalize eligible 1m bars and route post-grace/post-finalization events to corrections.

        Existing entries in ``final_bars`` are strictly immutable: a late event targeting an
        already-finalized bar never mutates its OHLCV or volume and instead appends an audit
        entry to ``late_bar_corrections``.
        """
        as_of_utc = _ensure_utc(logical_as_of)
        on_time_by_key: dict[tuple[str, datetime, datetime], list[dict[str, Any]]] = {}
        new_corrections: list[LateBarCorrection] = []

        seen_events: set[str] = set()
        for raw_event in sorted(
            events,
            key=lambda ev: _ensure_utc(
                ev.get("available_at") or ev.get("event_at") or ev["event_timestamp"]
            ),
        ):
            ticker = str(raw_event["ticker"]).strip().upper()
            event_at = _ensure_utc(raw_event.get("event_at") or raw_event["event_timestamp"])
            available_at = _ensure_utc(raw_event.get("available_at") or event_at)
            if raw_event.get("bar_start") is not None and raw_event.get("bar_end") is not None:
                bar_start = _ensure_utc(raw_event["bar_start"])
                bar_end = _ensure_utc(raw_event["bar_end"])
                if bar_end - bar_start != timedelta(seconds=self.contract.bar_interval_seconds):
                    raise ValueError("OHLCV interval does not match forecast contract")
            else:
                bar_start, bar_end = compute_bar_bounds(
                    event_at, self.contract.bar_interval_seconds
                )
            if available_at > as_of_utc:
                continue
            grace_cutoff = bar_end + timedelta(seconds=self.contract.grace_seconds)
            source_event_id = str(
                raw_event.get("source_event_id")
                or f"{ticker}:{event_at.isoformat()}:{raw_event.get('price') or raw_event.get('close')}"
            )

            if source_event_id in seen_events:
                continue
            seen_events.add(source_event_id)

            # Reject off-session ticks/bars from canonical regular-session bar stream
            if not self.contract.session_rules.is_bar_in_session(bar_end):
                continue

            bar_identity = (run_id, self.contract.contract_id, dataset_id, ticker, bar_end)
            already_finalized = bar_identity in self._final_bars
            arrived_late = available_at > grace_cutoff

            if already_finalized and not arrived_late:
                continue
            if arrived_late:
                reason = "post_finalization_arrival" if already_finalized else "after_grace_cutoff"
                corr = LateBarCorrection(
                    contract_id=self.contract.contract_id,
                    dataset_id=dataset_id,
                    source_event_id=source_event_id,
                    run_id=run_id,
                    ticker=ticker,
                    bar_end=bar_end,
                    event_at=event_at,
                    available_at=available_at,
                    grace_cutoff=grace_cutoff,
                    correction_reason=reason,
                    payload=dict(raw_event),
                )
                corr_key = (run_id, self.contract.contract_id, dataset_id, source_event_id)
                if corr_key not in self._late_corrections:
                    self._late_corrections[corr_key] = corr
                    new_corrections.append(corr)
                continue

            on_time_by_key.setdefault((ticker, bar_start, bar_end), []).append(dict(raw_event))

        newly_finalized: list[FinalBarRecord] = []
        for (ticker, bar_start, bar_end), bucket_events in sorted(
            on_time_by_key.items(), key=lambda item: (item[0][2], item[0][0])
        ):
            bar_identity = (run_id, self.contract.contract_id, dataset_id, ticker, bar_end)
            if bar_identity in self._final_bars:
                continue
            if not self.can_finalize_bar(
                bar_end,
                logical_as_of=as_of_utc,
                delivery_progress=delivery_progress,
                run_status=run_status,
            ):
                continue

            sorted_events = sorted(
                bucket_events,
                key=lambda ev: (
                    _ensure_utc(ev.get("event_at") or ev["event_timestamp"]),
                    str(ev.get("source_event_id", "")),
                ),
            )
            prices = [float(ev.get("price") or ev["close"]) for ev in sorted_events]
            highs = [
                float(ev.get("high") or ev.get("price") or ev["close"]) for ev in sorted_events
            ]
            lows = [float(ev.get("low") or ev.get("price") or ev["close"]) for ev in sorted_events]
            opens = [
                float(ev.get("open") or ev.get("price") or ev["close"]) for ev in sorted_events
            ]
            max_avail = max(
                _ensure_utc(ev.get("available_at") or ev.get("event_at") or ev["event_timestamp"])
                for ev in sorted_events
            )
            record = FinalBarRecord(
                contract_id=self.contract.contract_id,
                contract_version=self.contract.version,
                dataset_id=dataset_id,
                run_id=run_id,
                ticker=ticker,
                bar_start=bar_start,
                bar_end=bar_end,
                available_at=max(max_avail, bar_end),
                finalized_as_of=as_of_utc,
                open=opens[0],
                high=max(highs),
                low=min(lows),
                close=prices[-1],
                volume=sum(int(ev["volume"]) for ev in sorted_events),
                event_count=len(sorted_events),
                late_event_count=0,
                provenance=provenance,
            )
            self._final_bars[bar_identity] = record
            newly_finalized.append(record)

        if self.db_manager is not None:
            self._persist_bars_and_corrections(newly_finalized, new_corrections)

        return newly_finalized, new_corrections

    def freeze_feature_snapshots(
        self,
        *,
        dataset_id: str | None = None,
        run_id: str | None = None,
    ) -> list[FeatureSnapshotRecord]:
        """Compute canonical features from immutable final bars and write once per identity."""
        bars = [
            bar
            for bar in self._final_bars.values()
            if (dataset_id is None or bar.dataset_id == dataset_id) and bar.run_id == run_id
        ]
        if not bars:
            return []

        bars_df = pd.DataFrame(
            [
                {
                    "dataset_id": b.dataset_id,
                    "run_id": b.run_id,
                    "ticker": b.ticker,
                    "bar_start": b.bar_start,
                    "bar_end": b.bar_end,
                    "open": b.open,
                    "high": b.high,
                    "low": b.low,
                    "close": b.close,
                    "volume": b.volume,
                }
                for b in bars
            ]
        )
        if self.db_manager is not None:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        CANONICAL_FINAL_BAR_FEATURES_SQL,
                        (
                            self.contract.contract_id,
                            self.contract.version,
                            dataset_id,
                            run_id,
                            self.contract.grace_seconds,
                            self.contract.horizon_seconds,
                            self.contract.warmup_bars,
                        ),
                    )
                    computed_df = pd.DataFrame(
                        cur.fetchall(), columns=[d[0] for d in cur.description]
                    )
            computed_df["feature_mode"] = self.contract.feature_policy.mode
            computed_df["feature_hash"] = [
                compute_feature_hash(self.contract, row) for row in computed_df.to_dict("records")
            ]
        else:
            computed_df = compute_canonical_features_from_bars(bars_df, contract=self.contract)
        candidates = dataframe_to_snapshots(computed_df, contract=self.contract)

        newly_frozen: list[FeatureSnapshotRecord] = []
        for snap in candidates:
            key = (snap.run_id, snap.contract_id, snap.dataset_id, snap.ticker, snap.bar_end)
            if key not in self._snapshots:
                self._snapshots[key] = snap
                newly_frozen.append(snap)

        if self.db_manager is not None and newly_frozen:
            self._persist_snapshots(newly_frozen, bars)

        return newly_frozen

    def evaluate_target(
        self,
        *,
        dataset_id: str,
        ticker: str,
        bar_end: datetime,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Evaluate +5m target using exact timestamp lookup on finalized bars only."""
        anchor_utc = _ensure_utc(bar_end)
        contract_bars = {
            (rec.ticker, rec.bar_end): ContractBar(
                ticker=rec.ticker,
                bar_start=rec.bar_start,
                bar_end=rec.bar_end,
                open=rec.open,
                high=rec.high,
                low=rec.low,
                close=rec.close,
                volume=rec.volume,
                event_count=rec.event_count,
                late_event_count=rec.late_event_count,
                duplicate_event_count=0,
            )
            for rec in self._final_bars.values()
            if rec.contract_id == self.contract.contract_id
            and rec.dataset_id == dataset_id
            and rec.ticker == ticker
            and rec.run_id == run_id
        }
        outcome = evaluate_target_outcome(
            bars=contract_bars,
            ticker=ticker,
            anchor_bar_end=anchor_utc,
            contract=self.contract,
        )
        return {
            "target_status": outcome.status,
            "reason": outcome.reason,
            "target_direction": 1
            if outcome.target_direction == "UP"
            else (0 if outcome.target_direction == "NOT_UP" else None),
            "target_label": outcome.target_direction,
            "return_5m": outcome.return_5m,
            "target_at": outcome.target_at,
        }

    def _persist_bars_and_corrections(
        self,
        bars: Sequence[FinalBarRecord],
        corrections: Sequence[LateBarCorrection],
    ) -> None:
        if not bars and not corrections:
            return
        with self.db_manager.transaction() as cur:  # type: ignore[union-attr]
            for bar in bars:
                cur.execute(
                    FINAL_BAR_INSERT_SQL,
                    (
                        bar.contract_id,
                        bar.contract_version,
                        bar.dataset_id,
                        bar.run_id,
                        bar.ticker,
                        bar.bar_start,
                        bar.bar_end,
                        bar.available_at,
                        bar.finalized_as_of,
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                        bar.event_count,
                        bar.late_event_count,
                        bar.provenance,
                    ),
                )
            for corr in corrections:
                cur.execute(
                    LATE_CORRECTION_INSERT_SQL,
                    (
                        corr.contract_id,
                        corr.dataset_id,
                        corr.source_event_id,
                        corr.run_id,
                        corr.ticker,
                        corr.bar_end,
                        corr.event_at,
                        corr.available_at,
                        corr.grace_cutoff,
                        corr.correction_reason,
                        json.dumps(corr.payload, default=str),
                    ),
                )

    def _persist_snapshots(
        self,
        snapshots: Sequence[FeatureSnapshotRecord],
        bars: Sequence[FinalBarRecord],
    ) -> None:
        bar_lookup = {(b.dataset_id, b.ticker, b.bar_end): b for b in bars}
        with self.db_manager.transaction() as cur:  # type: ignore[union-attr]
            for snap in snapshots:
                bar = bar_lookup.get((snap.dataset_id, snap.ticker, snap.bar_end))
                feats = snap.features
                cur.execute(
                    FEATURE_SNAPSHOT_INSERT_SQL,
                    (
                        snap.contract_id,
                        snap.contract_version,
                        snap.dataset_id,
                        snap.run_id,
                        snap.ticker,
                        snap.bar_start,
                        snap.bar_end,
                        snap.decision_at,
                        snap.target_at,
                        snap.feature_mode,
                        snap.feature_hash,
                        snap.warmup_complete,
                        snap.contiguous_bar_count,
                        json.dumps(feats),
                        bar.open if bar else feats["open"],
                        bar.high if bar else feats["high"],
                        bar.low if bar else feats["low"],
                        bar.close if bar else feats["close"],
                        bar.volume if bar else int(feats["volume"] or 0),
                        feats["return_1m"],
                        feats["return_5m_past"],
                        feats["return_15m_past"],
                        feats["sma_5"],
                        feats["sma_20"],
                        feats["sma_spread_5_20"],
                        feats["volatility_20"],
                        feats["volume_ratio_20"],
                        feats["bar_range_pct"],
                    ),
                )
