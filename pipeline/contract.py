from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, model_validator

DAY_MAP = {
    "MON": 0,
    "TUE": 1,
    "WED": 2,
    "THU": 3,
    "FRI": 4,
    "SAT": 5,
    "SUN": 6,
}


class SessionRules(BaseModel):
    """Regular trading session rules evaluated in exchange timezone and converted to UTC."""

    market: str
    exchange_timezone: str = "America/New_York"
    open_local: str = "09:30:00"
    close_local: str = "16:00:00"
    trading_days: list[str] = Field(default_factory=lambda: ["MON", "TUE", "WED", "THU", "FRI"])
    exclude_extended_hours: bool = True

    def session_bounds_utc(self, dt_utc: datetime) -> tuple[datetime, datetime] | None:
        """Return (session_open_utc, session_close_utc) for dt_utc's local exchange date, or None on closed days."""
        dt_aware = dt_utc if dt_utc.tzinfo is not None else dt_utc.replace(tzinfo=UTC)
        tz = ZoneInfo(self.exchange_timezone)
        local_dt = dt_aware.astimezone(tz)
        allowed_weekdays = {DAY_MAP[d.upper()] for d in self.trading_days}
        if local_dt.weekday() not in allowed_weekdays:
            return None

        open_t = time.fromisoformat(self.open_local)
        close_t = time.fromisoformat(self.close_local)
        local_open = datetime.combine(local_dt.date(), open_t, tzinfo=tz)
        local_close = datetime.combine(local_dt.date(), close_t, tzinfo=tz)
        return local_open.astimezone(UTC), local_close.astimezone(UTC)

    def is_bar_in_session(self, bar_end_utc: datetime) -> bool:
        """Check whether a 1-minute bar ending at bar_end_utc belongs inside regular session hours.

        A 1-minute bar [09:30, 09:31) has bar_end = 09:31; the final session bar [15:59, 16:00)
        has bar_end = 16:00. Therefore bar_end must be in (session_open, session_close].
        """
        bounds = self.session_bounds_utc(bar_end_utc)
        if bounds is None:
            return False
        session_open_utc, session_close_utc = bounds
        dt_aware = (
            bar_end_utc if bar_end_utc.tzinfo is not None else bar_end_utc.replace(tzinfo=UTC)
        )
        return session_open_utc < dt_aware <= session_close_utc


class TargetPolicy(BaseModel):
    """Forecast target definition and missing/tie handling policy."""

    name: str
    formula: str
    up_condition: str
    not_up_condition: str
    flat_price_label: Literal["NOT_UP"] = "NOT_UP"
    allow_target_substitution: Literal[False] = False
    missing_target_status: Literal["unavailable"] = "unavailable"
    missing_target_reason: str = "missing_target_bar"
    session_boundary_reason: str = "session_end"
    dataset_boundary_reason: str = "dataset_end"


class AvailabilityPolicy(BaseModel):
    """Point-in-time availability and late-event handling rules."""

    record_separate_timestamps: list[str]
    historical_assumed_bar_lag_seconds: int = Field(ge=0)
    late_event_action: str


class SentimentPolicy(BaseModel):
    """Policy governing sentiment features in primary vs experimental runs."""

    included_in_primary_model: bool = False
    require_availability_timestamp: bool = True
    require_known_provenance: bool = True
    distinguish_unavailable_from_neutral: bool = True


class FeaturePolicy(BaseModel):
    """Canonical ordered feature list and mode."""

    mode: Literal["price_only", "price_and_sentiment"] = "price_only"
    features: list[str] = Field(min_length=1)
    sentiment_policy: SentimentPolicy


class ForecastContract(BaseModel):
    """Canonical forecast contract loaded from config/forecast_contract.json."""

    contract_id: str
    version: str
    description: str
    tickers: list[str] = Field(min_length=1)
    timezone: Literal["UTC"] = "UTC"
    bar_interval_seconds: int = Field(gt=0)
    horizon_seconds: int = Field(gt=0)
    horizon_bars: int = Field(gt=0)
    grace_seconds: int = Field(ge=0)
    warmup_bars: int = Field(ge=1)
    session_rules: SessionRules
    target_policy: TargetPolicy
    availability_policy: AvailabilityPolicy
    feature_policy: FeaturePolicy

    @model_validator(mode="after")
    def validate_timing_invariants(self) -> ForecastContract:
        """Ensure grace window is strictly smaller than forecast horizon and horizon aligns with bar interval."""
        if self.grace_seconds >= self.horizon_seconds:
            raise ValueError(
                f"grace_seconds ({self.grace_seconds}) must be < horizon_seconds ({self.horizon_seconds})"
            )
        if self.horizon_bars * self.bar_interval_seconds != self.horizon_seconds:
            raise ValueError("horizon_bars * bar_interval_seconds must equal horizon_seconds")
        return self


@dataclass(frozen=True)
class ContractBar:
    """Canonical 1-minute OHLCV bar aligned to UTC bar_end."""

    ticker: str
    bar_start: datetime
    bar_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    event_count: int
    late_event_count: int
    duplicate_event_count: int


@dataclass(frozen=True)
class TargetOutcome:
    """Result of evaluating +5m target strictly at target_at without substitution."""

    anchor_bar_end: datetime
    decision_at: datetime
    target_at: datetime
    status: Literal["realized", "unavailable"]
    target_direction: Literal["UP", "NOT_UP"] | None
    return_5m: float | None
    reason: str | None = None


@dataclass(frozen=True)
class RawTickInput:
    """Normalized tick input for deterministic contract aggregation tests."""

    dataset_id: str
    source_event_id: str
    ticker: str
    event_at: datetime
    available_at: datetime
    price: float
    volume: int


@dataclass(frozen=True)
class QuarantinedEvent:
    """Event excluded during deterministic aggregation (duplicate or late)."""

    source_event_id: str
    reason: Literal["duplicate_source_event", "late_after_grace"]
    event_at: datetime
    available_at: datetime


@dataclass(frozen=True)
class AggregationResult:
    """Deterministic bar aggregation outcome including final bars and quarantined events."""

    bars: dict[tuple[str, datetime], ContractBar]
    quarantined: list[QuarantinedEvent]


def load_forecast_contract(
    path: str | Path = "config/forecast_contract.json",
) -> ForecastContract:
    """Load and validate the forecast contract JSON file."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return ForecastContract.model_validate(raw)


def compute_bar_bounds(event_at: datetime, interval_seconds: int = 60) -> tuple[datetime, datetime]:
    """Map an event timestamp onto its UTC [bar_start, bar_end) bucket.

    Example: 10:00:00Z .. 10:00:59.999999Z -> bar_start=10:00:00Z, bar_end=10:01:00Z.
    """
    dt = event_at.astimezone(UTC) if event_at.tzinfo is not None else event_at.replace(tzinfo=UTC)
    epoch_sec = int(dt.timestamp())
    bucket_start_sec = (epoch_sec // interval_seconds) * interval_seconds
    bar_start = datetime.fromtimestamp(bucket_start_sec, tz=UTC)
    bar_end = bar_start + timedelta(seconds=interval_seconds)
    return bar_start, bar_end


def compute_anchor_timestamps(
    bar_end: datetime,
    contract: ForecastContract,
) -> tuple[datetime, datetime]:
    """Return (decision_at, target_at) for an anchor bar ending at bar_end."""
    end_utc = bar_end.astimezone(UTC) if bar_end.tzinfo is not None else bar_end.replace(tzinfo=UTC)
    decision_at = end_utc + timedelta(seconds=contract.grace_seconds)
    target_at = end_utc + timedelta(seconds=contract.horizon_seconds)
    return decision_at, target_at


def aggregate_events_to_bars(
    events: list[RawTickInput],
    contract: ForecastContract,
) -> AggregationResult:
    """Deterministically deduplicate, filter late arrivals, and build UTC 1m OHLCV bars."""
    seen_ids: set[tuple[str, str]] = set()
    quarantined: list[QuarantinedEvent] = []
    buckets: dict[tuple[str, datetime], list[RawTickInput]] = {}
    dup_counts: dict[tuple[str, datetime], int] = {}
    late_counts: dict[tuple[str, datetime], int] = {}

    for ev in sorted(events, key=lambda e: (e.available_at, e.dataset_id, e.source_event_id)):
        bar_start, bar_end = compute_bar_bounds(ev.event_at, contract.bar_interval_seconds)
        key = (ev.ticker, bar_end)

        biz_id = (ev.dataset_id, ev.source_event_id)
        if biz_id in seen_ids:
            dup_counts[key] = dup_counts.get(key, 0) + 1
            quarantined.append(
                QuarantinedEvent(
                    source_event_id=ev.source_event_id,
                    reason="duplicate_source_event",
                    event_at=ev.event_at,
                    available_at=ev.available_at,
                )
            )
            continue
        seen_ids.add(biz_id)

        avail_utc = (
            ev.available_at.astimezone(UTC)
            if ev.available_at.tzinfo is not None
            else ev.available_at.replace(tzinfo=UTC)
        )
        grace_cutoff = bar_end + timedelta(seconds=contract.grace_seconds)
        if avail_utc > grace_cutoff:
            late_counts[key] = late_counts.get(key, 0) + 1
            quarantined.append(
                QuarantinedEvent(
                    source_event_id=ev.source_event_id,
                    reason="late_after_grace",
                    event_at=ev.event_at,
                    available_at=ev.available_at,
                )
            )
            continue

        buckets.setdefault(key, []).append(ev)

    bars: dict[tuple[str, datetime], ContractBar] = {}
    for key, group in sorted(buckets.items(), key=lambda item: (item[0][1], item[0][0])):
        ticker, bar_end = key
        # Sort out-of-order arrivals by canonical event_at (tie-broken by source_event_id)
        ordered = sorted(group, key=lambda e: (e.event_at, e.source_event_id))
        bar_start = bar_end - timedelta(seconds=contract.bar_interval_seconds)
        prices = [e.price for e in ordered]
        bars[key] = ContractBar(
            ticker=ticker,
            bar_start=bar_start,
            bar_end=bar_end,
            open=prices[0],
            high=max(prices),
            low=min(prices),
            close=prices[-1],
            volume=sum(e.volume for e in ordered),
            event_count=len(ordered),
            late_event_count=late_counts.get(key, 0),
            duplicate_event_count=dup_counts.get(key, 0),
        )

    return AggregationResult(bars=bars, quarantined=quarantined)


def check_warmup_eligible(
    bars: dict[tuple[str, datetime], ContractBar],
    ticker: str,
    anchor_bar_end: datetime,
    contract: ForecastContract,
) -> bool:
    """Return True iff the anchor bar exists and at least warmup_bars strictly contiguous prior bars exist."""
    if not contract.session_rules.is_bar_in_session(anchor_bar_end):
        return False
    if (ticker, anchor_bar_end) not in bars:
        return False
    step = timedelta(seconds=contract.bar_interval_seconds)
    for idx in range(1, contract.warmup_bars):
        if (
            not contract.session_rules.is_bar_in_session(anchor_bar_end - idx * step)
            or (ticker, anchor_bar_end - idx * step) not in bars
        ):
            return False
    return True


def evaluate_target_outcome(
    bars: dict[tuple[str, datetime], ContractBar],
    ticker: str,
    anchor_bar_end: datetime,
    contract: ForecastContract,
    dataset_end_utc: datetime | None = None,
) -> TargetOutcome:
    """Evaluate exact +5m target outcome without ever substituting adjacent bars."""
    decision_at, target_at = compute_anchor_timestamps(anchor_bar_end, contract)
    anchor = bars.get((ticker, anchor_bar_end))
    if anchor is None:
        return TargetOutcome(
            anchor_bar_end=anchor_bar_end,
            decision_at=decision_at,
            target_at=target_at,
            status="unavailable",
            target_direction=None,
            return_5m=None,
            reason="missing_anchor_bar",
        )

    if not contract.session_rules.is_bar_in_session(
        anchor_bar_end
    ) or not contract.session_rules.is_bar_in_session(target_at):
        return TargetOutcome(
            anchor_bar_end=anchor_bar_end,
            decision_at=decision_at,
            target_at=target_at,
            status="unavailable",
            target_direction=None,
            return_5m=None,
            reason=contract.target_policy.session_boundary_reason,
        )

    if dataset_end_utc is not None:
        end_bound = (
            dataset_end_utc.astimezone(UTC)
            if dataset_end_utc.tzinfo is not None
            else dataset_end_utc.replace(tzinfo=UTC)
        )
        if target_at > end_bound:
            return TargetOutcome(
                anchor_bar_end=anchor_bar_end,
                decision_at=decision_at,
                target_at=target_at,
                status="unavailable",
                target_direction=None,
                return_5m=None,
                reason=contract.target_policy.dataset_boundary_reason,
            )

    target_bar = bars.get((ticker, target_at))
    if target_bar is None:
        return TargetOutcome(
            anchor_bar_end=anchor_bar_end,
            decision_at=decision_at,
            target_at=target_at,
            status="unavailable",
            target_direction=None,
            return_5m=None,
            reason=contract.target_policy.missing_target_reason,
        )

    ret_5m = (target_bar.close / anchor.close) - 1.0
    direction: Literal["UP", "NOT_UP"] = "UP" if ret_5m > 0.0 else "NOT_UP"
    return TargetOutcome(
        anchor_bar_end=anchor_bar_end,
        decision_at=decision_at,
        target_at=target_at,
        status="realized",
        target_direction=direction,
        return_5m=ret_5m,
        reason=None,
    )
