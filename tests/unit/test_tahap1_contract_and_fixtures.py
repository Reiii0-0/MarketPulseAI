from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from pipeline.contract import (
    ContractBar,
    RawTickInput,
    aggregate_events_to_bars,
    check_warmup_eligible,
    compute_anchor_timestamps,
    compute_bar_bounds,
    evaluate_target_outcome,
    load_forecast_contract,
)
from pipeline.dataset_manifest import (
    load_manifest,
    load_ohlcv_bars_from_csv,
    verify_manifest_checksum,
)
from producer.schema_validator import (
    MarketBarMessage,
    QuoteMessage,
    SentimentEventMessage,
)


@pytest.fixture
def contract():
    return load_forecast_contract("config/forecast_contract.json")


@pytest.fixture
def scenarios():
    return json.loads(Path("tests/fixtures/time_contract_scenarios.json").read_text())["scenarios"]


@pytest.mark.unit
def test_forecast_contract_defaults_and_invariants(contract):
    """Verify canonical 1m/+5m UTC contract invariants."""
    assert contract.contract_id == "mp_1m_5m_price_v1"
    assert contract.timezone == "UTC"
    assert contract.bar_interval_seconds == 60
    assert contract.horizon_seconds == 300
    assert contract.horizon_bars == 5
    assert 0 <= contract.grace_seconds < contract.horizon_seconds
    assert contract.target_policy.allow_target_substitution is False
    assert contract.target_policy.flat_price_label == "NOT_UP"
    assert contract.feature_policy.mode == "price_only"
    assert contract.feature_policy.sentiment_policy.included_in_primary_model is False


@pytest.mark.unit
def test_anchor_and_target_timestamps_example(contract, scenarios):
    """Verify bar [10:00, 10:01) produces anchor bar_end 10:01 and target 10:06."""
    sc = scenarios["anchor_10_01_to_10_06"]
    event_at = datetime.fromisoformat(sc["event_at"].replace("Z", "+00:00"))
    bar_start, bar_end = compute_bar_bounds(event_at, contract.bar_interval_seconds)
    decision_at, target_at = compute_anchor_timestamps(bar_end, contract)

    assert bar_start.isoformat() == sc["expected_bar_start"]
    assert bar_end.isoformat() == sc["expected_bar_end"]
    assert decision_at.isoformat() == sc["expected_decision_at"]
    assert target_at.isoformat() == sc["expected_target_at"]


@pytest.mark.unit
def test_duplicate_and_out_of_order_aggregation(contract, scenarios):
    """Verify business-ID dedup and chronological OHLC sorting for out-of-order events."""
    sc = scenarios["duplicate_and_out_of_order"]
    events = [
        RawTickInput(
            dataset_id=sc["dataset_id"],
            source_event_id=e["source_event_id"],
            ticker=e["ticker"],
            event_at=datetime.fromisoformat(e["event_at"].replace("Z", "+00:00")),
            available_at=datetime.fromisoformat(e["available_at"].replace("Z", "+00:00")),
            price=e["price"],
            volume=e["volume"],
        )
        for e in sc["events"]
    ]
    res = aggregate_events_to_bars(events, contract)
    exp = sc["expected_bar"]
    bar_end = datetime.fromisoformat(exp["bar_end"])
    bar = res.bars[("AAPL", bar_end)]

    assert bar.open == exp["open"]
    assert bar.high == exp["high"]
    assert bar.low == exp["low"]
    assert bar.close == exp["close"]
    assert bar.volume == exp["volume"]
    assert bar.event_count == exp["event_count"]
    assert bar.duplicate_event_count == exp["duplicate_event_count"]
    assert len(res.quarantined) == 1
    assert res.quarantined[0].reason == "duplicate_source_event"


@pytest.mark.unit
def test_late_event_quarantined_and_excluded(contract, scenarios):
    """Verify events arriving after bar_end + grace_seconds do not alter final bar."""
    sc = scenarios["late_event"]
    events = [
        RawTickInput(
            dataset_id=sc["dataset_id"],
            source_event_id=e["source_event_id"],
            ticker=e["ticker"],
            event_at=datetime.fromisoformat(e["event_at"].replace("Z", "+00:00")),
            available_at=datetime.fromisoformat(e["available_at"].replace("Z", "+00:00")),
            price=e["price"],
            volume=e["volume"],
        )
        for e in sc["events"]
    ]
    res = aggregate_events_to_bars(events, contract)
    bar_end = datetime(2025, 1, 15, 15, 1, 0, tzinfo=UTC)
    bar = res.bars[("AAPL", bar_end)]

    assert bar.close == sc["expected_bar"]["close"]
    assert bar.volume == sc["expected_bar"]["volume"]
    assert bar.late_event_count == sc["expected_bar"]["late_event_count"]
    assert [q.reason for q in res.quarantined] == sc["expected_quarantine_reasons"]


def _make_bar(ticker: str, bar_end: datetime, close: float) -> ContractBar:
    return ContractBar(
        ticker=ticker,
        bar_start=bar_end - timedelta(minutes=1),
        bar_end=bar_end,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=100,
        event_count=1,
        late_event_count=0,
        duplicate_event_count=0,
    )


@pytest.mark.unit
def test_gap_does_not_substitute_later_bar(contract, scenarios):
    """Missing target at 15:06Z must return unavailable even if 15:07Z bar is present."""
    sc = scenarios["gap_no_substitution"]
    bars = {
        ("AAPL", datetime.fromisoformat(ts.replace("Z", "+00:00"))): _make_bar(
            "AAPL", datetime.fromisoformat(ts.replace("Z", "+00:00")), price
        )
        for ts, price in sc["present_bars"].items()
    }
    anchor_end = datetime.fromisoformat(sc["anchor_bar_end"].replace("Z", "+00:00"))
    outcome = evaluate_target_outcome(bars, "AAPL", anchor_end, contract)

    assert outcome.status == sc["expected_status"]
    assert outcome.reason == sc["expected_reason"]
    assert outcome.target_direction is None


@pytest.mark.unit
def test_flat_price_yields_not_up(contract, scenarios):
    """Zero 5m return must classify as NOT_UP rather than UP or invalid."""
    sc = scenarios["flat_price_tie_policy"]
    bars = {
        ("AAPL", datetime.fromisoformat(ts.replace("Z", "+00:00"))): _make_bar(
            "AAPL", datetime.fromisoformat(ts.replace("Z", "+00:00")), price
        )
        for ts, price in sc["present_bars"].items()
    }
    anchor_end = datetime.fromisoformat(sc["anchor_bar_end"].replace("Z", "+00:00"))
    outcome = evaluate_target_outcome(bars, "AAPL", anchor_end, contract)

    assert outcome.status == sc["expected_status"]
    assert outcome.target_direction == sc["expected_direction"]
    assert outcome.return_5m == sc["expected_return_5m"]


@pytest.mark.unit
def test_session_close_marks_target_unavailable(contract, scenarios):
    """Bar whose +5m target falls after US equity close (16:00 local) must be unavailable: session_end."""
    sc = scenarios["session_close_boundary"]
    bars = {
        ("AAPL", datetime.fromisoformat(ts.replace("Z", "+00:00"))): _make_bar(
            "AAPL", datetime.fromisoformat(ts.replace("Z", "+00:00")), price
        )
        for ts, price in sc["present_bars"].items()
    }
    anchor_end = datetime.fromisoformat(sc["anchor_bar_end"].replace("Z", "+00:00"))
    outcome = evaluate_target_outcome(bars, "AAPL", anchor_end, contract)

    assert outcome.status == sc["expected_status"]
    assert outcome.reason == sc["expected_reason"]


@pytest.mark.unit
def test_dataset_end_boundary_marks_target_unavailable(contract):
    """Bar whose +5m target falls beyond dataset_end_utc must be unavailable: dataset_end."""
    anchor_end = datetime(2025, 1, 15, 15, 5, 0, tzinfo=UTC)
    bars = {("AAPL", anchor_end): _make_bar("AAPL", anchor_end, 190.0)}
    outcome = evaluate_target_outcome(
        bars,
        "AAPL",
        anchor_end,
        contract,
        dataset_end_utc=datetime(2025, 1, 15, 15, 8, 0, tzinfo=UTC),
    )
    assert outcome.status == "unavailable"
    assert outcome.reason == "dataset_end"


@pytest.mark.unit
def test_warmup_requires_contiguous_bars(contract):
    """Warm-up fails if fewer than 20 contiguous bars precede anchor or a gap breaks continuity."""
    start = datetime(2025, 1, 15, 14, 31, 0, tzinfo=UTC)
    bars = {}
    for i in range(19):
        be = start + timedelta(minutes=i)
        bars[("AAPL", be)] = _make_bar("AAPL", be, 190.0)
    assert check_warmup_eligible(bars, "AAPL", start + timedelta(minutes=18), contract) is False

    # Add 20th contiguous bar
    be_20 = start + timedelta(minutes=19)
    bars[("AAPL", be_20)] = _make_bar("AAPL", be_20, 190.5)
    assert check_warmup_eligible(bars, "AAPL", be_20, contract) is True

    # Introduce a gap at bar index 5 -> warm-up must fail
    del bars[("AAPL", start + timedelta(minutes=5))]
    assert check_warmup_eligible(bars, "AAPL", be_20, contract) is False


@pytest.mark.unit
def test_market_bar_and_quote_schema_separation():
    """Verify OHLCV bar preserves OHLCV, Quote has no fake trade volume, and sentiment eligibility checks timestamps."""
    bar = MarketBarMessage(
        ticker="AAPL",
        bar_start="2025-01-15T15:00:00Z",
        bar_end="2025-01-15T15:01:00Z",
        open=190.0,
        high=191.0,
        low=189.5,
        close=190.8,
        volume=12000,
        available_at="2025-01-15T15:01:05Z",
        availability_assumed=True,
        dataset_id="fixture_smoke_v1",
        source_event_id="evt-bar-1",
        provenance="synthetic_fixture",
    )
    assert bar.observation_type == "ohlcv_bar"
    assert bar.event_at == bar.bar_end

    with pytest.raises(ValidationError):
        MarketBarMessage(
            ticker="AAPL",
            bar_start="2025-01-15T15:00:00Z",
            bar_end="2025-01-15T15:01:00Z",
            open=190.0,
            high=189.0,  # Invalid: high < open
            low=188.0,
            close=190.5,
            volume=100,
            available_at="2025-01-15T15:01:05Z",
            dataset_id="fixture_smoke_v1",
            source_event_id="evt-bad",
            provenance="synthetic_fixture",
        )

    quote = QuoteMessage(
        ticker="MSFT",
        event_at="2025-01-15T15:00:01Z",
        available_at="2025-01-15T15:00:02Z",
        bid=420.10,
        ask=420.15,
        dataset_id="fixture_smoke_v1",
        source_event_id="evt-q-1",
        provenance="real_live",
    )
    assert quote.observation_type == "quote"

    no_avail_sentiment = SentimentEventMessage(
        ticker="AAPL",
        event_timestamp="2025-01-15T15:00:00Z",
        source="reuters",
        sentiment_score=0.2,
        sentiment_label="POSITIVE",
        confidence=0.8,
        replay_sequence=1,
        provenance="real_historical",
    )
    assert no_avail_sentiment.is_eligible_for_primary_experiment is False

    valid_sentiment = SentimentEventMessage(
        ticker="AAPL",
        event_timestamp="2025-01-15T15:00:00Z",
        available_at="2025-01-15T15:00:10Z",
        source="reuters",
        sentiment_score=0.2,
        sentiment_label="POSITIVE",
        confidence=0.8,
        replay_sequence=2,
        provenance="real_historical",
    )
    assert valid_sentiment.is_eligible_for_primary_experiment is True


@pytest.mark.unit
def test_smoke_fixture_manifest_checksum_and_adapter(contract):
    """Verify fixture_smoke_v1 manifest SHA-256 checksum and OHLCV bar loading."""
    manifest_path = Path("data/manifests/fixture_smoke_v1.json")
    manifest = load_manifest(manifest_path)

    assert manifest.provenance == "synthetic_fixture"
    assert manifest.record_count == 89
    assert verify_manifest_checksum(manifest_path) is True

    loaded_bars = load_ohlcv_bars_from_csv(
        manifest.file_path,
        dataset_id=manifest.dataset_id,
        provenance=manifest.provenance,
    )
    assert len(loaded_bars) == 89

    contract_bars = {
        (b.ticker, b.bar_end): ContractBar(
            ticker=b.ticker,
            bar_start=b.bar_start,
            bar_end=b.bar_end,
            open=b.open,
            high=b.high,
            low=b.low,
            close=b.close,
            volume=b.volume,
            event_count=1,
            late_event_count=0,
            duplicate_event_count=0,
        )
        for b in loaded_bars
    }
    # Flat-price tie encoded at AAPL idx=20 (14:51Z) -> idx=25 (14:56Z)
    tie_outcome = evaluate_target_outcome(
        contract_bars,
        "AAPL",
        datetime(2025, 1, 15, 14, 51, 0, tzinfo=UTC),
        contract,
    )
    assert tie_outcome.status == "realized"
    assert tie_outcome.target_direction == "NOT_UP"

    # Deterministic gap encoded at AAPL idx=30 (15:01Z) -> anchor 14:56Z must be unavailable
    gap_outcome = evaluate_target_outcome(
        contract_bars,
        "AAPL",
        datetime(2025, 1, 15, 14, 56, 0, tzinfo=UTC),
        contract,
    )
    assert gap_outcome.status == "unavailable"
    assert gap_outcome.reason == "missing_target_bar"
