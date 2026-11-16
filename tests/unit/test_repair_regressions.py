from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from model.evaluate import activate_bundle, load_and_verify_bundle_manifest, publish_model_bundle
from model.predictor import Predictor
from pipeline.dataset_manifest import load_manifest, load_ohlcv_bars_from_csv
from pipeline.features import (
    BarFinalizationService,
    compute_feature_hash,
    verify_snapshot_integrity,
)
from producer.schema_validator import MarketTickMessage


def fixture_bars():
    manifest = load_manifest("data/manifests/fixture_smoke_v1.json")
    return load_ohlcv_bars_from_csv(
        manifest.file_path,
        dataset_id=manifest.dataset_id,
        provenance=manifest.provenance,
        availability_assumed=manifest.availability_assumed,
    )


def test_bar_intervals_run_isolation_duplicates_and_future_cutoff():
    bars = fixture_bars()
    service = BarFinalizationService()
    end = max(b.bar_end for b in bars) + timedelta(seconds=15)
    events = [bar.model_dump() for bar in bars]
    for run in ("first", "second"):
        done, corrections = service.finalize_events(
            events + events, dataset_id=bars[0].dataset_id, run_id=run, logical_as_of=end
        )
        assert len(done) == 89 and corrections == []
        assert {(b.ticker, b.bar_start, b.bar_end) for b in done} == {
            (b.ticker, b.bar_start, b.bar_end) for b in bars
        }
        service.freeze_feature_snapshots(dataset_id=bars[0].dataset_id, run_id=run)
    assert len(service.final_bars) == len(service.snapshots) == 178
    first = {
        (s.ticker, s.bar_end): s.feature_hash
        for s in service.snapshots.values()
        if s.run_id == "first"
    }
    second = {
        (s.ticker, s.bar_end): s.feature_hash
        for s in service.snapshots.values()
        if s.run_id == "second"
    }
    assert first == second
    fresh = BarFinalizationService()
    done, _ = fresh.finalize_events(
        events,
        dataset_id=bars[0].dataset_id,
        run_id="future",
        logical_as_of=min(b.available_at for b in bars) - timedelta(seconds=1),
    )
    assert done == []


def test_assumed_availability_and_timestamp_alias_validation():
    assert all(bar.availability_assumed for bar in fixture_bars())
    with pytest.raises(ValidationError, match="event_at must equal"):
        MarketTickMessage(
            ticker="AAPL",
            replay_sequence=1,
            price=100,
            volume=1,
            event_timestamp="2025-01-15T14:30:00Z",
            event_at="2025-01-15T14:31:00Z",
        )


def test_jsonb_feature_order_does_not_invalidate_hash():
    bars = fixture_bars()
    service = BarFinalizationService()
    service.finalize_events(
        [b.model_dump() for b in bars],
        dataset_id=bars[0].dataset_id,
        run_id="jsonb",
        logical_as_of=max(b.bar_end for b in bars) + timedelta(seconds=15),
    )
    snap = service.freeze_feature_snapshots(dataset_id=bars[0].dataset_id, run_id="jsonb")[-1]
    verify_snapshot_integrity(replace(snap, features=dict(reversed(list(snap.features.items())))))
    features = dict(snap.features)
    features["sma_spread_5_20"] = -1e-14
    legacy_hash = compute_feature_hash(service.contract, features, preserve_signed_zero=True)
    verify_snapshot_integrity(replace(snap, features=features, feature_hash=legacy_hash))
    normalized = compute_feature_hash(service.contract, features)
    features["sma_spread_5_20"] = 1e-14
    assert compute_feature_hash(service.contract, features) == normalized


def test_retained_release_can_be_activated_after_new_publish(tmp_path: Path) -> None:
    import pandas as pd

    from model.evaluate import run_walk_forward_evaluation

    report, objects = run_walk_forward_evaluation(
        pd.read_csv("data/fixtures/smoke_1m_v1.csv"),
        dataset_id="fixture_smoke_v1",
        dataset_provenance="synthetic_fixture",
    )
    root = tmp_path / "bundle"
    _, first = publish_model_bundle(root, report, objects)
    _, second = publish_model_bundle(root, report, objects)
    assert first.bundle_id != second.bundle_id and first.model_version != second.model_version
    assert Predictor.load(str(root)).metadata["bundle_id"] == second.bundle_id
    activate_bundle(root, first.bundle_id)
    assert load_and_verify_bundle_manifest(root).bundle_id == first.bundle_id
    predictor = Predictor.load(str(root))
    assert predictor.metadata["bundle_id"] == first.bundle_id
    from pipeline.features import compute_canonical_features_from_bars

    frame = compute_canonical_features_from_bars(pd.read_csv("data/fixtures/smoke_1m_v1.csv"))
    features = frame.loc[frame.warmup_complete].iloc[-1].to_dict()
    features["sma_spread_5_20"] = -1e-14
    features["feature_hash"] = compute_feature_hash(
        predictor.contract, features, preserve_signed_zero=True
    )
    assert 0 <= predictor.predict(features)["probability_up"] <= 1
    features["close"] += 1
    with pytest.raises(ValueError, match="hash mismatch"):
        predictor.predict(features)
