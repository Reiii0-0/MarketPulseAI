"""Measure a four-ticker, 1x replay tail after accelerated synthetic warm-up.

Run inside an API container with Kafka, PostgreSQL, bar-consumer and worker running.
Writes a new isolated benchmark run; never deletes existing runs or source data.
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from consumer.db import DatabaseConfig, DatabaseManager
from pipeline.dataset_manifest import compute_sha256, load_ohlcv_bars_from_csv
from pipeline.replay import PostgresReplayStateStore, ReplayCoordinator, ReplayEvent
from producer.config import ProducerConfig
from producer.replay_producer import ReplayProducer


def run_probe(output: Path, measured_minutes: int = 5) -> dict[str, Any]:
    if not 1 <= measured_minutes <= 8:
        raise ValueError("measured_minutes must be between 1 and 8 (post-fit fixture tail)")
    run_id = f"latency-4ticker-{uuid.uuid4().hex[:12]}"
    dataset_id = "synthetic_latency_4ticker_v1"
    source = Path("data/fixtures/smoke_1m_v1.csv")
    bars = [
        b
        for b in load_ohlcv_bars_from_csv(source, dataset_id, "synthetic_fixture")
        if b.ticker == "MSFT"
    ]
    end = max(b.bar_end for b in bars)
    warmup_end = end - timedelta(minutes=measured_minutes) + timedelta(seconds=15)
    events = []
    for ticker in ("AAPL", "MSFT", "NVDA", "TSLA"):
        for bar in bars:
            payload = bar.model_copy(
                update={
                    "ticker": ticker,
                    "run_id": run_id,
                    "source_event_id": f"{dataset_id}:{ticker}:{bar.bar_end.isoformat()}",
                }
            )
            events.append(
                ReplayEvent(
                    topic="raw.market.bars",
                    key=ticker,
                    payload=payload,
                    available_at=payload.available_at,
                    source_event_id=payload.source_event_id,
                )
            )
    db = DatabaseManager(DatabaseConfig())  # type: ignore[call-arg]
    store = PostgresReplayStateStore(db)
    producer = ReplayProducer(ProducerConfig())
    realtime = threading.Event()
    stop = threading.Event()
    observed: dict[tuple[str, str], float] = {}
    errors: list[str] = []
    measured_start: float | None = None

    def observe() -> None:
        while not stop.wait(0.2):
            try:
                with db.get_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        """SELECT p.ticker, p.bar_end, b.finalized_at
                        FROM marts.model_predictions p JOIN marts.final_bars b
                        ON p.run_id=b.run_id AND p.dataset_id=b.dataset_id
                        AND p.contract_id=b.contract_id AND p.ticker=b.ticker
                        AND p.bar_end=b.bar_end
                        WHERE p.run_id=%s AND p.bar_end > %s""",
                        (run_id, warmup_end),
                    )
                    now = datetime.now(UTC)
                    for ticker, bar_end, finalized_at in cur.fetchall():
                        key = (ticker, bar_end.isoformat())
                        observed.setdefault(key, (now - finalized_at).total_seconds())
            except Exception as exc:
                errors.append(type(exc).__name__)
                return

    def advance(_run: str, logical: datetime) -> None:
        nonlocal measured_start
        if logical >= warmup_end and not realtime.is_set():
            measured_start = time.perf_counter()
            realtime.set()

    def paced_sleep(seconds: float) -> None:
        time.sleep(seconds if realtime.is_set() or seconds < 1 else seconds / 120)

    observer = threading.Thread(target=observe, daemon=True)
    observer.start()
    try:
        ReplayCoordinator(state_store=store, sleep_fn=paced_sleep).execute(
            events,
            publish_batch_fn=producer._publish_batch,
            run_id=run_id,
            dataset_id=dataset_id,
            provenance="synthetic_fixture",
            speed_multiplier=1,
            on_clock_advance=advance,
        )
        expected = measured_minutes * 4
        deadline = time.monotonic() + 30
        while len(observed) < expected and not errors and time.monotonic() < deadline:
            time.sleep(0.2)
        elapsed = time.perf_counter() - (measured_start or time.perf_counter())
    finally:
        stop.set()
        observer.join(timeout=10)
        db.close()
    values = list(observed.values())
    passed = len(values) == expected and not errors and all(0 <= v <= 15 for v in values)
    report = {
        "measured_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "dataset_id": dataset_id,
        "source_sha256": compute_sha256(source),
        "provenance": "synthetic_fixture",
        "fixture_construction": "MSFT fixture OHLCV cloned to four ticker keys; no market claim",
        "warmup_speed_multiplier": 120,
        "measured_speed_multiplier": 1,
        "measured_after_logical_at": warmup_end.isoformat(),
        "measured_wall_seconds": round(elapsed, 3),
        "expected_predictions": expected,
        "observed_predictions": len(values),
        "errors": errors,
        "latency_seconds": {
            "p50": float(np.percentile(values, 50)) if values else None,
            "p95": float(np.percentile(values, 95)) if values else None,
            "max": max(values, default=None),
        },
        "definition": "DB finalization transaction timestamp to first observer read of committed prediction",
        "limits": "Conservative polling estimate (0.2s interval), same-host clocks, warm steady-state tail; excludes historical warm-up and is not live-provider latency",
        "samples": [
            {"ticker": k[0], "bar_end": k[1], "seconds": v} for k, v in sorted(observed.items())
        ],
        "passed_15s_gate": passed,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    if not passed:
        raise RuntimeError(f"Replay latency gate failed; inspect {output}")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("/tmp/replay_latency.json"))
    parser.add_argument("--measured-minutes", type=int, default=5)
    args = parser.parse_args()
    result = run_probe(args.output, args.measured_minutes)
    logging.info(
        json.dumps(
            {
                k: result[k]
                for k in ("run_id", "observed_predictions", "latency_seconds", "passed_15s_gate")
            }
        )
    )
