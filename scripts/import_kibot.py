"""Normalize locally downloaded Kibot standard 1m samples; never fill absent bars."""

from __future__ import annotations

import argparse
import csv
import json
import logging
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from pipeline.dataset_manifest import DatasetManifest, compute_sha256
from producer.schema_validator import MarketBarMessage


def import_samples(inputs: dict[str, Path], output_dir: Path, dataset_id: str) -> Path:
    rows: list[dict[str, str | int | float]] = []
    counts: dict[str, dict[str, int]] = {}
    sources: dict[str, dict[str, str]] = {}
    seen: set[tuple[str, datetime]] = set()
    for ticker, path in inputs.items():
        sessions: Counter[str] = Counter()
        with path.open(newline="") as stream:
            for number, row in enumerate(csv.reader(stream), 1):
                if len(row) != 7:
                    raise ValueError(f"{ticker} row {number}: expected seven fields")
                start = datetime.strptime(f"{row[0]} {row[1]}", "%m/%d/%Y %H:%M").replace(
                    tzinfo=ZoneInfo("America/New_York")
                )
                if start.weekday() > 4 or not ("09:30" <= row[1] < "16:00"):
                    raise ValueError(f"{ticker} row {number}: outside regular session")
                start = start.astimezone(UTC)
                end = start + timedelta(minutes=1)
                key = (ticker, start)
                if key in seen:
                    raise ValueError(f"{ticker} row {number}: duplicate minute")
                seen.add(key)
                bar = MarketBarMessage(
                    ticker=ticker,
                    bar_start=start,
                    bar_end=end,
                    event_at=end,
                    available_at=end + timedelta(seconds=5),
                    ingested_at=datetime.now(UTC),
                    open=float(row[2]),
                    high=float(row[3]),
                    low=float(row[4]),
                    close=float(row[5]),
                    volume=int(row[6]),
                    dataset_id=dataset_id,
                    source_event_id=f"{dataset_id}:{ticker}:{end.isoformat()}",
                    provenance="real_historical",
                    availability_assumed=True,
                )
                rows.append(
                    {
                        "ticker": ticker,
                        "bar_start": start.isoformat(),
                        "bar_end": end.isoformat(),
                        "available_at": bar.available_at.isoformat(),
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "source_event_id": bar.source_event_id,
                    }
                )
                sessions[start.astimezone(ZoneInfo("America/New_York")).date().isoformat()] += 1
        counts[ticker] = dict(sorted(sessions.items()))
        sources[ticker] = {
            "file": str(path),
            "sha256": compute_sha256(path),
            "adjustment": "unadjusted",
        }
    if not rows:
        raise ValueError("No bars imported")
    rows.sort(key=lambda r: (str(r["bar_end"]), str(r["ticker"])))
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{dataset_id}.csv"
    manifest_path = output_dir / f"{dataset_id}.json"
    if csv_path.exists() or manifest_path.exists():
        raise FileExistsError("Dataset already exists; use a new dataset ID for a new snapshot")
    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    manifest = DatasetManifest(
        dataset_id=dataset_id,
        source="Kibot public unadjusted 1-minute samples",
        license_and_redistribution="Internal use only; no redistribution of raw or derived data. https://www.kibot.com/license.html",
        checksum_sha256=compute_sha256(csv_path),
        file_path=str(csv_path),
        schema_type="ohlcv_1m_bars",
        tickers=sorted(inputs),
        period_start=datetime.fromisoformat(str(rows[0]["bar_start"])),
        period_end=datetime.fromisoformat(str(rows[-1]["bar_end"])),
        availability_method="ASSUMED bar_end + 5 seconds; actual vendor delivery/corrections unavailable",
        availability_assumed=True,
        provenance="real_historical",
        record_count=len(rows),
        notes="Standard Kibot timestamps are bar OPEN in America/New_York. DST via zoneinfo; unadjusted prices; omitted minutes remain missing. IBM/OIH is a vendor-selected sample, not a representative market universe. Keep evaluation and bundle private.",
    )
    payload = manifest.model_dump(mode="json")
    payload["source_files"] = sources
    payload["session_counts"] = {t: len(v) for t, v in counts.items()}
    payload["session_bar_counts"] = counts
    payload["source_urls"] = {
        "IBM": "https://api.kibot.com/?get=e4Vuqcxk",
        "OIH": "https://api.kibot.com/?get=NaatWNJD",
    }
    payload["retrieved_at"] = datetime.now(UTC).isoformat()
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n")
    logging.info(
        json.dumps(
            {
                "manifest": str(manifest_path),
                "records": len(rows),
                "sessions": payload["session_counts"],
            }
        )
    )
    return manifest_path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", action="append", required=True, help="TICKER=local_path (repeatable)"
    )
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("data/private"))
    args = parser.parse_args()
    inputs = dict(value.split("=", 1) for value in args.input)
    import_samples(
        {ticker: Path(path) for ticker, path in inputs.items()}, args.output_dir, args.dataset_id
    )


if __name__ == "__main__":
    main()
