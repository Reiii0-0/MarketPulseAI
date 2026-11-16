from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from producer.schema_validator import MarketBarMessage, ProvenanceType


class DatasetManifest(BaseModel):
    """Provenance, license, checksum, and availability manifest for any dataset."""

    dataset_id: str
    source: str
    license_and_redistribution: str
    checksum_sha256: str
    file_path: str
    schema_type: Literal["ohlcv_1m_bars", "trade_ticks", "quotes", "sentiment_events"]
    tickers: list[str] = Field(min_length=1)
    period_start: datetime
    period_end: datetime
    timezone: Literal["UTC"] = "UTC"
    availability_method: str
    availability_assumed: bool = True
    provenance: ProvenanceType
    seed: int | None = None
    record_count: int = Field(ge=0)
    notes: str | None = None


def compute_sha256(file_path: str | Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    hasher = hashlib.sha256()
    with Path(file_path).open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def load_manifest(manifest_path: str | Path) -> DatasetManifest:
    """Load and validate a DatasetManifest JSON file."""
    raw = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    return DatasetManifest.model_validate(raw)


def verify_manifest_checksum(
    manifest_path: str | Path,
    repo_root: str | Path = ".",
) -> bool:
    """Verify the dataset file referenced by manifest matches its recorded SHA-256 checksum."""
    manifest = load_manifest(manifest_path)
    target = Path(repo_root) / manifest.file_path
    if not target.exists():
        return False
    return compute_sha256(target) == manifest.checksum_sha256


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def load_ohlcv_bars_from_csv(
    csv_path: str | Path,
    dataset_id: str,
    provenance: ProvenanceType,
    availability_lag_seconds: int = 5,
    availability_assumed: bool = True,
    ingested_at: datetime | None = None,
) -> list[MarketBarMessage]:
    """Read 1-minute OHLCV CSV rows into MarketBarMessage records without turning bars into fake ticks."""
    ingest_ts = ingested_at or datetime.now(UTC)
    bars: list[MarketBarMessage] = []
    with Path(csv_path).open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bar_start = _parse_utc(row["bar_start"])
            bar_end = _parse_utc(row["bar_end"])
            raw_avail = row.get("available_at", "").strip()
            if raw_avail:
                available_at = _parse_utc(raw_avail)
                row_assumed = availability_assumed
            else:
                available_at = bar_end + timedelta(seconds=availability_lag_seconds)
                row_assumed = True

            source_event_id = (
                row.get("source_event_id", "").strip()
                or f"{dataset_id}:{row['ticker']}:{bar_end.isoformat()}"
            )
            bars.append(
                MarketBarMessage(
                    ticker=row["ticker"].strip(),
                    bar_start=bar_start,
                    bar_end=bar_end,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=int(row["volume"]),
                    event_at=bar_end,
                    available_at=available_at,
                    ingested_at=ingest_ts,
                    availability_assumed=row_assumed,
                    dataset_id=dataset_id,
                    source_event_id=source_event_id,
                    provenance=provenance,
                )
            )
    return bars
