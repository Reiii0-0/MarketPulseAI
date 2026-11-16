from __future__ import annotations

import json
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from pydantic import BaseModel

from consumer.db import DatabaseManager
from pipeline.contract import compute_bar_bounds, load_forecast_contract

logger = logging.getLogger(__name__)


class ReplayBarrierTimeoutError(TimeoutError):
    """Raised when partition delivery barrier does not complete within timeout or is parked."""


@dataclass(frozen=True)
class ReplayEvent:
    """Single canonical event scheduled for availability-ordered deterministic replay."""

    topic: str
    key: str
    payload: BaseModel
    available_at: datetime
    source_event_id: str

    def __post_init__(self) -> None:
        if self.available_at.tzinfo is None:
            object.__setattr__(self, "available_at", self.available_at.replace(tzinfo=UTC))
        else:
            object.__setattr__(self, "available_at", self.available_at.astimezone(UTC))


@dataclass(frozen=True)
class DeliveredKafkaOffset:
    """Confirmed Kafka broker delivery location for barrier tracking."""

    topic: str
    partition: int
    offset: int
    available_at: datetime


@dataclass(frozen=True)
class PartitionDeliveryProgress:
    """Persisted consumer delivery state for one (run_id, topic, partition)."""

    topic: str
    partition: int
    last_kafka_offset: int
    last_available_at: datetime | None
    status: str = "active"


@dataclass(frozen=True)
class ReplaySummary:
    """Deterministic summary produced by ReplayCoordinator.execute."""

    run_id: str
    dataset_id: str
    provenance: str
    events_published: int
    batches_completed: int
    logical_as_of: datetime | None
    dataset_end_at: datetime | None
    status: str = "completed"


def generate_run_id(prefix: str = "replay") -> str:
    """Generate a unique run identifier isolating delivery barriers and clock state."""
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{ts}_{uuid.uuid4().hex[:8]}"


def resolve_dataset_end_status(
    target_at: datetime,
    dataset_end_at: datetime | None,
) -> str | None:
    """Return 'unavailable: dataset_end' when target_at exceeds dataset coverage end."""
    if dataset_end_at is None:
        return None
    target_utc = target_at if target_at.tzinfo else target_at.replace(tzinfo=UTC)
    end_utc = dataset_end_at if dataset_end_at.tzinfo else dataset_end_at.replace(tzinfo=UTC)
    if target_utc > end_utc:
        return "unavailable: dataset_end"
    return None


def partition_barrier_satisfied(
    *,
    num_partitions: int,
    expected_offsets: dict[int, int],
    prior_completed_offsets: dict[int, int],
    observed_progress: dict[int, PartitionDeliveryProgress],
    batch_available_at: datetime | None = None,
) -> tuple[bool, dict[int, int]]:
    """Evaluate per-partition replay barrier, allowing idle partitions to inherit prior progress.

    Active partitions (present in ``expected_offsets``) must reach at least their expected
    Kafka offset (or ``batch_available_at`` when broker offset metadata is unavailable).
    Idle partitions (absent from ``expected_offsets``) carry forward their last completed
    offset and never stall finalization or ``logical_as_of`` advancement.
    """
    next_completed = dict(prior_completed_offsets)
    for partition in range(num_partitions):
        prog = observed_progress.get(partition)
        if prog is not None and prog.status == "parked_error":
            raise ReplayBarrierTimeoutError(
                f"Partition {partition} is in parked_error state; aborting replay barrier"
            )

        if partition in expected_offsets:
            target_offset = expected_offsets[partition]
            if prog is None:
                return False, prior_completed_offsets
            if target_offset >= 0:
                if prog.last_kafka_offset < target_offset:
                    return False, prior_completed_offsets
                next_completed[partition] = max(
                    next_completed.get(partition, -1), prog.last_kafka_offset
                )
            elif batch_available_at is not None:
                if prog.last_available_at is None or prog.last_available_at < batch_available_at:
                    return False, prior_completed_offsets
                next_completed[partition] = max(
                    next_completed.get(partition, -1), prog.last_kafka_offset
                )
        else:
            # Idle partition inherits last completed offset without blocking advancement
            inherited = prior_completed_offsets.get(partition, -1)
            if prog is not None and prog.last_kafka_offset > inherited:
                inherited = prog.last_kafka_offset
            next_completed[partition] = inherited

    return True, next_completed


class ReplayStateStore(Protocol):
    """Persistence contract for run metadata and per-run partition delivery progress."""

    def start_run(
        self,
        run_id: str,
        *,
        mode: str,
        dataset_id: str,
        provenance: str,
        speed_multiplier: float | None,
        dataset_end_at: datetime | None,
        contract_version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None: ...

    def advance_logical_as_of(self, run_id: str, logical_as_of: datetime) -> None: ...

    def mark_run_completed(self, run_id: str, dataset_end_at: datetime | None) -> None: ...

    def get_delivery_progress(
        self, run_id: str, topic: str
    ) -> dict[int, PartitionDeliveryProgress]: ...

    def get_dataset_end_at(self, run_id: str) -> datetime | None: ...


class PostgresReplayStateStore:
    """PostgreSQL implementation of ReplayStateStore backed by DatabaseManager."""

    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db = db_manager

    def start_run(
        self,
        run_id: str,
        *,
        mode: str,
        dataset_id: str,
        provenance: str,
        speed_multiplier: float | None,
        dataset_end_at: datetime | None,
        contract_version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        query = """
            INSERT INTO raw.ingestion_runs (
                run_id, mode, dataset_id, provenance, contract_version,
                status, logical_as_of, dataset_end_at, speed_multiplier, metadata,
                started_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, 'running', NULL, %s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (run_id) DO UPDATE SET
                status = 'running',
                dataset_end_at = EXCLUDED.dataset_end_at,
                speed_multiplier = EXCLUDED.speed_multiplier,
                metadata = EXCLUDED.metadata,
                updated_at = NOW()
            WHERE raw.ingestion_runs.dataset_id = EXCLUDED.dataset_id
              AND raw.ingestion_runs.provenance = EXCLUDED.provenance
            RETURNING run_id;
        """
        with self.db.transaction() as cur:
            cur.execute(
                query,
                (
                    run_id,
                    mode,
                    dataset_id,
                    provenance,
                    contract_version,
                    dataset_end_at,
                    speed_multiplier,
                    json.dumps(metadata or {}),
                ),
            )
            if cur.fetchone() is None:
                raise ValueError("Cannot reuse run_id with different dataset or provenance")

    def advance_logical_as_of(self, run_id: str, logical_as_of: datetime) -> None:
        query = """
            UPDATE raw.ingestion_runs
            SET logical_as_of = CASE
                    WHEN logical_as_of IS NULL THEN %s
                    ELSE GREATEST(logical_as_of, %s)
                END,
                updated_at = NOW()
            WHERE run_id = %s;
        """
        with self.db.transaction() as cur:
            cur.execute(query, (logical_as_of, logical_as_of, run_id))

    def mark_run_completed(self, run_id: str, dataset_end_at: datetime | None) -> None:
        query = """
            UPDATE raw.ingestion_runs
            SET status = 'completed',
                dataset_end_at = COALESCE(%s, dataset_end_at),
                completed_at = NOW(),
                updated_at = NOW()
            WHERE run_id = %s;
        """
        with self.db.transaction() as cur:
            cur.execute(query, (dataset_end_at, run_id))

    def get_delivery_progress(
        self, run_id: str, topic: str
    ) -> dict[int, PartitionDeliveryProgress]:
        query = """
            SELECT kafka_partition, last_kafka_offset, last_available_at, status
            FROM raw.run_delivery_progress
            WHERE run_id = %s AND topic = %s;
        """
        with self.db.transaction() as cur:
            cur.execute(query, (run_id, topic))
            rows = cur.fetchall()
        result: dict[int, PartitionDeliveryProgress] = {}
        for partition, last_offset, last_avail, status in rows:
            result[int(partition)] = PartitionDeliveryProgress(
                topic=topic,
                partition=int(partition),
                last_kafka_offset=int(last_offset),
                last_available_at=last_avail,
                status=str(status),
            )
        return result

    def get_dataset_end_at(self, run_id: str) -> datetime | None:
        query = "SELECT dataset_end_at FROM raw.ingestion_runs WHERE run_id = %s;"
        with self.db.transaction() as cur:
            cur.execute(query, (run_id,))
            row = cur.fetchone()
        return row[0] if row else None


@dataclass
class ReplayCoordinator:
    """Coordinates availability-ordered replay batches, per-run barriers, and logical clock."""

    state_store: ReplayStateStore | None = None
    topic_partitions: dict[str, int] = field(
        default_factory=lambda: {
            "raw.market.ticks": 3,
            "raw.sentiment.events": 3,
            "raw.market.bars": 3,
        }
    )
    default_num_partitions: int = 3
    barrier_poll_interval_seconds: float = 0.05
    barrier_timeout_seconds: float = 30.0
    sleep_fn: Callable[[float], None] = time.sleep
    monotonic_fn: Callable[[], float] = time.monotonic

    @staticmethod
    def sort_by_availability(events: Sequence[ReplayEvent]) -> list[ReplayEvent]:
        """Sort events strictly by (available_at, topic, key, source_event_id)."""
        return sorted(
            events,
            key=lambda e: (e.available_at, e.topic, e.key, e.source_event_id),
        )

    @staticmethod
    def iter_availability_batches(
        sorted_events: Sequence[ReplayEvent],
    ) -> list[tuple[datetime, list[ReplayEvent]]]:
        """Group sorted events so all items sharing the same available_at form one barrier step."""
        grouped: dict[datetime, list[ReplayEvent]] = defaultdict(list)
        ordered_times: list[datetime] = []
        for event in sorted_events:
            if event.available_at not in grouped:
                ordered_times.append(event.available_at)
            grouped[event.available_at].append(event)
        return [(ts, grouped[ts]) for ts in ordered_times]

    def wait_for_batch_barrier(
        self,
        *,
        run_id: str,
        batch_available_at: datetime,
        deliveries: Sequence[DeliveredKafkaOffset],
        completed_offsets: dict[str, dict[int, int]],
    ) -> None:
        """Wait until all active partitions catch up in run_delivery_progress for run_id."""
        if self.state_store is None:
            return

        expected_by_topic: dict[str, dict[int, int]] = defaultdict(dict)
        for item in deliveries:
            prev = expected_by_topic[item.topic].get(item.partition, -1)
            expected_by_topic[item.topic][item.partition] = max(prev, item.offset)

        deadline = self.monotonic_fn() + self.barrier_timeout_seconds
        while True:
            all_ok = True
            next_completed_snapshot: dict[str, dict[int, int]] = {}
            for topic, expected_offsets in expected_by_topic.items():
                num_parts = self.topic_partitions.get(topic, self.default_num_partitions)
                observed = self.state_store.get_delivery_progress(run_id, topic)
                ok, next_offsets = partition_barrier_satisfied(
                    num_partitions=num_parts,
                    expected_offsets=expected_offsets,
                    prior_completed_offsets=completed_offsets.get(topic, {}),
                    observed_progress=observed,
                    batch_available_at=batch_available_at,
                )
                if not ok:
                    all_ok = False
                    break
                next_completed_snapshot[topic] = next_offsets

            if all_ok:
                completed_offsets.update(next_completed_snapshot)
                return

            if self.monotonic_fn() >= deadline:
                raise ReplayBarrierTimeoutError(
                    f"Timed out waiting for delivery barrier on run_id={run_id} "
                    f"at available_at={batch_available_at.isoformat()}"
                )
            self.sleep_fn(self.barrier_poll_interval_seconds)

    def execute(
        self,
        events: Sequence[ReplayEvent],
        *,
        publish_batch_fn: Callable[[str, datetime, list[ReplayEvent]], list[DeliveredKafkaOffset]],
        run_id: str | None = None,
        dataset_id: str = "replay_dataset",
        provenance: str = "real_historical",
        speed_multiplier: float = 0.0,
        contract_version: str | None = None,
        on_clock_advance: Callable[[str, datetime], None] | None = None,
    ) -> ReplaySummary:
        """Execute availability-ordered replay across batches with barrier & clock isolation."""
        resolved_run_id = run_id or generate_run_id("replay")
        sorted_events = self.sort_by_availability(events)
        if not sorted_events:
            if self.state_store is not None:
                self.state_store.start_run(
                    resolved_run_id,
                    mode="replay",
                    dataset_id=dataset_id,
                    provenance=provenance,
                    speed_multiplier=speed_multiplier,
                    dataset_end_at=None,
                    contract_version=contract_version,
                )
                self.state_store.mark_run_completed(resolved_run_id, None)
            return ReplaySummary(
                run_id=resolved_run_id,
                dataset_id=dataset_id,
                provenance=provenance,
                events_published=0,
                batches_completed=0,
                logical_as_of=None,
                dataset_end_at=None,
            )

        contract = load_forecast_contract()
        market_ends = [
            getattr(event.payload, "bar_end", None)
            or (
                compute_bar_bounds(
                    cast(datetime, getattr(event.payload, "event_at", None)),
                    contract.bar_interval_seconds,
                )[1]
                if event.topic.endswith("ticks")
                else None
            )
            for event in sorted_events
        ]
        dataset_end_at = max(
            (end for end in market_ends if end is not None), default=sorted_events[-1].available_at
        )
        scheduled = dict(self.iter_availability_batches(sorted_events))
        # Empty decision steps allow finalization during sparse arrivals and drain the final bar.
        for end in market_ends:
            if end is not None:
                scheduled.setdefault(end + timedelta(seconds=contract.grace_seconds), [])
        batches = sorted(scheduled.items())
        if self.state_store is not None:
            self.state_store.start_run(
                resolved_run_id,
                mode="replay",
                dataset_id=dataset_id,
                provenance=provenance,
                speed_multiplier=speed_multiplier,
                dataset_end_at=dataset_end_at,
                contract_version=contract_version,
            )

        completed_offsets: dict[str, dict[int, int]] = defaultdict(dict)
        total_published = 0
        prev_available_at = batches[0][0]
        logical_as_of: datetime | None = None

        for idx, (batch_available_at, batch_events) in enumerate(batches):
            if idx > 0 and speed_multiplier > 0.0:
                delta_seconds = (batch_available_at - prev_available_at).total_seconds()
                if delta_seconds > 0:
                    self.sleep_fn(delta_seconds / speed_multiplier)
            prev_available_at = batch_available_at

            deliveries = publish_batch_fn(resolved_run_id, batch_available_at, batch_events)
            total_published += len(batch_events)

            self.wait_for_batch_barrier(
                run_id=resolved_run_id,
                batch_available_at=batch_available_at,
                deliveries=deliveries,
                completed_offsets=completed_offsets,
            )

            logical_as_of = batch_available_at
            if self.state_store is not None:
                self.state_store.advance_logical_as_of(resolved_run_id, batch_available_at)
            if on_clock_advance is not None:
                on_clock_advance(resolved_run_id, batch_available_at)

        if self.state_store is not None:
            self.state_store.mark_run_completed(resolved_run_id, dataset_end_at)

        return ReplaySummary(
            run_id=resolved_run_id,
            dataset_id=dataset_id,
            provenance=provenance,
            events_published=total_published,
            batches_completed=len(batches),
            logical_as_of=logical_as_of,
            dataset_end_at=dataset_end_at,
        )
