from __future__ import annotations

import json
import urllib.error
from collections import defaultdict
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from email.message import Message as EmailMessage
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg2
import pytest

from consumer.bar_consumer import BarConsumer, BarConsumerConfig
from consumer.base import PROGRESS_UPSERT_QUERY, QUARANTINE_INSERT_QUERY, DatabaseUnavailableError
from consumer.db import DatabaseManager
from consumer.tick_consumer import ConsumerConfig, TickConsumer
from pipeline.replay import (
    DeliveredKafkaOffset,
    PartitionDeliveryProgress,
    PostgresReplayStateStore,
    ReplayBarrierTimeoutError,
    ReplayCoordinator,
    ReplayEvent,
    partition_barrier_satisfied,
    resolve_dataset_end_status,
)
from producer.config import ProducerConfig
from producer.live_producer import LiveProducer
from producer.replay_producer import ReplayProducer
from producer.schema_validator import MarketTickMessage


class _InMemoryIngestionStore:
    """In-memory DB + ReplayStateStore harness enforcing Stage 2 SQL constraints."""

    def __init__(self) -> None:
        self.business_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.quarantine_rows: dict[tuple[str, int, int], tuple[Any, ...]] = {}
        self.progress: dict[tuple[str, str, int], PartitionDeliveryProgress] = {}
        self.runs: dict[str, dict[str, Any]] = {}

    def make_db_manager(self) -> MagicMock:
        db_mgr = MagicMock(spec=DatabaseManager)
        store = self

        class _Cursor:
            def execute(self, query: str, params: Any) -> None:
                if "ON CONFLICT (dataset_id, source_event_id) DO NOTHING" in query:
                    key = (str(params["dataset_id"]), str(params["source_event_id"]))
                    if key not in store.business_rows:
                        store.business_rows[key] = dict(params)
                elif query == PROGRESS_UPSERT_QUERY:
                    (
                        run_id,
                        topic,
                        partition,
                        offset,
                        available_at,
                        _src_id,
                        _proc_inc,
                        _quar_inc,
                        status,
                        _last_err,
                    ) = params
                    pkey = (str(run_id), str(topic), int(partition))
                    prev = store.progress.get(pkey)
                    new_offset = max(prev.last_kafka_offset, int(offset)) if prev else int(offset)
                    if prev and prev.last_available_at and available_at:
                        new_avail = max(prev.last_available_at, available_at)
                    else:
                        new_avail = available_at or (prev.last_available_at if prev else None)
                    store.progress[pkey] = PartitionDeliveryProgress(
                        topic=str(topic),
                        partition=int(partition),
                        last_kafka_offset=new_offset,
                        last_available_at=new_avail,
                        status=str(status),
                    )
                elif query == QUARANTINE_INSERT_QUERY:
                    qkey = (str(params[3]), int(params[4]), int(params[5]))
                    if qkey not in store.quarantine_rows:
                        store.quarantine_rows[qkey] = tuple(params)

            def __enter__(self) -> _Cursor:
                return self

            def __exit__(self, *_args: Any) -> None:
                return None

        @contextmanager
        def _tx():
            yield _Cursor()

        db_mgr.transaction.side_effect = _tx
        return db_mgr

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
        self.runs[run_id] = {
            "mode": mode,
            "dataset_id": dataset_id,
            "provenance": provenance,
            "logical_as_of": None,
            "dataset_end_at": dataset_end_at,
            "speed_multiplier": speed_multiplier,
            "contract_version": contract_version,
            "metadata": metadata or {},
            "status": "running",
        }

    def advance_logical_as_of(self, run_id: str, logical_as_of: datetime) -> None:
        self.runs[run_id]["logical_as_of"] = logical_as_of

    def mark_run_completed(self, run_id: str, dataset_end_at: datetime | None) -> None:
        self.runs[run_id]["status"] = "completed"
        if dataset_end_at is not None:
            self.runs[run_id]["dataset_end_at"] = dataset_end_at

    def get_delivery_progress(
        self, run_id: str, topic: str
    ) -> dict[int, PartitionDeliveryProgress]:
        return {
            partition: prog
            for (r_id, top, partition), prog in self.progress.items()
            if r_id == run_id and top == topic
        }

    def get_dataset_end_at(self, run_id: str) -> datetime | None:
        run = self.runs.get(run_id)
        return run["dataset_end_at"] if run else None


def _kafka_msg(payload: bytes | None, *, topic: str, partition: int, offset: int) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = payload
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


def test_duplicate_publish_with_new_kafka_offset_keeps_business_rows_unchanged() -> None:
    """Duplicate event delivered at a new partition/offset does not add duplicate business rows."""
    store = _InMemoryIngestionStore()
    db_mgr = store.make_db_manager()
    payload = {
        "ticker": "AAPL",
        "event_timestamp": "2026-01-02T14:30:00+00:00",
        "price": 185.10,
        "volume": 300,
        "replay_sequence": 1,
        "dataset_id": "ds_dup_test",
        "source_event_id": "ds_dup_test:AAPL:1",
        "run_id": "run_dup_1",
        "provenance": "real_historical",
    }
    raw = json.dumps(payload).encode("utf-8")

    with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        consumer = TickConsumer(ConsumerConfig(kafka_bootstrap_servers="localhost:9092"), db_mgr)

        consumer._process_message(_kafka_msg(raw, topic="raw.market.ticks", partition=0, offset=1))
        # Re-published after broker rebuild onto partition 2, offset 999
        consumer._process_message(
            _kafka_msg(raw, topic="raw.market.ticks", partition=2, offset=999)
        )

    assert len(store.business_rows) == 1
    assert ("ds_dup_test", "ds_dup_test:AAPL:1") in store.business_rows
    assert mock_kafka.commit.call_count == 2


def test_crash_between_db_commit_and_kafka_commit_recovers_cleanly() -> None:
    """Crash after DB commit but before Kafka offset commit leaves no loss and no duplicate."""
    store = _InMemoryIngestionStore()
    db_mgr = store.make_db_manager()
    payload = {
        "ticker": "NVDA",
        "event_timestamp": "2026-01-02T14:31:00+00:00",
        "price": 140.5,
        "volume": 500,
        "replay_sequence": 1,
        "dataset_id": "ds_crash",
        "source_event_id": "ds_crash:NVDA:1",
        "run_id": "run_crash",
        "provenance": "real_historical",
    }
    raw = json.dumps(payload).encode("utf-8")
    msg = _kafka_msg(raw, topic="raw.market.ticks", partition=1, offset=7)

    with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
        mock_kafka = MagicMock()
        mock_kafka.commit.side_effect = [RuntimeError("worker crashed pre-commit"), None]
        mock_consumer_cls.return_value = mock_kafka
        consumer = TickConsumer(ConsumerConfig(kafka_bootstrap_servers="localhost:9092"), db_mgr)

        with pytest.raises(RuntimeError, match="worker crashed pre-commit"):
            consumer._process_message(msg)

        # DB already persisted once; on Kafka redelivery it completes commit without duplicating
        assert len(store.business_rows) == 1
        consumer._process_message(msg)

    assert len(store.business_rows) == 1
    assert mock_kafka.commit.call_count == 2


def test_transient_db_down_retries_then_parks_partition_without_skipping_offset() -> None:
    """Exhausted transient DB errors park the partition and never commit/skip the Kafka offset."""
    db_mgr = MagicMock(spec=DatabaseManager)

    @contextmanager
    def _failing_tx():
        raise psycopg2.OperationalError("connection refused")
        yield  # pragma: no cover

    db_mgr.transaction.side_effect = _failing_tx
    sleeps: list[float] = []

    with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        consumer = TickConsumer(
            ConsumerConfig(kafka_bootstrap_servers="localhost:9092"),
            db_mgr,
            max_db_retries=2,
            retry_backoff_seconds=0.25,
            sleep_fn=sleeps.append,
        )
        msg = _kafka_msg(
            json.dumps(
                {
                    "ticker": "AAPL",
                    "event_timestamp": "2026-01-02T14:30:00+00:00",
                    "price": 180.0,
                    "volume": 100,
                    "replay_sequence": 1,
                }
            ).encode("utf-8"),
            topic="raw.market.ticks",
            partition=1,
            offset=42,
        )

        with pytest.raises(DatabaseUnavailableError):
            consumer._process_message(msg)

    assert sleeps == [0.25, 0.5]
    assert ("raw.market.ticks", 1) in consumer.parked_partitions
    mock_kafka.pause.assert_called_once()
    mock_kafka.commit.assert_not_called()


def test_poison_message_quarantined_and_traceable_without_secret_leakage() -> None:
    """Poison payload is sanitized, stored in raw.ingestion_quarantine, and then offset committed."""
    store = _InMemoryIngestionStore()
    db_mgr = store.make_db_manager()
    poison = (
        b'{"ticker": "AAPL", "price": "bad", "api_token": "secret-token-xyz", '
        b'"run_id": "run_q1", "dataset_id": "ds_q1", "source_event_id": "ev_q1"}'
    )
    msg = _kafka_msg(poison, topic="raw.market.ticks", partition=0, offset=11)

    with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        consumer = TickConsumer(ConsumerConfig(kafka_bootstrap_servers="localhost:9092"), db_mgr)
        consumer._process_message(msg)

    assert ("raw.market.ticks", 0, 11) in store.quarantine_rows
    qrow = store.quarantine_rows[("raw.market.ticks", 0, 11)]
    assert qrow[0] == "run_q1"
    assert qrow[1] == "ds_q1"
    assert qrow[2] == "ev_q1"
    assert "secret-token-xyz" not in qrow[8]
    assert "[REDACTED]" in qrow[8]
    mock_kafka.commit.assert_called_once_with(message=msg, asynchronous=False)


def test_two_availability_replays_produce_identical_business_state_even_out_of_order_input() -> (
    None
):
    """Two deterministic replays emit identical availability-ordered sequence regardless of speed."""
    t1 = datetime(2026, 1, 2, 14, 31, 5, tzinfo=UTC)
    t2 = datetime(2026, 1, 2, 14, 32, 5, tzinfo=UTC)
    events = [
        ReplayEvent(
            topic="raw.market.ticks",
            key="MSFT",
            payload=MarketTickMessage(
                ticker="MSFT",
                event_timestamp=t2,
                event_at=t2,
                available_at=t2,
                price=400.0,
                volume=10,
                replay_sequence=1,
                dataset_id="ds_det",
                source_event_id="ev_t2",
                provenance="real_historical",
            ),
            available_at=t2,
            source_event_id="ev_t2",
        ),
        ReplayEvent(
            topic="raw.market.ticks",
            key="AAPL",
            payload=MarketTickMessage(
                ticker="AAPL",
                event_timestamp=t1,
                event_at=t1,
                available_at=t1,
                price=185.0,
                volume=20,
                replay_sequence=2,
                dataset_id="ds_det",
                source_event_id="ev_t1",
                provenance="real_historical",
            ),
            available_at=t1,
            source_event_id="ev_t1",
        ),
    ]

    def _run_once(
        speed: float, run_id: str
    ) -> tuple[list[tuple[str, datetime]], dict[tuple[str, str], dict[str, Any]], datetime | None]:
        store = _InMemoryIngestionStore()
        db_mgr = store.make_db_manager()
        observed_clock_steps: list[tuple[str, datetime]] = []
        sleeps: list[float] = []

        with patch("consumer.tick_consumer.Consumer") as mock_consumer_cls:
            mock_consumer_cls.return_value = MagicMock()
            consumer = TickConsumer(
                ConsumerConfig(kafka_bootstrap_servers="localhost:9092"),
                db_mgr,
            )

            offsets: dict[int, int] = defaultdict(int)

            def _publish(
                r_id: str, batch_avail: datetime, batch_events: list[ReplayEvent]
            ) -> list[DeliveredKafkaOffset]:
                deliveries: list[DeliveredKafkaOffset] = []
                for ev in batch_events:
                    part = 0 if ev.key == "AAPL" else 2
                    off = offsets[part]
                    offsets[part] += 1
                    payload = ev.payload.model_copy(update={"run_id": r_id})
                    # Ensure future data cannot be visible in consumer DB before barrier completes
                    assert ev.available_at == batch_avail
                    consumer._process_message(
                        _kafka_msg(
                            payload.model_dump_json().encode("utf-8"),
                            topic=ev.topic,
                            partition=part,
                            offset=off,
                        )
                    )
                    deliveries.append(
                        DeliveredKafkaOffset(
                            topic=ev.topic,
                            partition=part,
                            offset=off,
                            available_at=batch_avail,
                        )
                    )
                return deliveries

            coordinator = ReplayCoordinator(state_store=store, sleep_fn=sleeps.append)
            summary = coordinator.execute(
                events,
                publish_batch_fn=_publish,
                run_id=run_id,
                dataset_id="ds_det",
                provenance="real_historical",
                speed_multiplier=speed,
                on_clock_advance=lambda rid, ts: observed_clock_steps.append((rid, ts)),
            )

        comparable_rows = {
            k: {
                "ticker": v["ticker"],
                "price": v["price"],
                "available_at": v["available_at"],
                "dataset_id": v["dataset_id"],
                "source_event_id": v["source_event_id"],
            }
            for k, v in store.business_rows.items()
        }
        return observed_clock_steps, comparable_rows, summary.dataset_end_at

    steps_fast, rows_fast, end_fast = _run_once(speed=100.0, run_id="replay_fast")
    steps_slow, rows_slow, end_slow = _run_once(speed=1.0, run_id="replay_slow")

    expected_steps = [
        t1,
        t2,
        t1.replace(second=0) + timedelta(minutes=1, seconds=15),
        t2.replace(second=0) + timedelta(minutes=1, seconds=15),
    ]
    assert [ts for _, ts in steps_fast] == sorted(set(expected_steps))
    assert [ts for _, ts in steps_slow] == sorted(set(expected_steps))
    assert rows_fast == rows_slow
    assert end_fast == end_slow == t2.replace(second=0) + timedelta(minutes=1)


def test_idle_partition_inherits_progress_and_prior_run_progress_does_not_bypass_barrier() -> None:
    """Idle partitions never block finalization, and old run progress cannot satisfy a new run."""
    t1 = datetime(2026, 1, 2, 14, 31, 5, tzinfo=UTC)
    ok, completed = partition_barrier_satisfied(
        num_partitions=3,
        expected_offsets={0: 4},
        prior_completed_offsets={1: 12},
        observed_progress={
            0: PartitionDeliveryProgress(
                "raw.market.ticks", 0, last_kafka_offset=4, last_available_at=t1
            ),
        },
        batch_available_at=t1,
    )
    assert ok is True
    assert completed == {0: 4, 1: 12, 2: -1}

    # New run_id MUST NOT pass barrier using stale old_run progress
    store = _InMemoryIngestionStore()
    store.progress[("old_run", "raw.market.ticks", 0)] = PartitionDeliveryProgress(
        "raw.market.ticks", 0, last_kafka_offset=999, last_available_at=t1
    )
    tick_now = [0.0]

    def _mono() -> float:
        tick_now[0] += 0.2
        return tick_now[0]

    coordinator = ReplayCoordinator(
        state_store=store,
        barrier_timeout_seconds=0.3,
        barrier_poll_interval_seconds=0.01,
        sleep_fn=lambda _: None,
        monotonic_fn=_mono,
    )
    with pytest.raises(ReplayBarrierTimeoutError):
        coordinator.wait_for_batch_barrier(
            run_id="new_run",
            batch_available_at=t1,
            deliveries=[DeliveredKafkaOffset("raw.market.ticks", 0, 0, t1)],
            completed_offsets={},
        )


def test_dataset_end_boundary_marks_out_of_coverage_target_unavailable() -> None:
    """Targets after dataset_end_at resolve as unavailable: dataset_end."""
    end_at = datetime(2026, 1, 2, 15, 0, 5, tzinfo=UTC)
    assert resolve_dataset_end_status(datetime(2026, 1, 2, 15, 0, 5, tzinfo=UTC), end_at) is None
    assert (
        resolve_dataset_end_status(datetime(2026, 1, 2, 15, 5, 5, tzinfo=UTC), end_at)
        == "unavailable: dataset_end"
    )


def test_manifest_smoke_fixture_replays_into_bar_consumer_via_replay_producer() -> None:
    """Default smoke_1m_v1 manifest replays 89 canonical OHLCV bars into BarConsumer."""
    store = _InMemoryIngestionStore()
    db_mgr = store.make_db_manager()
    manifest_path = Path("data/manifests/fixture_smoke_v1.json")

    with (
        patch("producer.replay_producer.Producer") as mock_prod_cls,
        patch("consumer.bar_consumer.Consumer") as mock_cons_cls,
    ):
        mock_prod = MagicMock()
        mock_prod_cls.return_value = mock_prod
        mock_cons_cls.return_value = MagicMock()

        bar_consumer = BarConsumer(
            BarConsumerConfig(kafka_bootstrap_servers="localhost:9092"), db_mgr
        )
        produced_records: list[tuple[str, bytes]] = []

        def _fake_produce(topic: str, key: str, value: str, on_delivery: Any = None) -> None:
            raw_bytes = value.encode("utf-8")
            produced_records.append((topic, raw_bytes))
            msg = _kafka_msg(raw_bytes, topic=topic, partition=0, offset=len(produced_records) - 1)
            if on_delivery is not None:
                on_delivery(None, msg)
            bar_consumer._process_message(msg)

        mock_prod.produce.side_effect = _fake_produce

        cfg = ProducerConfig(
            kafka_bootstrap_servers="localhost:9092",
            replay_manifest_path=str(manifest_path),
            replay_speed_multiplier=0.0,
            replay_wait_for_barrier=True,
        )
        summary = ReplayProducer(cfg, sleep_fn=lambda _: None).run(state_store=store)

    assert summary is not None
    assert summary.dataset_id == "fixture_smoke_v1"
    assert summary.provenance == "synthetic_fixture"
    assert summary.events_published == 89
    assert summary.status == "completed"
    assert len(store.business_rows) == 89
    assert all(topic == "raw.market.bars" for topic, _ in produced_records)


def test_live_producer_real_mode_reports_unavailable_without_fabricating_mock_prices() -> None:
    """In real_live mode, HTTP 403 / empty responses mark unavailable and never fabricate prices."""
    with patch("producer.live_producer.Producer") as mock_prod_cls:
        mock_prod = MagicMock()
        mock_prod_cls.return_value = mock_prod
        cfg = ProducerConfig(
            kafka_bootstrap_servers="localhost:9092",
            live_mode="real",
            finnhub_api_key="real-finnhub-key",
            marketaux_api_key="real-marketaux-key",
        )
        live = LiveProducer(cfg, sleep_fn=lambda _: None)
        assert live.mode == "real_live"
        assert live.provenance == "real_live"

        err_403 = urllib.error.HTTPError(
            url="https://finnhub.io/api/v1/stock/candle?token=real-finnhub-key",
            code=403,
            msg="Forbidden",
            hdrs=EmailMessage(),
            fp=None,
        )
        with patch("urllib.request.urlopen", side_effect=err_403):
            live.backfill_history()
            live.get_market_ticks()
            live.get_sentiment()

        assert mock_prod.produce.call_count == 0
        assert all(live.unavailable_tickers[t].startswith("unavailable:") for t in live.tickers)


def test_live_producer_synthetic_vs_real_http_200_provenance() -> None:
    """Explicit synthetic mode tags synthetic_mock while real HTTP 200 quotes tag real_live."""
    with patch("producer.live_producer.Producer") as mock_prod_cls:
        mock_prod = MagicMock()
        mock_prod_cls.return_value = mock_prod

        synth = LiveProducer(
            ProducerConfig(kafka_bootstrap_servers="localhost:9092", live_mode="synthetic"),
            sleep_fn=lambda _: None,
        )
        assert synth.mode == "synthetic_mock"
        synth.get_market_ticks()
        synth.get_sentiment()
        assert mock_prod.produce.call_count == len(synth.tickers) + 1
        for call in mock_prod.produce.call_args_list:
            payload = json.loads(call.kwargs["value"])
            assert payload["provenance"] == "synthetic_mock"

        mock_prod.produce.reset_mock()
        real = LiveProducer(
            ProducerConfig(
                kafka_bootstrap_servers="localhost:9092",
                live_mode="real",
                finnhub_api_key="real-finnhub-key",
                marketaux_api_key="real-marketaux-key",
            ),
            sleep_fn=lambda _: None,
        )

        class _FakeResponse:
            def __init__(self, body: dict[str, Any]) -> None:
                self._body = json.dumps(body).encode("utf-8")

            def read(self) -> bytes:
                return self._body

            def __enter__(self) -> _FakeResponse:
                return self

            def __exit__(self, *_args: Any) -> None:
                return None

        def _fake_urlopen(req: Any, timeout: float = 5) -> _FakeResponse:
            if "marketaux.com" in req.full_url:
                return _FakeResponse(
                    {
                        "data": [
                            {
                                "uuid": "art-1",
                                "title": "AAPL climbs on earnings",
                                "published_at": "2026-01-02T15:00:00Z",
                                "entities": [{"symbol": "AAPL", "sentiment_score": 0.65}],
                            }
                        ]
                    }
                )
            return _FakeResponse({"c": 191.25, "t": 1767366000})

        with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            real.get_market_ticks()
            real.get_sentiment()

        assert mock_prod.produce.call_count == len(real.tickers) + 1
        for call in mock_prod.produce.call_args_list:
            payload = json.loads(call.kwargs["value"])
            assert payload["provenance"] == "real_live"


def test_postgres_replay_state_store_executes_parameterized_run_queries() -> None:
    """PostgresReplayStateStore uses DatabaseManager.transaction() for lifecycle updates."""
    db_mgr = MagicMock(spec=DatabaseManager)
    cur = MagicMock()
    cur.fetchone.return_value = (datetime(2026, 1, 2, 16, 0, tzinfo=UTC),)
    cur.fetchall.return_value = [(0, 12, datetime(2026, 1, 2, 15, 0, tzinfo=UTC), "active")]

    @contextmanager
    def _tx():
        yield cur

    db_mgr.transaction.side_effect = _tx
    store = PostgresReplayStateStore(db_mgr)
    end_at = datetime(2026, 1, 2, 16, 0, tzinfo=UTC)

    store.start_run(
        "run_pg_1",
        mode="replay",
        dataset_id="fixture_smoke_v1",
        provenance="synthetic_fixture",
        speed_multiplier=0.0,
        dataset_end_at=end_at,
    )
    store.advance_logical_as_of("run_pg_1", end_at)
    store.mark_run_completed("run_pg_1", end_at)
    prog = store.get_delivery_progress("run_pg_1", "raw.market.bars")
    loaded_end = store.get_dataset_end_at("run_pg_1")

    assert prog[0].last_kafka_offset == 12
    assert loaded_end == end_at
    assert cur.execute.call_count == 5
