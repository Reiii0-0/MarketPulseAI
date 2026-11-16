from __future__ import annotations

import hashlib
import json
import logging
import re
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.pool
from confluent_kafka import Consumer, KafkaError, TopicPartition
from pydantic import BaseModel, ValidationError

from consumer.db import DatabaseManager

logger = logging.getLogger(__name__)

DEFAULT_RUN_ID = "live_default"
_SENSITIVE_KV_RE = re.compile(
    r"(?i)(['\"]?(?:api[_-]?key|apikey|token|secret|password|authorization)['\"]?\s*[:=]\s*)(['\"]?)([^'\",\s}]+)(\2)"
)
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9\-._~+/]+=*")


class DatabaseUnavailableError(RuntimeError):
    """Raised when transient database failures exhaust retries and a partition is parked."""


@dataclass(frozen=True)
class KafkaMessageMeta:
    """Metadata extracted from a consumed Kafka message."""

    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class BusinessUpsertSpec:
    """Parameterized business insert statement plus delivery-progress metadata."""

    query: str
    params: tuple[Any, ...] | dict[str, Any]
    run_id: str
    source_event_id: str
    available_at: datetime | None


PROGRESS_UPSERT_QUERY = """
INSERT INTO raw.run_delivery_progress (
    run_id,
    topic,
    kafka_partition,
    last_kafka_offset,
    last_available_at,
    last_source_event_id,
    processed_count,
    quarantined_count,
    status,
    last_error,
    updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (run_id, topic, kafka_partition) DO UPDATE SET
    last_kafka_offset = GREATEST(
        raw.run_delivery_progress.last_kafka_offset,
        EXCLUDED.last_kafka_offset
    ),
    last_available_at = COALESCE(
        GREATEST(
            raw.run_delivery_progress.last_available_at,
            EXCLUDED.last_available_at
        ),
        EXCLUDED.last_available_at,
        raw.run_delivery_progress.last_available_at
    ),
    last_source_event_id = COALESCE(
        EXCLUDED.last_source_event_id,
        raw.run_delivery_progress.last_source_event_id
    ),
    processed_count = raw.run_delivery_progress.processed_count + EXCLUDED.processed_count,
    quarantined_count = raw.run_delivery_progress.quarantined_count + EXCLUDED.quarantined_count,
    status = EXCLUDED.status,
    last_error = EXCLUDED.last_error,
    updated_at = NOW();
"""

QUARANTINE_INSERT_QUERY = """
INSERT INTO raw.ingestion_quarantine (
    run_id,
    dataset_id,
    source_event_id,
    topic,
    kafka_partition,
    kafka_offset,
    error_type,
    error_reason,
    sanitized_payload,
    payload_sha256
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING;
"""


def sanitize_payload(raw: bytes | str | None, max_length: int = 2048) -> tuple[str, str]:
    """Redact common credential fields, strip NUL bytes, and truncate poison payloads."""
    if raw is None:
        raw_bytes = b""
        text = "<empty>"
    elif isinstance(raw, bytes):
        raw_bytes = raw
        text = raw.decode("utf-8", errors="replace")
    else:
        text = str(raw)
        raw_bytes = text.encode("utf-8", errors="replace")

    sha256_hex = hashlib.sha256(raw_bytes).hexdigest()
    cleaned = text.replace("\x00", "")
    cleaned = _BEARER_RE.sub("Bearer [REDACTED]", cleaned)
    cleaned = _SENSITIVE_KV_RE.sub(r"\1\2[REDACTED]\4", cleaned)
    if len(cleaned) > max_length:
        cleaned = f"{cleaned[:max_length]}...[truncated]"
    return cleaned, sha256_hex


def compute_fallback_source_event_id(prefix: str, parts: tuple[Any, ...]) -> str:
    """Compute a deterministic business event ID when a legacy producer omits source_event_id."""
    canonical = json.dumps([str(part) for part in parts], separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]
    return f"{prefix}:{digest}"


def is_transient_db_error(exc: BaseException) -> bool:
    """Identify transient database connectivity/pool errors safe for bounded retry."""
    if isinstance(
        exc,
        psycopg2.OperationalError
        | psycopg2.InterfaceError
        | psycopg2.pool.PoolError
        | ConnectionError
        | TimeoutError
        | OSError,
    ):
        return True
    non_transient = (psycopg2.DataError, psycopg2.IntegrityError, psycopg2.ProgrammingError)
    return isinstance(exc, psycopg2.DatabaseError) and not isinstance(exc, non_transient)


def _extract_trace_field(payload: dict[str, Any] | None, key: str, max_len: int) -> str | None:
    if not isinstance(payload, dict):
        return None
    val = payload.get(key)
    if val is None:
        return None
    text = str(val).strip()
    if not text:
        return None
    return text[:max_len]


class ReliableKafkaConsumer:
    """Shared transactional Kafka consumer with business-identity dedup and poison quarantine."""

    def __init__(
        self,
        config: Any,
        db_manager: DatabaseManager,
        *,
        topic: str,
        group_id: str,
        max_db_retries: int | None = None,
        retry_backoff_seconds: float | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        consumer_cls: Any = Consumer,
    ) -> None:
        self.config = config
        self.db = db_manager
        self.topic = topic
        self.group_id = group_id
        raw_retries = getattr(config, "max_db_retries", 3)
        self.max_db_retries = (
            max_db_retries
            if max_db_retries is not None
            else (
                raw_retries
                if isinstance(raw_retries, int) and not isinstance(raw_retries, bool)
                else 3
            )
        )
        raw_backoff = getattr(config, "retry_backoff_seconds", 0.5)
        self.retry_backoff_seconds = (
            retry_backoff_seconds
            if retry_backoff_seconds is not None
            else (
                float(raw_backoff)
                if isinstance(raw_backoff, int | float) and not isinstance(raw_backoff, bool)
                else 0.5
            )
        )
        self.sleep_fn = sleep_fn
        self.running = False
        self.parked_partitions: set[tuple[str, int]] = set()
        self.consumer = consumer_cls(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": getattr(config, "kafka_auto_offset_reset", "earliest"),
                "enable.auto.commit": False,
            }
        )

    def _extract_meta(self, msg: Any) -> KafkaMessageMeta:
        raw_topic = msg.topic()
        topic = raw_topic if isinstance(raw_topic, str) and raw_topic else self.topic
        raw_part = msg.partition()
        partition = raw_part if isinstance(raw_part, int) and not isinstance(raw_part, bool) else 0
        raw_offset = msg.offset()
        offset = (
            raw_offset if isinstance(raw_offset, int) and not isinstance(raw_offset, bool) else 0
        )
        return KafkaMessageMeta(topic=topic, partition=partition, offset=offset)

    def _commit_offset(self, msg: Any) -> None:
        self.consumer.commit(message=msg, asynchronous=False)

    def _park_partition(self, meta: KafkaMessageMeta, exc: BaseException) -> None:
        self.parked_partitions.add((meta.topic, meta.partition))
        logger.error(
            "Transient DB error exhausted retries; parking %s[%s] at offset=%s without committing: %s",
            meta.topic,
            meta.partition,
            meta.offset,
            exc,
        )
        try:
            self.consumer.pause([TopicPartition(meta.topic, meta.partition, meta.offset)])
        except Exception as pause_exc:
            logger.warning("Could not pause %s[%s]: %s", meta.topic, meta.partition, pause_exc)

    def _run_with_db_retry(
        self,
        meta: KafkaMessageMeta,
        operation: Callable[[], None],
    ) -> None:
        for attempt in range(self.max_db_retries + 1):
            try:
                operation()
                return
            except Exception as exc:
                if not is_transient_db_error(exc):
                    raise
                if attempt < self.max_db_retries:
                    delay = self.retry_backoff_seconds * (2**attempt)
                    logger.warning(
                        "Transient DB error for %s[%s]@%s (attempt %s/%s); retrying in %.2fs: %s",
                        meta.topic,
                        meta.partition,
                        meta.offset,
                        attempt + 1,
                        self.max_db_retries + 1,
                        delay,
                        exc,
                    )
                    self.sleep_fn(delay)
                else:
                    self._park_partition(meta, exc)
                    raise DatabaseUnavailableError(
                        f"Database unavailable after {self.max_db_retries + 1} attempts "
                        f"on {meta.topic}[{meta.partition}]@{meta.offset}"
                    ) from exc

    def _persist_quarantine(
        self,
        msg: Any,
        meta: KafkaMessageMeta,
        raw_bytes: bytes | str | None,
        parsed_dict: dict[str, Any] | None,
        error_type: str,
        error_reason: str,
    ) -> None:
        sanitized_payload, payload_sha256 = sanitize_payload(raw_bytes)
        run_id = _extract_trace_field(parsed_dict, "run_id", 120) or DEFAULT_RUN_ID
        dataset_id = _extract_trace_field(parsed_dict, "dataset_id", 120)
        source_event_id = _extract_trace_field(parsed_dict, "source_event_id", 255)
        truncated_reason, _ = sanitize_payload(error_reason, max_length=1000)

        def _op() -> None:
            with self.db.transaction() as cur:
                cur.execute(
                    QUARANTINE_INSERT_QUERY,
                    (
                        run_id,
                        dataset_id,
                        source_event_id,
                        meta.topic,
                        meta.partition,
                        meta.offset,
                        error_type[:64],
                        truncated_reason,
                        sanitized_payload,
                        payload_sha256,
                    ),
                )
                cur.execute(
                    PROGRESS_UPSERT_QUERY,
                    (
                        run_id,
                        meta.topic,
                        meta.partition,
                        meta.offset,
                        None,
                        source_event_id,
                        0,
                        1,
                        "active",
                        None,
                    ),
                )

        self._run_with_db_retry(meta, _op)
        self._commit_offset(msg)

    def _persist_business_record(self, meta: KafkaMessageMeta, spec: BusinessUpsertSpec) -> None:
        def _op() -> None:
            with self.db.transaction() as cur:
                cur.execute(spec.query, spec.params)
                cur.execute(
                    PROGRESS_UPSERT_QUERY,
                    (
                        spec.run_id,
                        meta.topic,
                        meta.partition,
                        meta.offset,
                        spec.available_at,
                        spec.source_event_id,
                        1,
                        0,
                        "active",
                        None,
                    ),
                )

        self._run_with_db_retry(meta, _op)

    def parse_record(self, payload: dict[str, Any]) -> BaseModel:
        """Validate decoded JSON payload into a schema model."""
        raise NotImplementedError

    def build_upsert_spec(self, model: BaseModel, meta: KafkaMessageMeta) -> BusinessUpsertSpec:
        """Construct the idempotent SQL upsert statement and progress metadata."""
        raise NotImplementedError

    def _process_message(self, msg: Any) -> None:
        meta = self._extract_meta(msg)
        raw_value = msg.value()
        parsed_dict: dict[str, Any] | None = None

        if raw_value is None:
            self._persist_quarantine(
                msg,
                meta,
                None,
                None,
                "EmptyPayloadError",
                "Kafka message payload is empty/None",
            )
            return

        try:
            text_payload = (
                raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value)
            )
            loaded = json.loads(text_payload)
            if not isinstance(loaded, dict):
                raise ValueError(f"Expected JSON object payload, got {type(loaded).__name__}")
            parsed_dict = loaded
            model = self.parse_record(parsed_dict)
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            ValidationError,
            ValueError,
            TypeError,
        ) as exc:
            safe_reason = (
                json.dumps(exc.errors(include_input=False, include_context=False))
                if isinstance(exc, ValidationError)
                else sanitize_payload(str(exc), max_length=1000)[0]
            )
            logger.warning(
                "Quarantining poison message on %s[%s]@%s (%s): %s",
                meta.topic,
                meta.partition,
                meta.offset,
                type(exc).__name__,
                safe_reason,
            )
            self._persist_quarantine(
                msg,
                meta,
                raw_value,
                parsed_dict,
                type(exc).__name__,
                safe_reason,
            )
            return

        spec = self.build_upsert_spec(model, meta)
        self._persist_business_record(meta, spec)
        self._commit_offset(msg)

    def _handle_exit(self, signum: int, _frame: Any) -> None:
        """Graceful shutdown handler."""
        logger.info("Received signal %s; shutting down consumer...", signum)
        self.running = False

    def run(self) -> None:
        """Consume messages until stopped or until a partition is parked due to DB failure."""
        self.running = True
        signal.signal(signal.SIGTERM, self._handle_exit)
        signal.signal(signal.SIGINT, self._handle_exit)
        self.consumer.subscribe([self.topic])
        logger.info("Subscribed to topic=%s group_id=%s", self.topic, self.group_id)
        raw_timeout = getattr(self.config, "kafka_consumer_poll_timeout_seconds", 1.0)
        poll_timeout = (
            float(raw_timeout)
            if isinstance(raw_timeout, int | float) and not isinstance(raw_timeout, bool)
            else 1.0
        )

        try:
            while self.running:
                msg = self.consumer.poll(timeout=poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error on %s: %s", self.topic, msg.error())
                    continue
                try:
                    self._process_message(msg)
                except DatabaseUnavailableError as exc:
                    logger.critical("Stopping consumer because DB is unavailable: %s", exc)
                    self.running = False
                    break
        finally:
            self.close()
            if hasattr(self.db, "close"):
                self.db.close()

    def close(self) -> None:
        """Close the underlying Kafka consumer cleanly."""
        self.consumer.close()
        logger.info("Kafka consumer closed for topic=%s", self.topic)
