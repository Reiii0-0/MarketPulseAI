-- Stage 2 migration: Business identity dedup, quarantine, run tracking,
-- delivery progress barriers, and canonical 1m OHLCV bar storage.

CREATE SCHEMA IF NOT EXISTS raw;

-- 1. Upgrade raw.market_ticks from Kafka offset dedup to business identity (dataset_id, source_event_id)
ALTER TABLE raw.market_ticks
    ADD COLUMN IF NOT EXISTS dataset_id       VARCHAR(120) NOT NULL DEFAULT 'legacy_live',
    ADD COLUMN IF NOT EXISTS source_event_id  VARCHAR(255),
    ADD COLUMN IF NOT EXISTS event_at         TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS available_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS observation_type VARCHAR(32)  NOT NULL DEFAULT 'trade_tick',
    ADD COLUMN IF NOT EXISTS provenance       VARCHAR(32)  NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS run_id           VARCHAR(120);

UPDATE raw.market_ticks
SET
    source_event_id = COALESCE(
        source_event_id,
        'legacy_tick:' || kafka_partition::text || ':' || kafka_offset::text
    ),
    event_at = COALESCE(event_at, event_timestamp),
    available_at = COALESCE(available_at, event_timestamp)
WHERE source_event_id IS NULL
   OR event_at IS NULL
   OR available_at IS NULL;

ALTER TABLE raw.market_ticks
    ALTER COLUMN source_event_id SET NOT NULL,
    ALTER COLUMN event_at SET NOT NULL,
    ALTER COLUMN available_at SET NOT NULL;

ALTER TABLE raw.market_ticks
    DROP CONSTRAINT IF EXISTS uq_tick_offset;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_market_ticks_business_identity'
          AND conrelid = 'raw.market_ticks'::regclass
    ) THEN
        ALTER TABLE raw.market_ticks
            ADD CONSTRAINT uq_market_ticks_business_identity
            UNIQUE (dataset_id, source_event_id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_market_ticks_available_at
    ON raw.market_ticks (ticker, available_at);

CREATE INDEX IF NOT EXISTS idx_market_ticks_kafka_pos
    ON raw.market_ticks (kafka_partition, kafka_offset);

-- 2. Upgrade raw.sentiment_events from Kafka offset dedup to business identity (dataset_id, source_event_id)
ALTER TABLE raw.sentiment_events
    ADD COLUMN IF NOT EXISTS dataset_id       VARCHAR(120) NOT NULL DEFAULT 'legacy_live',
    ADD COLUMN IF NOT EXISTS source_event_id  VARCHAR(255),
    ADD COLUMN IF NOT EXISTS event_at         TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS available_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS observation_type VARCHAR(32)  NOT NULL DEFAULT 'sentiment_event',
    ADD COLUMN IF NOT EXISTS provenance       VARCHAR(32)  NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS run_id           VARCHAR(120);

UPDATE raw.sentiment_events
SET
    source_event_id = COALESCE(
        source_event_id,
        'legacy_sentiment:' || kafka_partition::text || ':' || kafka_offset::text
    ),
    event_at = COALESCE(event_at, event_timestamp),
    available_at = COALESCE(available_at, event_timestamp)
WHERE source_event_id IS NULL
   OR event_at IS NULL
   OR available_at IS NULL;

ALTER TABLE raw.sentiment_events
    ALTER COLUMN source_event_id SET NOT NULL,
    ALTER COLUMN event_at SET NOT NULL,
    ALTER COLUMN available_at SET NOT NULL;

ALTER TABLE raw.sentiment_events
    DROP CONSTRAINT IF EXISTS uq_sentiment_offset;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_sentiment_events_business_identity'
          AND conrelid = 'raw.sentiment_events'::regclass
    ) THEN
        ALTER TABLE raw.sentiment_events
            ADD CONSTRAINT uq_sentiment_events_business_identity
            UNIQUE (dataset_id, source_event_id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_sentiment_available_at
    ON raw.sentiment_events (ticker, available_at);

CREATE INDEX IF NOT EXISTS idx_sentiment_kafka_pos
    ON raw.sentiment_events (kafka_partition, kafka_offset);

-- 3. Canonical 1-minute OHLCV bar storage (distinct from trade ticks; no fabricated ticks)
CREATE TABLE IF NOT EXISTS raw.market_bars (
    id                   BIGSERIAL       PRIMARY KEY,
    dataset_id           VARCHAR(120)    NOT NULL,
    source_event_id      VARCHAR(255)    NOT NULL,
    run_id               VARCHAR(120),
    ticker               VARCHAR(10)     NOT NULL,
    bar_start            TIMESTAMPTZ     NOT NULL,
    bar_end              TIMESTAMPTZ     NOT NULL,
    event_at             TIMESTAMPTZ     NOT NULL,
    available_at         TIMESTAMPTZ     NOT NULL,
    ingested_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    open                 NUMERIC(12, 4)  NOT NULL,
    high                 NUMERIC(12, 4)  NOT NULL,
    low                  NUMERIC(12, 4)  NOT NULL,
    close                NUMERIC(12, 4)  NOT NULL,
    volume               BIGINT          NOT NULL,
    trade_count          BIGINT,
    vwap                 NUMERIC(12, 4),
    observation_type     VARCHAR(32)     NOT NULL DEFAULT 'ohlcv_bar',
    provenance           VARCHAR(32)     NOT NULL,
    availability_assumed BOOLEAN         NOT NULL DEFAULT TRUE,
    kafka_offset         BIGINT          NOT NULL,
    kafka_partition      SMALLINT        NOT NULL,
    CONSTRAINT uq_market_bars_business_identity UNIQUE (dataset_id, source_event_id)
);

CREATE INDEX IF NOT EXISTS idx_market_bars_ticker_bar_end
    ON raw.market_bars (ticker, bar_end);

CREATE INDEX IF NOT EXISTS idx_market_bars_available_at
    ON raw.market_bars (ticker, available_at);

-- 4. Replay & live ingestion run tracking (isolates run clock & barriers from prior runs)
CREATE TABLE IF NOT EXISTS raw.ingestion_runs (
    run_id            VARCHAR(120)     PRIMARY KEY,
    mode              VARCHAR(32)      NOT NULL,
    dataset_id        VARCHAR(120)     NOT NULL,
    provenance        VARCHAR(32)      NOT NULL,
    contract_version  VARCHAR(64),
    status            VARCHAR(32)      NOT NULL DEFAULT 'running',
    logical_as_of     TIMESTAMPTZ,
    dataset_end_at    TIMESTAMPTZ,
    speed_multiplier  DOUBLE PRECISION,
    metadata          JSONB            NOT NULL DEFAULT '{}'::jsonb,
    started_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    completed_at      TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- 5. Per-run, per-topic, per-partition delivery progress (replay barrier authority)
CREATE TABLE IF NOT EXISTS raw.run_delivery_progress (
    run_id               VARCHAR(120) NOT NULL,
    topic                VARCHAR(120) NOT NULL,
    kafka_partition      SMALLINT     NOT NULL,
    last_kafka_offset    BIGINT       NOT NULL DEFAULT -1,
    last_available_at    TIMESTAMPTZ,
    last_source_event_id VARCHAR(255),
    processed_count      BIGINT       NOT NULL DEFAULT 0,
    quarantined_count    BIGINT       NOT NULL DEFAULT 0,
    status               VARCHAR(32)  NOT NULL DEFAULT 'active',
    last_error           TEXT,
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, topic, kafka_partition)
);

-- 6. Poison / invalid message quarantine
CREATE TABLE IF NOT EXISTS raw.ingestion_quarantine (
    id                BIGSERIAL    PRIMARY KEY,
    run_id            VARCHAR(120),
    dataset_id        VARCHAR(120),
    source_event_id   VARCHAR(255),
    topic             VARCHAR(120) NOT NULL,
    kafka_partition   SMALLINT     NOT NULL,
    kafka_offset      BIGINT       NOT NULL,
    error_type        VARCHAR(64)  NOT NULL,
    error_reason      TEXT         NOT NULL,
    sanitized_payload TEXT         NOT NULL,
    payload_sha256    CHAR(64)     NOT NULL,
    quarantined_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_quarantine_topic_partition_offset UNIQUE (topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_quarantine_run_id
    ON raw.ingestion_quarantine (run_id, quarantined_at);
