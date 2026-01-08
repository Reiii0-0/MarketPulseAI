-- project/scripts/init.sql
-- PostgreSQL schema and table bootstrap for MarketPulseAI

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- 1.1 raw.market_ticks
CREATE TABLE IF NOT EXISTS raw.market_ticks (
    id               BIGSERIAL       PRIMARY KEY,
    ticker           VARCHAR(10)     NOT NULL,
    event_timestamp  TIMESTAMPTZ     NOT NULL,
    ingested_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    price            NUMERIC(12, 4)  NOT NULL,
    bid              NUMERIC(12, 4),
    ask              NUMERIC(12, 4),
    volume           BIGINT          NOT NULL,
    trade_condition  VARCHAR(20),
    kafka_offset     BIGINT          NOT NULL,
    kafka_partition  SMALLINT        NOT NULL,

    CONSTRAINT uq_tick_offset UNIQUE (kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_market_ticks_ticker_ts ON raw.market_ticks (ticker, event_timestamp DESC);

-- 1.2 raw.sentiment_events
CREATE TABLE IF NOT EXISTS raw.sentiment_events (
    id               BIGSERIAL       PRIMARY KEY,
    ticker           VARCHAR(10)     NOT NULL,
    event_timestamp  TIMESTAMPTZ     NOT NULL,
    ingested_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source           VARCHAR(50)     NOT NULL,
    headline         TEXT,
    sentiment_score  NUMERIC(5, 4)   NOT NULL,
    sentiment_label  VARCHAR(10)     NOT NULL,
    confidence       NUMERIC(5, 4)   NOT NULL,
    kafka_offset     BIGINT          NOT NULL,
    kafka_partition  SMALLINT        NOT NULL,

    CONSTRAINT chk_sentiment_range CHECK (sentiment_score BETWEEN -1.0 AND 1.0),
    CONSTRAINT chk_confidence_range CHECK (confidence BETWEEN 0.0 AND 1.0),
    CONSTRAINT chk_sentiment_label CHECK (sentiment_label IN ('POSITIVE', 'NEGATIVE', 'NEUTRAL')),
    CONSTRAINT uq_sentiment_offset UNIQUE (kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_sentiment_ticker_ts ON raw.sentiment_events (ticker, event_timestamp DESC);

-- 1.4 marts.predictions
CREATE TABLE IF NOT EXISTS marts.predictions (
    id                    BIGSERIAL       PRIMARY KEY,
    request_id            UUID            NOT NULL DEFAULT gen_random_uuid(),
    ticker                VARCHAR(10)     NOT NULL,
    predicted_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    window_start          TIMESTAMPTZ     NOT NULL,
    predicted_direction   SMALLINT        NOT NULL,
    probability_up        NUMERIC(6, 4)   NOT NULL,
    probability_down      NUMERIC(6, 4)   NOT NULL,
    confidence_tier       VARCHAR(10)     NOT NULL,
    shap_factors          JSONB           NOT NULL,
    actual_direction      SMALLINT,
    was_correct           BOOLEAN,

    CONSTRAINT chk_prob_up_range CHECK (probability_up BETWEEN 0.0 AND 1.0),
    CONSTRAINT chk_prob_down_range CHECK (probability_down BETWEEN 0.0 AND 1.0)
);

CREATE INDEX IF NOT EXISTS idx_predictions_ticker_ts ON marts.predictions (ticker, predicted_at DESC);
