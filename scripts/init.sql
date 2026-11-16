-- project/scripts/init.sql
-- PostgreSQL schema and table bootstrap for MarketPulseAI

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- 1.1 raw.market_ticks
CREATE TABLE IF NOT EXISTS raw.market_ticks (
    id               BIGSERIAL       PRIMARY KEY,
    dataset_id       VARCHAR(120)    NOT NULL DEFAULT 'legacy_live',
    source_event_id  VARCHAR(255)    NOT NULL,
    run_id           VARCHAR(120),
    ticker           VARCHAR(10)     NOT NULL,
    event_timestamp  TIMESTAMPTZ     NOT NULL,
    event_at         TIMESTAMPTZ     NOT NULL,
    available_at     TIMESTAMPTZ     NOT NULL,
    ingested_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    price            NUMERIC(12, 4)  NOT NULL,
    bid              NUMERIC(12, 4),
    ask              NUMERIC(12, 4),
    volume           BIGINT          NOT NULL,
    trade_condition  VARCHAR(20),
    observation_type VARCHAR(32)     NOT NULL DEFAULT 'trade_tick',
    provenance       VARCHAR(32)     NOT NULL DEFAULT 'unknown',
    kafka_offset     BIGINT          NOT NULL,
    kafka_partition  SMALLINT        NOT NULL,
    CONSTRAINT uq_market_ticks_business_identity UNIQUE (dataset_id, source_event_id)
);

CREATE INDEX IF NOT EXISTS idx_market_ticks_ticker_ts ON raw.market_ticks (ticker, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_ticks_available_at ON raw.market_ticks (ticker, available_at);
CREATE INDEX IF NOT EXISTS idx_market_ticks_kafka_pos ON raw.market_ticks (kafka_partition, kafka_offset);

-- 1.2 raw.sentiment_events
CREATE TABLE IF NOT EXISTS raw.sentiment_events (
    id               BIGSERIAL       PRIMARY KEY,
    dataset_id       VARCHAR(120)    NOT NULL DEFAULT 'legacy_live',
    source_event_id  VARCHAR(255)    NOT NULL,
    run_id           VARCHAR(120),
    ticker           VARCHAR(10)     NOT NULL,
    event_timestamp  TIMESTAMPTZ     NOT NULL,
    event_at         TIMESTAMPTZ     NOT NULL,
    available_at     TIMESTAMPTZ     NOT NULL,
    ingested_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source           VARCHAR(50)     NOT NULL,
    headline         TEXT,
    sentiment_score  NUMERIC(5, 4)   NOT NULL,
    sentiment_label  VARCHAR(10)     NOT NULL,
    confidence       NUMERIC(5, 4)   NOT NULL,
    observation_type VARCHAR(32)     NOT NULL DEFAULT 'sentiment_event',
    provenance       VARCHAR(32)     NOT NULL DEFAULT 'unknown',
    kafka_offset     BIGINT          NOT NULL,
    kafka_partition  SMALLINT        NOT NULL,

    CONSTRAINT chk_sentiment_range CHECK (sentiment_score BETWEEN -1.0 AND 1.0),
    CONSTRAINT chk_confidence_range CHECK (confidence BETWEEN 0.0 AND 1.0),
    CONSTRAINT chk_sentiment_label CHECK (sentiment_label IN ('POSITIVE', 'NEGATIVE', 'NEUTRAL')),
    CONSTRAINT uq_sentiment_events_business_identity UNIQUE (dataset_id, source_event_id)
);

CREATE INDEX IF NOT EXISTS idx_sentiment_ticker_ts ON raw.sentiment_events (ticker, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_available_at ON raw.sentiment_events (ticker, available_at);
CREATE INDEX IF NOT EXISTS idx_sentiment_kafka_pos ON raw.sentiment_events (kafka_partition, kafka_offset);

-- 1.3 raw.market_bars
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

CREATE INDEX IF NOT EXISTS idx_market_bars_ticker_bar_end ON raw.market_bars (ticker, bar_end);
CREATE INDEX IF NOT EXISTS idx_market_bars_available_at ON raw.market_bars (ticker, available_at);

-- 1.4 raw.ingestion_runs & delivery progress & quarantine
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

CREATE INDEX IF NOT EXISTS idx_quarantine_run_id ON raw.ingestion_quarantine (run_id, quarantined_at);

-- 1.5 raw.predictions
CREATE TABLE IF NOT EXISTS raw.predictions (
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
    model_version         VARCHAR(50)     NOT NULL,
    actual_direction      SMALLINT,
    was_correct           BOOLEAN,

    CONSTRAINT chk_prob_up_range CHECK (probability_up BETWEEN 0.0 AND 1.0),
    CONSTRAINT chk_prob_down_range CHECK (probability_down BETWEEN 0.0 AND 1.0)
);

CREATE INDEX IF NOT EXISTS idx_predictions_ticker_ts ON raw.predictions (ticker, predicted_at DESC);

-- Stage 3: Canonical schemas, immutable finalized 1m bars,
-- late arrival audit corrections, and immutable canonical feature snapshots.
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS marts.final_bars (
    id                BIGSERIAL       PRIMARY KEY,
    contract_id       VARCHAR(64)     NOT NULL,
    contract_version  VARCHAR(32)     NOT NULL,
    dataset_id        VARCHAR(120)    NOT NULL,
    run_id            VARCHAR(120),
    ticker            VARCHAR(10)     NOT NULL,
    bar_start         TIMESTAMPTZ     NOT NULL,
    bar_end           TIMESTAMPTZ     NOT NULL,
    available_at      TIMESTAMPTZ     NOT NULL,
    finalized_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finalized_as_of   TIMESTAMPTZ     NOT NULL,
    open              NUMERIC(12, 4)  NOT NULL,
    high              NUMERIC(12, 4)  NOT NULL,
    low               NUMERIC(12, 4)  NOT NULL,
    close             NUMERIC(12, 4)  NOT NULL,
    volume            BIGINT          NOT NULL,
    event_count       INTEGER         NOT NULL DEFAULT 1,
    late_event_count  INTEGER         NOT NULL DEFAULT 0,
    provenance        VARCHAR(32)     NOT NULL,

    CONSTRAINT chk_final_bar_ohlcv_positive CHECK (
        open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0
    ),
    CONSTRAINT chk_final_bar_ohlcv_bounds CHECK (
        high >= GREATEST(open, close)
        AND low <= LEAST(open, close)
        AND low <= high
        AND bar_end > bar_start
    ),
    CONSTRAINT uq_final_bars_identity UNIQUE NULLS NOT DISTINCT (run_id, contract_id, dataset_id, ticker, bar_end)
);

CREATE INDEX IF NOT EXISTS idx_final_bars_dataset_ticker_end
    ON marts.final_bars (dataset_id, ticker, bar_end);

CREATE TABLE IF NOT EXISTS marts.late_bar_corrections (
    id                BIGSERIAL    PRIMARY KEY,
    contract_id       VARCHAR(64)  NOT NULL,
    dataset_id        VARCHAR(120) NOT NULL,
    source_event_id   VARCHAR(255) NOT NULL,
    run_id            VARCHAR(120),
    ticker            VARCHAR(10)  NOT NULL,
    bar_end           TIMESTAMPTZ  NOT NULL,
    event_at          TIMESTAMPTZ  NOT NULL,
    available_at      TIMESTAMPTZ  NOT NULL,
    grace_cutoff      TIMESTAMPTZ  NOT NULL,
    correction_reason VARCHAR(64)  NOT NULL,
    payload           JSONB        NOT NULL DEFAULT '{}'::jsonb,
    recorded_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_late_corrections_identity UNIQUE NULLS NOT DISTINCT (run_id, contract_id, dataset_id, source_event_id)
);

CREATE INDEX IF NOT EXISTS idx_late_corrections_ticker_bar
    ON marts.late_bar_corrections (dataset_id, ticker, bar_end);

CREATE TABLE IF NOT EXISTS marts.feature_snapshots (
    id                   BIGSERIAL        PRIMARY KEY,
    contract_id          VARCHAR(64)      NOT NULL,
    contract_version     VARCHAR(32)      NOT NULL,
    dataset_id           VARCHAR(120)     NOT NULL,
    run_id               VARCHAR(120),
    ticker               VARCHAR(10)      NOT NULL,
    bar_start            TIMESTAMPTZ      NOT NULL,
    bar_end              TIMESTAMPTZ      NOT NULL,
    decision_at          TIMESTAMPTZ      NOT NULL,
    target_at            TIMESTAMPTZ      NOT NULL,
    feature_mode         VARCHAR(32)      NOT NULL,
    feature_hash         CHAR(64)         NOT NULL,
    warmup_complete      BOOLEAN          NOT NULL,
    contiguous_bar_count INTEGER          NOT NULL,
    features             JSONB            NOT NULL,
    open                 NUMERIC(12, 4)   NOT NULL,
    high                 NUMERIC(12, 4)   NOT NULL,
    low                  NUMERIC(12, 4)   NOT NULL,
    close                NUMERIC(12, 4)   NOT NULL,
    volume               BIGINT           NOT NULL,
    return_1m            DOUBLE PRECISION,
    return_5m_past       DOUBLE PRECISION,
    return_15m_past      DOUBLE PRECISION,
    sma_5                DOUBLE PRECISION,
    sma_20               DOUBLE PRECISION,
    sma_spread_5_20      DOUBLE PRECISION,
    volatility_20        DOUBLE PRECISION,
    volume_ratio_20      DOUBLE PRECISION,
    bar_range_pct        DOUBLE PRECISION,
    created_at           TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_feature_snapshots_identity UNIQUE NULLS NOT DISTINCT (run_id, contract_id, dataset_id, ticker, bar_end)
);

CREATE INDEX IF NOT EXISTS idx_feature_snapshots_lookup
    ON marts.feature_snapshots (contract_id, dataset_id, warmup_complete, bar_end);

-- Stage 5: Immutable model predictions, single-worker run state, and non-mutating realized outcomes view
CREATE TABLE IF NOT EXISTS marts.model_predictions (
    id                  BIGSERIAL        PRIMARY KEY,
    prediction_id       UUID             NOT NULL DEFAULT gen_random_uuid(),
    contract_id         VARCHAR(64)      NOT NULL,
    contract_version    VARCHAR(32)      NOT NULL,
    dataset_id          VARCHAR(120)     NOT NULL,
    run_id              VARCHAR(120),
    ticker              VARCHAR(10)      NOT NULL,
    bar_start           TIMESTAMPTZ      NOT NULL,
    bar_end             TIMESTAMPTZ      NOT NULL,
    decision_at         TIMESTAMPTZ      NOT NULL,
    target_at           TIMESTAMPTZ      NOT NULL,
    feature_hash        CHAR(64)         NOT NULL,
    bundle_id           VARCHAR(120),
    model_version       VARCHAR(64)      NOT NULL,
    champion_model      VARCHAR(64)      NOT NULL,
    fit_information_end TIMESTAMPTZ,
    predicted_direction SMALLINT         NOT NULL,
    signal_label        VARCHAR(16)      NOT NULL,
    probability_up      NUMERIC(8, 6)    NOT NULL,
    probability_not_up  NUMERIC(8, 6)    NOT NULL,
    raw_probability_up  NUMERIC(8, 6),
    confidence_tier     VARCHAR(10)      NOT NULL,
    attribution_space   VARCHAR(64)      NOT NULL DEFAULT 'base_model_log_odds',
    shap_factors        JSONB            NOT NULL DEFAULT '[]'::jsonb,
    run_mode            VARCHAR(32)      NOT NULL DEFAULT 'replay_holdout_demo',
    timeliness_status   VARCHAR(32)      NOT NULL DEFAULT 'timely',
    logical_as_of       TIMESTAMPTZ      NOT NULL,
    predicted_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    compute_duration_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    CONSTRAINT uq_model_predictions_id UNIQUE (prediction_id),
    CONSTRAINT uq_model_predictions_snapshot_model UNIQUE NULLS NOT DISTINCT (run_id, contract_id, dataset_id, ticker, bar_end, bundle_id),
    CONSTRAINT chk_model_pred_direction CHECK (predicted_direction IN (0, 1)),
    CONSTRAINT chk_model_pred_signal CHECK (signal_label IN ('UP', 'NOT_UP', 'NO_SIGNAL')),
    CONSTRAINT chk_model_pred_prob_range CHECK (
        probability_up BETWEEN 0.0 AND 1.0
        AND probability_not_up BETWEEN 0.0 AND 1.0
    ),
    CONSTRAINT chk_model_pred_timeliness CHECK (
        timeliness_status IN ('timely', 'expired_backlog')
    )
);

CREATE INDEX IF NOT EXISTS idx_model_predictions_run_bar
    ON marts.model_predictions (run_id, ticker, bar_end DESC);

CREATE INDEX IF NOT EXISTS idx_model_predictions_dataset_target
    ON marts.model_predictions (dataset_id, ticker, target_at);

CREATE TABLE IF NOT EXISTS marts.prediction_worker_state (
    run_id                    VARCHAR(120)     PRIMARY KEY,
    dataset_id                VARCHAR(120)     NOT NULL,
    worker_owner              VARCHAR(120),
    lease_expires_at          TIMESTAMPTZ,
    last_prepared_bar_end     TIMESTAMPTZ,
    last_predicted_bar_end    TIMESTAMPTZ,
    predictions_written       BIGINT           NOT NULL DEFAULT 0,
    expired_backlog_count     BIGINT           NOT NULL DEFAULT 0,
    last_compute_duration_ms  DOUBLE PRECISION,
    status                    VARCHAR(32)      NOT NULL DEFAULT 'idle',
    last_error                TEXT,
    updated_at                TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE VIEW marts.realized_outcomes_v AS
WITH base AS (
    SELECT
        p.prediction_id,
        p.contract_id,
        p.contract_version,
        p.dataset_id,
        p.run_id,
        p.ticker,
        p.bar_start,
        p.bar_end,
        p.decision_at,
        p.target_at,
        p.feature_hash,
        p.bundle_id,
        p.model_version,
        p.champion_model,
        p.fit_information_end,
        p.predicted_direction,
        p.signal_label,
        p.probability_up,
        p.probability_not_up,
        p.raw_probability_up,
        p.confidence_tier,
        p.attribution_space,
        p.shap_factors,
        p.run_mode,
        p.timeliness_status,
        p.logical_as_of,
        p.predicted_at,
        p.compute_duration_ms,
        eb.close::DOUBLE PRECISION AS entry_close,
        tb.close::DOUBLE PRECISION AS target_close,
        COALESCE(ir.logical_as_of, NOW()) AS current_cutoff_at,
        ir.dataset_end_at,
        ir.status AS run_status,
        (eb.bar_end AT TIME ZONE 'America/New_York')::date AS entry_session_date_ny,
        (tb.bar_end AT TIME ZONE 'America/New_York')::date AS target_session_date_ny
    FROM marts.model_predictions p
    LEFT JOIN marts.final_bars eb
        ON eb.contract_id = p.contract_id
       AND eb.dataset_id = p.dataset_id
       AND eb.run_id IS NOT DISTINCT FROM p.run_id
       AND eb.ticker = p.ticker
       AND eb.bar_end = p.bar_end
    LEFT JOIN marts.final_bars tb
        ON tb.contract_id = p.contract_id
       AND tb.dataset_id = p.dataset_id
       AND tb.run_id IS NOT DISTINCT FROM p.run_id
       AND tb.ticker = p.ticker
       AND tb.bar_end = p.target_at
    LEFT JOIN raw.ingestion_runs ir
        ON ir.run_id = p.run_id
)
SELECT
    b.*,
    CASE
        WHEN b.timeliness_status = 'expired_backlog' THEN 'expired'
        WHEN b.run_status = 'completed' AND b.target_at > b.dataset_end_at THEN 'unavailable'
        WHEN b.current_cutoff_at < b.target_at + INTERVAL '15 seconds' THEN 'pending'
        WHEN b.target_close IS NOT NULL
         AND b.entry_close IS NOT NULL
         AND b.entry_session_date_ny = b.target_session_date_ny
         AND (b.bar_end AT TIME ZONE 'America/New_York')::time > TIME '09:30:00'
         AND (b.target_at AT TIME ZONE 'America/New_York')::time <= TIME '16:00:00'
         AND ((b.target_at AT TIME ZONE 'America/New_York')::time <= TIME '16:00:00')
        THEN 'realized'
        ELSE 'unavailable'
    END AS outcome_status,
    CASE
        WHEN b.timeliness_status = 'expired_backlog' THEN 'live_backlog_past_target'
        WHEN b.run_status = 'completed' AND b.target_at > b.dataset_end_at THEN 'dataset_end'
        WHEN b.current_cutoff_at < b.target_at + INTERVAL '15 seconds' THEN 'horizon_not_matured'
        WHEN ((b.target_at AT TIME ZONE 'America/New_York')::time > TIME '16:00:00')
          OR (b.target_session_date_ny IS NOT NULL AND b.entry_session_date_ny <> b.target_session_date_ny)
        THEN 'session_end'
        WHEN b.target_close IS NOT NULL AND b.entry_close IS NOT NULL THEN NULL
        WHEN b.dataset_end_at IS NOT NULL AND b.target_at > b.dataset_end_at THEN 'dataset_end'
        ELSE 'missing_target_bar'
    END AS outcome_reason,
    CASE
        WHEN b.timeliness_status = 'timely'
         AND b.current_cutoff_at >= b.target_at + INTERVAL '15 seconds'
         AND b.target_close IS NOT NULL
         AND b.entry_close IS NOT NULL
         AND b.entry_close > 0
         AND b.entry_session_date_ny = b.target_session_date_ny
         AND (b.bar_end AT TIME ZONE 'America/New_York')::time > TIME '09:30:00'
         AND (b.target_at AT TIME ZONE 'America/New_York')::time <= TIME '16:00:00'
        THEN ((b.target_close - b.entry_close) / b.entry_close)
        ELSE NULL
    END AS return_5m,
    CASE
        WHEN b.timeliness_status = 'timely'
         AND b.current_cutoff_at >= b.target_at + INTERVAL '15 seconds'
         AND b.target_close IS NOT NULL
         AND b.entry_close IS NOT NULL
         AND b.entry_session_date_ny = b.target_session_date_ny
         AND (b.bar_end AT TIME ZONE 'America/New_York')::time > TIME '09:30:00'
         AND (b.target_at AT TIME ZONE 'America/New_York')::time <= TIME '16:00:00'
        THEN CASE WHEN b.target_close > b.entry_close THEN 'UP' ELSE 'NOT_UP' END
        ELSE NULL
    END AS actual_direction,
    CASE
        WHEN b.timeliness_status = 'timely'
         AND b.current_cutoff_at >= b.target_at + INTERVAL '15 seconds'
         AND b.target_close IS NOT NULL
         AND b.entry_close IS NOT NULL
         AND b.entry_session_date_ny = b.target_session_date_ny
         AND (b.bar_end AT TIME ZONE 'America/New_York')::time > TIME '09:30:00'
         AND (b.target_at AT TIME ZONE 'America/New_York')::time <= TIME '16:00:00'
         AND b.signal_label IN ('UP', 'NOT_UP')
        THEN (b.signal_label = CASE WHEN b.target_close > b.entry_close THEN 'UP' ELSE 'NOT_UP' END)
        ELSE NULL
    END AS was_correct
FROM base b;
