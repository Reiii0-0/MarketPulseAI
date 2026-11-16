-- Stage 3 migration: Canonical schemas, immutable finalized 1m bars,
-- late arrival audit corrections, and immutable canonical feature snapshots.

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- 1. Immutable finalized 1-minute UTC bars (never silently overwritten after grace)
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
    CONSTRAINT uq_final_bars_identity UNIQUE (contract_id, dataset_id, ticker, bar_end)
);

CREATE INDEX IF NOT EXISTS idx_final_bars_dataset_ticker_end
    ON marts.final_bars (dataset_id, ticker, bar_end);

-- 2. Late events / post-finalization arrivals audit log (never mutates old finalized bars or predictions)
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
    CONSTRAINT uq_late_corrections_identity UNIQUE (contract_id, dataset_id, source_event_id)
);

CREATE INDEX IF NOT EXISTS idx_late_corrections_ticker_bar
    ON marts.late_bar_corrections (dataset_id, ticker, bar_end);

-- 3. Immutable canonical feature snapshots (frozen with feature_hash + contract_version)
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
    CONSTRAINT uq_feature_snapshots_identity UNIQUE (contract_id, dataset_id, ticker, bar_end)
);

CREATE INDEX IF NOT EXISTS idx_feature_snapshots_lookup
    ON marts.feature_snapshots (contract_id, dataset_id, warmup_complete, bar_end);
