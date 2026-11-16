-- Stage 5 migration: Immutable model predictions (one per snapshot+model),
-- single-worker run checkpoint/lease state, and non-mutating realized outcomes view.

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
    CONSTRAINT uq_model_predictions_snapshot_model UNIQUE (
        contract_id, dataset_id, ticker, bar_end, model_version
    ),
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

-- Non-mutating realized outcome view: predictions and stored SHAP remain write-once.
-- Matches immutable predictions against finalized anchor & target bars using current cutoff clock.
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
       AND eb.ticker = p.ticker
       AND eb.bar_end = p.bar_end
    LEFT JOIN marts.final_bars tb
        ON tb.contract_id = p.contract_id
       AND tb.dataset_id = p.dataset_id
       AND tb.ticker = p.ticker
       AND tb.bar_end = p.target_at
    LEFT JOIN raw.ingestion_runs ir
        ON ir.run_id = p.run_id
)
SELECT
    b.*,
    CASE
        WHEN b.timeliness_status = 'expired_backlog' THEN 'expired'
        WHEN b.current_cutoff_at < b.target_at + INTERVAL '15 seconds' THEN 'pending'
        WHEN b.target_close IS NOT NULL
         AND b.entry_close IS NOT NULL
         AND b.entry_session_date_ny = b.target_session_date_ny
         AND ((b.target_at AT TIME ZONE 'America/New_York')::time <= TIME '16:00:00')
        THEN 'realized'
        ELSE 'unavailable'
    END AS outcome_status,
    CASE
        WHEN b.timeliness_status = 'expired_backlog' THEN 'live_backlog_past_target'
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
        THEN ((b.target_close - b.entry_close) / b.entry_close)
        ELSE NULL
    END AS return_5m,
    CASE
        WHEN b.timeliness_status = 'timely'
         AND b.current_cutoff_at >= b.target_at + INTERVAL '15 seconds'
         AND b.target_close IS NOT NULL
         AND b.entry_close IS NOT NULL
         AND b.entry_session_date_ny = b.target_session_date_ny
        THEN CASE WHEN b.target_close > b.entry_close THEN 'UP' ELSE 'NOT_UP' END
        ELSE NULL
    END AS actual_direction,
    CASE
        WHEN b.timeliness_status = 'timely'
         AND b.current_cutoff_at >= b.target_at + INTERVAL '15 seconds'
         AND b.target_close IS NOT NULL
         AND b.entry_close IS NOT NULL
         AND b.entry_session_date_ny = b.target_session_date_ny
         AND b.signal_label IN ('UP', 'NOT_UP')
        THEN (b.signal_label = CASE WHEN b.target_close > b.entry_close THEN 'UP' ELSE 'NOT_UP' END)
        ELSE NULL
    END AS was_correct
FROM base b;
