-- Preserve existing rows; isolate derived state by run and immutable model bundle.
BEGIN;
ALTER TABLE marts.final_bars DROP CONSTRAINT IF EXISTS uq_final_bars_identity;
ALTER TABLE marts.final_bars ADD CONSTRAINT uq_final_bars_identity UNIQUE NULLS NOT DISTINCT (run_id, contract_id, dataset_id, ticker, bar_end);
ALTER TABLE marts.feature_snapshots DROP CONSTRAINT IF EXISTS uq_feature_snapshots_identity;
ALTER TABLE marts.feature_snapshots ADD CONSTRAINT uq_feature_snapshots_identity UNIQUE NULLS NOT DISTINCT (run_id, contract_id, dataset_id, ticker, bar_end);
ALTER TABLE marts.late_bar_corrections DROP CONSTRAINT IF EXISTS uq_late_corrections_identity;
ALTER TABLE marts.late_bar_corrections ADD CONSTRAINT uq_late_corrections_identity UNIQUE NULLS NOT DISTINCT (run_id, contract_id, dataset_id, source_event_id);
UPDATE marts.model_predictions SET bundle_id = 'legacy:' || model_version WHERE bundle_id IS NULL;
ALTER TABLE marts.model_predictions DROP CONSTRAINT IF EXISTS uq_model_predictions_snapshot_model;
ALTER TABLE marts.model_predictions ADD CONSTRAINT uq_model_predictions_snapshot_model UNIQUE NULLS NOT DISTINCT (run_id, contract_id, dataset_id, ticker, bar_end, bundle_id);

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

COMMIT;
