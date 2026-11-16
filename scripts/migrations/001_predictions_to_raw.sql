-- Move the API-owned predictions table into the raw schema and add model version.
-- Safe to re-run after a successful migration. Stops if both schemas contain a
-- predictions table so data is never silently discarded or merged. The dbt
-- pass-through view in marts may coexist with the API-owned table in raw.

DO $$
DECLARE
    raw_predictions_exists BOOLEAN := to_regclass('raw.predictions') IS NOT NULL;
    marts_predictions_exists BOOLEAN := to_regclass('marts.predictions') IS NOT NULL;
    marts_predictions_kind "char";
BEGIN
    SELECT relkind INTO marts_predictions_kind
    FROM pg_class WHERE oid = to_regclass('marts.predictions');
    IF raw_predictions_exists AND marts_predictions_exists
       AND marts_predictions_kind NOT IN ('v', 'm') THEN
        RAISE EXCEPTION
            'Both raw.predictions and marts.predictions exist; reconcile the tables before migrating';
    ELSIF NOT raw_predictions_exists AND marts_predictions_exists THEN
        IF marts_predictions_kind NOT IN ('r', 'p') THEN
            RAISE EXCEPTION
                'marts.predictions must be a table to migrate into raw';
        END IF;
        ALTER TABLE marts.predictions SET SCHEMA raw;
    ELSIF NOT raw_predictions_exists AND NOT marts_predictions_exists THEN
        RAISE EXCEPTION
            'Neither raw.predictions nor marts.predictions exists; initialize the database first';
    END IF;
END;
$$;

ALTER TABLE raw.predictions
    ADD COLUMN IF NOT EXISTS model_version VARCHAR(50);

UPDATE raw.predictions
SET model_version = 'unknown'
WHERE model_version IS NULL;

ALTER TABLE raw.predictions
    ALTER COLUMN model_version SET NOT NULL;
