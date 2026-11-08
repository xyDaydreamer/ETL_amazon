\set ON_ERROR_STOP on
SET ROLE movie_app;

ALTER TABLE meta.product_harvest_batch
    ADD COLUMN IF NOT EXISTS selection_method text,
    ADD COLUMN IF NOT EXISTS random_seed bigint,
    ADD COLUMN IF NOT EXISTS candidate_limit integer,
    ADD COLUMN IF NOT EXISTS access_basis text;

ALTER TABLE meta.product_harvest_batch
    DROP CONSTRAINT IF EXISTS product_harvest_batch_candidate_limit_check;
ALTER TABLE meta.product_harvest_batch
    ADD CONSTRAINT product_harvest_batch_candidate_limit_check CHECK (
        candidate_limit IS NULL OR candidate_limit BETWEEN 1 AND 100
    );

ALTER TABLE staging.product_harvest_work_item
    ADD COLUMN IF NOT EXISTS selection_rank integer;

CREATE UNIQUE INDEX IF NOT EXISTS ux_product_harvest_work_selection_rank
    ON staging.product_harvest_work_item(batch_id, selection_rank)
    WHERE selection_rank IS NOT NULL;

COMMENT ON COLUMN meta.product_harvest_batch.selection_method IS
    'Auditable candidate selection method; independent of request rate and access policy.';
COMMENT ON COLUMN meta.product_harvest_batch.random_seed IS
    'Seed used for deterministic candidate ordering; not a network or retry setting.';
COMMENT ON COLUMN meta.product_harvest_batch.candidate_limit IS
    'Maximum distinct ASIN candidates in this bounded sample (1..100), not a request count.';
COMMENT ON COLUMN meta.product_harvest_batch.access_basis IS
    'Documented access decision. A prepared plan does not imply permission to send requests.';
COMMENT ON COLUMN staging.product_harvest_work_item.selection_rank IS
    'Stable one-based position in the selected candidate sample.';

RESET ROLE;
