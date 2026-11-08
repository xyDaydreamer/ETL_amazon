\set ON_ERROR_STOP on
SET ROLE movie_app;

ALTER TABLE staging.product_harvest
    DROP CONSTRAINT IF EXISTS product_harvest_status_check;
ALTER TABLE staging.product_harvest
    ADD CONSTRAINT product_harvest_status_check CHECK (status IN (
        'not_attempted', 'success', 'not_found', 'access_restricted',
        'timeout', 'parse_failed', 'network_failed', 'robots_denied'
    ));

ALTER TABLE staging.product_harvest
    ADD COLUMN IF NOT EXISTS last_attempt_status text,
    ADD COLUMN IF NOT EXISTS latest_source_record_id bigint REFERENCES raw.source_record(source_record_id),
    ADD COLUMN IF NOT EXISTS valid_source_record_id bigint REFERENCES raw.source_record(source_record_id),
    ADD COLUMN IF NOT EXISTS last_attempt_at timestamptz;

ALTER TABLE staging.product_harvest
    DROP CONSTRAINT IF EXISTS product_harvest_last_attempt_status_check;
ALTER TABLE staging.product_harvest
    ADD CONSTRAINT product_harvest_last_attempt_status_check CHECK (
        last_attempt_status IS NULL OR last_attempt_status IN (
            'success', 'not_found', 'access_restricted', 'timeout',
            'parse_failed', 'network_failed', 'robots_denied'
        )
    );

UPDATE staging.product_harvest
SET latest_source_record_id = coalesce(latest_source_record_id, source_record_id),
    valid_source_record_id = CASE WHEN status = 'success'
        THEN coalesce(valid_source_record_id, source_record_id) ELSE valid_source_record_id END,
    last_attempt_status = CASE WHEN status <> 'not_attempted'
        THEN coalesce(last_attempt_status, status) ELSE last_attempt_status END,
    last_attempt_at = coalesce(last_attempt_at, fetched_at)
WHERE source_record_id IS NOT NULL OR status <> 'not_attempted';

ALTER TABLE staging.product_harvest_attempt
    DROP CONSTRAINT IF EXISTS product_harvest_attempt_outcome_check;
ALTER TABLE staging.product_harvest_attempt
    ADD CONSTRAINT product_harvest_attempt_outcome_check CHECK (outcome IN (
        'success', 'not_found', 'access_restricted', 'timeout',
        'parse_failed', 'network_failed', 'robots_denied'
    ));

ALTER TABLE staging.product_harvest_attempt
    ADD COLUMN IF NOT EXISTS request_ordinal integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS retry_reason text,
    ADD COLUMN IF NOT EXISTS cache_hit boolean,
    ADD COLUMN IF NOT EXISTS redirect_chain jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS validation_evidence jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS meta.product_harvest_batch (
    batch_id text PRIMARY KEY,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    status text NOT NULL CHECK (status IN ('prepared', 'running', 'succeeded', 'failed', 'stopped')),
    max_products integer NOT NULL CHECK (max_products > 0),
    max_attempts_per_product integer NOT NULL CHECK (max_attempts_per_product BETWEEN 1 AND 10),
    request_delay_seconds numeric(8,3) NOT NULL CHECK (request_delay_seconds >= 1),
    concurrency integer NOT NULL CHECK (concurrency BETWEEN 1 AND 4),
    prepared_count integer NOT NULL DEFAULT 0,
    claimed_count integer NOT NULL DEFAULT 0,
    completed_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS staging.product_harvest_work_item (
    batch_id text NOT NULL REFERENCES meta.product_harvest_batch(batch_id),
    asin varchar(10) NOT NULL REFERENCES staging.product_harvest(asin),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN (
        'pending', 'claimed', 'completed', 'deferred', 'failed'
    )),
    claim_token uuid,
    claimed_at timestamptz,
    completed_at timestamptz,
    request_attempts integer NOT NULL DEFAULT 0 CHECK (request_attempts >= 0),
    final_outcome text,
    last_error_code text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (batch_id, asin)
);

CREATE INDEX IF NOT EXISTS ix_product_harvest_work_claim
    ON staging.product_harvest_work_item(batch_id, state, asin);

CREATE INDEX IF NOT EXISTS ix_product_harvest_attempt_run
    ON staging.product_harvest_attempt(run_id, asin, attempt_number);

ALTER TABLE staging.product_harvest_attempt ALTER COLUMN cache_hit DROP NOT NULL;
ALTER TABLE staging.product_harvest_attempt ALTER COLUMN cache_hit DROP DEFAULT;

COMMENT ON COLUMN staging.product_harvest.valid_source_record_id IS
    'Latest known valid Amazon product-page snapshot; later failures never erase it.';
COMMENT ON COLUMN staging.product_harvest.latest_source_record_id IS
    'Most recent response evidence, including restricted/error responses.';
COMMENT ON TABLE staging.product_harvest_work_item IS
    'Database-driven logical collection tasks; atomically claimed and resumable.';

RESET ROLE;
