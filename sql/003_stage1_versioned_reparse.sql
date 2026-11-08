\set ON_ERROR_STOP on
SET ROLE movie_app;

-- Preserve the populated v1 table as versioned parse history.
DO $$
BEGIN
    IF to_regclass('staging.review_parse_result') IS NULL
       AND to_regclass('staging.review_record') IS NOT NULL THEN
        ALTER TABLE staging.review_record RENAME TO review_parse_result;
    END IF;
END $$;

ALTER TABLE target.review_mapping
    DROP CONSTRAINT IF EXISTS review_mapping_source_review_id_fkey;

-- The views are recreated at the end. Dropping them here makes the migration
-- repeatable after they have already been installed once.
DROP VIEW IF EXISTS staging.current_review_stats;
DROP VIEW IF EXISTS staging.current_review_parse_result;

UPDATE staging.review_parse_result
SET parser_version = 'snap-movies-review-v1'
WHERE parser_version IS NULL;

ALTER TABLE staging.review_parse_result
    DROP CONSTRAINT IF EXISTS review_record_pkey,
    DROP CONSTRAINT IF EXISTS review_record_source_record_id_key,
    DROP CONSTRAINT IF EXISTS review_parse_result_pkey,
    DROP CONSTRAINT IF EXISTS review_parse_result_source_record_id_key;

ALTER TABLE staging.review_parse_result
    ALTER COLUMN parser_version SET NOT NULL,
    ALTER COLUMN helpful_votes TYPE bigint,
    ALTER COLUMN total_votes TYPE bigint,
    ALTER COLUMN score TYPE numeric;

ALTER TABLE staging.review_parse_result
    ADD CONSTRAINT review_parse_result_pkey PRIMARY KEY (source_review_id, parser_version);

-- 002 created this as a one-result-per-source uniqueness rule. Once parsing is
-- versioned, source_record_id is intentionally repeated across parser releases.
DROP INDEX IF EXISTS staging.ux_staging_review_source_record;
CREATE INDEX IF NOT EXISTS ix_review_parse_result_source_record
    ON staging.review_parse_result (source_record_id, parser_version);

CREATE TABLE IF NOT EXISTS staging.review_record (
    source_review_id char(64) PRIMARY KEY,
    source_record_id bigint NOT NULL UNIQUE REFERENCES raw.source_record(source_record_id),
    record_number bigint NOT NULL CHECK (record_number > 0),
    content_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO staging.review_record (
    source_review_id, source_record_id, record_number, content_sha256
)
SELECT DISTINCT ON (source_review_id)
    source_review_id, source_record_id, record_number, content_sha256
FROM staging.review_parse_result
ORDER BY source_review_id, parser_version
ON CONFLICT (source_review_id) DO NOTHING;

ALTER TABLE staging.review_parse_result
    DROP CONSTRAINT IF EXISTS review_parse_result_identity_fkey;

ALTER TABLE staging.review_parse_result
    ADD CONSTRAINT review_parse_result_identity_fkey
    FOREIGN KEY (source_review_id) REFERENCES staging.review_record(source_review_id);

ALTER TABLE target.review_mapping
    ADD CONSTRAINT review_mapping_source_review_id_fkey
    FOREIGN KEY (source_review_id) REFERENCES staging.review_record(source_review_id);

ALTER TABLE staging.review_parse_result
    ADD COLUMN IF NOT EXISTS asin_status text NOT NULL DEFAULT 'usable';

UPDATE staging.review_parse_result
SET asin_status = CASE
    WHEN asin IS NOT NULL AND asin ~ '^[A-Z0-9]{10}$' THEN 'usable'
    WHEN asin_raw IS NULL THEN 'missing'
    WHEN asin_raw = '' THEN 'empty'
    ELSE 'invalid'
END
WHERE parser_version = 'snap-movies-review-v1';

ALTER TABLE staging.review_parse_result
    DROP CONSTRAINT IF EXISTS ck_review_parse_asin_status;
ALTER TABLE staging.review_parse_result
    ADD CONSTRAINT ck_review_parse_asin_status
    CHECK (asin_status IN ('usable', 'missing', 'empty', 'invalid', 'decode_error', 'conflict'));

CREATE TABLE IF NOT EXISTS meta.parser_release (
    parser_version text PRIMARY KEY,
    released_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    is_current boolean NOT NULL DEFAULT false,
    description text NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_parser_release_one_current
    ON meta.parser_release ((is_current)) WHERE is_current;

INSERT INTO meta.parser_release (parser_version, is_current, description)
VALUES
    ('snap-movies-review-v1', true, 'Stage 1 original parser; preserved history'),
    ('snap-movies-review-v2', false, 'Versioned parser with field-level ASIN, safe abnormal values and exact reviewer IDs')
ON CONFLICT (parser_version) DO UPDATE
SET description = EXCLUDED.description;

ALTER TABLE staging.review_parse_result
    DROP CONSTRAINT IF EXISTS review_parse_result_parser_release_fkey,
    DROP CONSTRAINT IF EXISTS ck_review_parse_normalized_votes,
    DROP CONSTRAINT IF EXISTS ck_review_parse_normalized_score;
ALTER TABLE staging.review_parse_result
    ADD CONSTRAINT review_parse_result_parser_release_fkey
        FOREIGN KEY (parser_version) REFERENCES meta.parser_release(parser_version),
    ADD CONSTRAINT ck_review_parse_normalized_votes CHECK (
        (helpful_votes IS NULL OR helpful_votes >= 0)
        AND (total_votes IS NULL OR total_votes >= 0)
        AND (helpful_votes IS NULL OR total_votes IS NULL OR helpful_votes <= total_votes)
    ),
    ADD CONSTRAINT ck_review_parse_normalized_score CHECK (
        score IS NULL OR (score BETWEEN 1 AND 5 AND scale(score) <= 1)
    );

ALTER TABLE meta.batch_progress
    ADD COLUMN IF NOT EXISTS scope_start_record bigint NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS scope_end_record bigint;

UPDATE meta.batch_progress
SET parser_version = COALESCE(parser_version, 'snap-movies-review-v1'),
    scope_end_record = COALESCE(scope_end_record, requested_record_limit)
WHERE parser_version IS NULL OR scope_end_record IS NULL;

ALTER TABLE meta.batch_progress
    ALTER COLUMN parser_version SET NOT NULL;

ALTER TABLE meta.batch_progress
    DROP CONSTRAINT IF EXISTS batch_progress_pkey,
    DROP CONSTRAINT IF EXISTS ck_batch_progress_scope_range;

ALTER TABLE meta.batch_progress
    ADD CONSTRAINT batch_progress_pkey
        PRIMARY KEY (pipeline_name, input_version, parser_version, partition_key),
    ADD CONSTRAINT ck_batch_progress_scope_range
        CHECK (scope_start_record > 0 AND (scope_end_record IS NULL OR scope_end_record >= scope_start_record));

ALTER TABLE meta.quality_issue
    ADD COLUMN IF NOT EXISTS parser_version text;

UPDATE meta.quality_issue q
SET parser_version = COALESCE(r.parser_version, 'snap-movies-review-v1')
FROM staging.review_parse_result r
WHERE q.source_record_id = r.source_record_id
  AND q.parser_version IS NULL;

CREATE TABLE IF NOT EXISTS staging.product_asin_observation (
    source_review_id char(64) NOT NULL REFERENCES staging.review_record(source_review_id),
    parser_version text NOT NULL REFERENCES meta.parser_release(parser_version),
    asin_raw text,
    asin varchar(10),
    asin_status text NOT NULL CHECK (asin_status IN ('usable', 'missing', 'empty', 'invalid', 'decode_error', 'conflict')),
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    PRIMARY KEY (source_review_id, parser_version),
    CHECK (asin_status <> 'usable' OR (asin IS NOT NULL AND asin ~ '^[A-Z0-9]{10}$')),
    CHECK (asin_status = 'usable' OR asin IS NULL)
);

CREATE INDEX IF NOT EXISTS ix_product_asin_observation_asin
    ON staging.product_asin_observation (asin) WHERE asin_status = 'usable';

CREATE TABLE IF NOT EXISTS staging.reviewer_profile_observation (
    source_review_id char(64) NOT NULL REFERENCES staging.review_record(source_review_id),
    parser_version text NOT NULL REFERENCES meta.parser_release(parser_version),
    reviewer_id text NOT NULL,
    profile_name_raw text,
    profile_name_normalized text,
    observed_at timestamptz,
    record_number bigint NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    PRIMARY KEY (source_review_id, parser_version)
);

CREATE TABLE IF NOT EXISTS staging.review_parse_locator (
    source_review_id char(64) NOT NULL REFERENCES staging.review_record(source_review_id),
    parser_version text NOT NULL REFERENCES meta.parser_release(parser_version),
    start_line bigint NOT NULL CHECK (start_line > 0),
    end_line bigint NOT NULL CHECK (end_line >= start_line),
    uncompressed_start bigint NOT NULL CHECK (uncompressed_start >= 0),
    uncompressed_end bigint NOT NULL CHECK (uncompressed_end >= uncompressed_start),
    content_sha256 char(64) NOT NULL,
    line_semantics text NOT NULL,
    byte_range_semantics text NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    PRIMARY KEY (source_review_id, parser_version)
);

ALTER TABLE target.reviewer
    ADD COLUMN IF NOT EXISTS latest_profile_observed_at timestamptz,
    ADD COLUMN IF NOT EXISTS latest_profile_record_number bigint,
    ADD COLUMN IF NOT EXISTS latest_profile_source_review_id char(64);

CREATE OR REPLACE VIEW staging.current_review_parse_result AS
SELECT r.*
FROM staging.review_parse_result r
JOIN meta.parser_release p USING (parser_version)
WHERE p.is_current;

CREATE OR REPLACE VIEW staging.current_review_stats AS
SELECT parser_version, parse_status, count(*) AS record_count
FROM staging.current_review_parse_result
GROUP BY parser_version, parse_status;

COMMENT ON TABLE staging.review_record IS
    'Stable source review identity: one row per immutable raw source record.';
COMMENT ON TABLE staging.review_parse_result IS
    'Versioned parser output; primary key is (source_review_id, parser_version).';
COMMENT ON VIEW staging.current_review_parse_result IS
    'Only the explicitly activated parser version; prevents double counting across versions.';
COMMENT ON TABLE staging.product_asin_observation IS
    'Field-level ASIN decision independent of whole-review validity.';
COMMENT ON TABLE staging.review_parse_locator IS
    'Versioned location interpretation. v2 end_line is the last content line and byte range excludes the blank separator.';
COMMENT ON COLUMN target.reviewer.latest_profile_observed_at IS
    'Latest means greatest review_time_utc; ties use greatest record_number then source_review_id.';

RESET ROLE;
