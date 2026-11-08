\set ON_ERROR_STOP on
SET ROLE movie_app;

ALTER TABLE meta.etl_run
    ADD COLUMN IF NOT EXISTS scope_kind text NOT NULL DEFAULT 'unspecified',
    ADD COLUMN IF NOT EXISTS requested_record_limit bigint,
    ADD COLUMN IF NOT EXISTS is_full_source boolean NOT NULL DEFAULT false;

ALTER TABLE meta.etl_run
    DROP CONSTRAINT IF EXISTS ck_etl_run_requested_record_limit;
ALTER TABLE meta.etl_run
    ADD CONSTRAINT ck_etl_run_requested_record_limit
    CHECK (requested_record_limit IS NULL OR requested_record_limit > 0);

ALTER TABLE meta.batch_progress
    ADD COLUMN IF NOT EXISTS parser_version text,
    ADD COLUMN IF NOT EXISTS requested_record_limit bigint,
    ADD COLUMN IF NOT EXISTS resume_strategy text NOT NULL DEFAULT 'scan_from_start',
    ADD COLUMN IF NOT EXISTS scope_status text NOT NULL DEFAULT 'sample_in_progress';

ALTER TABLE meta.batch_progress
    DROP CONSTRAINT IF EXISTS ck_batch_progress_record_limit,
    DROP CONSTRAINT IF EXISTS ck_batch_progress_resume_strategy,
    DROP CONSTRAINT IF EXISTS ck_batch_progress_scope_status;
ALTER TABLE meta.batch_progress
    ADD CONSTRAINT ck_batch_progress_record_limit
        CHECK (requested_record_limit IS NULL OR requested_record_limit > 0),
    ADD CONSTRAINT ck_batch_progress_resume_strategy
        CHECK (resume_strategy IN ('scan_from_start')),
    ADD CONSTRAINT ck_batch_progress_scope_status
        CHECK (scope_status IN ('sample_in_progress', 'sample_complete', 'full_in_progress', 'full_complete'));

ALTER TABLE meta.source_file
    ADD COLUMN IF NOT EXISTS hash_verification_method text NOT NULL DEFAULT 'recorded',
    ADD COLUMN IF NOT EXISTS hash_verified_at timestamptz,
    ADD COLUMN IF NOT EXISTS gzip_integrity_status text NOT NULL DEFAULT 'not_checked';

ALTER TABLE meta.source_file
    DROP CONSTRAINT IF EXISTS ck_source_file_gzip_integrity_status;
ALTER TABLE meta.source_file
    ADD CONSTRAINT ck_source_file_gzip_integrity_status
    CHECK (gzip_integrity_status IN ('not_checked', 'sample_readable', 'verified_complete', 'failed'));

ALTER TABLE staging.review_record
    ADD COLUMN IF NOT EXISTS asin_raw text,
    ADD COLUMN IF NOT EXISTS reviewer_id_raw text,
    ADD COLUMN IF NOT EXISTS profile_name_normalized text,
    ADD COLUMN IF NOT EXISTS helpfulness_raw text,
    ADD COLUMN IF NOT EXISTS helpful_ratio numeric,
    ADD COLUMN IF NOT EXISTS score_raw text,
    ADD COLUMN IF NOT EXISTS review_time_raw text,
    ADD COLUMN IF NOT EXISTS summary_normalized text,
    ADD COLUMN IF NOT EXISTS review_text_normalized text,
    ADD COLUMN IF NOT EXISTS content_sha256 char(64),
    ADD COLUMN IF NOT EXISTS parser_version text,
    ADD COLUMN IF NOT EXISTS record_number bigint;

ALTER TABLE staging.review_record
    ALTER COLUMN asin DROP NOT NULL;

ALTER TABLE staging.review_record
    DROP CONSTRAINT IF EXISTS review_record_asin_check,
    DROP CONSTRAINT IF EXISTS review_record_helpful_votes_check,
    DROP CONSTRAINT IF EXISTS review_record_total_votes_check,
    DROP CONSTRAINT IF EXISTS review_record_check,
    DROP CONSTRAINT IF EXISTS review_record_score_check,
    DROP CONSTRAINT IF EXISTS ck_review_valid_asin,
    DROP CONSTRAINT IF EXISTS ck_review_valid_votes,
    DROP CONSTRAINT IF EXISTS ck_review_valid_score;

ALTER TABLE staging.review_record
    ADD CONSTRAINT ck_review_valid_asin
        CHECK (parse_status = 'quarantined' OR (asin IS NOT NULL AND asin ~ '^[A-Z0-9]{10}$')),
    ADD CONSTRAINT ck_review_valid_votes
        CHECK (parse_status = 'quarantined' OR (
            (helpful_votes IS NULL OR helpful_votes >= 0)
            AND (total_votes IS NULL OR total_votes >= 0)
            AND (helpful_votes IS NULL OR total_votes IS NULL OR helpful_votes <= total_votes)
        )),
    ADD CONSTRAINT ck_review_valid_score
        CHECK (parse_status = 'quarantined' OR score IS NULL OR score BETWEEN 1 AND 5);

ALTER TABLE staging.review_record
    DROP CONSTRAINT IF EXISTS ck_review_helpful_ratio,
    DROP CONSTRAINT IF EXISTS ck_review_record_number;
ALTER TABLE staging.review_record
    ADD CONSTRAINT ck_review_helpful_ratio
        CHECK (helpful_ratio IS NULL OR helpful_ratio BETWEEN 0 AND 1),
    ADD CONSTRAINT ck_review_record_number
        CHECK (record_number IS NULL OR record_number > 0);

CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_review_source_record_number
    ON raw.source_record (source_file_id, record_number)
    WHERE source_kind = 'review_file';

CREATE UNIQUE INDEX IF NOT EXISTS ux_staging_review_source_record
    ON staging.review_record (source_record_id);

CREATE INDEX IF NOT EXISTS ix_staging_review_record_number
    ON staging.review_record (record_number);

CREATE INDEX IF NOT EXISTS ix_batch_progress_run
    ON meta.batch_progress (run_id);

COMMENT ON COLUMN raw.source_record.field_locator IS
    'For gzip review records: record number, line range and uncompressed offsets. Offsets are not compressed random-access checkpoints.';
COMMENT ON COLUMN meta.batch_progress.resume_strategy IS
    'Stage 1 supports scan_from_start: reopen gzip at byte zero, parse boundaries and skip through committed_record_number.';
COMMENT ON COLUMN meta.source_file.gzip_integrity_status IS
    'sample_readable is not proof of complete gzip integrity; only a full stream validation may set verified_complete.';

RESET ROLE;
