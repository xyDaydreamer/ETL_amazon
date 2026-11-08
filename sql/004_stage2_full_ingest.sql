\set ON_ERROR_STOP on
SET ROLE movie_app;

ALTER TABLE meta.batch_progress
    ADD COLUMN IF NOT EXISTS actual_read_end_record bigint,
    ADD COLUMN IF NOT EXISTS reached_eof boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS source_integrity_status text NOT NULL DEFAULT 'not_checked',
    ADD COLUMN IF NOT EXISTS stop_reason text,
    ADD COLUMN IF NOT EXISTS last_free_bytes bigint;

ALTER TABLE meta.batch_progress
    DROP CONSTRAINT IF EXISTS batch_progress_check,
    DROP CONSTRAINT IF EXISTS ck_batch_progress_scope_status,
    DROP CONSTRAINT IF EXISTS ck_batch_progress_source_integrity;
ALTER TABLE meta.batch_progress
    ADD CONSTRAINT ck_batch_progress_scope_status CHECK (scope_status IN (
        'sample_in_progress', 'sample_complete', 'sample_range_short',
        'sample_start_past_eof', 'full_in_progress', 'full_complete',
        'stopped_low_disk', 'source_failed'
    )),
    ADD CONSTRAINT ck_batch_progress_source_integrity CHECK (
        source_integrity_status IN ('not_checked', 'sample_only', 'verified_complete', 'failed')
    ),
    ADD CONSTRAINT ck_batch_progress_next_record CHECK (
        next_record_number = CASE
            WHEN committed_record_number = 0 THEN scope_start_record
            ELSE committed_record_number + 1
        END
    );

ALTER TABLE meta.parser_release
    ADD COLUMN IF NOT EXISTS coverage_kind text NOT NULL DEFAULT 'sample',
    ADD COLUMN IF NOT EXISTS coverage_start_record bigint,
    ADD COLUMN IF NOT EXISTS coverage_end_record bigint,
    ADD COLUMN IF NOT EXISTS verified_source_eof boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS activated_at timestamptz;

ALTER TABLE meta.parser_release
    DROP CONSTRAINT IF EXISTS ck_parser_release_coverage_kind;
ALTER TABLE meta.parser_release
    ADD CONSTRAINT ck_parser_release_coverage_kind
        CHECK (coverage_kind IN ('sample', 'full'));

INSERT INTO meta.parser_release (parser_version, is_current, description, coverage_kind)
VALUES (
    'snap-movies-review-v3', false,
    'Stage 2 parser: corrected EOF completion, reviewer-ID conflicts and full-source validation',
    'sample'
)
ON CONFLICT (parser_version) DO UPDATE
SET description = EXCLUDED.description;

COMMENT ON COLUMN meta.batch_progress.actual_read_end_record IS
    'Highest source record actually observed while scanning, independent of requested range.';
COMMENT ON COLUMN meta.batch_progress.reached_eof IS
    'True only after the gzip stream was read through a clean EOF; a requested limit is not EOF.';
COMMENT ON COLUMN meta.batch_progress.last_free_bytes IS
    'Last free-byte observation from the configured PostgreSQL-volume monitor path.';
COMMENT ON COLUMN meta.parser_release.verified_source_eof IS
    'A full release may be activated only after clean gzip EOF and relational coverage validation.';

RESET ROLE;
