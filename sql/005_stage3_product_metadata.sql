\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE TABLE IF NOT EXISTS staging.product_metadata_record (
    product_metadata_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asin varchar(10) NOT NULL CHECK (asin ~ '^[A-Z0-9]{10}$'),
    source_name text NOT NULL,
    source_version text NOT NULL,
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    source_record_number bigint NOT NULL CHECK (source_record_number > 0),
    title_raw text,
    title_normalized text,
    description_raw jsonb NOT NULL DEFAULT '[]'::jsonb,
    categories_raw jsonb NOT NULL DEFAULT '[]'::jsonb,
    brand_raw text,
    source_date_raw text,
    proposed_classification text NOT NULL CHECK (proposed_classification IN (
        'movie', 'non_movie', 'collection', 'series', 'concert_or_other', 'pending'
    )),
    classification_basis jsonb NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (asin, source_name, source_version, source_record_number)
);

CREATE INDEX IF NOT EXISTS ix_product_metadata_asin
    ON staging.product_metadata_record (asin);

CREATE TABLE IF NOT EXISTS integration.product_movie_candidate (
    candidate_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asin varchar(10) NOT NULL CHECK (asin ~ '^[A-Z0-9]{10}$'),
    candidate_key text NOT NULL,
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    candidate_title text NOT NULL,
    candidate_year smallint,
    candidate_type text,
    title_similarity numeric(6,5) CHECK (title_similarity BETWEEN 0 AND 1),
    description_similarity numeric(6,5) CHECK (description_similarity BETWEEN 0 AND 1),
    corroborating_facts jsonb NOT NULL DEFAULT '[]'::jsonb,
    decision_status text NOT NULL CHECK (decision_status IN ('verified', 'review', 'rejected')),
    decision_reason text NOT NULL,
    rule_version text NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (asin, candidate_key, rule_version)
);

CREATE TABLE IF NOT EXISTS integration.review_queue (
    review_queue_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue_type text NOT NULL CHECK (queue_type IN ('product_classification', 'product_movie', 'person_match')),
    entity_key text NOT NULL,
    candidate_key text,
    reason_code text NOT NULL,
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved', 'dismissed')),
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    UNIQUE NULLS NOT DISTINCT (queue_type, entity_key, candidate_key, reason_code)
);

CREATE INDEX IF NOT EXISTS ix_review_queue_open
    ON integration.review_queue (status, queue_type);

CREATE TABLE IF NOT EXISTS meta.product_metadata_progress (
    source_sha256 char(64) NOT NULL,
    partition_key text NOT NULL,
    committed_record_number bigint NOT NULL DEFAULT 0 CHECK (committed_record_number >= 0),
    next_record_number bigint NOT NULL DEFAULT 1 CHECK (next_record_number >= 1),
    reached_eof boolean NOT NULL DEFAULT false,
    status text NOT NULL CHECK (status IN ('running', 'verified_complete', 'failed')),
    run_id bigint REFERENCES meta.etl_run(run_id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_sha256, partition_key),
    CHECK (next_record_number = committed_record_number + 1)
);

COMMENT ON TABLE staging.product_metadata_record IS
    'ASIN-linked offline product evidence; source position and raw source_record remain queryable.';
COMMENT ON TABLE integration.product_movie_candidate IS
    'Title-generated candidates kept separate from multi-fact adoption decisions.';
COMMENT ON TABLE integration.review_queue IS
    'Algorithmic uncertainty queue; rows are not human labels.';
COMMENT ON TABLE meta.product_metadata_progress IS
    'Local UCSD gzip full-scan checkpoint; recovery scans from gzip start to the committed source record.';

RESET ROLE;
