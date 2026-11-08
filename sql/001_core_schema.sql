\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS integration;
CREATE SCHEMA IF NOT EXISTS target;

CREATE TABLE IF NOT EXISTS meta.etl_run (
    run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name text NOT NULL,
    batch_id text NOT NULL,
    input_version text NOT NULL,
    rule_version text NOT NULL,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    stats jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (pipeline_name, batch_id, input_version, rule_version)
);

CREATE TABLE IF NOT EXISTS meta.batch_progress (
    pipeline_name text NOT NULL,
    input_version text NOT NULL,
    partition_key text NOT NULL,
    next_record_number bigint NOT NULL CHECK (next_record_number >= 1),
    committed_record_number bigint NOT NULL CHECK (committed_record_number >= 0),
    run_id bigint REFERENCES meta.etl_run(run_id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pipeline_name, input_version, partition_key),
    CHECK (next_record_number = committed_record_number + 1)
);

CREATE TABLE IF NOT EXISTS meta.source_file (
    source_file_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_name text NOT NULL,
    source_uri text NOT NULL,
    sha256 char(64) NOT NULL UNIQUE,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    acquired_at timestamptz,
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    parser_version text,
    immutable boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS raw.source_record (
    source_record_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_kind text NOT NULL CHECK (source_kind IN ('review_file', 'http_response', 'third_party_file', 'manual_label')),
    source_file_id bigint REFERENCES meta.source_file(source_file_id),
    source_uri text,
    record_number bigint,
    field_locator jsonb NOT NULL DEFAULT '{}'::jsonb,
    content_sha256 char(64),
    snapshot_path text,
    fetched_at timestamptz,
    http_status integer,
    access_status text,
    raw_payload jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (source_file_id IS NOT NULL OR source_uri IS NOT NULL),
    UNIQUE NULLS NOT DISTINCT (source_file_id, record_number, source_uri, content_sha256)
);

CREATE TABLE IF NOT EXISTS staging.review_record (
    source_review_id char(64) PRIMARY KEY,
    source_record_id bigint NOT NULL UNIQUE REFERENCES raw.source_record(source_record_id),
    asin varchar(10) NOT NULL,
    reviewer_id text,
    profile_name_raw text,
    helpful_votes integer,
    total_votes integer,
    score numeric(2,1),
    review_unix_seconds bigint,
    review_time_utc timestamptz,
    summary_raw text,
    review_text_raw text,
    parse_status text NOT NULL CHECK (parse_status IN ('valid', 'quarantined')),
    parse_issues jsonb NOT NULL DEFAULT '[]'::jsonb,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    CHECK (asin ~ '^[A-Z0-9]{10}$'),
    CHECK (helpful_votes IS NULL OR helpful_votes >= 0),
    CHECK (total_votes IS NULL OR total_votes >= 0),
    CHECK (helpful_votes IS NULL OR total_votes IS NULL OR helpful_votes <= total_votes),
    CHECK (score IS NULL OR score BETWEEN 1 AND 5)
);

CREATE INDEX IF NOT EXISTS ix_staging_review_asin ON staging.review_record (asin);
CREATE INDEX IF NOT EXISTS ix_staging_review_reviewer ON staging.review_record (reviewer_id);

CREATE TABLE IF NOT EXISTS staging.product_harvest (
    asin varchar(10) PRIMARY KEY CHECK (asin ~ '^[A-Z0-9]{10}$'),
    original_url text NOT NULL,
    final_url text,
    status text NOT NULL CHECK (status IN (
        'not_attempted', 'success', 'not_found', 'access_restricted',
        'timeout', 'parse_failed', 'network_failed'
    )),
    http_status integer,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    fetched_at timestamptz,
    source_record_id bigint REFERENCES raw.source_record(source_record_id),
    last_error_code text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (status <> 'success' OR source_record_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS staging.product_harvest_attempt (
    asin varchar(10) NOT NULL REFERENCES staging.product_harvest(asin),
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    requested_url text NOT NULL,
    final_url text,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome text NOT NULL CHECK (outcome IN (
        'success', 'not_found', 'access_restricted', 'timeout',
        'parse_failed', 'network_failed'
    )),
    http_status integer,
    source_record_id bigint REFERENCES raw.source_record(source_record_id),
    error_code text,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    PRIMARY KEY (asin, attempt_number),
    CHECK (outcome <> 'success' OR source_record_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ix_harvest_attempt_source
    ON staging.product_harvest_attempt (source_record_id);

CREATE TABLE IF NOT EXISTS target.product_edition (
    product_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asin varchar(10) NOT NULL UNIQUE CHECK (asin ~ '^[A-Z0-9]{10}$'),
    media_format text,
    language_codes text[],
    subtitle_codes text[],
    region_code text,
    cut_label text,
    publisher text,
    product_release_date date,
    classification text NOT NULL DEFAULT 'pending' CHECK (classification IN (
        'movie', 'non_movie', 'collection', 'series', 'concert_or_other', 'pending'
    )),
    created_run_id bigint REFERENCES meta.etl_run(run_id)
);

CREATE TABLE IF NOT EXISTS target.movie (
    movie_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stable_key text NOT NULL UNIQUE,
    canonical_title text,
    original_title text,
    release_date date,
    release_year smallint,
    release_precision text CHECK (release_precision IN ('day', 'month', 'year', 'unknown')),
    release_region text,
    release_type text,
    status text NOT NULL DEFAULT 'candidate' CHECK (status IN ('candidate', 'verified', 'conflicted', 'retired')),
    CHECK (release_year IS NULL OR release_year BETWEEN 1800 AND 2200),
    CHECK (release_date IS NULL OR release_year IS NULL OR EXTRACT(YEAR FROM release_date) = release_year)
);

CREATE TABLE IF NOT EXISTS target.product_movie (
    product_id bigint NOT NULL REFERENCES target.product_edition(product_id),
    movie_id bigint NOT NULL REFERENCES target.movie(movie_id),
    relationship_type text NOT NULL CHECK (relationship_type IN ('single_work', 'collection_member', 'episode_or_series', 'uncertain')),
    decision_id bigint,
    mapping_status text NOT NULL CHECK (mapping_status IN ('verified', 'predicted', 'manual', 'rejected', 'pending')),
    PRIMARY KEY (product_id, movie_id, relationship_type)
);

CREATE TABLE IF NOT EXISTS target.person (
    person_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stable_key text NOT NULL UNIQUE,
    canonical_name text NOT NULL,
    status text NOT NULL DEFAULT 'candidate' CHECK (status IN ('candidate', 'verified', 'conflicted', 'retired'))
);

CREATE TABLE IF NOT EXISTS target.person_alias (
    person_alias_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    person_id bigint NOT NULL REFERENCES target.person(person_id),
    alias_raw text NOT NULL,
    alias_normalized text NOT NULL,
    language_code text,
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    UNIQUE (person_id, alias_normalized, source_record_id)
);

CREATE TABLE IF NOT EXISTS target.movie_person (
    movie_id bigint NOT NULL REFERENCES target.movie(movie_id),
    person_id bigint NOT NULL REFERENCES target.person(person_id),
    role_type text NOT NULL CHECK (role_type IN ('director', 'starring', 'cast', 'writer', 'other')),
    credited_as text,
    billing_order integer CHECK (billing_order IS NULL OR billing_order > 0),
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    PRIMARY KEY (movie_id, person_id, role_type, source_record_id)
);

CREATE TABLE IF NOT EXISTS target.reviewer (
    reviewer_id text PRIMARY KEY,
    latest_profile_name text
);

CREATE TABLE IF NOT EXISTS target.logical_review (
    logical_review_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stable_key char(64) NOT NULL UNIQUE,
    product_id bigint NOT NULL REFERENCES target.product_edition(product_id),
    reviewer_id text REFERENCES target.reviewer(reviewer_id),
    review_time_utc timestamptz,
    score numeric(2,1) CHECK (score BETWEEN 1 AND 5),
    summary text,
    review_text text
);

CREATE TABLE IF NOT EXISTS target.review_mapping (
    logical_review_id bigint NOT NULL REFERENCES target.logical_review(logical_review_id),
    source_review_id char(64) NOT NULL REFERENCES staging.review_record(source_review_id),
    mapping_reason text NOT NULL,
    rule_version text NOT NULL,
    PRIMARY KEY (logical_review_id, source_review_id)
);

CREATE TABLE IF NOT EXISTS integration.merge_decision (
    merge_decision_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_type text NOT NULL CHECK (entity_type IN ('movie', 'person', 'logical_review', 'product_movie')),
    left_entity_key text NOT NULL,
    right_entity_key text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('merge', 'do_not_merge', 'manual_review')),
    decision_origin text NOT NULL CHECK (decision_origin IN ('rule', 'algorithm_prediction', 'human_label')),
    rule_version text NOT NULL,
    confidence numeric(6,5) CHECK (confidence BETWEEN 0 AND 1),
    evidence jsonb NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (entity_type, left_entity_key, right_entity_key, rule_version)
);

ALTER TABLE target.product_movie
    DROP CONSTRAINT IF EXISTS fk_product_movie_decision;
ALTER TABLE target.product_movie
    ADD CONSTRAINT fk_product_movie_decision
    FOREIGN KEY (decision_id) REFERENCES integration.merge_decision(merge_decision_id);

CREATE TABLE IF NOT EXISTS integration.field_assertion (
    assertion_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_type text NOT NULL,
    entity_key text NOT NULL,
    field_name text NOT NULL,
    raw_value jsonb,
    normalized_value jsonb,
    value_semantics text,
    source_record_id bigint REFERENCES raw.source_record(source_record_id),
    field_locator jsonb NOT NULL DEFAULT '{}'::jsonb,
    extraction_rule_version text NOT NULL,
    confidence numeric(6,5) CHECK (confidence BETWEEN 0 AND 1),
    quality_status text NOT NULL CHECK (quality_status IN ('observed', 'inferred', 'manual', 'invalid', 'conflicted')),
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id)
);

CREATE INDEX IF NOT EXISTS ix_assertion_entity_field
    ON integration.field_assertion (entity_type, entity_key, field_name);
CREATE INDEX IF NOT EXISTS ix_assertion_source
    ON integration.field_assertion (source_record_id);

CREATE TABLE IF NOT EXISTS integration.assertion_dependency (
    output_assertion_id bigint NOT NULL REFERENCES integration.field_assertion(assertion_id),
    input_assertion_id bigint NOT NULL REFERENCES integration.field_assertion(assertion_id),
    operation_type text NOT NULL,
    rule_id text NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    PRIMARY KEY (output_assertion_id, input_assertion_id, operation_type, rule_id),
    CHECK (output_assertion_id <> input_assertion_id)
);

CREATE TABLE IF NOT EXISTS integration.field_selection (
    field_selection_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_type text NOT NULL,
    entity_key text NOT NULL,
    field_name text NOT NULL,
    selection_version text NOT NULL,
    selected_assertion_id bigint REFERENCES integration.field_assertion(assertion_id),
    selected_assertion_ids bigint[],
    decision_status text NOT NULL CHECK (decision_status IN ('selected', 'conflicted', 'missing')),
    decision_reason text NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (entity_type, entity_key, field_name, selection_version),
    CHECK (selected_assertion_id IS NOT NULL OR selected_assertion_ids IS NOT NULL OR decision_status IN ('conflicted', 'missing'))
);

CREATE TABLE IF NOT EXISTS meta.quality_issue (
    quality_issue_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    issue_code text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'fatal')),
    entity_type text,
    entity_key text,
    source_record_id bigint REFERENCES raw.source_record(source_record_id),
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS ix_quality_issue_run_code ON meta.quality_issue (run_id, issue_code);

COMMENT ON TABLE integration.field_assertion IS 'One queryable candidate value with its raw evidence and extraction rule.';
COMMENT ON TABLE integration.assertion_dependency IS 'Field-level transformation edges; task logs are not lineage.';
COMMENT ON TABLE integration.field_selection IS 'Versioned final choice while rejected assertions remain queryable.';
COMMENT ON TABLE meta.batch_progress IS 'Must be updated in the same transaction as the corresponding target writes.';

RESET ROLE;

