\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE SCHEMA IF NOT EXISTS evaluation;

CREATE TABLE IF NOT EXISTS meta.external_snapshot (
    snapshot_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_name text NOT NULL,
    snapshot_version text NOT NULL,
    snapshot_date date NOT NULL,
    source_uri text NOT NULL,
    license_uri text NOT NULL,
    manifest_sha256 char(64),
    verification_status text NOT NULL DEFAULT 'registered' CHECK (
        verification_status IN ('registered', 'headers_verified', 'fully_verified', 'failed')
    ),
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    verified_at timestamptz,
    UNIQUE (source_name, snapshot_version)
);

CREATE TABLE IF NOT EXISTS meta.external_snapshot_file (
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    file_name text NOT NULL,
    path_hint text NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    file_sha256 char(64),
    expected_header jsonb NOT NULL,
    observed_header jsonb NOT NULL,
    header_verified boolean NOT NULL,
    gzip_stream_verified boolean NOT NULL DEFAULT false,
    uncompressed_record_count bigint CHECK (uncompressed_record_count IS NULL OR uncompressed_record_count >= 0),
    PRIMARY KEY (snapshot_id, file_name)
);

CREATE TABLE IF NOT EXISTS staging.imdb_title_basics (
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    tconst text NOT NULL CHECK (tconst ~ '^tt[0-9]+$'),
    title_type text NOT NULL,
    primary_title text NOT NULL,
    original_title text NOT NULL,
    is_adult boolean,
    start_year smallint CHECK (start_year IS NULL OR start_year BETWEEN 1800 AND 2200),
    end_year smallint CHECK (end_year IS NULL OR end_year BETWEEN 1800 AND 2200),
    runtime_minutes integer CHECK (runtime_minutes IS NULL OR runtime_minutes >= 0),
    genres text[] NOT NULL DEFAULT ARRAY[]::text[],
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    PRIMARY KEY (snapshot_id, tconst)
);

CREATE TABLE IF NOT EXISTS staging.imdb_title_aka (
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    title_id text NOT NULL CHECK (title_id ~ '^tt[0-9]+$'),
    ordering integer NOT NULL CHECK (ordering > 0),
    title text NOT NULL,
    region text,
    language text,
    title_types text[] NOT NULL DEFAULT ARRAY[]::text[],
    attributes text[] NOT NULL DEFAULT ARRAY[]::text[],
    is_original_title boolean,
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    PRIMARY KEY (snapshot_id, title_id, ordering)
);

CREATE TABLE IF NOT EXISTS staging.imdb_title_crew (
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    tconst text NOT NULL CHECK (tconst ~ '^tt[0-9]+$'),
    director_nconsts text[] NOT NULL DEFAULT ARRAY[]::text[],
    writer_nconsts text[] NOT NULL DEFAULT ARRAY[]::text[],
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    PRIMARY KEY (snapshot_id, tconst)
);

CREATE TABLE IF NOT EXISTS staging.imdb_title_principal (
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    tconst text NOT NULL CHECK (tconst ~ '^tt[0-9]+$'),
    ordering integer NOT NULL CHECK (ordering > 0),
    nconst text NOT NULL CHECK (nconst ~ '^nm[0-9]+$'),
    category text NOT NULL,
    job text,
    characters jsonb,
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    PRIMARY KEY (snapshot_id, tconst, ordering)
);

CREATE TABLE IF NOT EXISTS staging.imdb_name_basics (
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    nconst text NOT NULL CHECK (nconst ~ '^nm[0-9]+$'),
    primary_name text NOT NULL,
    birth_year smallint CHECK (birth_year IS NULL OR birth_year BETWEEN 1700 AND 2200),
    death_year smallint CHECK (death_year IS NULL OR death_year BETWEEN 1700 AND 2200),
    primary_professions text[] NOT NULL DEFAULT ARRAY[]::text[],
    known_for_tconsts text[] NOT NULL DEFAULT ARRAY[]::text[],
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    PRIMARY KEY (snapshot_id, nconst)
);

CREATE INDEX IF NOT EXISTS ix_imdb_aka_title
    ON staging.imdb_title_aka (snapshot_id, title_id);
CREATE INDEX IF NOT EXISTS ix_imdb_principal_name
    ON staging.imdb_title_principal (snapshot_id, nconst);

CREATE TABLE IF NOT EXISTS integration.movie_external_identifier (
    movie_id bigint NOT NULL REFERENCES target.movie(movie_id),
    source_name text NOT NULL,
    external_id text NOT NULL,
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    status text NOT NULL CHECK (status IN ('observed', 'adopted', 'rejected', 'conflicted')),
    PRIMARY KEY (movie_id, source_name, external_id, snapshot_id),
    UNIQUE (source_name, external_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS integration.person_external_identifier (
    person_id bigint NOT NULL REFERENCES target.person(person_id),
    source_name text NOT NULL,
    external_id text NOT NULL,
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    status text NOT NULL CHECK (status IN ('observed', 'adopted', 'rejected', 'conflicted')),
    PRIMARY KEY (person_id, source_name, external_id, snapshot_id),
    UNIQUE (source_name, external_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS integration.person_match_candidate (
    candidate_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    left_person_key text NOT NULL,
    imdb_nconst text NOT NULL CHECK (imdb_nconst ~ '^nm[0-9]+$'),
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    rule_version text NOT NULL,
    match_status text NOT NULL CHECK (match_status IN (
        'rule_verified', 'needs_review_ambiguous', 'needs_review_insufficient',
        'no_suitable_candidate', 'rejected'
    )),
    score_breakdown jsonb NOT NULL DEFAULT '{}'::jsonb,
    evidence jsonb NOT NULL DEFAULT '[]'::jsonb,
    conflict_points jsonb NOT NULL DEFAULT '[]'::jsonb,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (left_person_key, imdb_nconst, snapshot_id, rule_version)
);

CREATE TABLE IF NOT EXISTS evaluation.dataset (
    evaluation_dataset_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_key text NOT NULL UNIQUE,
    entity_type text NOT NULL CHECK (entity_type IN ('movie', 'person')),
    sample_role text NOT NULL DEFAULT 'independent_evaluation' CHECK (sample_role = 'independent_evaluation'),
    snapshot_id bigint REFERENCES meta.external_snapshot(snapshot_id),
    sampling_rule_version text NOT NULL,
    development_exclusion jsonb NOT NULL,
    status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'frozen', 'labeling', 'labeled', 'evaluated')),
    manifest_sha256 char(64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    frozen_at timestamptz
);

CREATE TABLE IF NOT EXISTS evaluation.entity_pair (
    review_item_id char(64) PRIMARY KEY,
    evaluation_dataset_id bigint NOT NULL REFERENCES evaluation.dataset(evaluation_dataset_id),
    display_order integer NOT NULL CHECK (display_order > 0),
    left_entity_key text NOT NULL,
    right_entity_key text NOT NULL,
    stratum text NOT NULL,
    left_display jsonb NOT NULL,
    right_display jsonb NOT NULL,
    evidence_display jsonb NOT NULL DEFAULT '[]'::jsonb,
    source_links jsonb NOT NULL DEFAULT '[]'::jsonb,
    pair_payload_sha256 char(64) NOT NULL,
    UNIQUE (evaluation_dataset_id, display_order),
    UNIQUE (evaluation_dataset_id, left_entity_key, right_entity_key)
);

CREATE TABLE IF NOT EXISTS evaluation.algorithm_prediction (
    evaluation_dataset_id bigint NOT NULL REFERENCES evaluation.dataset(evaluation_dataset_id),
    review_item_id char(64) NOT NULL REFERENCES evaluation.entity_pair(review_item_id),
    rule_version text NOT NULL,
    prediction text NOT NULL,
    evidence jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (evaluation_dataset_id, review_item_id, rule_version)
);

CREATE TABLE IF NOT EXISTS evaluation.human_label (
    label_hash char(64) PRIMARY KEY,
    review_item_id char(64) NOT NULL REFERENCES evaluation.entity_pair(review_item_id),
    human_label text NOT NULL CHECK (human_label IN ('same_entity', 'different_entity', 'cannot_determine')),
    reason text NOT NULL CHECK (length(btrim(reason)) > 0),
    annotator text NOT NULL,
    labeled_at timestamptz NOT NULL,
    submission_channel text NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS ix_evaluation_pair_dataset
    ON evaluation.entity_pair (evaluation_dataset_id, display_order);
CREATE INDEX IF NOT EXISTS ix_evaluation_label_item
    ON evaluation.human_label (review_item_id, labeled_at DESC);

COMMENT ON TABLE meta.external_snapshot IS
    'Versioned external snapshot registration. Daily IMDb data must be pinned by date and per-file hashes before use.';
COMMENT ON TABLE staging.imdb_title_principal IS
    'IMDb principals are principal credits, not a complete cast list.';
COMMENT ON TABLE evaluation.entity_pair IS
    'Independent evaluation pairs. Display evidence is frozen before labels and excludes algorithm predictions.';
COMMENT ON TABLE evaluation.algorithm_prediction IS
    'Algorithm output kept separate from blind human material and human labels.';
COMMENT ON TABLE evaluation.human_label IS
    'Append-only human decisions; development labels must not be inserted here.';

RESET ROLE;
