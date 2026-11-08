\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE TABLE IF NOT EXISTS meta.stage6_candidate_scope (
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    scope_key text NOT NULL,
    rule_version text NOT NULL,
    scope_sha256 char(64) NOT NULL,
    random_seed bigint NOT NULL,
    target_product_count integer NOT NULL CHECK (target_product_count > 0),
    development_exclusion jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('registered','scanning','candidates_ready','failed')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (snapshot_id,scope_key)
);

CREATE TABLE IF NOT EXISTS meta.stage6_candidate_scope_product (
    snapshot_id bigint NOT NULL,
    scope_key text NOT NULL,
    asin varchar(10) NOT NULL CHECK (asin ~ '^[A-Z0-9]{10}$'),
    product_title_raw text NOT NULL,
    product_title_normalized text NOT NULL,
    product_source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    product_source_record_number bigint NOT NULL,
    PRIMARY KEY (snapshot_id,scope_key,asin),
    FOREIGN KEY (snapshot_id,scope_key) REFERENCES meta.stage6_candidate_scope(snapshot_id,scope_key)
);

CREATE TABLE IF NOT EXISTS meta.stage6_candidate_progress (
    snapshot_id bigint NOT NULL,
    scope_key text NOT NULL,
    file_name text NOT NULL CHECK (file_name IN ('title.akas.tsv.gz','title.basics.tsv.gz')),
    scope_sha256 char(64) NOT NULL,
    committed_record_number bigint NOT NULL DEFAULT 0 CHECK (committed_record_number >= 0),
    next_record_number bigint NOT NULL DEFAULT 1 CHECK (next_record_number = committed_record_number + 1),
    matched_record_count bigint NOT NULL DEFAULT 0 CHECK (matched_record_count >= 0),
    reached_eof boolean NOT NULL DEFAULT false,
    status text NOT NULL CHECK (status IN ('pending','running','verified_complete','failed')),
    run_id bigint REFERENCES meta.etl_run(run_id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (snapshot_id,scope_key,file_name),
    FOREIGN KEY (snapshot_id,scope_key) REFERENCES meta.stage6_candidate_scope(snapshot_id,scope_key),
    CHECK (status <> 'verified_complete' OR reached_eof)
);

CREATE TABLE IF NOT EXISTS integration.stage6_movie_candidate_cache (
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    scope_key text NOT NULL,
    asin varchar(10) NOT NULL CHECK (asin ~ '^[A-Z0-9]{10}$'),
    tconst text NOT NULL CHECK (tconst ~ '^tt[0-9]+$'),
    best_title_similarity numeric(6,5) NOT NULL CHECK (best_title_similarity BETWEEN 0 AND 1),
    title_match_level text NOT NULL CHECK (title_match_level IN ('exact','strong','close','blocked')),
    matched_titles jsonb NOT NULL DEFAULT '[]'::jsonb,
    primary_title text,
    original_title text,
    title_type text,
    start_year smallint,
    genres text[] NOT NULL DEFAULT ARRAY[]::text[],
    basics_source_record_id bigint REFERENCES raw.source_record(source_record_id),
    type_compatibility text NOT NULL DEFAULT 'unknown' CHECK (type_compatibility IN ('compatible','unknown','conflict')),
    shortlist_rank integer CHECK (shortlist_rank IS NULL OR shortlist_rank > 0),
    rule_version text NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (snapshot_id,scope_key,asin,tconst),
    FOREIGN KEY (snapshot_id,scope_key,asin)
        REFERENCES meta.stage6_candidate_scope_product(snapshot_id,scope_key,asin)
);

CREATE INDEX IF NOT EXISTS ix_stage6_candidate_tconst
    ON integration.stage6_movie_candidate_cache(snapshot_id,scope_key,tconst);
CREATE INDEX IF NOT EXISTS ix_stage6_candidate_shortlist
    ON integration.stage6_movie_candidate_cache(snapshot_id,scope_key,asin,shortlist_rank)
    WHERE shortlist_rank IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_stage6_scope_product_title
    ON meta.stage6_candidate_scope_product(snapshot_id,scope_key,product_title_normalized);

COMMENT ON TABLE integration.stage6_movie_candidate_cache IS
    'Reusable Stage 6 title/alias candidate cache. A row is a candidate, never an adopted mapping or human fact.';
COMMENT ON TABLE meta.stage6_candidate_progress IS
    'Resume checkpoint for one shared title.akas/title.basics scan. Recovery decompresses from file start.';

RESET ROLE;
