\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE TABLE IF NOT EXISTS integration.movie_match_development_sample (
    asin varchar(10) PRIMARY KEY REFERENCES target.product_edition(asin),
    sample_rank integer NOT NULL UNIQUE CHECK (sample_rank > 0),
    selection_score integer NOT NULL,
    product_source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    product_title_raw text NOT NULL,
    query_title text NOT NULL,
    product_release_date_raw text,
    product_year_evidence jsonb NOT NULL DEFAULT '[]'::jsonb,
    selection_evidence jsonb NOT NULL,
    sample_role text NOT NULL DEFAULT 'development' CHECK (sample_role = 'development'),
    selection_rule_version text NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    selected_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS integration.movie_match_candidate_result (
    asin varchar(10) NOT NULL REFERENCES integration.movie_match_development_sample(asin),
    candidate_key text NOT NULL,
    candidate_source_record_id bigint REFERENCES raw.source_record(source_record_id),
    imdb_candidate_id text,
    imdb_title text,
    imdb_year smallint,
    imdb_title_type text,
    imdb_principal_names jsonb NOT NULL DEFAULT '[]'::jsonb,
    title_similarity numeric(6,5) CHECK (title_similarity BETWEEN 0 AND 1),
    matched_facts jsonb NOT NULL DEFAULT '[]'::jsonb,
    conflict_points jsonb NOT NULL DEFAULT '[]'::jsonb,
    match_status text NOT NULL CHECK (match_status IN (
        'rule_verified', 'needs_review', 'no_candidate', 'fetch_failed'
    )),
    review_question text,
    candidate_pool jsonb NOT NULL DEFAULT '[]'::jsonb,
    rule_version text NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (asin, rule_version)
);

CREATE INDEX IF NOT EXISTS ix_movie_match_candidate_status
    ON integration.movie_match_candidate_result (rule_version, match_status);

COMMENT ON TABLE integration.movie_match_development_sample IS
    'Stage 4 development sample only; it must not be reused as the independent human evaluation set.';
COMMENT ON TABLE integration.movie_match_candidate_result IS
    'Algorithmic IMDb candidates and evidence; rule_verified is not a human label.';

RESET ROLE;
