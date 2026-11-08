\set ON_ERROR_STOP on
SET ROLE movie_app;

ALTER TABLE integration.movie_match_candidate_result
    ADD COLUMN IF NOT EXISTS confirmation_paths jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS review_reason_code text,
    ADD COLUMN IF NOT EXISTS score_breakdown jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS integration.movie_match_human_label (
    label_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    label_hash character(64) NOT NULL UNIQUE,
    asin varchar(10) NOT NULL REFERENCES integration.movie_match_development_sample(asin),
    imdb_candidate_id text,
    rule_version text NOT NULL,
    label_scope text NOT NULL CHECK (label_scope IN ('candidate','pool','product')),
    human_label text NOT NULL CHECK (human_label IN (
        'correct_match','incorrect_match','uncertain','no_match_in_pool','not_single_movie'
    )),
    reason text NOT NULL CHECK (length(btrim(reason)) > 0),
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    annotator text NOT NULL CHECK (length(btrim(annotator)) > 0),
    labeled_at timestamptz NOT NULL,
    import_batch text NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (label_scope <> 'candidate' OR imdb_candidate_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ix_movie_match_human_label_lookup
    ON integration.movie_match_human_label (asin,rule_version,imdb_candidate_id,labeled_at DESC);

COMMENT ON TABLE integration.movie_match_human_label IS
    'Human labels are separate from algorithm predictions and retain annotator, time, reason and evidence.';

RESET ROLE;
