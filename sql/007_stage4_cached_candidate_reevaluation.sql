\set ON_ERROR_STOP on
SET ROLE movie_app;

ALTER TABLE integration.movie_match_candidate_result
    DROP CONSTRAINT IF EXISTS movie_match_candidate_result_match_status_check;

ALTER TABLE integration.movie_match_candidate_result
    ADD CONSTRAINT movie_match_candidate_result_match_status_check CHECK (
        match_status IN (
            'rule_verified',
            'needs_review',
            'needs_review_ambiguous',
            'needs_review_insufficient',
            'no_suitable_candidate',
            'no_candidate',
            'fetch_failed'
        )
    );

COMMENT ON COLUMN integration.movie_match_candidate_result.candidate_pool IS
    'Versioned candidate pool. Stage4 v2 stores per-candidate title, type and unique-person evidence including source locations.';

RESET ROLE;
