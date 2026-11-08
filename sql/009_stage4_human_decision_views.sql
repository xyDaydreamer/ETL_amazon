\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE OR REPLACE VIEW integration.current_movie_match_human_decision AS
SELECT DISTINCT ON (
    asin,
    rule_version,
    label_scope,
    coalesce(imdb_candidate_id, '')
)
    label_id,
    asin,
    imdb_candidate_id,
    rule_version,
    label_scope,
    human_label,
    reason,
    evidence,
    annotator,
    labeled_at,
    import_batch,
    imported_at
FROM integration.movie_match_human_label
ORDER BY
    asin,
    rule_version,
    label_scope,
    coalesce(imdb_candidate_id, ''),
    labeled_at DESC,
    label_id DESC;

CREATE OR REPLACE VIEW integration.approved_development_movie_mapping AS
WITH current_candidate AS (
    SELECT *
    FROM integration.current_movie_match_human_decision
    WHERE label_scope = 'candidate'
),
current_product_exclusion AS (
    SELECT asin, rule_version
    FROM integration.current_movie_match_human_decision
    WHERE label_scope = 'product'
      AND human_label = 'not_single_movie'
),
confirmed AS (
    SELECT c.*,
           count(*) OVER (PARTITION BY c.asin, c.rule_version) AS confirmed_candidates
    FROM current_candidate c
    WHERE c.human_label = 'correct_match'
)
SELECT
    c.asin,
    c.imdb_candidate_id,
    c.rule_version,
    c.label_id AS human_label_id,
    c.reason,
    c.evidence,
    c.annotator,
    c.labeled_at
FROM confirmed c
JOIN integration.movie_match_development_sample s USING (asin)
LEFT JOIN current_product_exclusion x
  ON x.asin = c.asin AND x.rule_version = c.rule_version
WHERE c.confirmed_candidates = 1
  AND s.sample_role = 'development'
  AND x.asin IS NULL;

COMMENT ON VIEW integration.current_movie_match_human_decision IS
    'Latest immutable human decision per ASIN, rule version, scope and candidate; algorithm predictions remain separate.';

COMMENT ON VIEW integration.approved_development_movie_mapping IS
    'Development-only mappings with exactly one current correct_match candidate and no current not_single_movie product exclusion.';

RESET ROLE;
