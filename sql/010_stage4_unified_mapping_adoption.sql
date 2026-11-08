\set ON_ERROR_STOP on
SET ROLE movie_app;

COMMENT ON VIEW integration.approved_development_movie_mapping IS
    'Human-confirmed development subset only; this is not the unified adoption rule and does not require every mapping to have a human label.';

CREATE OR REPLACE VIEW integration.current_movie_match_human_decision_any_version AS
SELECT DISTINCT ON (
    asin,
    label_scope,
    coalesce(imdb_candidate_id, '')
)
    label_id,
    asin,
    imdb_candidate_id,
    rule_version AS label_rule_version,
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
    label_scope,
    coalesce(imdb_candidate_id, ''),
    labeled_at DESC,
    label_id DESC;

CREATE OR REPLACE VIEW integration.unified_development_movie_mapping_decision AS
WITH algorithm AS (
    SELECT s.asin,
           s.product_title_raw,
           r.rule_version,
           r.match_status,
           r.imdb_candidate_id AS algorithm_candidate_id,
           r.review_reason_code,
           r.confirmation_paths
    FROM integration.movie_match_development_sample s
    JOIN integration.movie_match_candidate_result r USING (asin)
    WHERE s.sample_role = 'development'
      AND r.rule_version = 'stage4-first50-movie-match-v3.1-evidence-extraction'
),
candidate_labels AS (
    SELECT asin,
           count(*) FILTER (WHERE human_label = 'correct_match') AS correct_count,
           min(imdb_candidate_id) FILTER (WHERE human_label = 'correct_match') AS correct_candidate_id,
           array_agg(imdb_candidate_id ORDER BY imdb_candidate_id)
               FILTER (WHERE human_label = 'incorrect_match') AS rejected_candidate_ids
    FROM integration.current_movie_match_human_decision_any_version
    WHERE label_scope = 'candidate'
    GROUP BY asin
),
product_labels AS (
    SELECT asin,
           bool_or(human_label = 'not_single_movie') AS not_single_movie
    FROM integration.current_movie_match_human_decision_any_version
    WHERE label_scope = 'product'
    GROUP BY asin
),
pool_labels AS (
    SELECT asin,
           bool_or(human_label = 'no_match_in_pool') AS no_match_in_pool
    FROM integration.current_movie_match_human_decision_any_version
    WHERE label_scope = 'pool'
    GROUP BY asin
),
resolved AS (
    SELECT a.*,
           coalesce(c.correct_count,0) AS human_correct_count,
           c.correct_candidate_id AS human_correct_candidate_id,
           coalesce(c.rejected_candidate_ids,ARRAY[]::text[]) AS human_rejected_candidate_ids,
           coalesce(p.not_single_movie,false) AS human_not_single_movie,
           coalesce(q.no_match_in_pool,false) AS human_no_match_in_pool,
           coalesce(a.algorithm_candidate_id = ANY(c.rejected_candidate_ids),false)
               AS algorithm_candidate_human_rejected
    FROM algorithm a
    LEFT JOIN candidate_labels c USING (asin)
    LEFT JOIN product_labels p USING (asin)
    LEFT JOIN pool_labels q USING (asin)
)
SELECT r.*,
       CASE
           WHEN human_correct_count > 1
             OR (human_correct_count > 0 AND (human_not_single_movie OR human_no_match_in_pool))
               THEN 'deferred_human_conflict'
           WHEN human_not_single_movie THEN 'excluded_not_single_movie'
           WHEN human_no_match_in_pool THEN 'excluded_no_match_in_pool'
           WHEN human_correct_count = 1 THEN 'adopted_human_confirmed'
           WHEN algorithm_candidate_human_rejected THEN 'deferred_human_rejected_algorithm_candidate'
           WHEN match_status = 'rule_verified' THEN 'adopted_rule_verified'
           ELSE 'deferred_algorithm_review'
       END AS adoption_status,
       CASE
           WHEN human_correct_count = 1
             AND NOT (human_not_single_movie OR human_no_match_in_pool)
               THEN human_correct_candidate_id
           WHEN human_correct_count = 0
             AND NOT human_not_single_movie
             AND NOT human_no_match_in_pool
             AND NOT algorithm_candidate_human_rejected
             AND match_status = 'rule_verified'
               THEN algorithm_candidate_id
           ELSE NULL
       END AS adopted_imdb_candidate_id,
       CASE
           WHEN human_correct_count = 1
             AND NOT (human_not_single_movie OR human_no_match_in_pool)
               THEN 'human_confirmed'
           WHEN human_correct_count = 0
             AND NOT human_not_single_movie
             AND NOT human_no_match_in_pool
             AND NOT algorithm_candidate_human_rejected
             AND match_status = 'rule_verified'
               THEN 'rule_verified'
           ELSE NULL
       END AS adoption_origin
FROM resolved r;

CREATE OR REPLACE VIEW integration.adopted_development_movie_mapping AS
SELECT asin,
       product_title_raw,
       adopted_imdb_candidate_id AS imdb_candidate_id,
       adoption_origin,
       rule_version,
       match_status,
       confirmation_paths,
       human_correct_candidate_id,
       human_rejected_candidate_ids
FROM integration.unified_development_movie_mapping_decision
WHERE adoption_status IN ('adopted_human_confirmed','adopted_rule_verified');

CREATE OR REPLACE VIEW integration.development_movie_mapping_review_queue AS
SELECT *
FROM integration.unified_development_movie_mapping_decision
WHERE adoption_status LIKE 'deferred_%';

COMMENT ON VIEW integration.unified_development_movie_mapping_decision IS
    'Unified development adoption: current human decisions take precedence; otherwise v3.1 rule_verified predictions may be adopted automatically.';

COMMENT ON VIEW integration.adopted_development_movie_mapping IS
    'Adopted development mappings from either human confirmation or v3.1 rule verification; review/deferred/excluded products are omitted.';

COMMENT ON VIEW integration.development_movie_mapping_review_queue IS
    'Development products deferred for ambiguity, insufficient evidence, source gaps, or human-decision conflict; no immediate human completion is required.';

RESET ROLE;
