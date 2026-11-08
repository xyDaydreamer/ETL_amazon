\set ON_ERROR_STOP on

SELECT count(*) AS selected,
       count(DISTINCT asin) AS distinct_asins,
       count(*) FILTER (WHERE sample_role = 'development') AS development_rows,
       count(*) FILTER (WHERE product_release_date_raw IS NOT NULL) AS product_dates_present
FROM integration.movie_match_development_sample
WHERE selection_rule_version = 'stage4-first50-movie-match-v1';

SELECT match_status, count(*)
FROM integration.movie_match_candidate_result
WHERE rule_version = 'stage4-first50-movie-match-v3.1-evidence-extraction'
GROUP BY match_status
ORDER BY match_status;

SELECT count(*) AS results,
       count(DISTINCT c.asin) AS distinct_asins,
       count(c.candidate_source_record_id) AS response_evidence,
       count(*) FILTER (WHERE pr.source_record_id IS NULL) AS missing_product_evidence,
       count(*) FILTER (WHERE cr.source_record_id IS NULL) AS missing_candidate_evidence
FROM integration.movie_match_candidate_result c
JOIN integration.movie_match_development_sample s USING (asin)
LEFT JOIN raw.source_record pr ON pr.source_record_id = s.product_source_record_id
LEFT JOIN raw.source_record cr ON cr.source_record_id = c.candidate_source_record_id
WHERE c.rule_version = 'stage4-first50-movie-match-v3.1-evidence-extraction';

SELECT s.sample_rank,s.asin,s.product_title_raw,c.imdb_candidate_id,c.imdb_title,
       c.imdb_year,c.match_status,c.matched_facts,c.conflict_points
FROM integration.movie_match_development_sample s
JOIN integration.movie_match_candidate_result c USING (asin)
WHERE c.rule_version = 'stage4-first50-movie-match-v3.1-evidence-extraction'
ORDER BY s.sample_rank;

SELECT v3.match_status AS old_status,v31.match_status AS new_status,count(*)
FROM integration.movie_match_candidate_result v3
JOIN integration.movie_match_candidate_result v31 USING (asin)
WHERE v3.rule_version = 'stage4-first50-movie-match-v3-evidence'
  AND v31.rule_version = 'stage4-first50-movie-match-v3.1-evidence-extraction'
GROUP BY 1,2 ORDER BY 1,2;

SELECT sum(jsonb_array_length(candidate_pool)) AS evaluated_pool_items,
       count(*) FILTER (
           WHERE EXISTS (
               SELECT 1 FROM jsonb_array_elements(candidate_pool) e
               WHERE NOT e ? 'title_evidence'
                  OR NOT e ? 'type_evidence'
                  OR NOT e ? 'person_evidence'
           )
       ) AS results_with_missing_candidate_evaluation
FROM integration.movie_match_candidate_result
WHERE rule_version = 'stage4-first50-movie-match-v3.1-evidence-extraction';

SELECT count(*) AS result_rows,
       sum(jsonb_array_length(candidate_pool)) AS all_deduplicated_tt_candidates,
       count(*) FILTER (WHERE jsonb_array_length(candidate_pool)=0) AS empty_pools,
       count(*) FILTER (WHERE confirmation_paths <> '[]'::jsonb) AS selected_with_paths,
       count(*) FILTER (WHERE review_reason_code IS NULL) AS missing_reason_codes
FROM integration.movie_match_candidate_result
WHERE rule_version = 'stage4-first50-movie-match-v3.1-evidence-extraction';

SELECT count(*) AS human_labels,
       count(*) FILTER (WHERE label_scope='candidate') AS candidate_labels,
       count(*) FILTER (WHERE label_scope='pool') AS pool_labels,
       count(*) FILTER (WHERE label_scope='product') AS product_labels
FROM integration.movie_match_human_label;

SELECT label_scope,human_label,count(*)
FROM integration.current_movie_match_human_decision_any_version
GROUP BY 1,2 ORDER BY 1,2;

SELECT adoption_status,count(*)
FROM integration.unified_development_movie_mapping_decision
GROUP BY 1 ORDER BY 1;

SELECT asin,imdb_candidate_id,adoption_origin,rule_version,match_status
FROM integration.adopted_development_movie_mapping
ORDER BY asin;
