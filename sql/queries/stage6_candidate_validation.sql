\set ON_ERROR_STOP on
\pset pager off

SELECT s.scope_key,s.status,s.rule_version,s.random_seed,s.target_product_count,s.scope_sha256,
       count(p.file_name) AS progress_files,
       count(*) FILTER (WHERE p.status='verified_complete' AND p.reached_eof) AS files_verified_complete
FROM meta.stage6_candidate_scope s
LEFT JOIN meta.stage6_candidate_progress p USING(snapshot_id,scope_key)
WHERE s.scope_key='stage6-independent-candidates-v1'
GROUP BY s.scope_key,s.status,s.rule_version,s.random_seed,s.target_product_count,s.scope_sha256;

SELECT file_name,committed_record_number,matched_record_count,reached_eof,status,run_id
FROM meta.stage6_candidate_progress
WHERE scope_key='stage6-independent-candidates-v1' ORDER BY file_name;

SELECT count(*) AS candidate_rows,count(DISTINCT asin) AS covered_products,
       count(DISTINCT tconst) AS distinct_tconsts,
       count(*) FILTER (WHERE shortlist_rank IS NOT NULL) AS shortlisted_pairs,
       count(*) FILTER (WHERE basics_source_record_id IS NULL) AS candidates_without_basics,
       count(*) FILTER (WHERE type_compatibility='conflict') AS type_conflicts
FROM integration.stage6_movie_candidate_cache
WHERE scope_key='stage6-independent-candidates-v1';

SELECT title_match_level,type_compatibility,count(*)
FROM integration.stage6_movie_candidate_cache
WHERE scope_key='stage6-independent-candidates-v1'
GROUP BY title_match_level,type_compatibility ORDER BY title_match_level,type_compatibility;

SELECT count(*) AS accidental_stage4_asin_overlap
FROM meta.stage6_candidate_scope_product p
JOIN integration.movie_match_development_sample d USING(asin)
WHERE p.scope_key='stage6-independent-candidates-v1';

SELECT count(*) AS formal_datasets,count(*) FILTER (WHERE status<>'draft') AS non_draft_datasets
FROM evaluation.dataset;

SELECT count(*) AS formal_human_labels FROM evaluation.human_label;
