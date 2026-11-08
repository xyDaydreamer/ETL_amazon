\set ON_ERROR_STOP on
\pset pager off

SELECT adoption_origin, count(*) AS adopted_count
FROM integration.adopted_development_movie_mapping
GROUP BY adoption_origin
ORDER BY adoption_origin;

SELECT count(*) AS frozen_review_queue_count
FROM integration.development_movie_mapping_review_queue;

SELECT count(*) AS excluded_not_single_movie_count
FROM integration.unified_development_movie_mapping_decision
WHERE adoption_status = 'excluded_not_single_movie';

SELECT count(*) AS external_snapshot_count,
       count(*) FILTER (WHERE verification_status = 'fully_verified') AS fully_verified_snapshot_count
FROM meta.external_snapshot;

SELECT count(*) AS imdb_nconst_person_count
FROM integration.person_external_identifier
WHERE source_name = 'imdb';

SELECT count(*) AS independent_evaluation_dataset_count
FROM evaluation.dataset;

SELECT count(*) AS independent_human_label_count
FROM evaluation.human_label;

SELECT count(*) AS suggestion_name_candidate_count
FROM target.person
WHERE stable_key LIKE 'imdb-suggestion-name:%';
