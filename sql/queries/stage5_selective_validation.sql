\set ON_ERROR_STOP on
\pset pager off

SELECT snapshot_id,snapshot_version,verification_status,manifest_sha256,verified_at
FROM meta.external_snapshot
WHERE source_name='imdb_noncommercial'
ORDER BY snapshot_id;

SELECT s.scope_key,s.status,s.target_title_count,s.scope_sha256,s.updated_at,
       count(p.file_name) AS checkpoint_files,
       count(*) FILTER (WHERE p.reached_eof) AS files_at_eof
FROM meta.imdb_selective_scope s
LEFT JOIN meta.imdb_selective_progress p
  ON p.snapshot_id=s.snapshot_id AND p.scope_key=s.scope_key
GROUP BY s.scope_key,s.status,s.target_title_count,s.scope_sha256,s.updated_at
ORDER BY s.scope_key;

SELECT
    (SELECT count(*) FROM staging.imdb_title_basics WHERE snapshot_id=1) AS title_basics,
    (SELECT count(*) FROM staging.imdb_title_aka WHERE snapshot_id=1) AS title_akas,
    (SELECT count(*) FROM staging.imdb_title_crew WHERE snapshot_id=1) AS title_crew,
    (SELECT count(*) FROM staging.imdb_title_principal WHERE snapshot_id=1) AS title_principals,
    (SELECT count(*) FROM staging.imdb_name_basics WHERE snapshot_id=1) AS name_basics;

SELECT
    (SELECT count(*) FROM target.movie WHERE stable_key LIKE 'imdb:tt%') AS imdb_movies,
    (SELECT count(*) FROM target.person WHERE stable_key LIKE 'imdb:nm%') AS imdb_nconst_people,
    (SELECT count(*) FROM integration.movie_external_identifier WHERE snapshot_id=1) AS movie_external_ids,
    (SELECT count(*) FROM integration.person_external_identifier WHERE snapshot_id=1) AS person_external_ids,
    (SELECT count(DISTINCT (movie_id,person_id,role_type))
       FROM integration.movie_person_source_evidence WHERE snapshot_id=1) AS relationship_facts,
    (SELECT count(*) FROM integration.movie_person_source_evidence WHERE snapshot_id=1) AS relationship_evidence_rows;

SELECT role_type,selection_status,count(*)
FROM integration.movie_person_source_evidence
WHERE snapshot_id=1
GROUP BY role_type,selection_status
ORDER BY role_type,selection_status;

SELECT match_status,count(*)
FROM integration.person_match_candidate
WHERE snapshot_id=1
GROUP BY match_status
ORDER BY match_status;

SELECT issue_code,count(*)
FROM meta.quality_issue
WHERE run_id IN (
    SELECT run_id FROM meta.etl_run
    WHERE pipeline_name='imdb_selective_integration'
      AND input_version='imdb-noncommercial-2026-11-05'
)
GROUP BY issue_code
ORDER BY issue_code;

SELECT
    (SELECT count(*) FROM integration.adopted_development_movie_mapping) AS frozen_adopted,
    (SELECT count(*) FROM integration.development_movie_mapping_review_queue) AS frozen_review_queue,
    (SELECT count(*) FROM integration.movie_match_human_label) AS development_human_labels,
    (SELECT count(*) FROM evaluation.human_label) AS independent_evaluation_labels;
