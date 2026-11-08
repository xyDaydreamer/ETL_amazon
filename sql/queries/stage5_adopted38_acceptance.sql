\set ON_ERROR_STOP on
\pset pager off

-- Stage 5 acceptance for the frozen Stage 4 adopted-development scope.
-- This is read-only and deliberately does not scan any IMDb gzip file.
\set snapshot_id 1
\set scope_key 'stage5-adopted-38-v1'

SELECT s.snapshot_id,s.scope_key,s.status,s.target_title_count,s.scope_sha256,
       count(p.*) AS checkpoint_files,
       count(*) FILTER (WHERE p.status='verified_complete' AND p.reached_eof) AS verified_complete_files,
       min(p.updated_at) AS first_checkpoint_at,max(p.updated_at) AS last_checkpoint_at
FROM meta.imdb_selective_scope s
LEFT JOIN meta.imdb_selective_progress p
  ON p.snapshot_id=s.snapshot_id AND p.scope_key=s.scope_key
WHERE s.snapshot_id=:snapshot_id AND s.scope_key=:'scope_key'
GROUP BY s.snapshot_id,s.scope_key,s.status,s.target_title_count,s.scope_sha256;

SELECT p.file_name,p.committed_record_number,p.selected_record_count,p.reached_eof,p.status,p.run_id
FROM meta.imdb_selective_progress p
WHERE p.snapshot_id=:snapshot_id AND p.scope_key=:'scope_key'
ORDER BY p.file_name;

-- The 38 frozen product mappings must be identical to the scope membership.
WITH scope_product AS (
  SELECT asin,tconst FROM meta.imdb_selective_scope_product
  WHERE snapshot_id=:snapshot_id AND scope_key=:'scope_key'
), adopted AS (
  SELECT asin,imdb_candidate_id AS tconst
  FROM integration.adopted_development_movie_mapping
), diff AS (
  (SELECT 'scope_only' AS side,* FROM scope_product EXCEPT SELECT 'scope_only',* FROM adopted)
  UNION ALL
  (SELECT 'adopted_only' AS side,* FROM adopted EXCEPT SELECT 'adopted_only',* FROM scope_product)
)
SELECT (SELECT count(*) FROM scope_product) AS scope_rows,
       (SELECT count(DISTINCT asin) FROM scope_product) AS distinct_asins,
       (SELECT count(DISTINCT tconst) FROM scope_product) AS distinct_tconsts,
       (SELECT count(*) FROM diff) AS mapping_differences;

-- Why 808 principal rows materialize as 809 union relationship facts.
WITH p AS (
  SELECT tconst,nconst,
         CASE WHEN category='director' THEN 'director'
              WHEN category IN ('actor','actress','self','archive_footage','archive_sound') THEN 'cast'
              WHEN category='writer' THEN 'writer' ELSE 'other' END AS role_type
  FROM staging.imdb_title_principal
  WHERE snapshot_id=:snapshot_id
    AND tconst IN (SELECT tconst FROM meta.imdb_selective_scope_title
                   WHERE snapshot_id=:snapshot_id AND scope_key=:'scope_key')
), c AS (
  SELECT tconst,unnest(director_nconsts) AS nconst,'director'::text AS role_type
  FROM staging.imdb_title_crew
  WHERE snapshot_id=:snapshot_id
    AND tconst IN (SELECT tconst FROM meta.imdb_selective_scope_title
                   WHERE snapshot_id=:snapshot_id AND scope_key=:'scope_key')
  UNION ALL
  SELECT tconst,unnest(writer_nconsts),'writer'
  FROM staging.imdb_title_crew
  WHERE snapshot_id=:snapshot_id
    AND tconst IN (SELECT tconst FROM meta.imdb_selective_scope_title
                   WHERE snapshot_id=:snapshot_id AND scope_key=:'scope_key')
), pd AS (SELECT DISTINCT * FROM p), cd AS (SELECT DISTINCT * FROM c)
SELECT (SELECT count(*) FROM p) AS principal_rows,
       (SELECT count(*) FROM pd) AS principal_distinct_facts,
       (SELECT count(*) FROM c) AS crew_entries,
       (SELECT count(*) FROM cd) AS crew_distinct_facts,
       (SELECT count(*) FROM (SELECT * FROM pd INTERSECT SELECT * FROM cd) x) AS overlap_facts,
       (SELECT count(*) FROM (SELECT * FROM pd UNION SELECT * FROM cd) x) AS union_relationship_facts;

-- Scope entities, external identifiers and selected relationship facts.
WITH scope_movies AS (
  SELECT mei.movie_id,mei.external_id AS tconst
  FROM integration.movie_external_identifier mei
  JOIN meta.imdb_selective_scope_title st ON st.snapshot_id=mei.snapshot_id AND st.tconst=mei.external_id
  WHERE st.snapshot_id=:snapshot_id AND st.scope_key=:'scope_key' AND mei.source_name='imdb'
), scope_people AS (
  SELECT DISTINCT pei.person_id,pei.external_id AS nconst
  FROM integration.person_external_identifier pei
  JOIN integration.movie_person_source_evidence e
    ON e.person_id=pei.person_id AND e.snapshot_id=pei.snapshot_id
  JOIN scope_movies sm ON sm.movie_id=e.movie_id
  WHERE pei.snapshot_id=:snapshot_id AND pei.source_name='imdb'
)
SELECT (SELECT count(*) FROM scope_movies) AS movies,
       (SELECT count(*) FROM scope_people) AS nconst_people,
       (SELECT count(*) FROM integration.movie_person_source_evidence e
        JOIN scope_movies sm ON sm.movie_id=e.movie_id
        WHERE e.snapshot_id=:snapshot_id AND e.selection_status='selected') AS selected_relationship_sources,
       (SELECT count(DISTINCT (e.movie_id,e.person_id,e.role_type))
        FROM integration.movie_person_source_evidence e
        JOIN scope_movies sm ON sm.movie_id=e.movie_id
        WHERE e.snapshot_id=:snapshot_id) AS relationship_facts;

-- Field and relation lineage completeness. Each selected field/relation must reach a raw record.
WITH scope_movies AS (
  SELECT mei.movie_id FROM integration.movie_external_identifier mei
  JOIN meta.imdb_selective_scope_title st ON st.snapshot_id=mei.snapshot_id AND st.tconst=mei.external_id
  WHERE st.snapshot_id=:snapshot_id AND st.scope_key=:'scope_key' AND mei.source_name='imdb'
), scope_people AS (
  SELECT DISTINCT e.person_id FROM integration.movie_person_source_evidence e
  JOIN scope_movies sm ON sm.movie_id=e.movie_id WHERE e.snapshot_id=:snapshot_id
)
SELECT
  (SELECT count(*) FROM integration.field_assertion a
   WHERE a.entity_type='movie' AND a.run_id=75) AS movie_field_assertions,
  (SELECT count(*) FROM integration.field_assertion a
   WHERE a.entity_type='person' AND a.run_id=75) AS person_field_assertions,
  (SELECT count(*) FROM integration.field_assertion a LEFT JOIN raw.source_record r ON r.source_record_id=a.source_record_id
   WHERE a.run_id=75 AND r.source_record_id IS NULL) AS assertions_without_raw_source,
  (SELECT count(*) FROM integration.movie_person_source_evidence e JOIN scope_movies s ON s.movie_id=e.movie_id
   LEFT JOIN raw.source_record rr ON rr.source_record_id=e.relationship_source_record_id
   LEFT JOIN raw.source_record nr ON nr.source_record_id=e.name_source_record_id
   WHERE e.snapshot_id=:snapshot_id AND (rr.source_record_id IS NULL OR nr.source_record_id IS NULL)) AS relations_without_locatable_sources;

-- Orphan checks: every external ID and relationship must resolve to its target entity.
WITH scope_titles AS (
  SELECT tconst FROM meta.imdb_selective_scope_title
  WHERE snapshot_id=:snapshot_id AND scope_key=:'scope_key'
), scope_movies AS (
  SELECT mei.movie_id FROM integration.movie_external_identifier mei
  JOIN scope_titles s ON s.tconst=mei.external_id
  WHERE mei.snapshot_id=:snapshot_id AND mei.source_name='imdb'
)
SELECT
  (SELECT count(*) FROM scope_titles s LEFT JOIN integration.movie_external_identifier x
     ON x.snapshot_id=:snapshot_id AND x.source_name='imdb' AND x.external_id=s.tconst
   WHERE x.movie_id IS NULL) AS titles_without_movie_external_id,
  (SELECT count(*) FROM integration.movie_person_source_evidence e JOIN scope_movies sm ON sm.movie_id=e.movie_id
   LEFT JOIN target.movie m ON m.movie_id=e.movie_id LEFT JOIN target.person p ON p.person_id=e.person_id
   WHERE e.snapshot_id=:snapshot_id AND (m.movie_id IS NULL OR p.person_id IS NULL)) AS orphan_relationship_evidence,
  (SELECT count(*) FROM integration.movie_person_source_evidence e JOIN scope_movies sm ON sm.movie_id=e.movie_id
   LEFT JOIN target.movie_person mp ON mp.movie_id=e.movie_id AND mp.person_id=e.person_id AND mp.role_type=e.role_type
   WHERE e.snapshot_id=:snapshot_id AND e.selection_status='selected' AND mp.movie_id IS NULL) AS selected_facts_without_target_relation;

SELECT
  (SELECT count(*) FROM target.movie) AS database_movies_total,
  (SELECT count(*) FROM target.person) AS database_people_total,
  (SELECT count(*) FROM integration.movie_external_identifier WHERE snapshot_id=:snapshot_id AND source_name='imdb') AS database_imdb_movie_ids,
  (SELECT count(*) FROM integration.person_external_identifier WHERE snapshot_id=:snapshot_id AND source_name='imdb') AS database_imdb_person_ids,
  (SELECT count(DISTINCT (movie_id,person_id,role_type)) FROM integration.movie_person_source_evidence WHERE snapshot_id=:snapshot_id) AS database_imdb_relationship_facts,
  (SELECT count(*) FROM target.movie_person) AS database_target_movie_person_rows;

SELECT issue_code,count(*)
FROM meta.quality_issue WHERE run_id=75
GROUP BY issue_code ORDER BY issue_code;
