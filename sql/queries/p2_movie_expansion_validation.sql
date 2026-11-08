\set ON_ERROR_STOP on
\pset pager off

\if :{?scope_key}
\else
\set scope_key 'p2-movie-433-v1'
\endif

\echo 'P2 frozen baseline and current reconciliation (must total 433)'
SELECT scope_key,source_version,rule_version,scope_sha256,asin_count,
       baseline_adopted_asin_count,baseline_adopted_movie_count,status
FROM meta.p2_movie_scope WHERE scope_key=:'scope_key';

SELECT reconciliation_status,count(*) AS asin_count
FROM integration.p2_movie_reconciliation
WHERE scope_key=:'scope_key'
GROUP BY reconciliation_status ORDER BY reconciliation_status;

SELECT count(*) AS reconciliation_total,
       count(DISTINCT asin) AS distinct_asins
FROM integration.p2_movie_reconciliation WHERE scope_key=:'scope_key';

\echo 'Current adopted ASIN/work coverage inside fixed scope'
SELECT count(DISTINCT pe.asin) AS adopted_asins,
       count(DISTINCT pm.movie_id) AS adopted_movies
FROM target.product_movie pm
JOIN target.product_edition pe USING(product_id)
JOIN meta.p2_movie_scope_product sp ON sp.scope_key=:'scope_key' AND sp.asin=pe.asin
WHERE pm.relationship_type='single_work'
  AND pm.mapping_status IN ('verified','manual','predicted');

\echo 'Conflicts and orphans (all expected zero)'
SELECT
  (SELECT count(*) FROM (
     SELECT pe.asin FROM target.product_movie pm JOIN target.product_edition pe USING(product_id)
     JOIN meta.p2_movie_scope_product sp ON sp.scope_key=:'scope_key' AND sp.asin=pe.asin
     WHERE pm.relationship_type='single_work' AND pm.mapping_status IN ('verified','manual','predicted')
     GROUP BY pe.asin HAVING count(DISTINCT pm.movie_id)>1
   ) q) AS asin_multiple_current_single_work,
  (SELECT count(*) FROM integration.p2_movie_candidate_evaluation e
     LEFT JOIN raw.source_record p ON p.source_record_id=e.product_source_record_id
     LEFT JOIN raw.source_record w ON w.source_record_id=e.work_source_record_id
     WHERE e.scope_key=:'scope_key' AND (p.source_record_id IS NULL OR w.source_record_id IS NULL)) AS orphan_candidate_sources,
  (SELECT count(*) FROM integration.p2_movie_decision d
     LEFT JOIN meta.p2_movie_scope_product sp USING(scope_key,asin)
     WHERE d.scope_key=:'scope_key' AND sp.asin IS NULL) AS orphan_decisions,
  (SELECT count(*) FROM integration.p2_movie_decision d
     JOIN target.movie m ON m.stable_key='imdb:'||d.selected_tconst
     JOIN staging.imdb_title_basics b ON b.snapshot_id=1 AND b.tconst=d.selected_tconst
     WHERE d.scope_key=:'scope_key' AND d.decision_status='newly_adopted'
       AND (m.release_year IS DISTINCT FROM b.start_year
            OR lower(m.release_type) IS DISTINCT FROM lower(CASE b.title_type
                 WHEN 'tvMovie' THEN 'TV movie' WHEN 'tvSeries' THEN 'TV series'
                 WHEN 'tvMiniSeries' THEN 'TV mini-series' ELSE b.title_type END))) AS adopted_work_field_conflicts;

\echo 'New P2 mappings and relationship-source coverage'
WITH p2 AS (
  SELECT pm.product_id,pm.movie_id,pm.relationship_type,pe.asin,m.stable_key,md.evidence,
         concat_ws(':',pm.product_id,pm.movie_id,pm.relationship_type) relationship_key
  FROM target.product_movie pm JOIN target.product_edition pe USING(product_id)
  JOIN target.movie m USING(movie_id)
  JOIN integration.merge_decision md ON md.merge_decision_id=pm.decision_id
  WHERE md.rule_version='p2-movie-expansion-v1-stage4-evidence'
)
SELECT count(*) AS new_mapping_rows,
       count(DISTINCT asin) AS new_mapping_asins,
       count(DISTINCT stable_key) AS new_mapping_movies,
       count(*) FILTER (WHERE EXISTS (
         SELECT 1 FROM integration.relationship_source_link l
         WHERE l.relationship_type='product_movie' AND l.relationship_key=p2.relationship_key
           AND l.rule_version='p2-movie-expansion-v1-stage4-evidence'
       )) AS mappings_with_relationship_sources
FROM p2;

\echo 'Frozen evaluation baselines remain present'
SELECT entity_type,status,count(*)
FROM evaluation.dataset
WHERE dataset_key IN (
 'stage6-movie-eval-stage6-independent-candidates-v1-v2',
 'stage6-person-eval-stage6-independent-candidates-v1-v2'
)
GROUP BY entity_type,status ORDER BY entity_type;

\echo 'Collection review attribution remains product-level'
SELECT asin,count(*) AS member_count,max(product_source_review_count) AS product_source_reviews,
       sum(attributed_logical_review_count) AS logical_reviews_copied_to_members
FROM target.product_movie_review_attribution
WHERE relationship_type='collection_member'
GROUP BY asin ORDER BY asin;
