\set ON_ERROR_STOP on
\pset pager off

\echo 'Activated review parser balance'
SELECT parser_version, parse_status, count(*) AS records
FROM staging.current_review_parse_result
GROUP BY parser_version, parse_status ORDER BY parse_status;

\echo 'Source and product coverage'
SELECT (SELECT count(*) FROM staging.current_review_parse_result) AS current_reviews,
       (SELECT count(*) FROM target.product_edition) AS products,
       (SELECT count(*) FROM staging.product_metadata_record) AS offline_metadata_rows,
       (SELECT count(DISTINCT asin) FROM staging.product_metadata_record) AS offline_metadata_products,
       (SELECT count(*) FROM staging.product_harvest_attempt) AS page_attempts,
       (SELECT count(*) FROM staging.product_harvest_attempt WHERE outcome='success') AS page_successes;

\echo 'Collection relationship and review-attribution policy'
SELECT relationship_type,count(*) AS relationship_count
FROM target.product_movie GROUP BY relationship_type ORDER BY relationship_type;
SELECT asin,count(*) AS member_count,max(product_source_review_count) AS product_reviews,
       sum(attributed_logical_review_count) AS logical_reviews_copied_to_members
FROM target.product_movie_review_attribution
WHERE relationship_type='collection_member'
GROUP BY asin ORDER BY asin;

\echo 'Integrated entities and relationships'
SELECT (SELECT count(*) FROM target.movie) AS movies_total,
       (SELECT count(*) FROM target.person) AS persons_total,
       (SELECT count(*) FROM target.product_movie WHERE mapping_status IN ('verified','predicted','manual')) AS adopted_product_movie,
       (SELECT count(*) FROM target.movie_person) AS movie_person_rows,
       (SELECT count(*) FROM integration.movie_person_source_evidence) AS relationship_evidence_rows,
       (SELECT count(*) FROM integration.field_assertion) AS field_assertions,
       (SELECT count(*) FROM integration.field_selection) AS field_selections;

\echo 'Orphan and missing-lineage checks (all expected zero)'
SELECT
  (SELECT count(*) FROM integration.field_assertion fa LEFT JOIN raw.source_record sr USING(source_record_id)
   WHERE fa.source_record_id IS NOT NULL AND sr.source_record_id IS NULL) AS orphan_assertion_sources,
  (SELECT count(*) FROM integration.movie_person_source_evidence e LEFT JOIN raw.source_record sr
     ON sr.source_record_id=e.relationship_source_record_id WHERE sr.source_record_id IS NULL) AS orphan_relationship_sources,
  (SELECT count(*) FROM target.movie_person mp LEFT JOIN target.movie m USING(movie_id) WHERE m.movie_id IS NULL) AS orphan_movie_person_movies,
  (SELECT count(*) FROM target.movie_person mp LEFT JOIN target.person p USING(person_id) WHERE p.person_id IS NULL) AS orphan_movie_person_people,
  (SELECT count(*) FROM integration.movie_external_identifier mei LEFT JOIN raw.source_record sr USING(source_record_id)
   WHERE sr.source_record_id IS NULL) AS orphan_movie_external_sources,
  (SELECT count(*) FROM integration.person_external_identifier pei LEFT JOIN raw.source_record sr USING(source_record_id)
   WHERE sr.source_record_id IS NULL) AS orphan_person_external_sources;

\echo 'Quality issue summary'
SELECT coalesce(parser_version, 'not_versioned') AS parser_version,
       issue_code, severity, count(*) AS issue_count,
       count(DISTINCT source_record_id) FILTER (WHERE source_record_id IS NOT NULL) AS affected_sources
FROM meta.quality_issue
GROUP BY coalesce(parser_version, 'not_versioned'), issue_code, severity
ORDER BY parser_version, severity DESC, issue_count DESC, issue_code;

\echo 'Collection and matching decisions'
SELECT adoption_status, count(*) FROM integration.unified_development_movie_mapping_decision
GROUP BY adoption_status ORDER BY adoption_status;

\echo 'Evaluation state: do not expose person answers/predictions here'
SELECT d.dataset_key, d.entity_type, d.status, count(ep.review_item_id) AS pair_count,
       count(DISTINCT hl.review_item_id) AS labeled_items
FROM evaluation.dataset d
LEFT JOIN evaluation.entity_pair ep USING (evaluation_dataset_id)
LEFT JOIN evaluation.human_label hl USING (review_item_id)
WHERE d.dataset_key IN (
  'stage6-movie-eval-stage6-independent-candidates-v1-v2',
  'stage6-person-eval-stage6-independent-candidates-v1-v2'
)
GROUP BY d.evaluation_dataset_id
ORDER BY d.entity_type;

\echo 'IMDb snapshot and selective scope completion'
SELECT es.snapshot_version, es.verification_status, s.scope_key, s.status,
       s.target_title_count,
       count(*) FILTER (WHERE p.status='verified_complete') AS verified_files,
       count(*) AS progress_files
FROM meta.external_snapshot es
JOIN meta.imdb_selective_scope s USING(snapshot_id)
LEFT JOIN meta.imdb_selective_progress p
  ON p.snapshot_id=s.snapshot_id AND p.scope_key=s.scope_key
GROUP BY es.snapshot_version,es.verification_status,s.scope_key,s.status,s.target_title_count
ORDER BY s.scope_key;
