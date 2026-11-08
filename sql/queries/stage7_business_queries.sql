\set ON_ERROR_STOP on
\pset pager off

\if :{?movie_id}
\else
SELECT movie_id::text AS movie_id
FROM target.product_movie
WHERE mapping_status IN ('verified','predicted','manual')
GROUP BY movie_id
ORDER BY count(DISTINCT product_id) DESC, movie_id
LIMIT 1 \gset
\endif
\if :{?person_id}
\else
SELECT person_id::text AS person_id
FROM integration.movie_person_source_evidence
WHERE role_type = 'director' AND selection_status = 'selected'
GROUP BY person_id
ORDER BY count(DISTINCT movie_id) DESC, person_id
LIMIT 1 \gset
\endif
\if :{?asin}
\else
SELECT min(asin)::text AS asin FROM target.product_edition \gset
\endif

\echo 'B1 all product editions/versions for a work'
SELECT m.movie_id, m.canonical_title, m.original_title, m.release_year,
       pe.product_id, pe.asin, pe.media_format, pe.region_code, pe.cut_label,
       pe.publisher, pe.product_release_date, pe.classification,
       pm.relationship_type, pm.mapping_status,
       md.decision_origin, md.rule_version, md.evidence AS mapping_evidence
FROM target.movie m
JOIN target.product_movie pm USING (movie_id)
JOIN target.product_edition pe USING (product_id)
LEFT JOIN integration.merge_decision md ON md.merge_decision_id = pm.decision_id
WHERE m.movie_id = :movie_id
ORDER BY pe.product_release_date NULLS LAST, pe.asin;

\echo 'B1b collection members and explicit no-copy review attribution'
SELECT a.asin, a.movie_id, m.canonical_title, a.relationship_type,
       a.product_source_review_count, a.product_logical_review_count,
       a.attributed_logical_review_count,
       a.attribution_reason
FROM target.product_movie_review_attribution a
JOIN target.movie m USING (movie_id)
WHERE a.relationship_type = 'collection_member'
ORDER BY a.asin, m.release_year, m.canonical_title;

\echo 'B2 works for a director, with relationship and name sources'
SELECT p.person_id, p.canonical_name, pei.external_id AS imdb_nconst,
       m.movie_id, m.canonical_title, m.release_year,
       e.role_type, e.evidence_kind, e.selection_status, e.selection_reason,
       e.primary_name_raw, e.extraction_rule_version,
       rel.source_record_id AS relationship_source_record_id,
       rel.source_uri AS relationship_source_uri, rel.record_number AS relationship_record_number,
       rel.field_locator AS relationship_locator,
       nam.source_record_id AS name_source_record_id,
       nam.source_uri AS name_source_uri, nam.record_number AS name_record_number
FROM target.person p
JOIN integration.person_external_identifier pei USING (person_id)
JOIN integration.movie_person_source_evidence e USING (person_id)
JOIN target.movie m USING (movie_id)
JOIN raw.source_record rel ON rel.source_record_id = e.relationship_source_record_id
LEFT JOIN raw.source_record nam ON nam.source_record_id = e.name_source_record_id
WHERE p.person_id = :person_id AND e.role_type = 'director'
ORDER BY m.release_year NULLS LAST, m.canonical_title;

\echo 'B3 missing or inferred release information'
SELECT m.movie_id, m.stable_key, m.canonical_title, m.release_date, m.release_year,
       m.release_precision, m.release_region, m.release_type,
       cfs.decision_status, cfs.decision_reason,
       fa.quality_status, fa.value_semantics, fa.normalized_value,
       fa.source_record_id, fa.field_locator
FROM target.movie m
LEFT JOIN integration.current_field_selection cfs
  ON cfs.entity_type = 'movie' AND cfs.entity_key = m.stable_key
 AND cfs.field_name IN ('release_date', 'release_year')
LEFT JOIN integration.field_assertion fa
  ON fa.assertion_id = cfs.selected_assertion_id
WHERE m.release_year IS NULL OR m.release_date IS NULL
   OR fa.quality_status = 'inferred' OR cfs.decision_status IN ('missing', 'conflicted')
ORDER BY m.movie_id, cfs.field_name
LIMIT 200;

\echo 'B4 field conflicts and recorded reasons'
SELECT 'field_selection' AS conflict_source, fs.entity_type, fs.entity_key, fs.field_name,
       fs.selection_version AS rule_version, fs.decision_reason AS reason,
       jsonb_build_object('selected_assertion_id', fs.selected_assertion_id,
                          'selected_assertion_ids', fs.selected_assertion_ids) AS evidence,
       fs.decided_at
FROM integration.field_selection fs
WHERE fs.decision_status = 'conflicted'
UNION ALL
SELECT 'quality_issue', qi.entity_type, qi.entity_key,
       qi.issue_code, coalesce(qi.parser_version, r.rule_version), qi.issue_code,
       qi.details, qi.created_at
FROM meta.quality_issue qi
JOIN meta.etl_run r USING (run_id)
WHERE qi.issue_code ILIKE '%conflict%'
UNION ALL
SELECT 'merge_decision', md.entity_type, md.left_entity_key,
       md.right_entity_key, md.rule_version,
       md.decision || ':' || md.decision_origin, md.evidence, md.decided_at
FROM integration.merge_decision md
WHERE md.decision IN ('do_not_merge', 'manual_review')
ORDER BY decided_at DESC
LIMIT 300;

\echo 'B5a product review count and score (activated parser only)'
SELECT * FROM target.product_review_summary WHERE asin = :'asin';

\echo 'B5b work review count and score across adopted editions'
SELECT * FROM target.movie_review_summary WHERE movie_id = :movie_id;
