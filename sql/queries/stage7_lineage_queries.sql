\set ON_ERROR_STOP on
\pset pager off

-- Optional psql variables:
--   -v movie_id=... -v field_name=canonical_title
--   -v asin=... -v source_review_id=...
--   -v source_record_id=... -v person_id=...
-- Defaults choose the smallest fixed-snapshot entity with evidence.

\if :{?movie_id}
\else
SELECT min(m.movie_id)::text AS movie_id
FROM meta.imdb_selective_scope_product sp
JOIN target.movie m ON m.stable_key = 'imdb:' || sp.tconst
WHERE sp.asin IS NOT NULL \gset
\endif
\if :{?field_name}
\else
\set field_name canonical_title
\endif
\if :{?asin}
\else
SELECT min(sp.asin)::text AS asin
FROM meta.imdb_selective_scope_product sp
JOIN target.movie m ON m.stable_key = 'imdb:' || sp.tconst \gset
\endif
\if :{?source_review_id}
\else
SELECT min(source_review_id)::text AS source_review_id
FROM staging.current_review_parse_result
WHERE asin = :'asin' \gset
\endif
\if :{?source_record_id}
\else
SELECT min(source_record_id)::text AS source_record_id
FROM integration.field_assertion
WHERE source_record_id IS NOT NULL \gset
\endif
\if :{?person_id}
\else
SELECT min(person_id)::text AS person_id
FROM integration.person_external_identifier
WHERE status = 'adopted' \gset
\endif

\echo 'L1 movie field: final value, accepted/rejected candidates, raw evidence, chain, reason'
WITH chosen_movie AS (
    SELECT m.*,
           CASE :'field_name'
             WHEN 'canonical_title' THEN to_jsonb(m.canonical_title)
             WHEN 'original_title' THEN to_jsonb(m.original_title)
             WHEN 'release_year' THEN to_jsonb(m.release_year)
             WHEN 'release_date' THEN to_jsonb(m.release_date)
             WHEN 'release_type' THEN to_jsonb(m.release_type)
             ELSE NULL::jsonb
           END AS final_value
    FROM target.movie m WHERE m.movie_id = :movie_id
), selected AS (
    SELECT cfs.*
    FROM integration.current_field_selection cfs
    JOIN chosen_movie m ON cfs.entity_type = 'movie' AND cfs.entity_key = m.stable_key
    WHERE cfs.field_name = :'field_name'
), assertions AS (
    SELECT fa.assertion_id, fa.raw_value, fa.normalized_value, fa.value_semantics,
           fa.quality_status, fa.extraction_rule_version, fa.field_locator,
           sr.source_record_id, sr.source_uri, sr.record_number,
           sr.field_locator AS record_locator, sr.content_sha256
    FROM integration.field_assertion fa
    JOIN chosen_movie m ON fa.entity_type = 'movie' AND fa.entity_key = m.stable_key
    LEFT JOIN raw.source_record sr USING (source_record_id)
    WHERE fa.field_name = :'field_name'
)
SELECT m.movie_id, m.stable_key, :'field_name' AS field_name, m.final_value,
       s.decision_status, s.selection_version, s.decision_reason,
       a.assertion_id, a.raw_value, a.normalized_value, a.value_semantics,
       a.quality_status, a.extraction_rule_version,
       (s.selected_assertion_id = a.assertion_id
        OR a.assertion_id = ANY(coalesce(s.selected_assertion_ids, ARRAY[]::bigint[]))) AS adopted,
       a.source_record_id, a.source_uri, a.record_number, a.field_locator,
       a.record_locator, a.content_sha256
FROM chosen_movie m
LEFT JOIN selected s ON true
LEFT JOIN assertions a ON true
ORDER BY adopted DESC NULLS LAST, a.assertion_id;

WITH chosen_movie AS (
    SELECT movie_id, stable_key, replace(stable_key, 'imdb:', '') AS tconst
    FROM target.movie WHERE movie_id = :movie_id
), product_scope AS (
    SELECT sp.asin, sp.tconst, sp.adoption_origin, sp.product_title_raw
    FROM meta.imdb_selective_scope_product sp
    JOIN chosen_movie m USING (tconst)
), rejected AS (
    SELECT h.asin, h.imdb_candidate_id AS candidate_tconst,
           h.human_label AS candidate_status, h.reason AS selection_reason,
           h.evidence, h.labeled_at
    FROM integration.current_movie_match_human_decision_any_version h
    JOIN product_scope p USING (asin)
    WHERE h.label_scope = 'candidate'
      AND h.human_label = 'incorrect_match'
), adopted AS (
    SELECT p.asin, p.tconst AS candidate_tconst,
           'adopted'::text AS candidate_status,
           coalesce(p.adoption_origin, 'fixed_scope_mapping') AS selection_reason,
           NULL::jsonb AS evidence, NULL::timestamptz AS labeled_at
    FROM product_scope p
)
SELECT * FROM adopted
UNION ALL
SELECT * FROM rejected
ORDER BY asin, candidate_status, candidate_tconst;

-- Complete versioned algorithm candidate payloads are shown separately so a
-- rejected/low-ranked candidate is not lost merely because no human label exists.
WITH chosen_movie AS (
    SELECT replace(stable_key, 'imdb:', '') AS tconst
    FROM target.movie WHERE movie_id = :movie_id
), product_scope AS (
    SELECT sp.asin
    FROM meta.imdb_selective_scope_product sp
    JOIN chosen_movie m USING (tconst)
)
SELECT r.asin, r.rule_version, r.match_status AS product_match_status,
       r.imdb_candidate_id AS selected_algorithm_candidate,
       c.ordinality AS candidate_position,
       coalesce(c.item->>'imdb_candidate_id', c.item->>'id', c.item->>'tconst') AS candidate_tconst,
       c.item AS candidate_evidence,
       r.conflict_points AS product_conflicts,
       r.review_question, r.review_reason_code
FROM integration.movie_match_candidate_result r
JOIN product_scope p USING (asin)
LEFT JOIN LATERAL jsonb_array_elements(r.candidate_pool) WITH ORDINALITY AS c(item, ordinality) ON true
ORDER BY r.rule_version, r.asin, c.ordinality;

SELECT ad.output_assertion_id, ad.input_assertion_id, ad.operation_type, ad.rule_id,
       out_a.entity_key, out_a.field_name AS output_field,
       in_a.entity_type AS input_entity_type, in_a.entity_key AS input_entity_key,
       in_a.field_name AS input_field, in_a.source_record_id AS input_source_record_id
FROM integration.assertion_dependency ad
JOIN integration.field_assertion out_a ON out_a.assertion_id = ad.output_assertion_id
JOIN integration.field_assertion in_a ON in_a.assertion_id = ad.input_assertion_id
JOIN target.movie m ON out_a.entity_type = 'movie' AND out_a.entity_key = m.stable_key
WHERE m.movie_id = :movie_id AND out_a.field_name = :'field_name'
ORDER BY ad.output_assertion_id, ad.input_assertion_id;

\echo 'L2 ASIN/source_review_id: parser version, work and mapping state'
SELECT r.source_review_id, r.parser_version, r.parse_status, r.asin, r.score,
       rr.record_number, rr.content_sha256,
       pe.product_id, m.movie_id, m.stable_key, m.canonical_title,
       pm.relationship_type, pm.mapping_status,
       md.decision, md.decision_origin, md.rule_version, md.evidence
FROM staging.review_parse_result r
JOIN staging.review_record rr USING (source_review_id)
LEFT JOIN target.product_edition pe ON pe.asin = r.asin
LEFT JOIN target.product_movie pm ON pm.product_id = pe.product_id
LEFT JOIN target.movie m ON m.movie_id = pm.movie_id
LEFT JOIN integration.merge_decision md ON md.merge_decision_id = pm.decision_id
WHERE r.asin = :'asin' OR r.source_review_id = :'source_review_id'
ORDER BY r.source_review_id, r.parser_version, m.movie_id
LIMIT 200;

\echo 'L3 raw source record: affected entities and fields'
SELECT sr.source_record_id, sr.source_kind, sr.source_uri, sr.record_number,
       sr.field_locator, sr.content_sha256, sr.http_status, sr.access_status,
       i.entity_type, i.entity_key, i.field_name, i.impact_kind,
       i.impact_key, i.rule_version
FROM raw.source_record sr
LEFT JOIN integration.final_source_record_impact i USING (source_record_id)
WHERE sr.source_record_id = :source_record_id
ORDER BY i.entity_type, i.entity_key, i.field_name, i.impact_kind;

\echo 'L4 person: source names, alias/merge history, external ID and works'
SELECT p.person_id, p.stable_key, p.canonical_name, p.status,
       pei.external_id AS imdb_nconst, pei.status AS identifier_status,
       pa.alias_raw, pa.alias_normalized,
       nsr.source_record_id AS name_source_record_id,
       nsr.source_uri AS name_source_uri, nsr.record_number AS name_record_number,
       nsr.field_locator AS name_locator, nsr.content_sha256 AS name_content_sha256
FROM target.person p
LEFT JOIN integration.person_external_identifier pei USING (person_id)
LEFT JOIN target.person_alias pa USING (person_id)
LEFT JOIN raw.source_record nsr ON nsr.source_record_id = coalesce(pa.source_record_id, pei.source_record_id)
WHERE p.person_id = :person_id
ORDER BY pa.person_alias_id, pei.external_id;

SELECT md.left_entity_key, md.right_entity_key, md.decision, md.decision_origin,
       md.rule_version, md.confidence, md.evidence, md.decided_at
FROM integration.merge_decision md
JOIN target.person p ON p.person_id = :person_id
WHERE md.entity_type = 'person'
  AND (md.left_entity_key = p.stable_key OR md.right_entity_key = p.stable_key)
ORDER BY md.decided_at, md.merge_decision_id;

SELECT p.person_id, m.movie_id, m.stable_key AS movie_key, m.canonical_title,
       mp.role_type, mp.credited_as, mp.billing_order,
       e.evidence_kind, e.imdb_category, e.job_raw, e.characters_raw,
       e.selection_status, e.selection_reason, e.extraction_rule_version,
       rr.source_record_id AS relationship_source_record_id,
       rr.source_uri AS relationship_source_uri, rr.record_number AS relationship_record_number,
       rr.field_locator AS relationship_locator, rr.content_sha256
FROM target.person p
JOIN target.current_movie_person mp USING (person_id)
JOIN target.movie m USING (movie_id)
LEFT JOIN integration.movie_person_source_evidence e
  ON e.movie_id = mp.movie_id AND e.person_id = mp.person_id
 AND e.role_type = CASE WHEN mp.role_type = 'starring' THEN 'cast' ELSE mp.role_type END
 AND e.relationship_source_record_id = mp.source_record_id
LEFT JOIN raw.source_record rr ON rr.source_record_id = mp.source_record_id
WHERE p.person_id = :person_id
ORDER BY m.canonical_title, mp.role_type, mp.billing_order NULLS LAST;
