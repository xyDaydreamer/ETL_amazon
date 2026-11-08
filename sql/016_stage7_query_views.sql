\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE OR REPLACE VIEW integration.current_field_selection AS
SELECT DISTINCT ON (entity_type, entity_key, field_name)
       field_selection_id, entity_type, entity_key, field_name,
       selection_version, selected_assertion_id, selected_assertion_ids,
       decision_status, decision_reason, run_id, decided_at
FROM integration.field_selection
ORDER BY entity_type, entity_key, field_name, decided_at DESC, field_selection_id DESC;

CREATE OR REPLACE VIEW integration.source_record_impact AS
SELECT fa.source_record_id,
       fa.entity_type,
       fa.entity_key,
       fa.field_name,
       'field_assertion'::text AS impact_kind,
       fa.assertion_id::text AS impact_key,
       fa.extraction_rule_version AS rule_version
FROM integration.field_assertion fa
WHERE fa.source_record_id IS NOT NULL
UNION ALL
SELECT mei.source_record_id,
       'movie'::text,
       m.stable_key,
       'external_identifier'::text,
       'movie_external_identifier'::text,
       mei.external_id,
       es.snapshot_version
FROM integration.movie_external_identifier mei
JOIN target.movie m USING (movie_id)
JOIN meta.external_snapshot es USING (snapshot_id)
UNION ALL
SELECT pei.source_record_id,
       'person'::text,
       p.stable_key,
       'external_identifier'::text,
       'person_external_identifier'::text,
       pei.external_id,
       es.snapshot_version
FROM integration.person_external_identifier pei
JOIN target.person p USING (person_id)
JOIN meta.external_snapshot es USING (snapshot_id)
UNION ALL
SELECT pa.source_record_id,
       'person'::text,
       p.stable_key,
       'alias'::text,
       'person_alias'::text,
       pa.person_alias_id::text,
       NULL::text
FROM target.person_alias pa
JOIN target.person p USING (person_id)
UNION ALL
SELECT e.relationship_source_record_id,
       'movie_person'::text,
       m.stable_key || '|' || p.stable_key || '|' || e.role_type,
       'relationship'::text,
       e.evidence_kind,
       e.relationship_source_record_id::text,
       e.extraction_rule_version
FROM integration.movie_person_source_evidence e
JOIN target.movie m USING (movie_id)
JOIN target.person p USING (person_id)
UNION ALL
SELECT e.name_source_record_id,
       'person'::text,
       p.stable_key,
       'canonical_name'::text,
       'relationship_name_source'::text,
       e.name_source_record_id::text,
       e.extraction_rule_version
FROM integration.movie_person_source_evidence e
JOIN target.person p USING (person_id)
WHERE e.name_source_record_id IS NOT NULL;

CREATE OR REPLACE VIEW target.product_review_summary AS
SELECT pe.product_id,
       pe.asin,
       count(*) AS source_review_count,
       count(r.score) AS scored_review_count,
       avg(r.score) AS average_score,
       count(*) FILTER (WHERE r.parse_status = 'valid') AS valid_review_count,
       count(*) FILTER (WHERE r.parse_status = 'quarantined') AS quarantined_review_count
FROM target.product_edition pe
JOIN staging.current_review_parse_result r ON r.asin = pe.asin
GROUP BY pe.product_id, pe.asin;

CREATE OR REPLACE VIEW target.movie_review_summary AS
WITH mapped_editions AS (
    SELECT DISTINCT movie_id, product_id
    FROM target.product_movie
    WHERE mapping_status IN ('verified', 'predicted', 'manual')
)
SELECT m.movie_id,
       m.stable_key,
       m.canonical_title,
       count(DISTINCT pe.product_id) AS edition_count,
       coalesce(sum(prs.source_review_count), 0)::bigint AS source_review_count,
       coalesce(sum(prs.scored_review_count), 0)::bigint AS scored_review_count,
       CASE WHEN coalesce(sum(prs.scored_review_count), 0) = 0 THEN NULL
            ELSE sum(prs.average_score * prs.scored_review_count)
                 / sum(prs.scored_review_count)
       END AS average_score
FROM target.movie m
JOIN mapped_editions me ON me.movie_id = m.movie_id
JOIN target.product_edition pe ON pe.product_id = me.product_id
LEFT JOIN target.product_review_summary prs ON prs.product_id = pe.product_id
GROUP BY m.movie_id, m.stable_key, m.canonical_title;

COMMENT ON VIEW integration.current_field_selection IS
    'Latest selection event per entity field. Historical selections remain in integration.field_selection.';
COMMENT ON VIEW integration.source_record_impact IS
    'Reverse lineage from one raw source_record to affected fields, identifiers, aliases and movie-person facts.';
COMMENT ON VIEW target.product_review_summary IS
    'Review statistics from the explicitly activated parser version only; parser histories are not double counted.';
COMMENT ON VIEW target.movie_review_summary IS
    'Review statistics rolled up over adopted product editions; one logical review is counted once per mapped edition.';

RESET ROLE;
