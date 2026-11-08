\set ON_ERROR_STOP on
\pset pager off

SELECT proposed_classification, count(*)
FROM staging.product_metadata_record
GROUP BY proposed_classification
ORDER BY proposed_classification;

SELECT c.asin, c.candidate_key, c.candidate_title, c.candidate_year,
       c.decision_status, c.decision_reason
FROM integration.product_movie_candidate c
ORDER BY c.asin, c.candidate_key;

SELECT p.asin, m.stable_key, m.canonical_title, m.release_year,
       pm.relationship_type, pm.mapping_status
FROM target.product_movie pm
JOIN target.product_edition p USING (product_id)
JOIN target.movie m USING (movie_id)
ORDER BY p.asin, m.stable_key;

SELECT m.canonical_title, pe.canonical_name, mp.role_type, mp.billing_order,
       sr.source_uri, sr.content_sha256
FROM target.movie_person mp
JOIN target.movie m USING (movie_id)
JOIN target.person pe USING (person_id)
JOIN raw.source_record sr USING (source_record_id)
ORDER BY m.movie_id, mp.billing_order;

SELECT a.assertion_id, a.entity_type, a.entity_key, a.field_name,
       a.normalized_value, a.value_semantics, sr.source_uri
FROM integration.field_assertion a
LEFT JOIN raw.source_record sr USING (source_record_id)
WHERE a.entity_key = 'imdb:tt0078764'
ORDER BY a.assertion_id;

SELECT fs.entity_key, fs.field_name, fs.decision_status,
       fs.selected_assertion_id, fs.selected_assertion_ids, fs.decision_reason
FROM integration.field_selection fs
WHERE fs.entity_key = 'imdb:tt0078764'
ORDER BY fs.field_name;

SELECT queue_type, entity_key, candidate_key, reason_code, status
FROM integration.review_queue
ORDER BY review_queue_id;

SELECT asin, status, http_status, attempt_count, last_error_code
FROM staging.product_harvest
WHERE asin IN ('0001517791','0001526863','0005019281','0005054885','0005055121','0005059836')
ORDER BY asin;
