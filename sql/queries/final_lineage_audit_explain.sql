\set ON_ERROR_STOP on
\pset pager off

\echo 'Plan: ProductEdition ASIN source-chain probes (full scope)'
EXPLAIN (COSTS, VERBOSE, SETTINGS)
SELECT count(*) FROM target.product_edition pe
WHERE EXISTS (
  SELECT 1 FROM staging.product_asin_observation o
  JOIN staging.review_record rr USING(source_review_id)
  JOIN raw.source_record sr USING(source_record_id)
  JOIN staging.review_parse_locator l USING(source_review_id,parser_version)
  WHERE o.asin=pe.asin AND o.parser_version='snap-movies-review-v3'
    AND o.asin_status='usable' AND sr.content_sha256 IS NOT NULL
    AND l.content_sha256=rr.content_sha256 LIMIT 1
);

\echo 'Plan: active LogicalReview source chain, one shared scan'
EXPLAIN (COSTS, VERBOSE, SETTINGS)
SELECT count(*),count(*) FILTER(WHERE sr.content_sha256=rr.content_sha256
  AND l.content_sha256=rr.content_sha256)
FROM target.current_logical_review lr
LEFT JOIN staging.review_record rr ON rr.source_review_id=lr.representative_source_review_id
LEFT JOIN raw.source_record sr USING(source_record_id)
LEFT JOIN staging.review_parse_locator l
  ON l.source_review_id=lr.representative_source_review_id AND l.parser_version=lr.parser_version;

\echo 'Plan: active ReviewMapping source chain, one shared scan'
EXPLAIN (COSTS, VERBOSE, SETTINGS)
SELECT count(*),count(*) FILTER(WHERE sr.content_sha256=rr.content_sha256
  AND l.content_sha256=rr.content_sha256 AND rm.evidence<>'{}'::jsonb)
FROM target.current_review_mapping rm
JOIN staging.review_record rr USING(source_review_id)
JOIN raw.source_record sr USING(source_record_id)
JOIN staging.review_parse_locator l
  ON l.source_review_id=rm.source_review_id AND l.parser_version='snap-movies-review-v3';
