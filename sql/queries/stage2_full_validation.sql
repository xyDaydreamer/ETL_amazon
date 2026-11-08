\set ON_ERROR_STOP on
\pset pager off

-- Run against amazon_movies after the full DAG succeeds.
WITH current_rows AS (
    SELECT * FROM staging.current_review_parse_result
)
SELECT
    count(*) AS reviews_read,
    count(*) FILTER (WHERE parse_status='valid') AS valid_reviews,
    count(*) FILTER (WHERE parse_status='quarantined') AS quarantined_reviews,
    count(*) = count(*) FILTER (WHERE parse_status='valid')
             + count(*) FILTER (WHERE parse_status='quarantined') AS classification_balanced
FROM current_rows;

SELECT asin_status, count(*) AS review_observations, count(DISTINCT asin) AS distinct_asins
FROM staging.product_asin_observation
WHERE parser_version='snap-movies-review-v3'
GROUP BY asin_status ORDER BY asin_status;

SELECT status, count(*) AS products
FROM staging.product_harvest
GROUP BY status ORDER BY status;

SELECT
    (SELECT count(*) FROM target.product_edition) AS product_count,
    (SELECT count(*) FROM staging.product_harvest) AS harvest_count,
    (SELECT count(DISTINCT asin) FROM staging.product_asin_observation
      WHERE parser_version='snap-movies-review-v3' AND asin_status='usable') AS reliable_distinct_asins;

SELECT
    count(DISTINCT reviewer_id) FILTER (WHERE reviewer_id IS NOT NULL) AS all_reliable_users,
    count(DISTINCT reviewer_id) FILTER (
        WHERE reviewer_id IS NOT NULL AND parse_status='valid'
    ) AS valid_review_users,
    count(*) FILTER (WHERE reviewer_id IS NULL) AS reviews_without_reliable_user,
    count(*) FILTER (
        WHERE parse_issues @> '[{"code":"conflicting_reviewer_id_fields"}]'::jsonb
    ) AS reviewer_id_conflicts
FROM staging.current_review_parse_result;

SELECT issue_code,severity,count(*) AS issue_count
FROM meta.quality_issue
WHERE parser_version='snap-movies-review-v3'
GROUP BY issue_code,severity ORDER BY issue_count DESC,issue_code;

WITH current_rows AS (
    SELECT * FROM staging.current_review_parse_result
), issue_codes AS (
    SELECT c.source_review_id, issue->>'code' AS issue_code
    FROM current_rows c
    CROSS JOIN LATERAL jsonb_array_elements(c.parse_issues) issue
)
SELECT issue_code, count(*) AS issue_instances,
       count(DISTINCT source_review_id) AS affected_reviews
FROM issue_codes GROUP BY issue_code
ORDER BY affected_reviews DESC, issue_code;

WITH current_rows AS (
    SELECT * FROM staging.current_review_parse_result
)
SELECT
    count(*) FILTER (WHERE parse_status='quarantined' AND asin_status='usable')
        AS quarantined_with_usable_asin,
    count(DISTINCT asin) FILTER (WHERE parse_status='quarantined' AND asin_status='usable')
        AS quarantined_distinct_asins,
    count(*) FILTER (WHERE parse_status='quarantined' AND asin_status='missing')
        AS quarantined_missing_asin
FROM current_rows;

-- Physical malformed blocks explaining the difference from the official
-- product-linked review count. This emits only locators and hashes, not text.
SELECT p.record_number,l.start_line,l.end_line,
       l.uncompressed_start,l.uncompressed_end,r.content_sha256,
       p.parse_status,p.asin_status
FROM staging.current_review_parse_result p
JOIN staging.review_parse_locator l USING(source_review_id,parser_version)
JOIN raw.source_record r USING(source_record_id)
WHERE p.asin_status='missing'
ORDER BY p.record_number;

SELECT p.partition_key,p.committed_record_number,p.actual_read_end_record,
       p.scope_status,p.reached_eof,p.source_integrity_status,p.last_free_bytes,
       f.size_bytes,f.sha256,f.gzip_integrity_status
FROM meta.batch_progress p
JOIN meta.source_file f ON f.sha256=p.input_version
WHERE p.parser_version='snap-movies-review-v3' AND p.partition_key='full-source-v3';

SELECT parser_version,is_current,coverage_kind,coverage_start_record,
       coverage_end_record,verified_source_eof,activated_at
FROM meta.parser_release ORDER BY parser_version;

SELECT
    (SELECT count(*) FROM raw.source_record r
      JOIN meta.source_file f USING(source_file_id)
      WHERE r.source_kind='review_file'
        AND f.sha256='4b6ee09ea4880f3fb197b1407d45dfb08a892fbb48a9e7a2d74aa0572cdcc715') AS raw_records,
    (SELECT count(*) FROM staging.review_record) AS stable_identities,
    (SELECT count(*) FROM staging.review_parse_result WHERE parser_version='snap-movies-review-v3') AS v3_results,
    (SELECT count(*) FROM staging.review_parse_locator WHERE parser_version='snap-movies-review-v3') AS v3_locators,
    (SELECT count(*) FROM staging.product_asin_observation WHERE parser_version='snap-movies-review-v3') AS v3_asin_observations;
