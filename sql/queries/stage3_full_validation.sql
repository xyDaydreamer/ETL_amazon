\set ON_ERROR_STOP on
\pset pager off

SELECT run_id, pipeline_name, batch_id, input_version, rule_version, status, stats
FROM meta.etl_run
WHERE pipeline_name = 'stage3_ucsd_metadata_full'
ORDER BY run_id DESC;

SELECT source_sha256, partition_key, committed_record_number, next_record_number,
       reached_eof, status, run_id, updated_at
FROM meta.product_metadata_progress
WHERE partition_key = 'ucsd-movies-tv-full-v1';

WITH full_rows AS (
    SELECT *
    FROM staging.product_metadata_record
    WHERE source_version = 'sha256:b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9'
), per_asin AS (
    SELECT asin, count(*) AS records,
           count(DISTINCT title_normalized) AS titles,
           count(DISTINCT proposed_classification) AS classifications
    FROM full_rows
    GROUP BY asin
)
SELECT (SELECT count(*) FROM full_rows) AS metadata_rows,
       count(*) AS distinct_asins,
       count(*) FILTER (WHERE records > 1) AS duplicate_asins,
       max(records) AS max_records_per_asin,
       count(*) FILTER (WHERE titles > 1) AS title_conflict_asins,
       count(*) FILTER (WHERE classifications > 1) AS classification_conflict_asins
FROM per_asin;

SELECT proposed_classification, count(DISTINCT asin) AS distinct_asins
FROM staging.product_metadata_record
WHERE source_version = 'sha256:b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9'
GROUP BY proposed_classification
ORDER BY proposed_classification;

SELECT classification, count(*)
FROM target.product_edition
GROUP BY classification
ORDER BY classification;

SELECT queue_type, status, count(*)
FROM integration.review_queue
GROUP BY queue_type, status
ORDER BY queue_type, status;

SELECT count(*) AS orphan_metadata
FROM staging.product_metadata_record m
LEFT JOIN raw.source_record r USING (source_record_id)
WHERE m.source_version = 'sha256:b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9'
  AND r.source_record_id IS NULL;

SELECT count(*) AS metadata_without_product
FROM staging.product_metadata_record m
LEFT JOIN target.product_edition p USING (asin)
WHERE m.source_version = 'sha256:b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9'
  AND p.product_id IS NULL;
