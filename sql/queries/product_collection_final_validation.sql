\set ON_ERROR_STOP on
\pset pager off

SELECT status, count(*) AS products
FROM staging.product_harvest
GROUP BY status ORDER BY status;

SELECT count(*) AS checklist_products,
       count(*) FILTER (WHERE status = 'not_attempted') AS not_attempted,
       count(*) FILTER (WHERE status = 'success') AS valid_amazon_pages,
       count(*) FILTER (WHERE status = 'not_found') AS not_found,
       count(*) FILTER (WHERE status IN ('access_restricted','robots_denied')) AS access_limited,
       count(*) FILTER (WHERE status = 'timeout') AS timeout,
       count(*) FILTER (WHERE status = 'parse_failed') AS parse_failed,
       count(*) FILTER (WHERE status = 'network_failed') AS network_failed
FROM staging.product_harvest;

SELECT count(*) AS attempt_rows,
       count(DISTINCT asin) AS attempted_asins,
       count(*) FILTER (WHERE cache_hit = false AND requested_url ~ '^https?://') AS real_http_requests,
       count(*) FILTER (WHERE cache_hit = true) AS known_cache_hits,
       count(*) FILTER (WHERE requested_url !~ '^https?://') AS local_simulation_attempts,
       count(*) FILTER (WHERE cache_hit IS NULL) AS historical_cache_state_unknown,
       count(*) FILTER (WHERE request_ordinal > 1) AS retry_requests,
       count(*) FILTER (WHERE cache_hit = false AND requested_url ~ '^https?://'
                         AND outcome = 'success') AS real_successful_pages,
       count(*) FILTER (WHERE cache_hit = false AND requested_url ~ '^https?://'
                         AND outcome IN ('access_restricted','robots_denied')) AS real_access_limited,
       count(*) FILTER (WHERE source_record_id IS NULL) AS attempts_without_response_body
FROM staging.product_harvest_attempt;

SELECT count(*) FILTER (WHERE a.source_record_id IS NOT NULL) AS response_attempts,
       count(*) FILTER (WHERE a.source_record_id IS NOT NULL AND r.snapshot_path IS NOT NULL
                         AND r.content_sha256 IS NOT NULL) AS complete_snapshot_evidence,
       count(*) FILTER (WHERE a.source_record_id IS NOT NULL AND
                         (r.snapshot_path IS NULL OR r.content_sha256 IS NULL)) AS incomplete_snapshot_evidence
FROM staging.product_harvest_attempt a
LEFT JOIN raw.source_record r USING (source_record_id);

SELECT count(*) AS successful_products,
       count(*) FILTER (WHERE h.valid_source_record_id IS NULL) AS success_without_valid_snapshot,
       count(*) FILTER (WHERE r.access_status <> 'success' OR r.http_status <> 200) AS invalid_success_source
FROM staging.product_harvest h
LEFT JOIN raw.source_record r ON r.source_record_id = h.valid_source_record_id
WHERE h.status = 'success';

SELECT count(*) AS orphan_attempts
FROM staging.product_harvest_attempt a
LEFT JOIN staging.product_harvest h USING (asin)
WHERE h.asin IS NULL;

SELECT batch_id,status,prepared_count,claimed_count,completed_count,
       max_attempts_per_product,request_delay_seconds,concurrency,
       selection_method,random_seed,candidate_limit,access_basis,updated_at
FROM meta.product_harvest_batch ORDER BY created_at;
