-- Complete product harvest list. Use psql \copy around this SELECT when a TSV
-- artifact is required; no network request is performed here.
SELECT pe.asin,h.original_url,h.final_url,h.status,h.http_status,
       h.attempt_count,h.fetched_at,h.last_error_code
FROM target.product_edition pe
JOIN staging.product_harvest h USING(asin)
ORDER BY pe.asin;
