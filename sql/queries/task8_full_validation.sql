\set ON_ERROR_STOP on
\pset pager off
\if :{?rule_version}
\else
\set rule_version 'logical-review-exact-v1'
\endif

\echo 'Task 8 release and checkpoint'
SELECT * FROM meta.logical_review_release WHERE rule_version=:'rule_version';
SELECT * FROM meta.logical_review_progress WHERE rule_version=:'rule_version' ORDER BY scope_key;

\echo 'Input, exclusion, logical and mapping balance'
WITH input AS (
    SELECT source_review_id FROM staging.current_review_parse_result WHERE parse_status='valid'
), excluded AS (
    SELECT parse_status,count(*) records FROM staging.current_review_parse_result
    WHERE parse_status<>'valid' GROUP BY parse_status
), mapped AS (
    SELECT source_review_id,logical_review_id FROM target.review_mapping WHERE rule_version=:'rule_version'
), logical AS (
    SELECT logical_review_id FROM target.logical_review WHERE dedupe_rule_version=:'rule_version'
)
SELECT (SELECT count(*) FROM input) AS input_valid_reviews,
       (SELECT coalesce(sum(records),0) FROM excluded) AS excluded_parse_records,
       (SELECT count(*) FROM logical) AS logical_reviews,
       (SELECT count(*) FROM mapped) AS mappings,
       (SELECT count(*) FROM input)-(SELECT count(*) FROM logical) AS duplicate_records,
       (SELECT count(*) FROM mapped)-(SELECT count(*) FROM logical) AS duplicate_records_from_mapping;

\echo 'Duplicate groups and helpfulness conflicts'
WITH groups AS (
    SELECT l.logical_review_id,count(rm.source_review_id) source_count,l.helpfulness_resolution
    FROM target.logical_review l JOIN target.review_mapping rm USING(logical_review_id)
    WHERE l.dedupe_rule_version=:'rule_version' AND rm.rule_version=:'rule_version'
    GROUP BY l.logical_review_id,l.helpfulness_resolution
)
SELECT count(*) FILTER (WHERE source_count>1) AS duplicate_groups,
       coalesce(sum(source_count-1) FILTER (WHERE source_count>1),0) AS duplicate_records,
       count(*) FILTER (WHERE helpfulness_resolution='conflict_preserved_at_source') AS helpfulness_conflict_groups,
       max(source_count) AS largest_group
FROM groups;

\echo 'Missing, duplicate and orphan mapping checks; all expected zero'
WITH input AS (
    SELECT source_review_id FROM staging.current_review_parse_result WHERE parse_status='valid'
), mapped AS (
    SELECT source_review_id,logical_review_id FROM target.review_mapping WHERE rule_version=:'rule_version'
)
SELECT (SELECT count(*) FROM input i LEFT JOIN mapped m USING(source_review_id)
        WHERE m.source_review_id IS NULL) AS missing_mappings,
       (SELECT count(*) FROM (SELECT source_review_id FROM mapped GROUP BY source_review_id HAVING count(*)>1) d)
           AS multiply_mapped_sources,
       (SELECT count(*) FROM mapped m LEFT JOIN input i USING(source_review_id)
        WHERE i.source_review_id IS NULL) AS mappings_outside_valid_input,
       (SELECT count(*) FROM target.review_mapping rm LEFT JOIN target.logical_review l USING(logical_review_id)
        WHERE rm.rule_version=:'rule_version' AND l.logical_review_id IS NULL) AS orphan_logical_mappings,
       (SELECT count(*) FROM target.logical_review l LEFT JOIN target.review_mapping rm USING(logical_review_id)
        WHERE l.dedupe_rule_version=:'rule_version' AND rm.logical_review_id IS NULL) AS unmapped_logical_reviews;

\echo 'Independent preservation reasons'
SELECT dedupe_eligibility,count(*) logical_reviews
FROM target.logical_review WHERE dedupe_rule_version=:'rule_version'
GROUP BY dedupe_eligibility ORDER BY dedupe_eligibility;

\echo 'Product source versus logical counting mode'
SELECT counting_mode,count(*) products,sum(source_review_count) source_reviews,
       sum(valid_review_count) valid_source_reviews,sum(logical_review_count) logical_reviews,
       sum(duplicate_record_count) duplicate_records,
       sum(quarantined_review_count) quarantined_source_reviews
FROM target.product_review_summary GROUP BY counting_mode;

\echo 'Global score before and after exact deduplication'
SELECT (SELECT avg(score) FROM staging.current_review_parse_result
        WHERE parse_status='valid' AND score IS NOT NULL) AS source_valid_average_score,
       (SELECT avg(score) FROM target.current_logical_review
        WHERE score IS NOT NULL) AS logical_average_score;

\echo 'Products with the largest exact-duplicate reduction'
SELECT asin,valid_review_count,logical_review_count,duplicate_record_count,
       source_scored_review_count,scored_review_count,
       source_average_score,logical_average_score,counting_mode
FROM target.product_review_summary
ORDER BY duplicate_record_count DESC,asin LIMIT 5;

\echo 'Adopted single-work movie review count and score examples'
SELECT movie_id,canonical_title,edition_count,source_review_count,logical_review_count,
       duplicate_record_count,source_average_score,average_score AS logical_average_score
FROM target.movie_review_summary
ORDER BY duplicate_record_count DESC,movie_id LIMIT 5;

\echo 'Collection reviews remain product-level and are not copied'
SELECT asin,count(*) member_count,max(product_source_review_count) product_source_reviews,
       max(product_logical_review_count) product_logical_reviews,
       sum(attributed_logical_review_count) logical_reviews_copied_to_members
FROM target.product_movie_review_attribution
WHERE asin='B001PO54OM' AND relationship_type='collection_member'
GROUP BY asin;
