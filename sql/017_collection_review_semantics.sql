\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE OR REPLACE VIEW target.product_movie_review_attribution AS
SELECT pe.product_id,
       pe.asin,
       pm.movie_id,
       pm.relationship_type,
       pm.mapping_status,
       coalesce(prs.source_review_count, 0)::bigint AS product_source_review_count,
       CASE WHEN pm.relationship_type = 'single_work'
            THEN coalesce(prs.source_review_count, 0)::bigint
            ELSE 0::bigint
       END AS attributed_source_review_count,
       CASE WHEN pm.relationship_type = 'single_work'
            THEN 'single_work_product_reviews_may_contribute_to_work_summary'
            WHEN pm.relationship_type = 'collection_member'
            THEN 'collection_reviews_remain_product_level_and_are_not_copied_to_members'
            ELSE 'relationship_not_eligible_for_movie_review_attribution'
       END AS attribution_reason
FROM target.product_movie pm
JOIN target.product_edition pe USING (product_id)
LEFT JOIN target.product_review_summary prs USING (product_id)
WHERE pm.mapping_status IN ('verified','predicted','manual');

CREATE OR REPLACE VIEW target.movie_review_summary AS
WITH mapped_editions AS (
    SELECT DISTINCT movie_id, product_id
    FROM target.product_movie
    WHERE mapping_status IN ('verified','predicted','manual')
      AND relationship_type = 'single_work'
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

COMMENT ON VIEW target.product_movie_review_attribution IS
    'Explicit review attribution policy. Collection-member reviews remain on ProductEdition and contribute zero rows to member Movie summaries.';
COMMENT ON VIEW target.movie_review_summary IS
    'Movie review aggregates include verified/predicted/manual single-work editions only; collection, series and uncertain mappings are excluded.';

