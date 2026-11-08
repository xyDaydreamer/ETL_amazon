\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE TABLE IF NOT EXISTS meta.imdb_selective_scope_product (
    snapshot_id bigint NOT NULL,
    scope_key text NOT NULL,
    tconst text NOT NULL CHECK (tconst ~ '^tt[0-9]+$'),
    asin varchar(10) NOT NULL CHECK (asin ~ '^[A-Z0-9]{10}$'),
    adoption_origin text,
    product_title_raw text,
    PRIMARY KEY (snapshot_id, scope_key, tconst, asin),
    FOREIGN KEY (snapshot_id, scope_key)
        REFERENCES meta.imdb_selective_scope(snapshot_id, scope_key)
);

INSERT INTO meta.imdb_selective_scope_product(
    snapshot_id,scope_key,tconst,asin,adoption_origin,product_title_raw
)
SELECT snapshot_id,scope_key,tconst,asin,adoption_origin,product_title_raw
FROM meta.imdb_selective_scope_title
WHERE asin IS NOT NULL
ON CONFLICT DO NOTHING;

COMMENT ON TABLE meta.imdb_selective_scope_product IS
    'Product-to-tconst members of a selective scope. Kept separate so multiple ASIN editions can map to one scanned title.';

RESET ROLE;
