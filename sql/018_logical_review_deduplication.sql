\set ON_ERROR_STOP on
SET ROLE movie_app;

ALTER TABLE target.logical_review
    ADD COLUMN IF NOT EXISTS dedupe_rule_version text,
    ADD COLUMN IF NOT EXISTS parser_version text,
    ADD COLUMN IF NOT EXISTS asin varchar(10),
    ADD COLUMN IF NOT EXISTS review_time_raw text,
    ADD COLUMN IF NOT EXISTS dedupe_eligibility text,
    ADD COLUMN IF NOT EXISTS representative_source_review_id char(64),
    ADD COLUMN IF NOT EXISTS representative_record_number bigint,
    ADD COLUMN IF NOT EXISTS helpful_votes bigint,
    ADD COLUMN IF NOT EXISTS total_votes bigint,
    ADD COLUMN IF NOT EXISTS helpfulness_resolution text,
    ADD COLUMN IF NOT EXISTS created_run_id bigint,
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT clock_timestamp();

ALTER TABLE target.logical_review DROP CONSTRAINT IF EXISTS logical_review_stable_key_key;
CREATE UNIQUE INDEX IF NOT EXISTS ux_logical_review_stable_rule
    ON target.logical_review(stable_key,dedupe_rule_version);
CREATE INDEX IF NOT EXISTS ix_logical_review_product_rule
    ON target.logical_review(product_id,dedupe_rule_version);
CREATE INDEX IF NOT EXISTS ix_logical_review_representative
    ON target.logical_review(representative_source_review_id,dedupe_rule_version);

ALTER TABLE target.logical_review
    DROP CONSTRAINT IF EXISTS ck_logical_review_asin,
    DROP CONSTRAINT IF EXISTS ck_logical_review_eligibility,
    DROP CONSTRAINT IF EXISTS ck_logical_review_record_number,
    DROP CONSTRAINT IF EXISTS ck_logical_review_votes,
    DROP CONSTRAINT IF EXISTS ck_logical_review_helpfulness_resolution,
    DROP CONSTRAINT IF EXISTS fk_logical_review_representative,
    DROP CONSTRAINT IF EXISTS fk_logical_review_created_run;
ALTER TABLE target.logical_review
    ADD CONSTRAINT ck_logical_review_asin CHECK (asin IS NULL OR asin ~ '^[A-Z0-9]{10}$'),
    ADD CONSTRAINT ck_logical_review_eligibility CHECK (
        dedupe_eligibility IS NULL OR dedupe_eligibility IN ('exact_eligible','preserved_missing_key')
    ),
    ADD CONSTRAINT ck_logical_review_record_number CHECK (
        representative_record_number IS NULL OR representative_record_number > 0
    ),
    ADD CONSTRAINT ck_logical_review_votes CHECK (
        (helpful_votes IS NULL OR helpful_votes >= 0) AND
        (total_votes IS NULL OR total_votes >= 0) AND
        (helpful_votes IS NULL OR total_votes IS NULL OR helpful_votes <= total_votes)
    ),
    ADD CONSTRAINT ck_logical_review_helpfulness_resolution CHECK (
        helpfulness_resolution IS NULL OR helpfulness_resolution IN (
            'unanimous_source_value','conflict_preserved_at_source'
        )
    ),
    ADD CONSTRAINT fk_logical_review_representative FOREIGN KEY (representative_source_review_id)
        REFERENCES staging.review_record(source_review_id),
    ADD CONSTRAINT fk_logical_review_created_run FOREIGN KEY (created_run_id)
        REFERENCES meta.etl_run(run_id);

ALTER TABLE target.review_mapping
    ADD COLUMN IF NOT EXISTS source_role text,
    ADD COLUMN IF NOT EXISTS evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS mapped_run_id bigint;
ALTER TABLE target.review_mapping
    DROP CONSTRAINT IF EXISTS ck_review_mapping_source_role,
    DROP CONSTRAINT IF EXISTS fk_review_mapping_run;
ALTER TABLE target.review_mapping
    ADD CONSTRAINT ck_review_mapping_source_role CHECK (
        source_role IS NULL OR source_role IN ('representative','exact_duplicate','preserved_independent')
    ),
    ADD CONSTRAINT fk_review_mapping_run FOREIGN KEY (mapped_run_id) REFERENCES meta.etl_run(run_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_review_mapping_source_rule
    ON target.review_mapping(source_review_id,rule_version);
CREATE INDEX IF NOT EXISTS ix_review_mapping_rule_logical
    ON target.review_mapping(rule_version,logical_review_id);

CREATE TABLE IF NOT EXISTS meta.logical_review_release (
    rule_version text PRIMARY KEY,
    parser_version text NOT NULL REFERENCES meta.parser_release(parser_version),
    status text NOT NULL CHECK (status IN ('draft','running','sample_verified','full_verified','failed')),
    is_current boolean NOT NULL DEFAULT false,
    scope_start_record bigint,
    scope_end_record bigint,
    input_valid_count bigint,
    logical_review_count bigint,
    mapping_count bigint,
    duplicate_record_count bigint,
    activated_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (scope_start_record IS NULL OR scope_start_record > 0),
    CHECK (scope_end_record IS NULL OR scope_end_record >= scope_start_record)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_logical_review_release_current
    ON meta.logical_review_release((is_current)) WHERE is_current;

CREATE TABLE IF NOT EXISTS meta.logical_review_progress (
    rule_version text NOT NULL REFERENCES meta.logical_review_release(rule_version),
    parser_version text NOT NULL REFERENCES meta.parser_release(parser_version),
    scope_key text NOT NULL,
    scope_start_record bigint NOT NULL CHECK (scope_start_record > 0),
    scope_end_record bigint,
    committed_record_number bigint NOT NULL DEFAULT 0 CHECK (committed_record_number >= 0),
    processed_input_count bigint NOT NULL DEFAULT 0 CHECK (processed_input_count >= 0),
    status text NOT NULL CHECK (status IN (
        'running','sample_complete','full_complete','stopped_low_disk','failed'
    )),
    stop_reason text,
    last_free_bytes bigint,
    run_id bigint REFERENCES meta.etl_run(run_id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(rule_version,parser_version,scope_key),
    CHECK (scope_end_record IS NULL OR scope_end_record >= scope_start_record)
);

CREATE OR REPLACE VIEW target.current_logical_review AS
SELECT lr.*
FROM target.logical_review lr
JOIN meta.logical_review_release rel ON rel.rule_version=lr.dedupe_rule_version
WHERE rel.is_current AND rel.status='full_verified';

CREATE OR REPLACE VIEW target.current_review_mapping AS
SELECT rm.*
FROM target.review_mapping rm
JOIN meta.logical_review_release rel ON rel.rule_version=rm.rule_version
WHERE rel.is_current AND rel.status='full_verified';

DROP VIEW IF EXISTS target.movie_review_summary;
DROP VIEW IF EXISTS target.product_movie_review_attribution;
DROP VIEW IF EXISTS target.product_review_summary;

CREATE VIEW target.product_review_summary AS
WITH active AS (
    SELECT rule_version FROM meta.logical_review_release
    WHERE is_current AND status='full_verified'
), source_stats AS (
    SELECT pe.product_id,pe.asin,count(*) AS source_review_count,
           count(*) FILTER (WHERE r.parse_status='valid') AS valid_review_count,
           count(*) FILTER (WHERE r.parse_status='quarantined') AS quarantined_review_count,
           count(r.score) FILTER (WHERE r.parse_status='valid') AS source_scored_review_count,
           avg(r.score) FILTER (WHERE r.parse_status='valid') AS source_average_score
    FROM target.product_edition pe
    JOIN staging.current_review_parse_result r ON r.asin=pe.asin
    GROUP BY pe.product_id,pe.asin
), logical_stats AS (
    SELECT product_id,count(*) AS logical_review_count,
           count(score) AS scored_review_count,avg(score) AS average_score
    FROM target.current_logical_review GROUP BY product_id
)
SELECT s.product_id,s.asin,s.source_review_count,
       CASE WHEN EXISTS(SELECT 1 FROM active) THEN coalesce(l.logical_review_count,0)
            ELSE s.valid_review_count END::bigint AS logical_review_count,
       CASE WHEN EXISTS(SELECT 1 FROM active) THEN s.valid_review_count-coalesce(l.logical_review_count,0)
            ELSE 0 END::bigint AS duplicate_record_count,
       CASE WHEN EXISTS(SELECT 1 FROM active) THEN coalesce(l.scored_review_count,0)
            ELSE s.source_scored_review_count END::bigint AS scored_review_count,
       CASE WHEN EXISTS(SELECT 1 FROM active) THEN l.average_score
            ELSE s.source_average_score END AS average_score,
       s.valid_review_count,s.quarantined_review_count,
       CASE WHEN EXISTS(SELECT 1 FROM active) THEN 'logical_exact_dedupe'
            ELSE 'source_fallback_no_active_dedupe' END AS counting_mode,
       s.source_scored_review_count,
       s.source_average_score,
       CASE WHEN EXISTS(SELECT 1 FROM active) THEN l.average_score ELSE NULL END AS logical_average_score
FROM source_stats s LEFT JOIN logical_stats l USING(product_id);

CREATE OR REPLACE VIEW target.product_movie_review_attribution AS
SELECT pe.product_id,pe.asin,pm.movie_id,pm.relationship_type,pm.mapping_status,
       coalesce(prs.source_review_count,0)::bigint AS product_source_review_count,
       coalesce(prs.logical_review_count,0)::bigint AS product_logical_review_count,
       CASE WHEN pm.relationship_type='single_work' THEN coalesce(prs.logical_review_count,0)::bigint ELSE 0 END
           AS attributed_logical_review_count,
       CASE WHEN pm.relationship_type='single_work'
            THEN 'single_work_product_logical_reviews_may_contribute_to_work_summary'
            WHEN pm.relationship_type='collection_member'
            THEN 'collection_reviews_remain_product_level_and_are_not_copied_to_members'
            ELSE 'relationship_not_eligible_for_movie_review_attribution' END AS attribution_reason
FROM target.product_movie pm JOIN target.product_edition pe USING(product_id)
LEFT JOIN target.product_review_summary prs USING(product_id)
WHERE pm.mapping_status IN ('verified','predicted','manual');

CREATE OR REPLACE VIEW target.movie_review_summary AS
WITH mapped AS (
    SELECT DISTINCT movie_id,product_id FROM target.product_movie
    WHERE mapping_status IN ('verified','predicted','manual') AND relationship_type='single_work'
)
SELECT m.movie_id,m.stable_key,m.canonical_title,count(DISTINCT me.product_id) AS edition_count,
       coalesce(sum(prs.source_review_count),0)::bigint AS source_review_count,
       coalesce(sum(prs.logical_review_count),0)::bigint AS logical_review_count,
       coalesce(sum(prs.duplicate_record_count),0)::bigint AS duplicate_record_count,
       coalesce(sum(prs.scored_review_count),0)::bigint AS scored_review_count,
       CASE WHEN coalesce(sum(prs.scored_review_count),0)=0 THEN NULL
            ELSE sum(prs.average_score*prs.scored_review_count)/sum(prs.scored_review_count) END AS average_score,
       coalesce(sum(prs.source_scored_review_count),0)::bigint AS source_scored_review_count,
       CASE WHEN coalesce(sum(prs.source_scored_review_count),0)=0 THEN NULL
            ELSE sum(prs.source_average_score*prs.source_scored_review_count)
                 /sum(prs.source_scored_review_count) END AS source_average_score
FROM target.movie m JOIN mapped me USING(movie_id)
LEFT JOIN target.product_review_summary prs USING(product_id)
GROUP BY m.movie_id,m.stable_key,m.canonical_title;

COMMENT ON TABLE meta.logical_review_progress IS
    'Transactional source-record checkpoint for versioned logical-review deduplication.';
COMMENT ON VIEW target.current_logical_review IS
    'Exposes only a full-verified explicitly activated deduplication release.';
COMMENT ON VIEW target.product_review_summary IS
    'Uses source-valid fallback until a full logical-review release is activated; then average_score is the logical average while source_average_score remains the pre-dedup valid-source average.';

RESET ROLE;
