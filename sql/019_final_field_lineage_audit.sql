\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE TABLE IF NOT EXISTS meta.lineage_audit_run (
    audit_run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    audit_version text NOT NULL,
    batch_id text NOT NULL,
    scope_key text NOT NULL,
    status text NOT NULL CHECK (status IN ('running','completed','failed')),
    active_parser_version text NOT NULL,
    active_dedupe_rule_version text NOT NULL,
    full_scope boolean NOT NULL DEFAULT false,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    stats jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (audit_version,batch_id)
);

CREATE TABLE IF NOT EXISTS meta.lineage_audit_progress (
    audit_run_id bigint NOT NULL REFERENCES meta.lineage_audit_run(audit_run_id),
    component text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending','running','completed','failed')),
    last_entity_id bigint NOT NULL DEFAULT 0 CHECK (last_entity_id >= 0),
    examined_count bigint NOT NULL DEFAULT 0 CHECK (examined_count >= 0),
    gap_count bigint NOT NULL DEFAULT 0 CHECK (gap_count >= 0),
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (audit_run_id,component)
);

CREATE TABLE IF NOT EXISTS integration.lineage_field_catalog (
    entity_type text NOT NULL,
    field_name text NOT NULL,
    field_semantics text NOT NULL,
    applicable_scope text NOT NULL,
    final_value_location text NOT NULL,
    lineage_mode text NOT NULL CHECK (lineage_mode IN (
        'field_selection','direct_source_chain','relationship_evidence','generated_technical'
    )),
    generation_rule text,
    required_source boolean NOT NULL DEFAULT true,
    PRIMARY KEY (entity_type,field_name)
);

CREATE TABLE IF NOT EXISTS integration.lineage_audit_result (
    audit_run_id bigint NOT NULL REFERENCES meta.lineage_audit_run(audit_run_id),
    entity_type text NOT NULL,
    field_name text NOT NULL,
    applicable_count bigint NOT NULL,
    nonempty_count bigint NOT NULL,
    complete_lineage_count bigint NOT NULL,
    missing_value_count bigint NOT NULL,
    missing_lineage_count bigint NOT NULL,
    verified_count bigint NOT NULL DEFAULT 0,
    manual_count bigint NOT NULL DEFAULT 0,
    inferred_count bigint NOT NULL DEFAULT 0,
    conflict_count bigint NOT NULL DEFAULT 0,
    denominator_note text NOT NULL,
    PRIMARY KEY (audit_run_id,entity_type,field_name),
    CHECK (applicable_count >= 0 AND nonempty_count >= 0 AND complete_lineage_count >= 0),
    CHECK (missing_value_count >= 0 AND missing_lineage_count >= 0 AND conflict_count >= 0)
);

CREATE TABLE IF NOT EXISTS integration.lineage_audit_gap (
    audit_gap_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    audit_run_id bigint NOT NULL REFERENCES meta.lineage_audit_run(audit_run_id),
    entity_type text NOT NULL,
    entity_key text NOT NULL,
    field_name text NOT NULL,
    gap_code text NOT NULL,
    final_value jsonb,
    reason text NOT NULL,
    source_record_id bigint REFERENCES raw.source_record(source_record_id),
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (audit_run_id,entity_type,entity_key,field_name,gap_code)
);

CREATE TABLE IF NOT EXISTS integration.lineage_repair_event (
    repair_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repair_version text NOT NULL,
    entity_type text NOT NULL,
    entity_key text NOT NULL,
    field_name text NOT NULL,
    repair_action text NOT NULL,
    source_record_id bigint REFERENCES raw.source_record(source_record_id),
    prior_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    new_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    reason text NOT NULL,
    run_id bigint REFERENCES meta.etl_run(run_id),
    repaired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (repair_version,entity_type,entity_key,field_name,repair_action)
);

CREATE TABLE IF NOT EXISTS integration.relationship_source_link (
    relationship_type text NOT NULL,
    relationship_key text NOT NULL,
    source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    evidence_role text NOT NULL,
    field_names text[] NOT NULL,
    rule_version text NOT NULL,
    selection_reason text NOT NULL,
    repair_event_id bigint REFERENCES integration.lineage_repair_event(repair_event_id),
    PRIMARY KEY (relationship_type,relationship_key,source_record_id,evidence_role,rule_version)
);

ALTER TABLE target.movie_person
    ADD COLUMN IF NOT EXISTS relation_status text NOT NULL DEFAULT 'selected',
    ADD COLUMN IF NOT EXISTS superseded_by_person_id bigint,
    ADD COLUMN IF NOT EXISTS selection_rule_version text,
    ADD COLUMN IF NOT EXISTS relation_selection_reason text;

ALTER TABLE target.movie_person
    DROP CONSTRAINT IF EXISTS ck_movie_person_relation_status,
    DROP CONSTRAINT IF EXISTS fk_movie_person_superseded_by,
    DROP CONSTRAINT IF EXISTS ck_movie_person_supersession;
ALTER TABLE target.movie_person
    ADD CONSTRAINT ck_movie_person_relation_status CHECK (
        relation_status IN ('selected','superseded','conflicted','pending')
    ),
    ADD CONSTRAINT fk_movie_person_superseded_by FOREIGN KEY (superseded_by_person_id)
        REFERENCES target.person(person_id),
    ADD CONSTRAINT ck_movie_person_supersession CHECK (
        (relation_status='superseded' AND superseded_by_person_id IS NOT NULL)
        OR (relation_status<>'superseded' AND superseded_by_person_id IS NULL)
    );

CREATE OR REPLACE VIEW target.current_movie_person AS
SELECT mp.*
FROM target.movie_person mp
JOIN target.person p USING(person_id)
WHERE mp.relation_status='selected' AND p.status<>'retired';

CREATE OR REPLACE VIEW integration.final_source_record_impact AS
SELECT source_record_id,entity_type,entity_key,field_name,impact_kind,impact_key,rule_version
FROM integration.source_record_impact
UNION ALL
SELECT l.source_record_id,l.relationship_type,
       l.relationship_key,'relationship'::text,l.evidence_role,
       l.relationship_key,l.rule_version
FROM integration.relationship_source_link l
UNION ALL
SELECT rr.source_record_id,'logical_review'::text,lr.stable_key,
       'representative_source'::text,'logical_review_representative'::text,
       lr.logical_review_id::text,lr.dedupe_rule_version
FROM target.current_logical_review lr
JOIN staging.review_record rr ON rr.source_review_id=lr.representative_source_review_id
UNION ALL
SELECT rr.source_record_id,'review_mapping'::text,rm.source_review_id,
       'relationship'::text,coalesce(rm.source_role,'mapping'),
       rm.logical_review_id::text,rm.rule_version
FROM target.current_review_mapping rm
JOIN staging.review_record rr USING(source_review_id);

CREATE INDEX IF NOT EXISTS ix_lineage_gap_run_field
    ON integration.lineage_audit_gap(audit_run_id,entity_type,field_name,gap_code);
CREATE INDEX IF NOT EXISTS ix_relationship_source_record
    ON integration.relationship_source_link(source_record_id,relationship_type);
CREATE INDEX IF NOT EXISTS ix_movie_person_current
    ON target.movie_person(movie_id,person_id,role_type) WHERE relation_status='selected';

INSERT INTO integration.lineage_field_catalog(
    entity_type,field_name,field_semantics,applicable_scope,final_value_location,
    lineage_mode,generation_rule,required_source
) VALUES
('product_edition','asin','Amazon product identifier; one ProductEdition identity','all ProductEdition rows','target.product_edition.asin','direct_source_chain','usable ASIN observation or ASIN-linked product metadata record',true),
('product_edition','classification','product kind, with pending meaning unresolved','all ProductEdition rows','target.product_edition.classification','field_selection',NULL,true),
('product_edition','media_format','physical/digital carrier format','all ProductEdition rows','target.product_edition.media_format','field_selection',NULL,true),
('product_edition','language_codes','product audio languages','all ProductEdition rows','target.product_edition.language_codes','field_selection',NULL,true),
('product_edition','subtitle_codes','product subtitle languages','all ProductEdition rows','target.product_edition.subtitle_codes','field_selection',NULL,true),
('product_edition','region_code','edition playback region','all ProductEdition rows','target.product_edition.region_code','field_selection',NULL,true),
('product_edition','cut_label','edition/cut label','all ProductEdition rows','target.product_edition.cut_label','field_selection',NULL,true),
('product_edition','publisher','edition publisher or distributor','all ProductEdition rows','target.product_edition.publisher','field_selection',NULL,true),
('product_edition','product_release_date','product release date, not work release date','all ProductEdition rows','target.product_edition.product_release_date','field_selection',NULL,true),
('movie','canonical_title','selected work title','movies reached by adopted ProductMovie relationships','target.movie.canonical_title','field_selection',NULL,true),
('movie','original_title','fixed-snapshot original work title','movies reached by adopted ProductMovie relationships','target.movie.original_title','field_selection',NULL,true),
('movie','release_date','work release date with stated precision','movies reached by adopted ProductMovie relationships','target.movie.release_date','field_selection',NULL,true),
('movie','release_year','work release year, not product date','movies reached by adopted ProductMovie relationships','target.movie.release_year','field_selection',NULL,true),
('movie','release_precision','precision of the selected release value','movies reached by adopted ProductMovie relationships','target.movie.release_precision','field_selection','derived from the selected release field and its source semantics',true),
('movie','release_region','region semantics of selected release value','movies reached by adopted ProductMovie relationships','target.movie.release_region','field_selection',NULL,true),
('movie','release_type','work type from fixed external snapshot','movies reached by adopted ProductMovie relationships','target.movie.release_type','field_selection',NULL,true),
('person','canonical_name','selected display name for a stable person identity','persons in current adopted-movie credits','target.person.canonical_name','field_selection',NULL,true),
('person_alias','alias_raw','source-observed person name','aliases of persons in current adopted-movie credits','target.person_alias.alias_raw','direct_source_chain',NULL,true),
('person_alias','alias_normalized','search normalization only; not identity proof','aliases of persons in current adopted-movie credits','target.person_alias.alias_normalized','generated_technical','NFKC, casefold and surrounding-space normalization of alias_raw',false),
('product_movie','relationship','ProductEdition-to-Movie membership and relationship type','adopted verified/manual/predicted ProductMovie rows','target.product_movie composite row','relationship_evidence',NULL,true),
('movie_person','relationship','Movie-to-Person credit including role and ordering','current credits of adopted movies','target.current_movie_person composite row','relationship_evidence',NULL,true),
('logical_review','product_id','product owning the logical review','active logical-review release','target.current_logical_review.product_id','direct_source_chain',NULL,true),
('logical_review','reviewer_id','opaque source reviewer identity','active logical-review release','target.current_logical_review.reviewer_id','direct_source_chain',NULL,true),
('logical_review','review_time_utc','UTC conversion of source Unix time','active logical-review release','target.current_logical_review.review_time_utc','direct_source_chain','parser-versioned Unix timestamp conversion',true),
('logical_review','score','normalized source review rating','active logical-review release','target.current_logical_review.score','direct_source_chain',NULL,true),
('logical_review','summary','source review summary','active logical-review release','target.current_logical_review.summary','direct_source_chain',NULL,true),
('logical_review','review_text','source review body','active logical-review release','target.current_logical_review.review_text','direct_source_chain',NULL,true),
('logical_review','helpfulness','source helpful/total votes or explicit group conflict','active logical-review release','target.current_logical_review helpful_votes,total_votes,helpfulness_resolution','direct_source_chain','unanimous value retained; conflicting values remain on source mappings',true),
('review_mapping','relationship','one active source review to one logical review','active ReviewMapping rows','target.current_review_mapping composite row','relationship_evidence',NULL,true),
('logical_review','logical_review_id','database identity','active logical-review release','target.current_logical_review.logical_review_id','generated_technical','PostgreSQL identity; not a sourced business fact',false),
('logical_review','stable_key','stable exact-dedupe key','active logical-review release','target.current_logical_review.stable_key','generated_technical','SHA-256 over versioned exact identity/content tuple; hash only accelerates equality checks',false)
ON CONFLICT(entity_type,field_name) DO UPDATE SET
    field_semantics=EXCLUDED.field_semantics,
    applicable_scope=EXCLUDED.applicable_scope,
    final_value_location=EXCLUDED.final_value_location,
    lineage_mode=EXCLUDED.lineage_mode,
    generation_rule=EXCLUDED.generation_rule,
    required_source=EXCLUDED.required_source;

COMMENT ON TABLE integration.lineage_field_catalog IS
    'Auditable inventory of business and generated fields; candidate-only movies are excluded from adopted scope.';
COMMENT ON VIEW target.current_movie_person IS
    'Current relationship view; superseded suggestion-name facts remain in target.movie_person as history.';
COMMENT ON VIEW integration.final_source_record_impact IS
    'Reverse impact view including field assertions, current relationships, logical-review representatives and mappings.';

RESET ROLE;
