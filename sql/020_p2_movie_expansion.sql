\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE TABLE IF NOT EXISTS meta.p2_movie_scope (
    scope_key text PRIMARY KEY,
    source_name text NOT NULL,
    source_version text NOT NULL,
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    candidate_scope_key text NOT NULL,
    rule_version text NOT NULL,
    scope_sha256 char(64) NOT NULL,
    asin_count integer NOT NULL CHECK (asin_count > 0),
    baseline_adopted_asin_count integer NOT NULL CHECK (baseline_adopted_asin_count >= 0),
    baseline_adopted_movie_count integer NOT NULL CHECK (baseline_adopted_movie_count >= 0),
    status text NOT NULL CHECK (status IN ('registered','evaluated','bounded_applied','complete','failed')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS meta.p2_movie_scope_product (
    scope_key text NOT NULL REFERENCES meta.p2_movie_scope(scope_key),
    asin varchar(10) NOT NULL CHECK (asin ~ '^[A-Z0-9]{10}$'),
    product_id bigint NOT NULL REFERENCES target.product_edition(product_id),
    product_source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    product_source_record_number bigint NOT NULL CHECK (product_source_record_number > 0),
    product_title_raw text NOT NULL,
    baseline_adopted boolean NOT NULL,
    baseline_movie_ids bigint[] NOT NULL DEFAULT ARRAY[]::bigint[],
    stage4_scanned boolean NOT NULL,
    stage6_scanned boolean NOT NULL,
    stage6_candidate_count integer NOT NULL DEFAULT 0 CHECK (stage6_candidate_count >= 0),
    PRIMARY KEY (scope_key,asin),
    UNIQUE (scope_key,product_id)
);

CREATE TABLE IF NOT EXISTS integration.p2_movie_candidate_evaluation (
    scope_key text NOT NULL REFERENCES meta.p2_movie_scope(scope_key),
    asin varchar(10) NOT NULL,
    tconst text NOT NULL CHECK (tconst ~ '^tt[0-9]+$'),
    rule_version text NOT NULL,
    shortlist_rank integer NOT NULL CHECK (shortlist_rank > 0),
    product_source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    work_source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    title_evidence jsonb NOT NULL,
    type_evidence jsonb NOT NULL,
    year_evidence jsonb NOT NULL,
    person_evidence jsonb NOT NULL,
    evaluation_payload jsonb NOT NULL,
    score_breakdown jsonb NOT NULL,
    evidence_score integer NOT NULL,
    hard_conflicts text[] NOT NULL DEFAULT ARRAY[]::text[],
    confirmation_paths text[] NOT NULL DEFAULT ARRAY[]::text[],
    auto_confirmation_eligible boolean NOT NULL,
    evidence_sha256 char(64) NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    evaluated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (scope_key,asin,tconst,rule_version),
    FOREIGN KEY (scope_key,asin) REFERENCES meta.p2_movie_scope_product(scope_key,asin)
);

CREATE INDEX IF NOT EXISTS ix_p2_candidate_eligible
    ON integration.p2_movie_candidate_evaluation(scope_key,rule_version,auto_confirmation_eligible,asin);

CREATE TABLE IF NOT EXISTS integration.p2_movie_decision (
    scope_key text NOT NULL REFERENCES meta.p2_movie_scope(scope_key),
    asin varchar(10) NOT NULL,
    rule_version text NOT NULL,
    decision_status text NOT NULL CHECK (decision_status IN (
        'existing_adopted','rule_verified_pending','newly_adopted',
        'needs_review','explicit_mismatch','no_candidate'
    )),
    selected_tconst text CHECK (selected_tconst IS NULL OR selected_tconst ~ '^tt[0-9]+$'),
    reason_code text NOT NULL,
    decision_reason text NOT NULL,
    evidence jsonb NOT NULL,
    evidence_sha256 char(64) NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (scope_key,asin,rule_version),
    FOREIGN KEY (scope_key,asin) REFERENCES meta.p2_movie_scope_product(scope_key,asin)
);

CREATE TABLE IF NOT EXISTS meta.p2_movie_progress (
    scope_key text NOT NULL REFERENCES meta.p2_movie_scope(scope_key),
    phase text NOT NULL CHECK (phase IN ('register','evaluate','apply')),
    committed_asin varchar(10),
    processed_count integer NOT NULL DEFAULT 0 CHECK (processed_count >= 0),
    status text NOT NULL CHECK (status IN ('running','completed','failed')),
    run_id bigint REFERENCES meta.etl_run(run_id),
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (scope_key,phase)
);

CREATE OR REPLACE VIEW integration.p2_movie_reconciliation AS
SELECT sp.scope_key,sp.asin,sp.product_title_raw,
       CASE
         WHEN sp.baseline_adopted THEN 'existing_adopted'
         WHEN d.decision_status='newly_adopted' THEN 'newly_adopted'
         WHEN d.decision_status IN ('needs_review','rule_verified_pending') THEN 'needs_review'
         WHEN d.decision_status='explicit_mismatch' THEN 'explicit_mismatch'
         ELSE 'no_candidate'
       END AS reconciliation_status,
       d.selected_tconst,d.reason_code,d.decision_reason,
       sp.stage4_scanned,sp.stage6_scanned,sp.stage6_candidate_count,
       d.rule_version,d.evidence
FROM meta.p2_movie_scope_product sp
LEFT JOIN integration.p2_movie_decision d
  ON d.scope_key=sp.scope_key AND d.asin=sp.asin
 AND d.rule_version=(SELECT rule_version FROM meta.p2_movie_scope s WHERE s.scope_key=sp.scope_key);

COMMENT ON VIEW integration.p2_movie_reconciliation IS
    'Mutually exclusive P2 status for the fixed 433-ASIN movie-product scope; pending verified decisions remain review until transactionally adopted.';

RESET ROLE;
