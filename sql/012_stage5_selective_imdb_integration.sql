\set ON_ERROR_STOP on
SET ROLE movie_app;

CREATE TABLE IF NOT EXISTS meta.imdb_selective_scope (
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    scope_key text NOT NULL,
    selection_version text NOT NULL,
    scope_sha256 char(64) NOT NULL,
    target_title_count integer NOT NULL CHECK (target_title_count > 0),
    status text NOT NULL CHECK (status IN ('registered', 'loading', 'loaded', 'materialized', 'failed')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (snapshot_id, scope_key)
);

CREATE TABLE IF NOT EXISTS meta.imdb_selective_scope_title (
    snapshot_id bigint NOT NULL,
    scope_key text NOT NULL,
    tconst text NOT NULL CHECK (tconst ~ '^tt[0-9]+$'),
    asin varchar(10),
    adoption_origin text,
    product_title_raw text,
    PRIMARY KEY (snapshot_id, scope_key, tconst),
    FOREIGN KEY (snapshot_id, scope_key)
        REFERENCES meta.imdb_selective_scope(snapshot_id, scope_key),
    CHECK (asin IS NULL OR asin ~ '^[A-Z0-9]{10}$')
);

CREATE TABLE IF NOT EXISTS meta.imdb_selective_progress (
    snapshot_id bigint NOT NULL,
    scope_key text NOT NULL,
    file_name text NOT NULL,
    scope_sha256 char(64) NOT NULL,
    committed_record_number bigint NOT NULL DEFAULT 0 CHECK (committed_record_number >= 0),
    next_record_number bigint NOT NULL DEFAULT 1 CHECK (next_record_number >= 1),
    selected_record_count bigint NOT NULL DEFAULT 0 CHECK (selected_record_count >= 0),
    reached_eof boolean NOT NULL DEFAULT false,
    status text NOT NULL CHECK (status IN ('pending', 'running', 'verified_complete', 'failed')),
    run_id bigint REFERENCES meta.etl_run(run_id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (snapshot_id, scope_key, file_name),
    FOREIGN KEY (snapshot_id, scope_key)
        REFERENCES meta.imdb_selective_scope(snapshot_id, scope_key),
    CHECK (next_record_number = committed_record_number + 1),
    CHECK (status <> 'verified_complete' OR reached_eof)
);

CREATE TABLE IF NOT EXISTS integration.movie_person_source_evidence (
    movie_id bigint NOT NULL REFERENCES target.movie(movie_id),
    person_id bigint NOT NULL REFERENCES target.person(person_id),
    role_type text NOT NULL CHECK (role_type IN ('director', 'cast', 'writer', 'other')),
    snapshot_id bigint NOT NULL REFERENCES meta.external_snapshot(snapshot_id),
    relationship_source_record_id bigint NOT NULL REFERENCES raw.source_record(source_record_id),
    name_source_record_id bigint REFERENCES raw.source_record(source_record_id),
    evidence_kind text NOT NULL CHECK (evidence_kind IN ('crew_director', 'crew_writer', 'principal_credit')),
    principal_order integer CHECK (principal_order IS NULL OR principal_order > 0),
    imdb_category text,
    job_raw text,
    characters_raw jsonb,
    primary_name_raw text,
    extraction_rule_version text NOT NULL,
    selection_status text NOT NULL CHECK (selection_status IN ('selected', 'supporting', 'conflicted')),
    selection_reason text NOT NULL,
    run_id bigint NOT NULL REFERENCES meta.etl_run(run_id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        movie_id, person_id, role_type, snapshot_id,
        relationship_source_record_id, extraction_rule_version
    )
);

CREATE INDEX IF NOT EXISTS ix_imdb_selective_progress_status
    ON meta.imdb_selective_progress (snapshot_id, scope_key, status, file_name);
CREATE INDEX IF NOT EXISTS ix_movie_person_evidence_fact
    ON integration.movie_person_source_evidence (movie_id, person_id, role_type, snapshot_id);

COMMENT ON TABLE meta.imdb_selective_progress IS
    'Selective IMDb gzip recovery checkpoint. Resume always scans the gzip from its beginning and discards through committed_record_number.';
COMMENT ON TABLE integration.movie_person_source_evidence IS
    'Source-specific IMDb relationship evidence. Principals are principal credits, never claimed as a complete cast or starring list.';

RESET ROLE;
