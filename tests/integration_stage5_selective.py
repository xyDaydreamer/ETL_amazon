from __future__ import annotations

import gzip
import json
import os
import tempfile
import uuid
from datetime import date
from pathlib import Path

import psycopg

from amazon_movies.imdb_selective_loader import (
    InjectedFailure,
    SelectedRow,
    _register_scope,
    _scope_digest,
    _source_record,
    load_selective_snapshot,
)
from amazon_movies.imdb_snapshot import REQUIRED_HEADERS, inspect_snapshot, register_snapshot, write_manifest


def _write(path: Path, file_name: str, rows: list[list[str]]) -> None:
    with gzip.open(path / file_name, "wt", encoding="utf-8", newline="") as stream:
        stream.write("\t".join(REQUIRED_HEADERS[file_name]) + "\n")
        for row in rows:
            stream.write("\t".join(row) + "\n")


def _fixture(root: Path) -> None:
    _write(root, "title.basics.tsv.gz", [
        ["tt0000000", "short", "Other 0", "Other 0", "0", "1990", r"\N", "1", "Short"],
        ["tt0000001", "movie", "Fixture Film", "Fixture Film", "0", "2001", r"\N", "90", "Drama"],
        ["tt0000002", "movie", "Other 2", "Other 2", "0", "2002", r"\N", "90", "Drama"],
        ["tt0000003", "movie", "Other 3", "Other 3", "0", "2003", r"\N", "90", "Drama"],
        ["tt0000004", "movie", "Other 4", "Other 4", "0", "2004", r"\N", "90", "Drama"],
    ])
    _write(root, "title.akas.tsv.gz", [
        ["tt0000000", "1", "Other", "US", "en", r"\N", r"\N", "0"],
        ["tt0000001", "1", "Fixture Alias", "US", "en", "imdbDisplay", r"\N", "0"],
        ["tt0000002", "1", "Other 2", "US", "en", r"\N", r"\N", "0"],
    ])
    _write(root, "title.crew.tsv.gz", [
        ["tt0000000", r"\N", r"\N"],
        ["tt0000001", "nm0000001", "nm0000002"],
        ["tt0000002", r"\N", r"\N"],
    ])
    _write(root, "title.principals.tsv.gz", [
        ["tt0000000", "1", "nm0000009", "actor", r"\N", '["Other"]'],
        ["tt0000001", "1", "nm0000003", "actor", r"\N", '["Lead"]'],
        ["tt0000001", "2", "nm0000001", "director", r"\N", r"\N"],
        ["tt0000002", "1", "nm0000008", "actor", r"\N", '["Other"]'],
    ])
    _write(root, "name.basics.tsv.gz", [
        ["nm0000001", "Director One", "1970", r"\N", "director", "tt0000001"],
        ["nm0000002", "Writer Two", "1960", r"\N", "writer", "tt0000001"],
        ["nm0000003", "Actor Three", "1980", r"\N", "actor", "tt0000001"],
        ["nm0000008", "Other Eight", r"\N", r"\N", "actor", "tt0000002"],
        ["nm0000009", "Other Nine", r"\N", r"\N", "actor", "tt0000000"],
    ])


def main() -> None:
    database_url = os.environ["AMAZON_MOVIES_DATABASE_URL"]
    result: dict[str, object] = {"data_kind": "constructed_stage5_database_acceptance"}
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        _fixture(root)
        manifest = inspect_snapshot(root, date(2099, 1, 1), full_verify=True, progress_every=0)
        manifest["snapshot_version"] = f"imdb-constructed-2099-01-01-{uuid.uuid4().hex[:8]}"
        manifest_path = root / "manifest.json"
        write_manifest(manifest, manifest_path)
        registered = register_snapshot(database_url, manifest_path)
        version = registered["snapshot_version"]
        multi_targets = [
            {"tconst": "tt0000001", "asin": "T000000001", "adoption_origin": "rule_verified", "product_title_raw": "Edition A"},
            {"tconst": "tt0000001", "asin": "T000000002", "adoption_origin": "rule_verified", "product_title_raw": "Edition B"},
        ]
        with psycopg.connect(database_url) as conn:
            multi_sha = _scope_digest(multi_targets, version, "constructed-multi-product-v1")
            _register_scope(
                conn, registered["snapshot_id"], "constructed-multi-product",
                "constructed-multi-product-v1", multi_sha, multi_targets,
            )
            multi_counts = conn.execute(
                """SELECT
                    (SELECT count(*) FROM meta.imdb_selective_scope_title
                      WHERE snapshot_id=%s AND scope_key='constructed-multi-product'),
                    (SELECT count(*) FROM meta.imdb_selective_scope_product
                      WHERE snapshot_id=%s AND scope_key='constructed-multi-product')""",
                (registered["snapshot_id"], registered["snapshot_id"]),
            ).fetchone()

        pre_failed = False
        try:
            load_selective_snapshot(
                database_url, snapshot_dir=root, snapshot_version=version, scope="explicit",
                scope_key="constructed-precommit", batch_id="constructed-precommit",
                explicit_tconsts=["tt0000001"], batch_size=2, progress_every=0,
                fail_before_commit_batch=1,
            )
        except InjectedFailure:
            pre_failed = True
        with psycopg.connect(database_url) as conn:
            pre_checkpoint = conn.execute(
                """SELECT committed_record_number FROM meta.imdb_selective_progress p
                   JOIN meta.external_snapshot s USING(snapshot_id)
                   WHERE s.snapshot_version=%s AND p.scope_key='constructed-precommit'
                     AND p.file_name='title.basics.tsv.gz'""",
                (version,),
            ).fetchone()[0]
        pre_resumed = load_selective_snapshot(
            database_url, snapshot_dir=root, snapshot_version=version, scope="explicit",
            scope_key="constructed-precommit", batch_id="constructed-precommit",
            explicit_tconsts=["tt0000001"], batch_size=2, progress_every=0,
        )

        post_failed = False
        try:
            load_selective_snapshot(
                database_url, snapshot_dir=root, snapshot_version=version, scope="explicit",
                scope_key="constructed-postcommit", batch_id="constructed-postcommit",
                explicit_tconsts=["tt0000001"], batch_size=2, progress_every=0,
                fail_after_commit_batch=1,
            )
        except InjectedFailure:
            post_failed = True
        with psycopg.connect(database_url) as conn:
            post_checkpoint = conn.execute(
                """SELECT committed_record_number FROM meta.imdb_selective_progress p
                   JOIN meta.external_snapshot s USING(snapshot_id)
                   WHERE s.snapshot_version=%s AND p.scope_key='constructed-postcommit'
                     AND p.file_name='title.basics.tsv.gz'""",
                (version,),
            ).fetchone()[0]
        post_resumed = load_selective_snapshot(
            database_url, snapshot_dir=root, snapshot_version=version, scope="explicit",
            scope_key="constructed-postcommit", batch_id="constructed-postcommit",
            explicit_tconsts=["tt0000001"], batch_size=2, progress_every=0,
        )
        repeated = load_selective_snapshot(
            database_url, snapshot_dir=root, snapshot_version=version, scope="explicit",
            scope_key="constructed-postcommit", batch_id="constructed-repeat",
            explicit_tconsts=["tt0000001"], batch_size=2, progress_every=0,
        )

        conflict_detected = False
        with psycopg.connect(database_url) as conn:
            snapshot_id = registered["snapshot_id"]
            source_file_id = conn.execute(
                """SELECT f.source_file_id FROM meta.source_file f
                   JOIN meta.external_snapshot_file sf ON sf.file_sha256=f.sha256
                   WHERE sf.snapshot_id=%s AND sf.file_name='title.basics.tsv.gz'""",
                (snapshot_id,),
            ).fetchone()[0]
            run_id = conn.execute(
                "SELECT run_id FROM meta.etl_run WHERE batch_id='constructed-repeat'"
            ).fetchone()[0]
            try:
                _source_record(
                    conn, source_file_id=source_file_id, file_name="title.basics.tsv.gz", path=root,
                    row=SelectedRow(2, 3, {"tconst": "tt0000001"}, "f" * 64, b"changed"),
                    run_id=run_id,
                )
            except ValueError:
                conflict_detected = True
            counts = conn.execute(
                """SELECT
                    (SELECT count(*) FROM target.movie WHERE stable_key='imdb:tt0000001'),
                    (SELECT count(*) FROM target.person WHERE stable_key IN
                       ('imdb:nm0000001','imdb:nm0000002','imdb:nm0000003')),
                    (SELECT count(DISTINCT (mp.movie_id,mp.person_id,mp.role_type))
                       FROM target.movie_person mp JOIN target.movie m USING(movie_id)
                      WHERE m.stable_key='imdb:tt0000001'),
                    (SELECT count(*) FROM integration.movie_person_source_evidence e
                       JOIN target.movie m USING(movie_id) WHERE m.stable_key='imdb:tt0000001'),
                    (SELECT count(*) FROM raw.source_record r JOIN meta.source_file f USING(source_file_id)
                       JOIN meta.external_snapshot_file sf ON sf.file_sha256=f.sha256
                      WHERE sf.snapshot_id=%s)""",
                (snapshot_id,),
            ).fetchone()
        result.update({
            "snapshot_registered": registered["status"] == "fully_verified",
            "multi_asin_single_title_scope": {"titles": multi_counts[0], "products": multi_counts[1]},
            "precommit": {
                "failure_injected": pre_failed, "checkpoint_before_resume": pre_checkpoint,
                "resume_discarded": pre_resumed["files"]["title.basics.tsv.gz"]["resume_discarded"],
            },
            "postcommit": {
                "failure_injected": post_failed, "checkpoint_before_resume": post_checkpoint,
                "resume_discarded": post_resumed["files"]["title.basics.tsv.gz"]["resume_discarded"],
            },
            "idempotent_repeat": repeated["movies"] == 1 and repeated["persons"] == 3,
            "content_conflict_detected": conflict_detected,
            "movie_count": counts[0], "person_count": counts[1],
            "relationship_fact_count": counts[2], "relationship_evidence_count": counts[3],
            "raw_source_record_count": counts[4],
        })
    result["all_checks_passed"] = all([
        result["snapshot_registered"], pre_failed, post_failed, pre_checkpoint == 0,
        result["multi_asin_single_title_scope"] == {"titles": 1, "products": 2},
        post_checkpoint == 2, result["postcommit"]["resume_discarded"] == 2,
        result["idempotent_repeat"], conflict_detected, result["movie_count"] == 1,
        result["person_count"] == 3, result["relationship_fact_count"] == 3,
    ])
    print(json.dumps(result, indent=2, default=str))
    if not result["all_checks_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
