from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task


with DAG(
    dag_id="stage5_imdb_selective_integration",
    description="Load only scoped IMDb title/person evidence from a fully verified fixed snapshot",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("stage5-imdb-selective-manual", type="string", minLength=1, maxLength=120),
        "snapshot_version": Param("imdb-noncommercial-2026-11-05", type="string"),
        "snapshot_dir": Param("/opt/airflow/project/data/raw/imdb/2026-11-05", type="string"),
        "scope": Param("adopted-development", enum=["adopted-development", "explicit"]),
        "scope_key": Param("stage5-adopted-38-v1", type="string", minLength=1, maxLength=120),
        "load_mode": Param("scan", enum=["scan", "evidence_bundle"]),
        "evidence_file": Param(
            "/opt/airflow/project/data/work/stage5/tt0078764-evidence.json", type="string"
        ),
        "tconsts": Param([], type="array", items={"type": "string"}),
        "batch_size": Param(500000, type="integer", minimum=1000, maximum=5000000),
        "progress_every": Param(5000000, type="integer", minimum=1000, maximum=20000000),
    },
    tags=["stage5", "imdb", "fixed-snapshot", "selective", "person-integration"],
) as dag:

    @task(retries=1, retry_delay=timedelta(seconds=60))
    def load_subset() -> dict[str, object]:
        from amazon_movies.imdb_selective_loader import load_evidence_bundle, load_selective_snapshot

        params = get_current_context()["params"]
        if params["load_mode"] == "evidence_bundle":
            result = load_evidence_bundle(
                os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
                evidence_file=params["evidence_file"], snapshot_version=str(params["snapshot_version"]),
                scope_key=str(params["scope_key"]), batch_id=str(params["batch_id"]),
            )
        else:
            result = load_selective_snapshot(
                os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
                snapshot_dir=params["snapshot_dir"], snapshot_version=str(params["snapshot_version"]),
                scope=str(params["scope"]), scope_key=str(params["scope_key"]),
                batch_id=str(params["batch_id"]), explicit_tconsts=list(params["tconsts"]),
                batch_size=int(params["batch_size"]), progress_every=int(params["progress_every"]),
            )
        return {
            key: result[key]
            for key in (
                "run_id", "snapshot_id", "snapshot_version", "scope_key", "target_titles",
                "movies", "persons", "relationship_facts", "missing_titles", "missing_names",
                "person_merges",
            )
        }

    @task(retries=0)
    def validate_subset(summary: dict[str, object]) -> dict[str, object]:
        import psycopg

        params = get_current_context()["params"]
        with psycopg.connect(os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"]) as conn:
            scope = conn.execute(
                """SELECT s.status,s.target_title_count,
                          count(*) FILTER (WHERE p.reached_eof),count(p.file_name)
                   FROM meta.external_snapshot x
                   JOIN meta.imdb_selective_scope s USING(snapshot_id)
                   LEFT JOIN meta.imdb_selective_progress p
                     ON p.snapshot_id=s.snapshot_id AND p.scope_key=s.scope_key
                   WHERE x.snapshot_version=%s AND s.scope_key=%s
                   GROUP BY s.status,s.target_title_count""",
                (params["snapshot_version"], params["scope_key"]),
            ).fetchone()
            frozen = conn.execute(
                """SELECT
                       (SELECT count(*) FROM integration.adopted_development_movie_mapping),
                       (SELECT count(*) FROM integration.development_movie_mapping_review_queue)"""
            ).fetchone()
        expected_files = 0 if params["load_mode"] == "evidence_bundle" else 5
        if (
            not scope or scope[0] != "materialized"
            or scope[2] != expected_files or scope[3] != expected_files
        ):
            raise RuntimeError(f"selective IMDb scope incomplete: {scope}")
        if frozen != (38, 11):
            raise RuntimeError(f"Stage 4 frozen counts changed: {frozen}")
        return {
            "run_id": summary["run_id"], "scope_status": scope[0],
            "target_title_count": scope[1], "files_at_eof": scope[2],
            "stage4_adopted": frozen[0], "stage4_review_queue": frozen[1],
        }

    validate_subset(load_subset())
