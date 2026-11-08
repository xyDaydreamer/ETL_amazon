from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task


LOG = logging.getLogger(__name__)


with DAG(
    dag_id="p2_movie_expansion",
    description="Freeze 433 movie ASINs, evaluate saved offline candidates, and apply a bounded verified batch",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "scope_key": Param("p2-movie-433-v1", type="string", minLength=1, maxLength=120),
        "batch_id": Param("p2-movie-bounded-v1", type="string", minLength=1, maxLength=120),
        "apply_limit": Param(5, type="integer", minimum=1, maximum=250),
        "baseline_output": Param(
            "/opt/airflow/project/data/work/p2/movie_433_baseline.tsv", type="string"
        ),
    },
    tags=["p2", "movie", "offline", "entity-resolution"],
) as dag:

    @task(retries=1, retry_delay=timedelta(seconds=20))
    def register() -> dict[str, object]:
        from amazon_movies.p2_movie_expansion import register_scope

        p = get_current_context()["params"]
        result = register_scope(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
            scope_key=str(p["scope_key"]), batch_id=f"{p['batch_id']}-register",
            output=Path(str(p["baseline_output"])),
        )
        return {key: result[key] for key in (
            "run_id", "scope_key", "scope_sha256", "asin_count", "baseline_adopted_asins",
            "baseline_adopted_movies", "remaining_asins", "stage6_scope_coverage",
            "collection_member_asins", "output_sha256",
        )}

    @task(retries=1, retry_delay=timedelta(seconds=20))
    def evaluate(summary: dict[str, object]) -> dict[str, object]:
        from amazon_movies.p2_movie_expansion import evaluate_existing_candidates

        p = get_current_context()["params"]
        result = evaluate_existing_candidates(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
            scope_key=str(summary["scope_key"]), batch_id=f"{p['batch_id']}-evaluate",
        )
        return {key: result[key] for key in (
            "run_id", "scope_key", "products", "candidate_rows",
            "candidate_evaluations_inserted", "decisions_inserted", "decision_counts",
        )}

    @task(retries=1, retry_delay=timedelta(seconds=20))
    def apply(evaluated: dict[str, object]) -> dict[str, object]:
        from amazon_movies.p2_movie_expansion import apply_verified

        p = get_current_context()["params"]
        result = apply_verified(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
            scope_key=str(evaluated["scope_key"]), batch_id=f"{p['batch_id']}-apply",
            limit=int(p["apply_limit"]),
        )
        LOG.info("P2 reconciliation after bounded apply: %s", result["reconciliation"])
        return {key: result[key] for key in (
            "run_id", "scope_key", "applied", "relationship_source_links_inserted",
            "remaining_verified_pending", "single_work_conflict_asins", "reconciliation",
        )}

    apply(evaluate(register()))
