from __future__ import annotations

import os
from datetime import datetime, timezone

from airflow.sdk import DAG, Param, get_current_context, task


with DAG(
    dag_id="stage6_candidate_range",
    description="Build a reusable non-development IMDb title/alias candidate cache",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("stage6-candidate-range-v1", type="string", minLength=1, maxLength=120),
        "snapshot_version": Param("imdb-noncommercial-2026-11-05", type="string"),
        "snapshot_dir": Param("/opt/airflow/project/data/raw/imdb/2026-11-05", type="string"),
        "exclusions_file": Param("/opt/airflow/project/data/work/stage5/case_materials/development_exclusions.json", type="string"),
        "scope_key": Param("stage6-independent-candidates-v1", type="string", minLength=1, maxLength=120),
        "random_seed": Param(6022026, type="integer"),
        "batch_size": Param(500000, type="integer", minimum=10000, maximum=2000000),
        "progress_every": Param(5000000, type="integer", minimum=10000, maximum=20000000),
        "limit_per_product": Param(5, type="integer", minimum=1, maximum=20),
    },
    tags=["stage6", "imdb", "candidate-cache", "independent-evaluation"],
) as dag:

    @task(retries=0)
    def build_cache() -> dict[str, object]:
        from pathlib import Path
        from amazon_movies.stage6_candidates import scan_candidate_range

        p = get_current_context()["params"]
        result = scan_candidate_range(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
            snapshot_dir=Path(p["snapshot_dir"]), snapshot_version=str(p["snapshot_version"]),
            exclusions_file=Path(p["exclusions_file"]), scope_key=str(p["scope_key"]),
            batch_id=str(p["batch_id"]), random_seed=int(p["random_seed"]),
            batch_size=int(p["batch_size"]), progress_every=int(p["progress_every"]),
            limit_per_product=int(p["limit_per_product"]),
        )
        return {key: result[key] for key in (
            "run_id", "snapshot_id", "scope_key", "target_products", "candidate_rows",
            "covered_products", "shortlisted_pairs", "resume_strategy",
        )}

    @task(retries=0)
    def verify_cache(summary: dict[str, object]) -> dict[str, object]:
        from amazon_movies.stage6_candidates import candidate_status

        p = get_current_context()["params"]
        status = candidate_status(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], str(p["snapshot_version"]), str(p["scope_key"])
        )
        if status["status"] != "candidates_ready" or len(status["progress"]) != 2:
            raise RuntimeError(f"candidate cache incomplete: {status}")
        if not all(item["status"] == "verified_complete" and item["reached_eof"] for item in status["progress"]):
            raise RuntimeError(f"candidate files incomplete: {status['progress']}")
        return {"scope_key": status["scope_key"], "status": status["status"], "shortlisted_pairs": status["shortlisted_pairs"]}

    verify_cache(build_cache())
