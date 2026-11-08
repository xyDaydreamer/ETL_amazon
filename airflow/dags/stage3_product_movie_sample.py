from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task


CONFIG_PATH = Path("/opt/airflow/project/config/stage3_sample.json")
SNAPSHOT_DIR = Path("/opt/airflow/project/data/snapshots/stage3")


with DAG(
    dag_id="stage3_product_movie_sample",
    description="Bounded UCSD metadata, IMDb candidate verification, and one compliant page check",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("stage3-sample", type="string", minLength=1, maxLength=100),
        "network_authorized": Param(False, type="boolean"),
    },
    tags=["stage3", "bounded", "product-movie"],
) as dag:

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def load_offline_metadata() -> dict[str, object]:
        from amazon_movies.stage3_pipeline import load_ucsd_sample

        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        batch = str(get_current_context()["params"]["batch_id"])
        result = load_ucsd_sample(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], asins=config["asins"],
            max_records=int(config["ucsd_max_records"]), batch_id=f"{batch}-offline",
        )
        return {k: result[k] for k in ("run_id", "requested_asins", "matched_asins", "scanned_records")}

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def verify_movie_candidates(offline: dict[str, object]) -> dict[str, object]:
        from amazon_movies.stage3_pipeline import verify_imdb_candidate

        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        batch = str(get_current_context()["params"]["batch_id"])
        results = []
        for candidate in config["imdb_candidates"]:
            results.append(verify_imdb_candidate(
                os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], asin=candidate["asin"],
                tconst=candidate["tconst"], suggestion_url=candidate["suggestion_url"],
                support_url=candidate["support_url"], batch_id=f"{batch}-candidate-{candidate['asin']}",
            ))
        return {"offline_run_id": offline["run_id"], "verified": sum(r["decision"] == "verified" for r in results)}

    @task(retries=0)
    def validate_one_page(candidate_summary: dict[str, object]) -> dict[str, object]:
        from amazon_movies.stage3_crawler import crawl

        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        params = get_current_context()["params"]
        batch = str(params["batch_id"])
        if not bool(params["network_authorized"]):
            return {
                "candidate_verified": candidate_summary["verified"],
                "page_run_id": None, "network_blocked": True,
                "access_basis": "existing-captcha-no-written-authorization",
            }
        result = crawl(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], config["page_validation_asins"],
            f"{batch}-page", SNAPSHOT_DIR,
        )
        return {"candidate_verified": candidate_summary["verified"], "page_run_id": result["run_id"], **result}

    @task(retries=0)
    def report(page_summary: dict[str, object]) -> dict[str, object]:
        from amazon_movies.stage3_pipeline import sample_report

        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        result = sample_report(os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], config["asins"])
        return {"page_run_id": page_summary.get("page_run_id"),
                "network_blocked": page_summary.get("network_blocked", False), **result}

    report(validate_one_page(verify_movie_candidates(load_offline_metadata())))
