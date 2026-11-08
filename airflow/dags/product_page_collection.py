from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task


SNAPSHOT_DIR = Path("/opt/airflow/project/data/snapshots/stage3")


with DAG(
    dag_id="product_page_collection",
    description="Bounded, robots-aware, resumable Amazon product-page collection",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("product-page-bounded", type="string", minLength=1, maxLength=100),
        "asins": Param([], type="array", maxItems=100,
                       items={"type": "string", "pattern": "^[A-Z0-9]{10}$"}),
        "sample_limit": Param(100, type="integer", minimum=1, maximum=100),
        "random_seed": Param(20261107, type="integer"),
        "network_authorized": Param(False, type="boolean"),
        "access_basis": Param("existing-captcha-no-written-authorization", type="string", minLength=1),
        "max_attempts": Param(2, type="integer", minimum=1, maximum=3),
        "delay_seconds": Param(5.0, type="number", minimum=5.0, maximum=60.0),
    },
    tags=["product-page", "bounded", "scrapy", "robots-aware"],
) as dag:

    @task(retries=0)
    def collect() -> dict[str, object]:
        from amazon_movies.stage3_crawler import crawl, prepare_sample_plan

        params = get_current_context()["params"]
        explicit = list(params["asins"])
        if explicit:
            if not bool(params["network_authorized"]):
                raise RuntimeError("explicit network collection blocked without documented authorization")
            result = crawl(
                os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], explicit,
                str(params["batch_id"]), SNAPSHOT_DIR,
                max_attempts=int(params["max_attempts"]), delay=float(params["delay_seconds"]),
                concurrency=1,
            )
        else:
            plan = prepare_sample_plan(
                os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], batch_id=str(params["batch_id"]),
                candidate_limit=int(params["sample_limit"]), random_seed=int(params["random_seed"]),
                access_basis=str(params["access_basis"]),
            )
            if not bool(params["network_authorized"]):
                result = {k: v for k, v in plan.items() if k != "asins"}
            else:
                result = crawl(
                    os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], plan["asins"],
                    str(params["batch_id"]), SNAPSHOT_DIR,
                    max_attempts=int(params["max_attempts"]), delay=float(params["delay_seconds"]),
                    concurrency=1,
                )
        return {k: result[k] for k in (
            "run_id", "batch_id", "logical_tasks", "request_attempts",
            "actual_http_requests", "outcomes", "idempotent_replay",
            "selected_count", "candidate_limit", "selection_method", "random_seed",
            "network_requests", "access_basis"
        ) if k in result}

    @task(retries=0)
    def verify(summary: dict[str, object]) -> dict[str, object]:
        from amazon_movies.stage3_crawler import collection_status

        status = collection_status(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], str(summary["batch_id"])
        )
        expected = "succeeded" if bool(get_current_context()["params"]["network_authorized"]) else "prepared"
        if not status["batch"] or status["batch"]["status"] != expected:
            raise RuntimeError(f"collection batch did not reach expected {expected} state")
        return {"batch_id": summary["batch_id"], "batch": status["batch"],
                "product_status": status["product_status"],
                "actual_http_requests": status["actual_target_http_requests"],
                "valid_page_attempts": status["valid_page_attempts"],
                "note": "DAG success means the bounded job finished; see valid_page_attempts for actual page coverage"}

    verify(collect())
