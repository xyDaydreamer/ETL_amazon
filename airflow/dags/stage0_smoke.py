from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, Param, get_current_context, task


LOG = logging.getLogger(__name__)


with DAG(
    dag_id="stage0_smoke",
    description="Small runtime/log/database smoke test; never reads the review file at import time",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    params={
        "batch_id": Param("stage0-manual", type="string", minLength=1, maxLength=100),
        "force_failure": Param(False, type="boolean"),
    },
    tags=["stage0", "smoke"],
) as dag:

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def check_business_database() -> dict[str, str]:
        context = get_current_context()
        batch_id = context["params"]["batch_id"]
        if context["params"]["force_failure"]:
            raise RuntimeError(f"intentional stage0 smoke failure batch_id={batch_id}")
        hook = PostgresHook(postgres_conn_id="amazon_movies_db")
        database_name, user_name = hook.get_first("select current_database(), current_user")
        LOG.info("business database connection succeeded batch_id=%s database=%s user=%s", batch_id, database_name, user_name)
        return {"batch_id": batch_id, "database": database_name, "user": user_name}

    @task(trigger_rule="one_failed")
    def log_failure_branch() -> None:
        LOG.error("stage0 smoke entered the explicit failure branch; inspect the upstream task log")

    @task(trigger_rule="all_success")
    def log_success(summary: dict[str, str]) -> None:
        LOG.info("stage0 smoke completed summary=%s", summary)

    database_summary = check_business_database()
    log_success(database_summary)
    database_summary >> log_failure_branch()

