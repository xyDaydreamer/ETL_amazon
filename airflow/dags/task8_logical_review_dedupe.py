from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task


LOG = logging.getLogger(__name__)
RULE_VERSION = "logical-review-exact-v1"
MONITOR_PATH = "/opt/airflow/docker-disk-host"


with DAG(
    dag_id="task8_logical_review_dedupe",
    description="Conservative exact logical-review deduplication with transactional checkpoints",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("task8-logical-review-full-v1", type="string", minLength=1, maxLength=120),
        "batch_size": Param(10000, type="integer", minimum=1000, maximum=50000),
        "min_free_bytes": Param(53687091200, type="integer", minimum=10737418240),
        "monitor_interval_batches": Param(5, type="integer", minimum=1, maximum=100),
    },
    tags=["task8", "logical-review", "dedupe", "full"],
) as dag:

    @task(retries=0)
    def build_logical_reviews() -> dict[str, object]:
        from amazon_movies.logical_review_dedupe import deduplicate_reviews
        params = get_current_context()["params"]
        result = deduplicate_reviews(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
            batch_id=str(params["batch_id"]), scope_key="full-v3-exact-v1",
            rule_version=RULE_VERSION, batch_size=int(params["batch_size"]), full=True,
            monitor_path=Path(MONITOR_PATH), min_free_bytes=int(params["min_free_bytes"]),
            monitor_interval_batches=int(params["monitor_interval_batches"]),
        )
        LOG.info("task8 committed checkpoint=%s processed=%s status=%s",
                 result["checkpoint_after"], result["processed_input"], result["status"])
        return {key: result[key] for key in (
            "run_id", "rule_version", "parser_version", "scope_key", "checkpoint_after",
            "processed_input", "logical_reviews_inserted", "mappings_inserted",
            "batches_committed", "status", "last_free_bytes", "elapsed_seconds",
        )}

    @task(retries=0)
    def validate_and_activate(summary: dict[str, object]) -> dict[str, object]:
        from amazon_movies.logical_review_dedupe import activate_release
        if summary["status"] != "full_complete":
            raise RuntimeError(f"logical-review full scope is incomplete: {summary}")
        result = activate_release(os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], RULE_VERSION)
        LOG.info("task8 activated rule=%s input=%s logical=%s duplicates=%s",
                 RULE_VERSION, result["input_count"], result["logical_count"],
                 result["duplicate_record_count"])
        return result

    @task(trigger_rule="one_failed", retries=0)
    def fail_after_logging() -> None:
        LOG.error("task8 stopped; inspect disk threshold and meta.logical_review_progress before rerun")
        raise RuntimeError("task8 logical-review dedupe failed; resume with the same rule and scope")

    built = build_logical_reviews()
    activated = validate_and_activate(built)
    failure = fail_after_logging()
    [built, activated] >> failure
