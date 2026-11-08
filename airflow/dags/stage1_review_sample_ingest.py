from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task


LOG = logging.getLogger(__name__)
SOURCE_SHA256 = "4b6ee09ea4880f3fb197b1407d45dfb08a892fbb48a9e7a2d74aa0572cdcc715"
SOURCE_PATH = "/opt/airflow/data/movies.txt.gz"
SOURCE_URI = "file:///D:/amazon/movies.txt.gz"
PARSER_VERSION = "snap-movies-review-v3"


with DAG(
    dag_id="stage1_review_sample_ingest",
    description="Bounded SNAP review sample ingest using the independently tested business module",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("stage1-airflow-sample", type="string", minLength=1, maxLength=100),
        "sample_limit": Param(1000, type="integer", minimum=1, maximum=1000),
        "batch_size": Param(200, type="integer", minimum=1, maximum=500),
        "parser_version": Param(PARSER_VERSION, enum=[PARSER_VERSION]),
        "scope_start_record": Param(1, type="integer", const=1),
        "load_strategy": Param("copy", enum=["copy", "row"]),
        "verify_retry_after_commit": Param(False, type="boolean"),
    },
    tags=["stage1", "sample", "review-ingest"],
) as dag:

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def load_bounded_sample() -> dict[str, object]:
        # Imports and all I/O happen at task runtime, never while the DAG is imported.
        from amazon_movies.review_loader_v2 import load_review_sample

        context = get_current_context()
        params = context["params"]
        batch_id = str(params["batch_id"])
        sample_limit = int(params["sample_limit"])
        batch_size = int(params["batch_size"])
        verify_retry = bool(params["verify_retry_after_commit"])
        parser_version = str(params["parser_version"])
        if parser_version != PARSER_VERSION:
            raise ValueError(f"DAG only supports {PARSER_VERSION}, received {parser_version}")
        try_number = int(context["task_instance"].try_number)
        database_url = os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"]
        result = load_review_sample(
            database_url=database_url,
            data_file=Path(SOURCE_PATH),
            source_sha256=SOURCE_SHA256,
            source_uri=SOURCE_URI,
            batch_id=batch_id,
            partition_key=(
                f"airflow-retry-proof-1-{sample_limit}"
                if verify_retry
                else f"real-sample-1-{sample_limit}"
            ),
            sample_limit=sample_limit,
            batch_size=batch_size,
            scope_start_record=int(params["scope_start_record"]),
            load_strategy=str(params["load_strategy"]),
            fail_after_commit_batch=1 if verify_retry and try_number == 1 else None,
        )
        LOG.info(
            "stage1 sample load completed batch_id=%s limit=%s inserted=%s existing=%s "
            "valid=%s quarantined=%s skipped_checkpoint=%s checkpoint=%s",
            batch_id,
            sample_limit,
            result["parse_results_inserted"],
            result["parse_results_already_present"],
            result["valid_records_seen"],
            result["quarantined_records_seen"],
            result["records_skipped_by_checkpoint"],
            result["checkpoint_after"],
        )
        # XCom deliberately contains only identifiers, a path and small counters.
        return {
            "batch_id": batch_id,
            "source_path": SOURCE_PATH,
            "sample_limit": sample_limit,
            "parser_version": parser_version,
            "scope_start_record": int(params["scope_start_record"]),
            "parse_results_inserted": result["parse_results_inserted"],
            "parse_results_already_present": result["parse_results_already_present"],
            "records_skipped_by_checkpoint": result["records_skipped_by_checkpoint"],
            "checkpoint_after": result["checkpoint_after"],
            "scope_status": result["scope_status"],
        }

    @task(trigger_rule="all_success")
    def log_success(summary: dict[str, object]) -> None:
        LOG.info("stage1 bounded sample DAG succeeded summary=%s", summary)

    @task(trigger_rule="one_failed")
    def log_failure() -> None:
        LOG.error("stage1 bounded sample DAG entered failure branch; inspect load_bounded_sample logs")

    load_summary = load_bounded_sample()
    log_success(load_summary)
    load_summary >> log_failure()
