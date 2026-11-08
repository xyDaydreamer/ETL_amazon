from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task


LOG = logging.getLogger(__name__)
SOURCE_SHA256 = "4b6ee09ea4880f3fb197b1407d45dfb08a892fbb48a9e7a2d74aa0572cdcc715"
SOURCE_PATH = "/opt/airflow/data/movies.txt.gz"
SOURCE_URI = "file:///D:/amazon/movies.txt.gz"
PARSER_VERSION = "snap-movies-review-v3"
# Docker's virtual filesystem reports a 1 TiB logical limit. The Docker
# Desktop VHDX was migrated to Windows D:, so monitor its host directory.
MONITOR_PATH = "/opt/airflow/docker-disk-host"


with DAG(
    dag_id="stage2_review_full_ingest",
    description="Resume SNAP reviews to verified gzip EOF, validate coverage, then activate v3",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("stage2-full-v3", type="string", minLength=1, maxLength=100),
        "batch_size": Param(5000, type="integer", minimum=500, maximum=20000),
        "min_free_bytes": Param(32212254720, type="integer", minimum=10737418240),
        "monitor_interval_batches": Param(10, type="integer", minimum=1, maximum=100),
    },
    tags=["stage2", "full", "review-ingest"],
) as dag:

    @task(retries=0)
    def load_to_verified_eof() -> dict[str, object]:
        from amazon_movies.review_loader_v2 import load_review_full

        params = get_current_context()["params"]
        database_url = os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"]
        result = load_review_full(
            database_url=database_url,
            data_file=Path(SOURCE_PATH),
            source_sha256=SOURCE_SHA256,
            source_uri=SOURCE_URI,
            batch_id=str(params["batch_id"]),
            partition_key="full-source-v3",
            batch_size=int(params["batch_size"]),
            load_strategy="copy",
            monitor_path=Path(MONITOR_PATH),
            min_free_bytes=int(params["min_free_bytes"]),
            monitor_interval_batches=int(params["monitor_interval_batches"]),
        )
        LOG.info(
            "stage2 full load reached status=%s committed=%s eof=%s integrity=%s elapsed=%s",
            result["scope_status"], result["checkpoint_after"], result["reached_eof"],
            result["source_integrity_status"], result["elapsed_seconds"],
        )
        return {
            "batch_id": str(params["batch_id"]),
            "source_path": SOURCE_PATH,
            "parser_version": PARSER_VERSION,
            "checkpoint_after": result["checkpoint_after"],
            "scope_status": result["scope_status"],
            "reached_eof": result["reached_eof"],
            "source_integrity_status": result["source_integrity_status"],
            "elapsed_seconds": result["elapsed_seconds"],
        }

    @task(retries=0)
    def validate_and_activate(summary: dict[str, object]) -> dict[str, object]:
        from amazon_movies.review_loader_v2 import activate_parser

        if not (
            summary["scope_status"] == "full_complete"
            and summary["reached_eof"]
            and summary["source_integrity_status"] == "verified_complete"
        ):
            raise RuntimeError(f"full completion evidence is insufficient: {summary}")
        activate_parser(os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"], PARSER_VERSION, require_full=True)
        LOG.info("validated and activated parser version=%s", PARSER_VERSION)
        return {
            "parser_version": PARSER_VERSION,
            "activated": True,
            "committed_records": summary["checkpoint_after"],
        }

    @task(trigger_rule="one_failed", retries=0)
    def fail_after_logging() -> None:
        LOG.error("stage2 full ingest stopped; inspect checkpoint, disk evidence and task log before rerun")
        raise RuntimeError("stage2 full ingest failed; see the upstream task and committed checkpoint")

    loaded = load_to_verified_eof()
    activated = validate_and_activate(loaded)
    failure = fail_after_logging()
    [loaded, activated] >> failure
