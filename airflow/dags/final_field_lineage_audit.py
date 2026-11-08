from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task


with DAG(
    dag_id="final_field_lineage_audit",
    description="Idempotent evidence-link repair followed by bounded or full final-field lineage coverage audit",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("final-lineage-audit-manual-v1", type="string", minLength=1, maxLength=120),
        "scope_key": Param("bounded-10000-v1", type="string", minLength=1, maxLength=120),
        "full": Param(False, type="boolean"),
        "limit": Param(10000, type="integer", minimum=1, maximum=1000000),
    },
    tags=["lineage", "coverage", "audit", "final-fields"],
) as dag:

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def repair_saved_links() -> dict[str, object]:
        from amazon_movies.lineage_audit import repair_existing_lineage

        params = get_current_context()["params"]
        return repair_existing_lineage(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
            batch_id=f"{params['batch_id']}-repair",
        )

    @task(retries=0)
    def audit_coverage(_: dict[str, object]) -> dict[str, object]:
        from amazon_movies.lineage_audit import mark_audit_failed, run_lineage_audit

        params = get_current_context()["params"]
        database_url = os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"]
        try:
            result = run_lineage_audit(
                database_url, batch_id=str(params["batch_id"]), scope_key=str(params["scope_key"]),
                full=bool(params["full"]), limit=int(params["limit"]),
            )
        except Exception as exc:
            mark_audit_failed(database_url, batch_id=str(params["batch_id"]), error=str(exc))
            raise
        # XCom contains only identifiers and small totals, never field evidence or millions of rows.
        return {key: result[key] for key in (
            "audit_run_id", "audit_version", "full_scope", "active_parser_version",
            "active_dedupe_rule_version", "field_count", "nonempty_sum",
            "complete_lineage_sum", "missing_value_sum", "missing_lineage_sum", "conflict_sum",
        )}

    audit_coverage(repair_saved_links())
