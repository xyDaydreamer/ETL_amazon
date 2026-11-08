from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task


RULE_VERSION = "stage4-first50-movie-match-v3.1-evidence-extraction"
SELECTION_RULE_VERSION = "stage4-first50-movie-match-v1"


with DAG(
    dag_id="stage4_first_movie_matching",
    description="Reevaluate the development sample from cached IMDb responses with Stage 4 v3 evidence rules",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("stage4-airflow-smoke", type="string", minLength=1, maxLength=100),
        "match_limit": Param(50, type="integer", minimum=1, maximum=50),
    },
    tags=["stage4", "bounded", "development-sample", "movie-matching"],
) as dag:

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def select_first_50() -> dict[str, object]:
        from amazon_movies.stage4_matcher import select_development_sample

        batch = str(get_current_context()["params"]["batch_id"])
        result = select_development_sample(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
            batch_id=f"{batch}-select",
            limit=50,
        )
        return {key: result[key] for key in ("run_id", "eligible", "selected", "sample_role")}

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def generate_candidates(selection: dict[str, object]) -> dict[str, object]:
        from amazon_movies.stage4_v3 import reevaluate_cached

        context = get_current_context()
        batch = str(context["params"]["batch_id"])
        limit = int(context["params"]["match_limit"])
        result = reevaluate_cached(
            os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"],
            batch_id=f"{batch}-match-{limit}",
            limit=limit,
        )
        return {
            "selection_run_id": selection["run_id"],
            **{key: result[key] for key in (
                "run_id", "selected", "evaluated_candidates", "rule_verified",
                "needs_review_ambiguous", "needs_review_insufficient",
                "no_suitable_candidate", "no_candidate", "source",
            )},
        }

    @task(retries=0)
    def validate(candidate_stats: dict[str, object]) -> dict[str, object]:
        import psycopg

        with psycopg.connect(os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"]) as conn:
            sample_count, distinct_asins, result_count = conn.execute(
                """
                SELECT
                    (SELECT count(*)
                       FROM integration.movie_match_development_sample
                      WHERE selection_rule_version=%s AND sample_role='development'),
                    (SELECT count(DISTINCT asin)
                       FROM integration.movie_match_development_sample
                      WHERE selection_rule_version=%s AND sample_role='development'),
                    (SELECT count(*)
                       FROM integration.movie_match_candidate_result c
                       JOIN integration.movie_match_development_sample s USING (asin)
                      WHERE c.rule_version=%s
                        AND s.selection_rule_version=%s
                        AND s.sample_role='development')
                """,
                (
                    SELECTION_RULE_VERSION,
                    SELECTION_RULE_VERSION,
                    RULE_VERSION,
                    SELECTION_RULE_VERSION,
                ),
            ).fetchone()
        if sample_count != 50 or distinct_asins != 50 or result_count < int(candidate_stats["selected"]):
            raise RuntimeError(
                f"stage4 validation failed: samples={sample_count}, distinct={distinct_asins}, "
                f"results={result_count}, expected_at_least={candidate_stats['selected']}"
            )
        return {
            "match_run_id": candidate_stats["run_id"],
            "sample_count": sample_count,
            "distinct_asins": distinct_asins,
            "result_count": result_count,
            "rule_version": RULE_VERSION,
        }

    validate(generate_candidates(select_first_50()))
