from __future__ import annotations

import os

import psycopg

from amazon_movies.stage3_crawler import (
    BoundedAmazonProductSpider, _prepare_and_claim, prepare_sample_plan,
)
from amazon_movies.stage3_pipeline import _run


def main() -> None:
    production_url = os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"]
    database_url = production_url.rsplit("/", 1)[0] + "/amazon_movies_product_collection_test"
    asin = "TST0000001"
    plan_asins = ["TST0000001", "TST0000002", "TST0000003"]
    batch_id = "product-collection-integration-v1"
    plan_batch_id = "product-collection-plan-integration-v1"
    with psycopg.connect(database_url) as conn:
        conn.execute("DELETE FROM staging.product_harvest_work_item WHERE batch_id=%s", (plan_batch_id,))
        conn.execute("DELETE FROM meta.product_harvest_batch WHERE batch_id=%s", (plan_batch_id,))
        conn.execute("DELETE FROM staging.product_harvest_work_item WHERE batch_id=%s", (batch_id,))
        conn.execute("DELETE FROM meta.product_harvest_batch WHERE batch_id=%s", (batch_id,))
        conn.execute("DELETE FROM staging.product_harvest_attempt WHERE asin=ANY(%s)", (plan_asins,))
        conn.execute("DELETE FROM staging.product_harvest WHERE asin=ANY(%s)", (plan_asins,))
        conn.execute("DELETE FROM raw.source_record WHERE source_uri LIKE %s", (f"fixture://{asin}/%",))
        conn.execute(
            "INSERT INTO staging.product_harvest(asin,original_url,status) VALUES (%s,%s,'not_attempted')",
            (asin, f"https://www.amazon.com/dp/{asin}"),
        )
        with conn.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO staging.product_harvest(asin,original_url,status)
                   VALUES (%s,%s,'not_attempted')""",
                [(value, f"https://www.amazon.com/dp/{value}") for value in plan_asins[1:]],
            )
        run_id = _run(conn, "product_collection_integration_test", batch_id, "fixture-v1")

    plan = prepare_sample_plan(
        database_url, batch_id=plan_batch_id, candidate_limit=3, random_seed=6022026,
        access_basis="integration-test-no-network",
    )
    assert plan["selected_count"] == 3 and plan["network_requests"] == 0
    assert len(set(plan["asins"])) == 3
    replayed_plan = prepare_sample_plan(
        database_url, batch_id=plan_batch_id, candidate_limit=3, random_seed=6022026,
        access_basis="integration-test-no-network",
    )
    assert replayed_plan["idempotent_replay"] and replayed_plan["asins"] == plan["asins"]
    with psycopg.connect(database_url) as conn:
        assert conn.execute(
            "SELECT count(*) FROM staging.product_harvest_attempt WHERE run_id=%s", (plan["run_id"],)
        ).fetchone()[0] == 0

    claimed, replay = _prepare_and_claim(
        database_url, asins=[asin], batch_id=batch_id, run_id=run_id,
        max_attempts=2, delay=5.0, concurrency=1,
    )
    assert claimed == [asin] and not replay
    try:
        _prepare_and_claim(
            database_url, asins=[asin], batch_id=batch_id, run_id=run_id,
            max_attempts=2, delay=5.0, concurrency=1,
        )
    except RuntimeError as exc:
        assert "active claims" in str(exc)
    else:
        raise AssertionError("concurrent batch claim was not rejected")

    spider = BoundedAmazonProductSpider(
        database_url=database_url, batch_id=batch_id, run_id=run_id, asins=asin,
        snapshot_dir="/tmp/product-collection-test", max_attempts=2,
    )
    with psycopg.connect(database_url) as conn:
        valid_source = int(conn.execute(
            """INSERT INTO raw.source_record(source_kind,source_uri,content_sha256,http_status,access_status)
               VALUES ('http_response',%s,%s,200,'success') RETURNING source_record_id""",
            (f"fixture://{asin}/valid", "1" * 64),
        ).fetchone()[0])
        failed_source = int(conn.execute(
            """INSERT INTO raw.source_record(source_kind,source_uri,content_sha256,http_status,access_status)
               VALUES ('http_response',%s,%s,200,'access_restricted') RETURNING source_record_id""",
            (f"fixture://{asin}/restricted", "2" * 64),
        ).fetchone()[0])
    spider._record(
        asin=asin, ordinal=1, requested_url=f"fixture://{asin}", final_url=f"fixture://{asin}/valid",
        outcome="success", http_status=200, source_record_id=valid_source, error_code=None,
        retry_reason=None, cache_hit=False, redirect_chain=[], evidence={"fixture": "valid"}, final=False,
    )
    spider._record(
        asin=asin, ordinal=2, requested_url=f"fixture://{asin}", final_url=f"fixture://{asin}/restricted",
        outcome="access_restricted", http_status=200, source_record_id=failed_source,
        error_code="verification_page", retry_reason="fixture", cache_hit=False,
        redirect_chain=[], evidence={"fixture": "restricted"}, final=True,
    )
    with psycopg.connect(database_url) as conn:
        row = conn.execute(
            """SELECT status,attempt_count,source_record_id,valid_source_record_id,
                      latest_source_record_id,last_attempt_status
               FROM staging.product_harvest WHERE asin=%s""", (asin,),
        ).fetchone()
        assert row == ("success", 2, valid_source, valid_source, failed_source, "access_restricted")
        assert conn.execute(
            "SELECT state,request_attempts,final_outcome FROM staging.product_harvest_work_item WHERE batch_id=%s AND asin=%s",
            (batch_id, asin),
        ).fetchone() == ("completed", 2, "access_restricted")
        conn.execute("UPDATE meta.product_harvest_batch SET status='succeeded' WHERE batch_id=%s", (batch_id,))
    claimed, replay = _prepare_and_claim(
        database_url, asins=[asin], batch_id=batch_id, run_id=run_id,
        max_attempts=2, delay=5.0, concurrency=1,
    )
    assert claimed == [] and replay
    print("integration checks passed: seeded 3-ASIN plan/no network, claim lock, recovery state, valid snapshot preservation, replay idempotency")


if __name__ == "__main__":
    main()
