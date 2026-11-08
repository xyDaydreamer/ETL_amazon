"""Freeze and execute the user-approved, bounded 200-ASIN course sample."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import psycopg


BATCH_ID = "product-page-school-permit-sample200-20261108"
PLAN_BATCH = "product-page-sample100-plan-20261107"
ACCESS_BASIS = "用户确认老师已核实学校许可；许可文件未由本项目亲自审阅；用户称无频率和并发数值要求"
SEED = 20261107
WORK_DIR = Path("/opt/airflow/project/data/work/product_collection")


def freeze(conn: psycopg.Connection, path: Path) -> list[str]:
    if path.exists():
        saved = json.loads(path.read_text(encoding="utf-8"))
        if saved["batch_id"] != BATCH_ID or saved["access_basis"] != ACCESS_BASIS:
            raise RuntimeError("frozen plan metadata differs from current run")
        asins = saved["asins"]
    else:
        first = [r[0] for r in conn.execute(
            "SELECT asin FROM staging.product_harvest_work_item WHERE batch_id=%s "
            "ORDER BY selection_rank,asin", (PLAN_BATCH,))]
        if len(first) != 100 or len(set(first)) != 100:
            raise RuntimeError(f"expected 100 unique original ASINs; found {len(first)}")
        extra = [r[0] for r in conn.execute(
            """SELECT h.asin FROM staging.product_harvest h
               WHERE NOT EXISTS (SELECT 1 FROM staging.product_harvest_attempt a WHERE a.asin=h.asin)
                 AND NOT (h.asin = ANY(%s))
               ORDER BY md5(h.asin || ':' || %s::text),h.asin LIMIT 100""",
            (first, SEED))]
        asins = first + extra
        if len(asins) != 200 or len(set(asins)) != 200:
            raise RuntimeError("sample does not contain 200 unique ASINs")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"batch_id": BATCH_ID, "original_batch": PLAN_BATCH,
                                    "random_seed": SEED, "access_basis": ACCESS_BASIS,
                                    "asins": asins}, ensure_ascii=False, indent=2), encoding="utf-8")
    if len(asins) != 200 or len(set(asins)) != 200:
        raise RuntimeError("frozen sample integrity check failed")
    return asins


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    database_url = os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"]
    with psycopg.connect(database_url) as conn:
        asins = freeze(conn, WORK_DIR / "school_permit_sample200_manifest.json")
        attempted = {r[0] for r in conn.execute(
            "SELECT DISTINCT asin FROM staging.product_harvest_attempt WHERE asin=ANY(%s)",
            (asins,))}
        pending = [asin for asin in asins if asin not in attempted]
    print(json.dumps({"batch_id": BATCH_ID, "selected": len(asins),
                      "previously_attempted": len(attempted), "remaining_to_attempt": len(pending),
                      "manifest": str(WORK_DIR / "school_permit_sample200_manifest.json"),
                      "access_basis": ACCESS_BASIS}, ensure_ascii=False), flush=True)
    if args.check:
        return 0
    if not pending:
        return 0
    command = [sys.executable, "-m", "amazon_movies.stage3_crawler", "--database-url", database_url,
               "--asins", ",".join(pending), "--batch-id", BATCH_ID,
               "--snapshot-dir", "/opt/airflow/project/data/snapshots/stage3",
               "--network-authorized", "--max-attempts", "1", "--delay", "5", "--concurrency", "1"]
    result = subprocess.run(command, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
