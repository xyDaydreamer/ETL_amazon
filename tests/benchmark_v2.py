from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import psycopg

from amazon_movies.review_loader_v2 import load_review_sample


SOURCE_SHA256 = "4b6ee09ea4880f3fb197b1407d45dfb08a892fbb48a9e7a2d74aa0572cdcc715"


def database_size(url: str) -> int:
    with psycopg.connect(url) as conn:
        return int(conn.execute("SELECT pg_database_size(current_database())").fetchone()[0])


def run(url: str, strategy: str, limit: int, suffix: str) -> tuple[float, dict[str, object]]:
    started = time.perf_counter()
    result = load_review_sample(
        database_url=url,
        data_file=Path("/opt/airflow/data/movies.txt.gz"),
        source_sha256=SOURCE_SHA256,
        source_uri="file:///D:/amazon/movies.txt.gz",
        batch_id=f"benchmark-{strategy}-{limit}-{suffix}",
        partition_key=f"benchmark-{strategy}-{limit}-{suffix}",
        sample_limit=limit,
        batch_size=500,
        load_strategy=strategy,
    )
    return time.perf_counter() - started, result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", choices=("row", "copy"), required=True)
    parser.add_argument("--limit", type=int, choices=(1000, 10000), required=True)
    args = parser.parse_args()
    url = os.environ["AMAZON_MOVIES_DATABASE_URL"]
    size_before = database_size(url)
    first_seconds, first = run(url, args.strategy, args.limit, "first")
    size_after_first = database_size(url)
    repeat_seconds, repeat = run(url, args.strategy, args.limit, "repeat")
    size_after_repeat = database_size(url)
    output = {
        "strategy": args.strategy,
        "limit": args.limit,
        "first": {
            "seconds": round(first_seconds, 3),
            "records_per_second": round(args.limit / first_seconds, 1),
            "raw_inserted": first["raw_records_inserted"],
            "parse_inserted": first["parse_results_inserted"],
            "database_growth_bytes": size_after_first - size_before,
        },
        "repeat": {
            "seconds": round(repeat_seconds, 3),
            "records_per_second": round(args.limit / repeat_seconds, 1),
            "raw_inserted": repeat["raw_records_inserted"],
            "parse_inserted": repeat["parse_results_inserted"],
            "parse_existing": repeat["parse_results_already_present"],
            "database_growth_bytes": size_after_repeat - size_after_first,
        },
        "database_size_bytes": size_after_repeat,
        "scope": "bounded_sample_not_full_source_performance_guarantee",
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
