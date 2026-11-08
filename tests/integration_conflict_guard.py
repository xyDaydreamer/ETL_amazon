from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

import psycopg

from amazon_movies.review_loader import ContentConflictError, _insert_review
from amazon_movies.review_parser import iter_reviews


SOURCE_SHA256 = "4b6ee09ea4880f3fb197b1407d45dfb08a892fbb48a9e7a2d74aa0572cdcc715"


def main() -> None:
    database_url = os.environ["AMAZON_MOVIES_DATABASE_URL"]
    original = next(
        iter_reviews(
            Path("/opt/airflow/data/movies.txt.gz"),
            source_file_sha256=SOURCE_SHA256,
            max_records=1,
        )
    )
    constructed = replace(original, content_sha256="f" * 64)
    conflict_message = None

    with psycopg.connect(database_url, autocommit=True) as conn:
        source_row = conn.execute(
            "SELECT source_file_id FROM meta.source_file WHERE sha256 = %s",
            (SOURCE_SHA256,),
        ).fetchone()
        run_row = conn.execute(
            "SELECT run_id FROM meta.etl_run ORDER BY run_id LIMIT 1"
        ).fetchone()
        if source_row is None or run_row is None:
            raise RuntimeError("integration test requires the completed stage1 sample fixture")
        try:
            with conn.transaction():
                _insert_review(
                    conn,
                    review=constructed,
                    source_file_id=int(source_row[0]),
                    run_id=int(run_row[0]),
                )
        except ContentConflictError as exc:
            conflict_message = str(exc)

        stored = conn.execute(
            """
            SELECT sr.content_sha256, rr.content_sha256
            FROM raw.source_record sr
            JOIN staging.review_record rr USING (source_record_id)
            WHERE rr.source_review_id = %s
            """,
            (original.source_review_id,),
        ).fetchone()

    result = {
        "data_kind": "constructed_conflict_test",
        "source_review_id": original.source_review_id,
        "conflict_raised": conflict_message is not None,
        "conflict_message": conflict_message,
        "original_content_preserved": stored == (original.content_sha256, original.content_sha256),
    }
    print(json.dumps(result, indent=2))
    if not result["conflict_raised"] or not result["original_content_preserved"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
