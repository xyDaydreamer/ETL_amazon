from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
import tempfile
from dataclasses import replace
from pathlib import Path

import psycopg

from amazon_movies.review_loader_v2 import (
    ContentConflictError,
    SimulatedInterruption,
    _insert_review,
    load_review_sample,
)
from amazon_movies.review_parser import restore_record_bytes
from amazon_movies.review_parser_v2 import BIGINT_MAX, PARSER_VERSION, iter_reviews


def record(
    asin: bytes,
    *,
    user: bytes = b"opaque  user",
    profile: bytes = b"Profile",
    helpful: bytes = b"0/0",
    score: bytes = b"5.0",
    when: bytes = b"1000000000",
    summary: bytes = b"Summary",
    body: bytes = b"Body",
    extra: list[bytes] | None = None,
) -> bytes:
    lines = [
        b"product/productId: " + asin,
        b"review/userId: " + user,
        b"review/profileName: " + profile,
        b"review/helpfulness: " + helpful,
        b"review/score: " + score,
        b"review/time: " + when,
        b"review/summary: " + summary,
        b"review/text: " + body,
    ]
    lines.extend(extra or [])
    return b"\n".join(lines)


def write_gzip(records: list[bytes], path: Path) -> tuple[str, list[bytes]]:
    uncompressed = b"\n\n".join(records)
    with gzip.open(path, "wb") as stream:
        stream.write(uncompressed)
    return hashlib.sha256(path.read_bytes()).hexdigest(), records


def count(conn: psycopg.Connection, query: str, params: tuple = ()) -> int:
    return int(conn.execute(query, params).fetchone()[0])


def main() -> None:
    database_url = os.environ["AMAZON_MOVIES_DATABASE_URL"]
    results: dict[str, object] = {"data_kind": "constructed_database_acceptance"}
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "abnormal.gz"
        records = [
            record(b"Z000000001", body=b"bad\xffbody"),
            record(
                b"Z000000002",
                profile=b"p\x00n",
                summary=b"s\x00m",
                body=b"b\x00d",
                extra=[b"unknown: x\x00y"],
            ),
            record(b"", score=b"NaN"),
            record(b"TOO-LONG-ASIN", score=b"Infinity"),
            record(b"Z000000005", score=b"-Infinity"),
            record(b"Z000000006", score=b"4.25"),
            record(
                b"Z000000007",
                helpful=f"{BIGINT_MAX + 1}/{BIGINT_MAX + 1}".encode(),
                when=str(BIGINT_MAX + 1).encode(),
            ),
            record(b"Z000000008", extra=[b"product/productId: Z000000008"]),
            record(b"Z000000009", extra=[b"product/productId: Z000000010"]),
        ]
        source_hash, _ = write_gzip(records, path)
        first = load_review_sample(
            database_url=database_url,
            data_file=path,
            source_sha256=source_hash,
            source_uri="constructed://v2-abnormal",
            batch_id="v2-constructed-abnormal",
            partition_key="v2-constructed-abnormal",
            sample_limit=len(records),
            batch_size=4,
            load_strategy="copy",
        )
        parsed = list(iter_reviews(path, source_file_sha256=source_hash, max_records=len(records)))
        first_review = parsed[0]
        with psycopg.connect(database_url, autocommit=True) as conn:
            raw_payload = conn.execute(
                """SELECT raw_payload FROM raw.source_record r JOIN meta.source_file f USING(source_file_id)
                   WHERE f.sha256=%s AND r.record_number=2""",
                (source_hash,),
            ).fetchone()[0]
            product_exists = count(
                conn, "SELECT count(*) FROM target.product_edition WHERE asin='Z000000001'"
            )
            statuses = dict(
                conn.execute(
                    """SELECT record_number, asin_status FROM staging.review_parse_result
                       WHERE parser_version=%s
                         AND source_review_id IN (SELECT source_review_id FROM staging.review_record
                           WHERE source_record_id IN (SELECT source_record_id FROM raw.source_record
                             WHERE source_file_id=(SELECT source_file_id FROM meta.source_file WHERE sha256=%s)))
                       ORDER BY record_number""",
                    (PARSER_VERSION, source_hash),
                ).fetchall()
            )
            normalized_nulls = count(
                conn,
                """SELECT count(*) FROM staging.review_parse_result p JOIN staging.review_record i USING(source_review_id)
                   JOIN raw.source_record r ON r.source_record_id=i.source_record_id
                   JOIN meta.source_file f USING(source_file_id)
                   WHERE f.sha256=%s AND p.parser_version=%s
                     AND p.record_number IN (3,4,5,6) AND p.score IS NULL""",
                (source_hash, PARSER_VERSION),
            )
            conflict_raised = False
            try:
                with conn.transaction():
                    _insert_review(
                        conn,
                        review=replace(first_review, content_sha256="f" * 64),
                        source_file_id=int(
                            conn.execute(
                                "SELECT source_file_id FROM meta.source_file WHERE sha256=%s", (source_hash,)
                            ).fetchone()[0]
                        ),
                        run_id=int(first["run_id"]),
                    )
            except ContentConflictError:
                conflict_raised = True
        decoded = base64.b64decode(raw_payload["raw_record_base64"])
        restored, restored_hash = restore_record_bytes(
            path,
            uncompressed_start=first_review.uncompressed_start,
            uncompressed_end=first_review.uncompressed_end,
            expected_sha256=first_review.content_sha256,
        )
        results.update(
            {
                "constructed_records": len(records),
                "copy_inserted": first["parse_results_inserted"],
                "invalid_body_contributed_product": product_exists == 1,
                "asin_statuses": statuses,
                "score_nulls_for_nonfinite_precision": normalized_nulls,
                "jsonb_has_no_nul_and_base64_roundtrips": decoded == records[1] + b"\n",
                "locator_hash_roundtrip": restored == records[0] + b"\n" and restored_hash == first_review.content_sha256,
                "same_id_different_content_conflict": conflict_raised,
            }
        )

        recovery_path = Path(directory) / "recovery.gz"
        recovery_records = [record(f"R{i:09d}".encode()) for i in range(1, 7)]
        recovery_hash, _ = write_gzip(recovery_records, recovery_path)
        pre_failed = post_failed = False
        try:
            load_review_sample(
                database_url=database_url, data_file=recovery_path, source_sha256=recovery_hash,
                source_uri="constructed://v2-recovery", batch_id="v2-precommit-recovery",
                partition_key="v2-precommit-recovery", sample_limit=6, batch_size=2,
                fail_before_commit_batch=1,
            )
        except SimulatedInterruption:
            pre_failed = True
        pre_resumed = load_review_sample(
            database_url=database_url, data_file=recovery_path, source_sha256=recovery_hash,
            source_uri="constructed://v2-recovery", batch_id="v2-precommit-recovery",
            partition_key="v2-precommit-recovery", sample_limit=6, batch_size=2,
        )
        try:
            load_review_sample(
                database_url=database_url, data_file=recovery_path, source_sha256=recovery_hash,
                source_uri="constructed://v2-recovery", batch_id="v2-postcommit-recovery",
                partition_key="v2-postcommit-recovery", sample_limit=6, batch_size=2,
                fail_after_commit_batch=1,
            )
        except SimulatedInterruption:
            post_failed = True
        post_resumed = load_review_sample(
            database_url=database_url, data_file=recovery_path, source_sha256=recovery_hash,
            source_uri="constructed://v2-recovery", batch_id="v2-postcommit-recovery",
            partition_key="v2-postcommit-recovery", sample_limit=6, batch_size=2,
        )
        results["precommit_recovery"] = {
            "injected_failure": pre_failed,
            "checkpoint_before_resume": pre_resumed["checkpoint_before"],
            "inserted_after_resume": pre_resumed["parse_results_inserted"],
        }
        results["postcommit_recovery"] = {
            "injected_failure": post_failed,
            "checkpoint_before_resume": post_resumed["checkpoint_before"],
            "skipped_after_resume": post_resumed["records_skipped_by_checkpoint"],
            "parse_results_already_present": post_resumed["parse_results_already_present"],
        }

    checks = [
        results["invalid_body_contributed_product"],
        results["score_nulls_for_nonfinite_precision"] == 4,
        results["jsonb_has_no_nul_and_base64_roundtrips"],
        results["locator_hash_roundtrip"],
        results["same_id_different_content_conflict"],
        pre_failed,
        post_failed,
        pre_resumed["checkpoint_before"] == 0,
        post_resumed["checkpoint_before"] == 2,
        post_resumed["records_skipped_by_checkpoint"] == 2,
    ]
    results["all_checks_passed"] = all(checks)
    print(json.dumps(results, indent=2, default=str))
    if not results["all_checks_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
