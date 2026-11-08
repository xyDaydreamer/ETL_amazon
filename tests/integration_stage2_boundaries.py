from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
from pathlib import Path

import psycopg

from amazon_movies.review_loader_v2 import LowDiskSpaceError, load_review_full, load_review_sample
from amazon_movies.review_parser_v2 import PARSER_VERSION


def record(number: int, *, reviewer_conflict: bool = False) -> bytes:
    lines = [
        f"product/productId: S{number:09d}".encode(),
        f"review/userId: user-{number}".encode(),
        b"review/profileName: Profile",
        b"review/helpfulness: 0/0",
        b"review/score: 5.0",
        b"review/time: 1000000000",
        b"review/summary: Summary",
        b"review/text: Body",
        b"unknown/test: warning-evidence",
    ]
    if reviewer_conflict:
        lines.append(b"review/userId: conflicting-user")
    return b"\n".join(lines)


def make_gzip(path: Path, count: int, *, reviewer_conflict_at: int | None = None) -> str:
    payload = b"\n\n".join(
        record(i, reviewer_conflict=i == reviewer_conflict_at) for i in range(1, count + 1)
    )
    with gzip.open(path, "wb") as stream:
        stream.write(payload)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    url = os.environ["AMAZON_MOVIES_DATABASE_URL"]
    result: dict[str, object] = {"data_kind": "constructed_stage2_boundaries"}
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        short = root / "short600.gz"
        short_hash = make_gzip(short, 600, reviewer_conflict_at=550)
        first = load_review_sample(
            database_url=url, data_file=short, source_sha256=short_hash,
            source_uri="constructed://short600", batch_id="short-first300",
            partition_key="short-first300", sample_limit=300, batch_size=300,
        )
        mixed = load_review_sample(
            database_url=url, data_file=short, source_sha256=short_hash,
            source_uri="constructed://short600", batch_id="short-request1000",
            partition_key="short-request1000", sample_limit=1000, batch_size=600,
        )
        past = load_review_sample(
            database_url=url, data_file=short, source_sha256=short_hash,
            source_uri="constructed://short600", batch_id="short-past-eof",
            partition_key="short-past-eof", sample_limit=100, batch_size=100,
            scope_start_record=1001,
        )
        with psycopg.connect(url) as conn:
            conflict_observation = int(
                conn.execute(
                    """SELECT count(*) FROM staging.reviewer_profile_observation o
                       JOIN staging.review_record i USING(source_review_id)
                       JOIN raw.source_record r ON r.source_record_id=i.source_record_id
                       JOIN meta.source_file f USING(source_file_id)
                       WHERE f.sha256=%s AND i.record_number=550 AND o.parser_version=%s""",
                    (short_hash, PARSER_VERSION),
                ).fetchone()[0]
            )
            conflict_issue = int(
                conn.execute(
                    """SELECT count(*) FROM meta.quality_issue q JOIN raw.source_record r USING(source_record_id)
                       JOIN meta.source_file f USING(source_file_id)
                       WHERE f.sha256=%s AND r.record_number=550
                         AND q.parser_version=%s AND q.issue_code='conflicting_reviewer_id_fields'""",
                    (short_hash, PARSER_VERSION),
                ).fetchone()[0]
            )

        truncated = root / "truncated.gz"
        truncated_hash_original = make_gzip(truncated, 600)
        del truncated_hash_original
        truncated.write_bytes(truncated.read_bytes()[:-8])
        truncated_hash = hashlib.sha256(truncated.read_bytes()).hexdigest()
        truncated_failed = False
        try:
            load_review_sample(
                database_url=url, data_file=truncated, source_sha256=truncated_hash,
                source_uri="constructed://truncated", batch_id="truncated-request1000",
                partition_key="truncated-request1000", sample_limit=1000, batch_size=200,
            )
        except (EOFError, gzip.BadGzipFile):
            truncated_failed = True

        full = root / "full20.gz"
        full_hash = make_gzip(full, 20)
        full_result = load_review_full(
            database_url=url, data_file=full, source_sha256=full_hash,
            source_uri="constructed://full20", batch_id="full20",
            partition_key="full20", batch_size=10,
        )

        low_disk = root / "lowdisk10.gz"
        low_disk_hash = make_gzip(low_disk, 10)
        low_disk_failed = False
        try:
            load_review_full(
                database_url=url, data_file=low_disk, source_sha256=low_disk_hash,
                source_uri="constructed://lowdisk10", batch_id="lowdisk10",
                partition_key="lowdisk10", batch_size=10, monitor_path=root,
                min_free_bytes=2**63 - 1, monitor_interval_batches=1,
            )
        except LowDiskSpaceError:
            low_disk_failed = True

        with psycopg.connect(url) as conn:
            checkpoints = {
                row[0]: {
                    "committed": int(row[1]), "actual_end": int(row[2] or 0),
                    "status": row[3], "eof": bool(row[4]), "integrity": row[5],
                }
                for row in conn.execute(
                    """SELECT partition_key,committed_record_number,actual_read_end_record,
                              scope_status,reached_eof,source_integrity_status
                       FROM meta.batch_progress
                       WHERE parser_version=%s ORDER BY partition_key""",
                    (PARSER_VERSION,),
                ).fetchall()
            }

    result.update(
        {
            "first_300": first,
            "request_1000_on_600": mixed,
            "start_past_eof": past,
            "truncated_failed": truncated_failed,
            "full20": full_result,
            "low_disk_failed_without_retry": low_disk_failed,
            "conflicting_reviewer_observations": conflict_observation,
            "conflicting_reviewer_issues": conflict_issue,
            "checkpoints": checkpoints,
        }
    )
    checks = [
        first["scope_status"] == "sample_complete" and first["checkpoint_after"] == 300,
        mixed["scope_status"] == "sample_range_short" and mixed["checkpoint_after"] == 600,
        mixed["actual_read_end_record"] == 600 and mixed["reached_eof"],
        mixed["quality_issues_inserted"] == 302,
        past["scope_status"] == "sample_start_past_eof" and past["checkpoint_after"] == 0,
        truncated_failed and checkpoints["truncated-request1000"]["status"] == "source_failed",
        full_result["scope_status"] == "full_complete" and full_result["reached_eof"],
        full_result["source_integrity_status"] == "verified_complete",
        low_disk_failed and checkpoints["lowdisk10"]["committed"] == 0,
        checkpoints["lowdisk10"]["status"] == "stopped_low_disk",
        conflict_observation == 0 and conflict_issue == 1,
    ]
    result["all_checks_passed"] = all(checks)
    print(json.dumps(result, indent=2, default=str))
    if not result["all_checks_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
