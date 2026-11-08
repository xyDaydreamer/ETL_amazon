from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import psycopg
from psycopg.types.json import Jsonb

from amazon_movies.review_parser import PARSER_VERSION, ParsedReview, iter_reviews


PIPELINE_NAME = "snap_review_sample_ingest"
RESUME_STRATEGY = "scan_from_start"


class ContentConflictError(RuntimeError):
    def __init__(self, review: ParsedReview, existing_sha256: str) -> None:
        self.review = review
        self.existing_sha256 = existing_sha256
        super().__init__(
            f"content conflict for source_review_id={review.source_review_id} "
            f"record_number={review.record_number}: existing={existing_sha256} incoming={review.content_sha256}"
        )


class SimulatedInterruption(RuntimeError):
    pass


@dataclass
class LoadStats:
    batch_id: str
    run_id: int
    source_file_id: int
    sample_limit: int
    batch_size: int
    records_scanned_from_start: int = 0
    records_skipped_by_checkpoint: int = 0
    records_inserted: int = 0
    records_already_present: int = 0
    valid_records_seen: int = 0
    quarantined_records_seen: int = 0
    quality_issues_inserted: int = 0
    batches_committed: int = 0
    checkpoint_before: int = 0
    checkpoint_after: int = 0
    resume_strategy: str = RESUME_STRATEGY
    scope_status: str = "sample_in_progress"

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _register_source_file(
    conn: psycopg.Connection,
    *,
    source_uri: str,
    source_sha256: str,
    size_bytes: int,
) -> int:
    row = conn.execute(
        """
        INSERT INTO meta.source_file (
            source_name, source_uri, sha256, size_bytes, parser_version,
            hash_verification_method, hash_verified_at, gzip_integrity_status
        )
        VALUES ('SNAP Amazon Movies and TV reviews', %s, %s, %s, %s,
                'reused_stage0_sha256_after_size_check', NULL, 'sample_readable')
        ON CONFLICT (sha256) DO UPDATE
        SET parser_version = EXCLUDED.parser_version,
            gzip_integrity_status = CASE
                WHEN meta.source_file.gzip_integrity_status = 'verified_complete'
                    THEN 'verified_complete'
                ELSE 'sample_readable'
            END
        RETURNING source_file_id, size_bytes
        """,
        (source_uri, source_sha256.lower(), size_bytes, PARSER_VERSION),
    ).fetchone()
    assert row is not None
    source_file_id, recorded_size = row
    if int(recorded_size) != size_bytes:
        raise ValueError(
            f"source SHA-256 is already registered with size {recorded_size}, incoming size is {size_bytes}"
        )
    return int(source_file_id)


def _start_run(
    conn: psycopg.Connection,
    *,
    batch_id: str,
    source_sha256: str,
    sample_limit: int,
) -> int:
    row = conn.execute(
        """
        INSERT INTO meta.etl_run (
            pipeline_name, batch_id, input_version, rule_version, status,
            scope_kind, requested_record_limit, is_full_source, stats
        )
        VALUES (%s, %s, %s, %s, 'running', 'real_sample', %s, false, '{}'::jsonb)
        ON CONFLICT (pipeline_name, batch_id, input_version, rule_version)
        DO UPDATE SET status = 'running', started_at = clock_timestamp(), finished_at = NULL
        RETURNING run_id
        """,
        (PIPELINE_NAME, batch_id, source_sha256.lower(), PARSER_VERSION, sample_limit),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _checkpoint_before(
    conn: psycopg.Connection,
    *,
    source_sha256: str,
    partition_key: str,
) -> int:
    row = conn.execute(
        """
        SELECT committed_record_number
        FROM meta.batch_progress
        WHERE pipeline_name = %s AND input_version = %s AND partition_key = %s
        """,
        (PIPELINE_NAME, source_sha256.lower(), partition_key),
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _insert_review(
    conn: psycopg.Connection,
    *,
    review: ParsedReview,
    source_file_id: int,
    run_id: int,
) -> tuple[bool, int]:
    existing = conn.execute(
        """
        SELECT source_record_id, content_sha256
        FROM raw.source_record
        WHERE source_kind = 'review_file' AND source_file_id = %s AND record_number = %s
        """,
        (source_file_id, review.record_number),
    ).fetchone()
    if existing is not None:
        existing_sha = str(existing[1])
        if existing_sha != review.content_sha256:
            raise ContentConflictError(review, existing_sha)
        staged = conn.execute(
            "SELECT content_sha256 FROM staging.review_record WHERE source_review_id = %s",
            (review.source_review_id,),
        ).fetchone()
        if staged is None or str(staged[0]) != review.content_sha256:
            raise ContentConflictError(review, str(staged[0]) if staged else "missing_staging_row")
        return False, 0

    locator = {
        "record_number": review.record_number,
        "start_line": review.start_line,
        "end_line": review.end_line,
        "uncompressed_start": review.uncompressed_start,
        "uncompressed_end": review.uncompressed_end,
        "offset_semantics": "uncompressed_stream_offset_not_random_gzip_seek",
    }
    row = conn.execute(
        """
        INSERT INTO raw.source_record (
            source_kind, source_file_id, record_number, field_locator,
            content_sha256, access_status, raw_payload
        )
        VALUES ('review_file', %s, %s, %s, %s, 'sample_parsed', %s)
        RETURNING source_record_id
        """,
        (
            source_file_id,
            review.record_number,
            Jsonb(locator),
            review.content_sha256,
            Jsonb(review.raw_payload),
        ),
    ).fetchone()
    assert row is not None
    source_record_id = int(row[0])

    conn.execute(
        """
        INSERT INTO staging.review_record (
            source_review_id, source_record_id,
            asin_raw, asin, reviewer_id_raw, reviewer_id,
            profile_name_raw, profile_name_normalized,
            helpfulness_raw, helpful_votes, total_votes, helpful_ratio,
            score_raw, score, review_time_raw, review_unix_seconds, review_time_utc,
            summary_raw, summary_normalized, review_text_raw, review_text_normalized,
            parse_status, parse_issues, content_sha256, parser_version, record_number, run_id
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            review.source_review_id,
            source_record_id,
            review.asin_raw,
            review.asin,
            review.reviewer_id_raw,
            review.reviewer_id,
            review.profile_name_raw,
            review.profile_name_normalized,
            review.helpfulness_raw,
            review.helpful_votes,
            review.total_votes,
            review.helpful_ratio,
            review.score_raw,
            review.score,
            review.review_time_raw,
            review.review_unix_seconds,
            review.review_time_utc,
            review.summary_raw,
            review.summary_normalized,
            review.review_text_raw,
            review.review_text_normalized,
            review.parse_status,
            Jsonb([issue.as_dict() for issue in review.issues]),
            review.content_sha256,
            PARSER_VERSION,
            review.record_number,
            run_id,
        ),
    )

    issue_count = 0
    for issue in review.issues:
        conn.execute(
            """
            INSERT INTO meta.quality_issue (
                run_id, issue_code, severity, entity_type, entity_key,
                source_record_id, details
            )
            VALUES (%s, %s, %s, 'source_review', %s, %s, %s)
            """,
            (
                run_id,
                issue.code,
                issue.severity,
                review.source_review_id,
                source_record_id,
                Jsonb(issue.as_dict()),
            ),
        )
        issue_count += 1

    if review.parse_status == "valid" and review.asin is not None:
        conn.execute(
            """
            INSERT INTO target.product_edition (asin, created_run_id)
            VALUES (%s, %s)
            ON CONFLICT (asin) DO NOTHING
            """,
            (review.asin, run_id),
        )
    if review.parse_status == "valid" and review.reviewer_id is not None:
        conn.execute(
            """
            INSERT INTO target.reviewer (reviewer_id, latest_profile_name)
            VALUES (%s, %s)
            ON CONFLICT (reviewer_id) DO UPDATE
            SET latest_profile_name = COALESCE(EXCLUDED.latest_profile_name, target.reviewer.latest_profile_name)
            """,
            (review.reviewer_id, review.profile_name_normalized),
        )
    return True, issue_count


def _record_conflict(
    conn: psycopg.Connection,
    *,
    run_id: int,
    conflict: ContentConflictError,
) -> None:
    conn.execute(
        """
        INSERT INTO meta.quality_issue (
            run_id, issue_code, severity, entity_type, entity_key, details
        )
        VALUES (%s, 'source_review_content_conflict', 'fatal', 'source_review', %s, %s)
        """,
        (
            run_id,
            conflict.review.source_review_id,
            Jsonb(
                {
                    "record_number": conflict.review.record_number,
                    "existing_content_sha256": conflict.existing_sha256,
                    "incoming_content_sha256": conflict.review.content_sha256,
                }
            ),
        ),
    )


def load_review_sample(
    *,
    database_url: str,
    data_file: Path,
    source_sha256: str,
    source_uri: str,
    batch_id: str,
    partition_key: str,
    sample_limit: int = 1000,
    batch_size: int = 200,
    fail_before_commit_batch: int | None = None,
    fail_after_commit_batch: int | None = None,
) -> dict[str, object]:
    if sample_limit <= 0:
        raise ValueError("sample_limit must be positive")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    source_sha256 = source_sha256.lower()
    if len(source_sha256) != 64:
        raise ValueError("source_sha256 must contain 64 hexadecimal characters")
    size_bytes = data_file.stat().st_size

    with psycopg.connect(database_url, autocommit=True) as conn:
        source_file_id = _register_source_file(
            conn,
            source_uri=source_uri,
            source_sha256=source_sha256,
            size_bytes=size_bytes,
        )
        run_id = _start_run(
            conn,
            batch_id=batch_id,
            source_sha256=source_sha256,
            sample_limit=sample_limit,
        )
        committed_before = _checkpoint_before(
            conn,
            source_sha256=source_sha256,
            partition_key=partition_key,
        )
        stats = LoadStats(
            batch_id=batch_id,
            run_id=run_id,
            source_file_id=source_file_id,
            sample_limit=sample_limit,
            batch_size=batch_size,
            checkpoint_before=committed_before,
            checkpoint_after=committed_before,
        )

        pending: list[ParsedReview] = []
        batch_number = 0

        def commit_batch(batch: Iterable[ParsedReview]) -> None:
            nonlocal batch_number
            items = list(batch)
            if not items:
                return
            batch_number += 1
            batch_inserted = 0
            batch_existing = 0
            batch_issue_count = 0
            with conn.transaction():
                for review in items:
                    inserted, issue_count = _insert_review(
                        conn,
                        review=review,
                        source_file_id=source_file_id,
                        run_id=run_id,
                    )
                    if inserted:
                        batch_inserted += 1
                        batch_issue_count += issue_count
                    else:
                        batch_existing += 1
                last_record = items[-1].record_number
                if fail_before_commit_batch == batch_number:
                    raise SimulatedInterruption(
                        f"simulated interruption before commit for batch {batch_number}"
                    )
                conn.execute(
                    """
                    INSERT INTO meta.batch_progress (
                        pipeline_name, input_version, partition_key,
                        next_record_number, committed_record_number, run_id,
                        parser_version, requested_record_limit, resume_strategy, scope_status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'sample_in_progress')
                    ON CONFLICT (pipeline_name, input_version, partition_key)
                    DO UPDATE SET
                        next_record_number = EXCLUDED.next_record_number,
                        committed_record_number = EXCLUDED.committed_record_number,
                        run_id = EXCLUDED.run_id,
                        parser_version = EXCLUDED.parser_version,
                        requested_record_limit = EXCLUDED.requested_record_limit,
                        resume_strategy = EXCLUDED.resume_strategy,
                        scope_status = EXCLUDED.scope_status,
                        updated_at = clock_timestamp()
                    """,
                    (
                        PIPELINE_NAME,
                        source_sha256,
                        partition_key,
                        last_record + 1,
                        last_record,
                        run_id,
                        PARSER_VERSION,
                        sample_limit,
                        RESUME_STRATEGY,
                    ),
                )
            stats.records_inserted += batch_inserted
            stats.records_already_present += batch_existing
            stats.quality_issues_inserted += batch_issue_count
            stats.batches_committed += 1
            stats.checkpoint_after = items[-1].record_number
            if fail_after_commit_batch == batch_number:
                raise SimulatedInterruption(
                    f"simulated interruption after commit for batch {batch_number}"
                )

        try:
            for review in iter_reviews(
                data_file,
                source_file_sha256=source_sha256,
                max_records=sample_limit,
            ):
                stats.records_scanned_from_start += 1
                if review.record_number <= committed_before:
                    stats.records_skipped_by_checkpoint += 1
                    continue
                if review.parse_status == "valid":
                    stats.valid_records_seen += 1
                else:
                    stats.quarantined_records_seen += 1
                pending.append(review)
                if len(pending) >= batch_size:
                    commit_batch(pending)
                    pending = []
            commit_batch(pending)

            stats.scope_status = "sample_complete"
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE meta.batch_progress
                    SET scope_status = 'sample_complete', updated_at = clock_timestamp()
                    WHERE pipeline_name = %s AND input_version = %s AND partition_key = %s
                    """,
                    (PIPELINE_NAME, source_sha256, partition_key),
                )
                conn.execute(
                    """
                    UPDATE meta.etl_run
                    SET status = 'succeeded', finished_at = clock_timestamp(), stats = %s
                    WHERE run_id = %s
                    """,
                    (Jsonb(stats.as_dict()), run_id),
                )
            return stats.as_dict()
        except Exception as exc:
            with conn.transaction():
                if isinstance(exc, ContentConflictError):
                    _record_conflict(conn, run_id=run_id, conflict=exc)
                conn.execute(
                    """
                    UPDATE meta.etl_run
                    SET status = 'failed', finished_at = clock_timestamp(),
                        stats = %s
                    WHERE run_id = %s
                    """,
                    (
                        Jsonb({**stats.as_dict(), "error_type": type(exc).__name__, "error": str(exc)}),
                        run_id,
                    ),
                )
            raise


def database_counts(database_url: str) -> dict[str, int]:
    queries = {
        "source_files": "SELECT count(*) FROM meta.source_file",
        "source_records": "SELECT count(*) FROM raw.source_record",
        "staged_reviews": "SELECT count(*) FROM staging.review_record",
        "valid_reviews": "SELECT count(*) FROM staging.review_record WHERE parse_status = 'valid'",
        "quarantined_reviews": "SELECT count(*) FROM staging.review_record WHERE parse_status = 'quarantined'",
        "products": "SELECT count(*) FROM target.product_edition",
        "reviewers": "SELECT count(*) FROM target.reviewer",
        "quality_issues": "SELECT count(*) FROM meta.quality_issue",
        "checkpoints": "SELECT count(*) FROM meta.batch_progress",
    }
    with psycopg.connect(database_url) as conn:
        return {name: int(conn.execute(query).fetchone()[0]) for name, query in queries.items()}


def compact_result(result: dict[str, object], counts: dict[str, int] | None = None) -> str:
    payload = dict(result)
    if counts is not None:
        payload["database_counts"] = counts
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
