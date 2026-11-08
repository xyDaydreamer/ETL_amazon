from __future__ import annotations

import gzip
import json
import shutil
import time
import zlib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import psycopg
from psycopg.types.json import Jsonb

from amazon_movies.review_parser_v2 import PARSER_VERSION, ParsedReview, StreamReadState, iter_reviews


PIPELINE_NAME = "snap_review_sample_ingest"
RESUME_STRATEGY = "scan_from_start"


class ContentConflictError(RuntimeError):
    def __init__(self, review: ParsedReview, existing_sha256: str, layer: str) -> None:
        self.review = review
        self.existing_sha256 = existing_sha256
        self.layer = layer
        super().__init__(
            f"content conflict at {layer} for source_review_id={review.source_review_id} "
            f"record_number={review.record_number}: existing={existing_sha256} "
            f"incoming={review.content_sha256}"
        )


class SimulatedInterruption(RuntimeError):
    pass


class LowDiskSpaceError(RuntimeError):
    pass


@dataclass
class LoadStats:
    batch_id: str
    run_id: int
    source_file_id: int
    parser_version: str
    scope_start_record: int
    scope_end_record: int | None
    batch_size: int
    load_strategy: str
    records_scanned_from_start: int = 0
    records_skipped_by_checkpoint: int = 0
    raw_records_inserted: int = 0
    parse_results_inserted: int = 0
    parse_results_already_present: int = 0
    valid_records_seen: int = 0
    quarantined_records_seen: int = 0
    usable_asins_seen: int = 0
    quality_issues_inserted: int = 0
    batches_committed: int = 0
    checkpoint_before: int = 0
    checkpoint_after: int = 0
    resume_strategy: str = RESUME_STRATEGY
    scope_status: str = "sample_in_progress"
    actual_read_end_record: int = 0
    reached_eof: bool = False
    requested_range_satisfied: bool = False
    source_integrity_status: str = "not_checked"
    elapsed_seconds: float = 0.0
    last_free_bytes: int | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _register_source_file(
    conn: psycopg.Connection, *, source_uri: str, source_sha256: str, size_bytes: int
) -> int:
    row = conn.execute(
        """
        INSERT INTO meta.source_file (
            source_name, source_uri, sha256, size_bytes, parser_version,
            hash_verification_method, hash_verified_at, gzip_integrity_status
        )
        VALUES ('SNAP Amazon Movies and TV reviews', %s, %s, %s, NULL,
                'reused_stage0_sha256_after_size_check', NULL, 'sample_readable')
        ON CONFLICT (sha256) DO UPDATE
        SET gzip_integrity_status = CASE
                WHEN meta.source_file.gzip_integrity_status = 'verified_complete'
                    THEN 'verified_complete'
                ELSE 'sample_readable'
            END
        RETURNING source_file_id, size_bytes
        """,
        (source_uri, source_sha256, size_bytes),
    ).fetchone()
    assert row is not None
    if int(row[1]) != size_bytes:
        raise ValueError(f"source hash has recorded size {row[1]}, incoming size is {size_bytes}")
    return int(row[0])


def _start_run(
    conn: psycopg.Connection,
    *,
    batch_id: str,
    source_sha256: str,
    scope_start_record: int,
    requested_record_limit: int | None,
    is_full_source: bool,
) -> int:
    row = conn.execute(
        """
        INSERT INTO meta.etl_run (
            pipeline_name, batch_id, input_version, rule_version, status,
            scope_kind, requested_record_limit, is_full_source, stats
        )
        VALUES (%s, %s, %s, %s, 'running', %s, %s, %s, '{}'::jsonb)
        ON CONFLICT (pipeline_name, batch_id, input_version, rule_version)
        DO UPDATE SET status = 'running', started_at = clock_timestamp(), finished_at = NULL
        RETURNING run_id
        """,
        (
            PIPELINE_NAME, batch_id, source_sha256, PARSER_VERSION,
            "full_source" if is_full_source else "real_sample",
            requested_record_limit, is_full_source,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _checkpoint_before(
    conn: psycopg.Connection,
    *,
    source_sha256: str,
    partition_key: str,
    scope_start_record: int,
    scope_end_record: int | None,
) -> int:
    row = conn.execute(
        """
        SELECT committed_record_number, scope_start_record, scope_end_record
        FROM meta.batch_progress
        WHERE pipeline_name = %s AND input_version = %s
          AND parser_version = %s AND partition_key = %s
        """,
        (PIPELINE_NAME, source_sha256, PARSER_VERSION, partition_key),
    ).fetchone()
    if row is None:
        return 0
    recorded_end = int(row[2]) if row[2] is not None else None
    if (int(row[1]), recorded_end) != (scope_start_record, scope_end_record):
        raise ValueError(
            f"partition {partition_key!r} is already bound to range {row[1]}..{row[2]}, "
            f"not {scope_start_record}..{scope_end_record}"
        )
    return int(row[0])


def _locator(review: ParsedReview) -> dict[str, object]:
    return {
        "record_number": review.record_number,
        "start_line": review.start_line,
        "end_line": review.end_line,
        "line_semantics": "inclusive_content_lines_excluding_blank_separator",
        "uncompressed_start": review.uncompressed_start,
        "uncompressed_end": review.uncompressed_end,
        "byte_range_semantics": "half_open_content_bytes_excluding_blank_separator",
        "resume_note": "uncompressed_offset_requires_scan_from_gzip_start",
    }


def _ensure_raw_identity(
    conn: psycopg.Connection,
    *,
    review: ParsedReview,
    source_file_id: int,
) -> tuple[int, bool]:
    row = conn.execute(
        """
        SELECT source_record_id, content_sha256
        FROM raw.source_record
        WHERE source_kind = 'review_file' AND source_file_id = %s AND record_number = %s
        """,
        (source_file_id, review.record_number),
    ).fetchone()
    inserted = False
    if row is None:
        row = conn.execute(
            """
            INSERT INTO raw.source_record (
                source_kind, source_file_id, record_number, field_locator,
                content_sha256, access_status, raw_payload
            )
            VALUES ('review_file', %s, %s, %s, %s, 'sample_parsed', %s)
            RETURNING source_record_id, content_sha256
            """,
            (
                source_file_id,
                review.record_number,
                Jsonb(_locator(review)),
                review.content_sha256,
                Jsonb(review.raw_payload),
            ),
        ).fetchone()
        inserted = True
    assert row is not None
    if str(row[1]) != review.content_sha256:
        raise ContentConflictError(review, str(row[1]), "raw.source_record")
    source_record_id = int(row[0])
    identity = conn.execute(
        """
        SELECT source_record_id, content_sha256
        FROM staging.review_record WHERE source_review_id = %s
        """,
        (review.source_review_id,),
    ).fetchone()
    if identity is None:
        conn.execute(
            """
            INSERT INTO staging.review_record (
                source_review_id, source_record_id, record_number, content_sha256
            ) VALUES (%s, %s, %s, %s)
            """,
            (review.source_review_id, source_record_id, review.record_number, review.content_sha256),
        )
    elif int(identity[0]) != source_record_id or str(identity[1]) != review.content_sha256:
        raise ContentConflictError(review, str(identity[1]), "staging.review_record")
    return source_record_id, inserted


def _insert_versioned_result(
    conn: psycopg.Connection,
    *,
    review: ParsedReview,
    source_record_id: int,
    run_id: int,
) -> tuple[bool, int]:
    conn.execute(
        """
        INSERT INTO staging.review_parse_locator (
            source_review_id, parser_version, start_line, end_line,
            uncompressed_start, uncompressed_end, content_sha256,
            line_semantics, byte_range_semantics, run_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,
                  'inclusive_content_lines_excluding_blank_separator',
                  'half_open_content_bytes_excluding_blank_separator',%s)
        ON CONFLICT (source_review_id, parser_version) DO NOTHING
        """,
        (
            review.source_review_id, PARSER_VERSION, review.start_line, review.end_line,
            review.uncompressed_start, review.uncompressed_end, review.content_sha256, run_id,
        ),
    )
    row = conn.execute(
        """
        SELECT content_sha256 FROM staging.review_parse_result
        WHERE source_review_id = %s AND parser_version = %s
        """,
        (review.source_review_id, PARSER_VERSION),
    ).fetchone()
    if row is not None:
        if str(row[0]) != review.content_sha256:
            raise ContentConflictError(review, str(row[0]), "staging.review_parse_result")
        return False, 0

    conn.execute(
        """
        INSERT INTO staging.review_parse_result (
            source_review_id, source_record_id, asin_raw, asin, asin_status,
            reviewer_id_raw, reviewer_id, profile_name_raw, profile_name_normalized,
            helpfulness_raw, helpful_votes, total_votes, helpful_ratio,
            score_raw, score, review_time_raw, review_unix_seconds, review_time_utc,
            summary_raw, summary_normalized, review_text_raw, review_text_normalized,
            parse_status, parse_issues, content_sha256, parser_version, record_number, run_id
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            review.source_review_id, source_record_id, review.asin_raw, review.asin,
            review.asin_status, review.reviewer_id_raw, review.reviewer_id,
            review.profile_name_raw, review.profile_name_normalized, review.helpfulness_raw,
            review.helpful_votes, review.total_votes, review.helpful_ratio, review.score_raw,
            review.score, review.review_time_raw, review.review_unix_seconds,
            review.review_time_utc, review.summary_raw, review.summary_normalized,
            review.review_text_raw, review.review_text_normalized, review.parse_status,
            Jsonb([issue.as_dict() for issue in review.issues]), review.content_sha256,
            PARSER_VERSION, review.record_number, run_id,
        ),
    )
    for issue in review.issues:
        conn.execute(
            """
            INSERT INTO meta.quality_issue (
                run_id, issue_code, severity, entity_type, entity_key,
                source_record_id, details, parser_version
            ) VALUES (%s,%s,%s,'source_review',%s,%s,%s,%s)
            """,
            (
                run_id, issue.code, issue.severity, review.source_review_id,
                source_record_id, Jsonb(issue.as_dict()), PARSER_VERSION,
            ),
        )
    conn.execute(
        """
        INSERT INTO staging.product_asin_observation (
            source_review_id, parser_version, asin_raw, asin, asin_status, evidence, run_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (source_review_id, parser_version) DO NOTHING
        """,
        (
            review.source_review_id, PARSER_VERSION, review.asin_raw, review.asin,
            review.asin_status,
            Jsonb({"field": "product/productId", "decision": review.asin_status}), run_id,
        ),
    )
    if review.asin_status == "usable":
        conn.execute(
            """
            INSERT INTO target.product_edition (asin, created_run_id)
            VALUES (%s, %s) ON CONFLICT (asin) DO NOTHING
            """,
            (review.asin, run_id),
        )
        conn.execute(
            """
            INSERT INTO staging.product_harvest (asin, original_url, status)
            VALUES (%s, %s, 'not_attempted') ON CONFLICT (asin) DO NOTHING
            """,
            (review.asin, f"https://www.amazon.com/dp/{review.asin}"),
        )

    if review.reviewer_id is not None:
        conn.execute(
            """
            INSERT INTO staging.reviewer_profile_observation (
                source_review_id, parser_version, reviewer_id, profile_name_raw,
                profile_name_normalized, observed_at, record_number, run_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source_review_id, parser_version) DO NOTHING
            """,
            (
                review.source_review_id, PARSER_VERSION, review.reviewer_id,
                review.profile_name_raw, review.profile_name_normalized,
                review.review_time_utc, review.record_number, run_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO target.reviewer (
                reviewer_id, latest_profile_name, latest_profile_observed_at,
                latest_profile_record_number, latest_profile_source_review_id
            ) VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (reviewer_id) DO UPDATE SET
                latest_profile_name = EXCLUDED.latest_profile_name,
                latest_profile_observed_at = EXCLUDED.latest_profile_observed_at,
                latest_profile_record_number = EXCLUDED.latest_profile_record_number,
                latest_profile_source_review_id = EXCLUDED.latest_profile_source_review_id
            WHERE (
                COALESCE(EXCLUDED.latest_profile_observed_at, '-infinity'::timestamptz),
                EXCLUDED.latest_profile_record_number,
                EXCLUDED.latest_profile_source_review_id
            ) > (
                COALESCE(target.reviewer.latest_profile_observed_at, '-infinity'::timestamptz),
                COALESCE(target.reviewer.latest_profile_record_number, 0),
                COALESCE(target.reviewer.latest_profile_source_review_id, repeat('0',64)::char(64))
            )
            """,
            (
                review.reviewer_id, review.profile_name_normalized, review.review_time_utc,
                review.record_number, review.source_review_id,
            ),
        )
    return True, len(review.issues)


def _insert_review(
    conn: psycopg.Connection,
    *,
    review: ParsedReview,
    source_file_id: int,
    run_id: int,
) -> tuple[bool, bool, int]:
    source_record_id, raw_inserted = _ensure_raw_identity(
        conn, review=review, source_file_id=source_file_id
    )
    parse_inserted, issue_count = _insert_versioned_result(
        conn, review=review, source_record_id=source_record_id, run_id=run_id
    )
    return raw_inserted, parse_inserted, issue_count


def _copy_insert_batch(
    conn: psycopg.Connection,
    *,
    items: list[ParsedReview],
    source_file_id: int,
    run_id: int,
) -> tuple[int, int, int, int]:
    """COPY a batch to a temporary table and merge it with set-based SQL.

    Returns raw inserted, parse inserted, parse already present and quality issue
    counts. Conflict checks run before any durable insert in the same transaction.
    """
    conn.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS tmp_review_v2 (
            record_number bigint, source_review_id char(64), content_sha256 char(64),
            start_line bigint, end_line bigint, uncompressed_start bigint, uncompressed_end bigint,
            raw_payload jsonb, asin_raw text, asin varchar(10), asin_status text,
            reviewer_id_raw text, reviewer_id text, profile_name_raw text,
            profile_name_normalized text, helpfulness_raw text, helpful_votes bigint,
            total_votes bigint, helpful_ratio numeric, score_raw text, score numeric,
            review_time_raw text, review_unix_seconds bigint, review_time_utc timestamptz,
            summary_raw text, summary_normalized text, review_text_raw text,
            review_text_normalized text, parse_status text, parse_issues jsonb
        ) ON COMMIT PRESERVE ROWS
        """
    )
    conn.execute("TRUNCATE tmp_review_v2")
    with conn.cursor().copy(
        """
        COPY tmp_review_v2 (
            record_number, source_review_id, content_sha256, start_line, end_line,
            uncompressed_start, uncompressed_end, raw_payload, asin_raw, asin, asin_status,
            reviewer_id_raw, reviewer_id, profile_name_raw, profile_name_normalized,
            helpfulness_raw, helpful_votes, total_votes, helpful_ratio, score_raw, score,
            review_time_raw, review_unix_seconds, review_time_utc, summary_raw,
            summary_normalized, review_text_raw, review_text_normalized, parse_status, parse_issues
        ) FROM STDIN
        """
    ) as copy:
        for r in items:
            copy.write_row(
                (
                    r.record_number, r.source_review_id, r.content_sha256, r.start_line, r.end_line,
                    r.uncompressed_start, r.uncompressed_end, Jsonb(r.raw_payload), r.asin_raw,
                    r.asin, r.asin_status, r.reviewer_id_raw, r.reviewer_id, r.profile_name_raw,
                    r.profile_name_normalized, r.helpfulness_raw, r.helpful_votes, r.total_votes,
                    r.helpful_ratio, r.score_raw, r.score, r.review_time_raw,
                    r.review_unix_seconds, r.review_time_utc, r.summary_raw, r.summary_normalized,
                    r.review_text_raw, r.review_text_normalized, r.parse_status,
                    Jsonb([issue.as_dict() for issue in r.issues]),
                )
            )

    conflict = conn.execute(
        """
        SELECT t.record_number, t.source_review_id, r.content_sha256, t.content_sha256, 'raw.source_record'
        FROM tmp_review_v2 t JOIN raw.source_record r
          ON r.source_kind='review_file' AND r.source_file_id=%s AND r.record_number=t.record_number
        WHERE r.content_sha256 <> t.content_sha256
        UNION ALL
        SELECT t.record_number, t.source_review_id, p.content_sha256, t.content_sha256,
               'staging.review_parse_result'
        FROM tmp_review_v2 t JOIN staging.review_parse_result p
          ON p.source_review_id=t.source_review_id AND p.parser_version=%s
        WHERE p.content_sha256 <> t.content_sha256
        LIMIT 1
        """,
        (source_file_id, PARSER_VERSION),
    ).fetchone()
    if conflict is not None:
        review = next(r for r in items if r.record_number == int(conflict[0]))
        raise ContentConflictError(review, str(conflict[2]), str(conflict[4]))

    raw_existing = int(
        conn.execute(
            """SELECT count(*) FROM tmp_review_v2 t JOIN raw.source_record r
               ON r.source_kind='review_file' AND r.source_file_id=%s AND r.record_number=t.record_number""",
            (source_file_id,),
        ).fetchone()[0]
    )
    parse_existing = int(
        conn.execute(
            """SELECT count(*) FROM tmp_review_v2 t JOIN staging.review_parse_result p
               ON p.source_review_id=t.source_review_id AND p.parser_version=%s""",
            (PARSER_VERSION,),
        ).fetchone()[0]
    )
    # A fully repeated batch has already passed both raw and versioned-content
    # conflict checks above. Avoid rewriting observations or scanning issue
    # tables; the checkpoint is still committed by the caller's transaction.
    if raw_existing == len(items) and parse_existing == len(items):
        conn.execute(
            """
            INSERT INTO staging.review_parse_locator (
                source_review_id, parser_version, start_line, end_line,
                uncompressed_start, uncompressed_end, content_sha256,
                line_semantics, byte_range_semantics, run_id
            )
            SELECT source_review_id,%s,start_line,end_line,uncompressed_start,uncompressed_end,
                   content_sha256,'inclusive_content_lines_excluding_blank_separator',
                   'half_open_content_bytes_excluding_blank_separator',%s
            FROM tmp_review_v2
            ON CONFLICT (source_review_id, parser_version) DO NOTHING
            """,
            (PARSER_VERSION, run_id),
        )
        return 0, 0, len(items), 0
    conn.execute(
        """
        INSERT INTO raw.source_record (
            source_kind, source_file_id, record_number, field_locator,
            content_sha256, access_status, raw_payload
        )
        SELECT 'review_file', %s, t.record_number,
               jsonb_build_object(
                   'record_number',t.record_number,'start_line',t.start_line,'end_line',t.end_line,
                   'line_semantics','inclusive_content_lines_excluding_blank_separator',
                   'uncompressed_start',t.uncompressed_start,'uncompressed_end',t.uncompressed_end,
                   'byte_range_semantics','half_open_content_bytes_excluding_blank_separator',
                   'resume_note','uncompressed_offset_requires_scan_from_gzip_start'),
               t.content_sha256, 'sample_parsed', t.raw_payload
        FROM tmp_review_v2 t
        ON CONFLICT DO NOTHING
        """,
        (source_file_id,),
    )
    conn.execute(
        """
        INSERT INTO staging.review_record (source_review_id, source_record_id, record_number, content_sha256)
        SELECT t.source_review_id, r.source_record_id, t.record_number, t.content_sha256
        FROM tmp_review_v2 t JOIN raw.source_record r
          ON r.source_kind='review_file' AND r.source_file_id=%s AND r.record_number=t.record_number
        ON CONFLICT (source_review_id) DO NOTHING
        """,
        (source_file_id,),
    )
    conn.execute(
        """
        INSERT INTO staging.review_parse_locator (
            source_review_id, parser_version, start_line, end_line,
            uncompressed_start, uncompressed_end, content_sha256,
            line_semantics, byte_range_semantics, run_id
        )
        SELECT source_review_id,%s,start_line,end_line,uncompressed_start,uncompressed_end,
               content_sha256,'inclusive_content_lines_excluding_blank_separator',
               'half_open_content_bytes_excluding_blank_separator',%s
        FROM tmp_review_v2
        ON CONFLICT (source_review_id, parser_version) DO NOTHING
        """,
        (PARSER_VERSION, run_id),
    )
    conn.execute(
        """
        INSERT INTO staging.review_parse_result (
            source_review_id, source_record_id, asin_raw, asin, asin_status,
            reviewer_id_raw, reviewer_id, profile_name_raw, profile_name_normalized,
            helpfulness_raw, helpful_votes, total_votes, helpful_ratio, score_raw, score,
            review_time_raw, review_unix_seconds, review_time_utc, summary_raw,
            summary_normalized, review_text_raw, review_text_normalized, parse_status,
            parse_issues, content_sha256, parser_version, record_number, run_id
        )
        SELECT t.source_review_id, r.source_record_id, t.asin_raw, t.asin, t.asin_status,
               t.reviewer_id_raw, t.reviewer_id, t.profile_name_raw, t.profile_name_normalized,
               t.helpfulness_raw, t.helpful_votes, t.total_votes, t.helpful_ratio, t.score_raw,
               t.score, t.review_time_raw, t.review_unix_seconds, t.review_time_utc,
               t.summary_raw, t.summary_normalized, t.review_text_raw, t.review_text_normalized,
               t.parse_status, t.parse_issues, t.content_sha256, %s, t.record_number, %s
        FROM tmp_review_v2 t JOIN raw.source_record r
          ON r.source_kind='review_file' AND r.source_file_id=%s AND r.record_number=t.record_number
        ON CONFLICT (source_review_id, parser_version) DO NOTHING
        """,
        (PARSER_VERSION, run_id, source_file_id),
    )
    issue_insert = conn.execute(
        """
        INSERT INTO meta.quality_issue (
            run_id, issue_code, severity, entity_type, entity_key,
            source_record_id, details, parser_version
        )
        SELECT %s, issue->>'code', issue->>'severity', 'source_review', t.source_review_id,
               r.source_record_id, issue, %s
        FROM tmp_review_v2 t
        JOIN raw.source_record r
          ON r.source_kind='review_file' AND r.source_file_id=%s AND r.record_number=t.record_number
        CROSS JOIN LATERAL jsonb_array_elements(t.parse_issues) issue
        WHERE NOT EXISTS (
            SELECT 1 FROM meta.quality_issue q
            WHERE q.source_record_id=r.source_record_id AND q.parser_version=%s
              AND q.issue_code=issue->>'code'
              AND COALESCE(q.details->>'field_name','')=COALESCE(issue->>'field_name','')
        )
        """,
        (run_id, PARSER_VERSION, source_file_id, PARSER_VERSION),
    )
    conn.execute(
        """
        INSERT INTO staging.product_asin_observation (
            source_review_id, parser_version, asin_raw, asin, asin_status, evidence, run_id
        )
        SELECT source_review_id, %s, asin_raw, asin, asin_status,
               jsonb_build_object('field','product/productId','decision',asin_status), %s
        FROM tmp_review_v2
        ON CONFLICT (source_review_id, parser_version) DO NOTHING
        """,
        (PARSER_VERSION, run_id),
    )
    conn.execute(
        """INSERT INTO target.product_edition (asin, created_run_id)
           SELECT DISTINCT asin, %s FROM tmp_review_v2 WHERE asin_status='usable'
           ON CONFLICT (asin) DO NOTHING""",
        (run_id,),
    )
    conn.execute(
        """
        INSERT INTO staging.product_harvest (asin, original_url, status)
        SELECT DISTINCT asin, 'https://www.amazon.com/dp/' || asin, 'not_attempted'
        FROM tmp_review_v2 WHERE asin_status='usable'
        ON CONFLICT (asin) DO NOTHING
        """
    )
    conn.execute(
        """
        INSERT INTO staging.reviewer_profile_observation (
            source_review_id, parser_version, reviewer_id, profile_name_raw,
            profile_name_normalized, observed_at, record_number, run_id
        )
        SELECT source_review_id, %s, reviewer_id, profile_name_raw, profile_name_normalized,
               review_time_utc, record_number, %s
        FROM tmp_review_v2 WHERE reviewer_id IS NOT NULL
        ON CONFLICT (source_review_id, parser_version) DO NOTHING
        """,
        (PARSER_VERSION, run_id),
    )
    conn.execute(
        """
        INSERT INTO target.reviewer (
            reviewer_id, latest_profile_name, latest_profile_observed_at,
            latest_profile_record_number, latest_profile_source_review_id
        )
        SELECT DISTINCT ON (reviewer_id) reviewer_id, profile_name_normalized,
               review_time_utc, record_number, source_review_id
        FROM tmp_review_v2 WHERE reviewer_id IS NOT NULL
        ORDER BY reviewer_id, review_time_utc DESC NULLS LAST, record_number DESC, source_review_id DESC
        ON CONFLICT (reviewer_id) DO UPDATE SET
            latest_profile_name=EXCLUDED.latest_profile_name,
            latest_profile_observed_at=EXCLUDED.latest_profile_observed_at,
            latest_profile_record_number=EXCLUDED.latest_profile_record_number,
            latest_profile_source_review_id=EXCLUDED.latest_profile_source_review_id
        WHERE (
            COALESCE(EXCLUDED.latest_profile_observed_at,'-infinity'::timestamptz),
            EXCLUDED.latest_profile_record_number, EXCLUDED.latest_profile_source_review_id
        ) > (
            COALESCE(target.reviewer.latest_profile_observed_at,'-infinity'::timestamptz),
            COALESCE(target.reviewer.latest_profile_record_number,0),
            COALESCE(target.reviewer.latest_profile_source_review_id,repeat('0',64)::char(64))
        )
        """
    )
    inserted = len(items) - parse_existing
    issue_count = max(int(issue_insert.rowcount or 0), 0)
    return len(items) - raw_existing, inserted, parse_existing, issue_count


def _write_checkpoint(
    conn: psycopg.Connection,
    *,
    source_sha256: str,
    partition_key: str,
    last_record: int,
    run_id: int,
    scope_start_record: int,
    scope_end_record: int | None,
    status: str = "sample_in_progress",
    actual_read_end_record: int | None = None,
    reached_eof: bool = False,
    source_integrity_status: str = "not_checked",
    stop_reason: str | None = None,
    last_free_bytes: int | None = None,
) -> None:
    requested_limit = (
        scope_end_record - scope_start_record + 1 if scope_end_record is not None else None
    )
    next_record = scope_start_record if last_record == 0 else last_record + 1
    conn.execute(
        """
        INSERT INTO meta.batch_progress (
            pipeline_name, input_version, parser_version, partition_key,
            next_record_number, committed_record_number, run_id,
            requested_record_limit, resume_strategy, scope_status,
            scope_start_record, scope_end_record, actual_read_end_record,
            reached_eof, source_integrity_status, stop_reason, last_free_bytes
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (pipeline_name, input_version, parser_version, partition_key)
        DO UPDATE SET
            next_record_number=EXCLUDED.next_record_number,
            committed_record_number=EXCLUDED.committed_record_number,
            run_id=EXCLUDED.run_id, requested_record_limit=EXCLUDED.requested_record_limit,
            resume_strategy=EXCLUDED.resume_strategy, scope_status=EXCLUDED.scope_status,
            scope_start_record=EXCLUDED.scope_start_record,
            scope_end_record=EXCLUDED.scope_end_record,
            actual_read_end_record=EXCLUDED.actual_read_end_record,
            reached_eof=EXCLUDED.reached_eof,
            source_integrity_status=EXCLUDED.source_integrity_status,
            stop_reason=EXCLUDED.stop_reason,
            last_free_bytes=EXCLUDED.last_free_bytes,
            updated_at=clock_timestamp()
        """,
        (
            PIPELINE_NAME, source_sha256, PARSER_VERSION, partition_key,
            next_record, last_record, run_id, requested_limit, RESUME_STRATEGY, status,
            scope_start_record, scope_end_record, actual_read_end_record, reached_eof,
            source_integrity_status, stop_reason, last_free_bytes,
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
    scope_start_record: int = 1,
    load_strategy: str = "copy",
    fail_before_commit_batch: int | None = None,
    fail_after_commit_batch: int | None = None,
    full_source: bool = False,
    monitor_path: Path | None = None,
    min_free_bytes: int = 0,
    monitor_interval_batches: int = 10,
) -> dict[str, object]:
    if sample_limit <= 0 or batch_size <= 0 or scope_start_record <= 0:
        raise ValueError("sample_limit, batch_size and scope_start_record must be positive")
    if full_source and scope_start_record != 1:
        raise ValueError("full-source processing must start at record 1")
    if monitor_interval_batches <= 0 or min_free_bytes < 0:
        raise ValueError("monitor interval must be positive and minimum free bytes non-negative")
    if load_strategy not in {"row", "copy"}:
        raise ValueError("load_strategy must be row or copy")
    source_sha256 = source_sha256.lower()
    if len(source_sha256) != 64 or any(c not in "0123456789abcdef" for c in source_sha256):
        raise ValueError("source_sha256 must contain 64 hexadecimal characters")
    scope_end_record = None if full_source else scope_start_record + sample_limit - 1
    size_bytes = data_file.stat().st_size

    with psycopg.connect(database_url, autocommit=True) as conn:
        source_file_id = _register_source_file(
            conn, source_uri=source_uri, source_sha256=source_sha256, size_bytes=size_bytes
        )
        run_id = _start_run(
            conn, batch_id=batch_id, source_sha256=source_sha256,
            scope_start_record=scope_start_record,
            requested_record_limit=None if full_source else sample_limit,
            is_full_source=full_source,
        )
        committed_before = _checkpoint_before(
            conn, source_sha256=source_sha256, partition_key=partition_key,
            scope_start_record=scope_start_record, scope_end_record=scope_end_record,
        )
        stats = LoadStats(
            batch_id=batch_id, run_id=run_id, source_file_id=source_file_id,
            parser_version=PARSER_VERSION, scope_start_record=scope_start_record,
            scope_end_record=scope_end_record, batch_size=batch_size,
            load_strategy=load_strategy,
            checkpoint_before=committed_before, checkpoint_after=committed_before,
            scope_status="full_in_progress" if full_source else "sample_in_progress",
        )
        pending: list[ParsedReview] = []
        batch_number = 0
        read_state = StreamReadState()
        started = time.perf_counter()

        def commit_batch(items_iter: Iterable[ParsedReview]) -> None:
            nonlocal batch_number
            items = list(items_iter)
            if not items:
                return
            batch_number += 1
            raw_count = parse_count = existing_count = issue_count = 0
            if monitor_path is not None and (
                batch_number == 1 or batch_number % monitor_interval_batches == 0
            ):
                free_bytes = shutil.disk_usage(monitor_path).free
                stats.last_free_bytes = free_bytes
                if free_bytes < min_free_bytes:
                    raise LowDiskSpaceError(
                        f"free bytes {free_bytes} below configured threshold {min_free_bytes} at {monitor_path}"
                    )
            with conn.transaction():
                if load_strategy == "copy":
                    raw_count, parse_count, existing_count, issue_count = _copy_insert_batch(
                        conn, items=items, source_file_id=source_file_id, run_id=run_id
                    )
                else:
                    for review in items:
                        raw_inserted, parse_inserted, inserted_issues = _insert_review(
                            conn, review=review, source_file_id=source_file_id, run_id=run_id
                        )
                        raw_count += int(raw_inserted)
                        parse_count += int(parse_inserted)
                        existing_count += int(not parse_inserted)
                        issue_count += inserted_issues
                if fail_before_commit_batch == batch_number:
                    raise SimulatedInterruption(f"simulated interruption before batch {batch_number} commit")
                _write_checkpoint(
                    conn, source_sha256=source_sha256, partition_key=partition_key,
                    last_record=items[-1].record_number, run_id=run_id,
                    scope_start_record=scope_start_record, scope_end_record=scope_end_record,
                    status="full_in_progress" if full_source else "sample_in_progress",
                    actual_read_end_record=read_state.last_record_number,
                    reached_eof=read_state.reached_eof,
                    source_integrity_status="not_checked" if full_source else "sample_only",
                    last_free_bytes=stats.last_free_bytes,
                )
            stats.raw_records_inserted += raw_count
            stats.parse_results_inserted += parse_count
            stats.parse_results_already_present += existing_count
            stats.quality_issues_inserted += issue_count
            stats.batches_committed += 1
            stats.checkpoint_after = items[-1].record_number
            stats.elapsed_seconds = time.perf_counter() - started
            if batch_number % monitor_interval_batches == 0:
                print(
                    json.dumps(
                        {
                            "event": "ingest_progress",
                            "parser_version": PARSER_VERSION,
                            "batch": batch_number,
                            "committed_record": stats.checkpoint_after,
                            "elapsed_seconds": round(stats.elapsed_seconds, 3),
                            "free_bytes": stats.last_free_bytes,
                        }
                    ),
                    flush=True,
                )
            if fail_after_commit_batch == batch_number:
                raise SimulatedInterruption(f"simulated interruption after batch {batch_number} commit")

        try:
            for review in iter_reviews(
                data_file,
                source_file_sha256=source_sha256,
                max_records=scope_end_record,
                state=read_state,
            ):
                stats.records_scanned_from_start += 1
                if review.record_number < scope_start_record or review.record_number <= committed_before:
                    stats.records_skipped_by_checkpoint += 1
                    continue
                stats.valid_records_seen += int(review.parse_status == "valid")
                stats.quarantined_records_seen += int(review.parse_status == "quarantined")
                stats.usable_asins_seen += int(review.asin_status == "usable")
                pending.append(review)
                if len(pending) >= batch_size:
                    commit_batch(pending)
                    pending = []
            commit_batch(pending)
            stats.actual_read_end_record = read_state.last_record_number
            stats.reached_eof = read_state.reached_eof
            stats.elapsed_seconds = time.perf_counter() - started
            if monitor_path is not None:
                stats.last_free_bytes = shutil.disk_usage(monitor_path).free
                if stats.last_free_bytes < min_free_bytes:
                    raise LowDiskSpaceError(
                        f"free bytes {stats.last_free_bytes} below configured threshold "
                        f"{min_free_bytes} at {monitor_path}"
                    )
            if full_source:
                if not read_state.reached_eof:
                    raise RuntimeError("full-source iterator ended without a clean EOF")
                stats.scope_status = "full_complete"
                stats.requested_range_satisfied = True
                stats.source_integrity_status = "verified_complete"
            elif read_state.reached_eof and read_state.last_record_number < scope_start_record:
                stats.scope_status = "sample_start_past_eof"
                stats.requested_range_satisfied = False
                stats.source_integrity_status = "sample_only"
            elif read_state.reached_eof and read_state.last_record_number < int(scope_end_record):
                stats.scope_status = "sample_range_short"
                stats.requested_range_satisfied = False
                stats.source_integrity_status = "sample_only"
            else:
                stats.scope_status = "sample_complete"
                stats.requested_range_satisfied = True
                stats.source_integrity_status = "sample_only"
            with conn.transaction():
                _write_checkpoint(
                    conn, source_sha256=source_sha256, partition_key=partition_key,
                    last_record=stats.checkpoint_after, run_id=run_id,
                    scope_start_record=scope_start_record, scope_end_record=scope_end_record,
                    status=stats.scope_status,
                    actual_read_end_record=stats.actual_read_end_record,
                    reached_eof=stats.reached_eof,
                    source_integrity_status=stats.source_integrity_status,
                    last_free_bytes=stats.last_free_bytes,
                )
                if full_source:
                    _validate_full_coverage(
                        conn,
                        source_file_id=source_file_id,
                        expected_records=stats.actual_read_end_record,
                    )
                    conn.execute(
                        """UPDATE meta.source_file
                           SET gzip_integrity_status='verified_complete', hash_verified_at=clock_timestamp()
                           WHERE source_file_id=%s""",
                        (source_file_id,),
                    )
                    conn.execute(
                        """UPDATE meta.parser_release
                           SET coverage_kind='full', coverage_start_record=1,
                               coverage_end_record=%s, verified_source_eof=true
                           WHERE parser_version=%s""",
                        (stats.actual_read_end_record, PARSER_VERSION),
                    )
                conn.execute(
                    "UPDATE meta.etl_run SET status='succeeded', finished_at=clock_timestamp(), stats=%s WHERE run_id=%s",
                    (Jsonb(stats.as_dict()), run_id),
                )
            return stats.as_dict()
        except Exception as exc:
            stats.actual_read_end_record = read_state.last_record_number
            stats.reached_eof = read_state.reached_eof
            stats.elapsed_seconds = time.perf_counter() - started
            gzip_failure = isinstance(exc, (gzip.BadGzipFile, EOFError, zlib.error))
            if gzip_failure:
                stats.scope_status = "source_failed"
                stats.source_integrity_status = "failed"
            elif isinstance(exc, LowDiskSpaceError):
                stats.scope_status = "stopped_low_disk"
                stats.source_integrity_status = "not_checked"
            with conn.transaction():
                if gzip_failure:
                    conn.execute(
                        "UPDATE meta.source_file SET gzip_integrity_status='failed' WHERE source_file_id=%s",
                        (source_file_id,),
                    )
                if gzip_failure or isinstance(exc, LowDiskSpaceError):
                    _write_checkpoint(
                        conn, source_sha256=source_sha256, partition_key=partition_key,
                        last_record=stats.checkpoint_after, run_id=run_id,
                        scope_start_record=scope_start_record, scope_end_record=scope_end_record,
                        status=stats.scope_status,
                        actual_read_end_record=stats.actual_read_end_record,
                        reached_eof=stats.reached_eof,
                        source_integrity_status=stats.source_integrity_status,
                        stop_reason=type(exc).__name__, last_free_bytes=stats.last_free_bytes,
                    )
                if isinstance(exc, ContentConflictError):
                    conn.execute(
                        """
                        INSERT INTO meta.quality_issue (
                            run_id, issue_code, severity, entity_type, entity_key, details, parser_version
                        ) VALUES (%s,'source_review_content_conflict','fatal','source_review',%s,%s,%s)
                        """,
                        (
                            run_id, exc.review.source_review_id,
                            Jsonb({"layer": exc.layer, "existing": exc.existing_sha256,
                                   "incoming": exc.review.content_sha256,
                                   "record_number": exc.review.record_number}), PARSER_VERSION,
                        ),
                    )
                conn.execute(
                    "UPDATE meta.etl_run SET status='failed', finished_at=clock_timestamp(), stats=%s WHERE run_id=%s",
                    (Jsonb({**stats.as_dict(), "error_type": type(exc).__name__, "error": str(exc)}), run_id),
                )
            raise


def load_review_full(
    *,
    database_url: str,
    data_file: Path,
    source_sha256: str,
    source_uri: str,
    batch_id: str,
    partition_key: str = "full-source",
    batch_size: int = 5000,
    load_strategy: str = "copy",
    monitor_path: Path | None = None,
    min_free_bytes: int = 0,
    monitor_interval_batches: int = 10,
) -> dict[str, object]:
    return load_review_sample(
        database_url=database_url,
        data_file=data_file,
        source_sha256=source_sha256,
        source_uri=source_uri,
        batch_id=batch_id,
        partition_key=partition_key,
        sample_limit=1,
        batch_size=batch_size,
        scope_start_record=1,
        load_strategy=load_strategy,
        full_source=True,
        monitor_path=monitor_path,
        min_free_bytes=min_free_bytes,
        monitor_interval_batches=monitor_interval_batches,
    )


def _validate_full_coverage(
    conn: psycopg.Connection,
    *,
    source_file_id: int,
    expected_records: int,
    parser_version: str = PARSER_VERSION,
) -> dict[str, int]:
    row = conn.execute(
        """
        WITH source_rows AS (
            SELECT source_record_id, record_number
            FROM raw.source_record
            WHERE source_kind='review_file' AND source_file_id=%s
        ), identities AS (
            SELECT i.source_review_id, i.source_record_id
            FROM staging.review_record i JOIN source_rows s USING(source_record_id)
        )
        SELECT
            (SELECT count(*) FROM source_rows),
            (SELECT COALESCE(min(record_number),0) FROM source_rows),
            (SELECT COALESCE(max(record_number),0) FROM source_rows),
            (SELECT count(*) FROM identities),
            (SELECT count(*) FROM staging.review_parse_result p
                JOIN identities i USING(source_review_id) WHERE p.parser_version=%s),
            (SELECT count(*) FROM staging.review_parse_locator l
                JOIN identities i USING(source_review_id) WHERE l.parser_version=%s),
            (SELECT count(*) FROM staging.product_asin_observation a
                JOIN identities i USING(source_review_id) WHERE a.parser_version=%s)
        """,
        (source_file_id, parser_version, parser_version, parser_version),
    ).fetchone()
    assert row is not None
    keys = ("raw", "min_record", "max_record", "identities", "parse", "locators", "asin_observations")
    counts = {key: int(value) for key, value in zip(keys, row)}
    expected = {
        "raw": expected_records,
        "min_record": 1 if expected_records else 0,
        "max_record": expected_records,
        "identities": expected_records,
        "parse": expected_records,
        "locators": expected_records,
        "asin_observations": expected_records,
    }
    if counts != expected:
        raise RuntimeError(f"full coverage validation failed: actual={counts} expected={expected}")
    return counts


def activate_parser(
    database_url: str,
    parser_version: str = PARSER_VERSION,
    *,
    require_full: bool = True,
) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.transaction():
            found = conn.execute(
                """SELECT coverage_kind,coverage_start_record,coverage_end_record,verified_source_eof
                   FROM meta.parser_release WHERE parser_version=%s""",
                (parser_version,),
            ).fetchone()
            if found is None:
                raise ValueError(f"unknown parser version {parser_version}")
            if require_full:
                if not (found[0] == "full" and int(found[1]) == 1 and bool(found[3])):
                    raise RuntimeError(
                        f"parser {parser_version} is not a validated full-source release: {found}"
                    )
                progress = conn.execute(
                    """
                    SELECT p.input_version,p.committed_record_number,f.source_file_id
                    FROM meta.batch_progress p
                    JOIN meta.source_file f ON f.sha256=p.input_version
                    WHERE p.pipeline_name=%s AND p.parser_version=%s
                      AND p.scope_status='full_complete' AND p.reached_eof
                      AND p.source_integrity_status='verified_complete'
                      AND f.gzip_integrity_status='verified_complete'
                    ORDER BY p.updated_at DESC LIMIT 1
                    """,
                    (PIPELINE_NAME, parser_version),
                ).fetchone()
                if progress is None or int(progress[1]) != int(found[2]):
                    raise RuntimeError("full checkpoint/source verification is missing or inconsistent")
                _validate_full_coverage(
                    conn, source_file_id=int(progress[2]), expected_records=int(progress[1]),
                    parser_version=parser_version,
                )
            conn.execute("UPDATE meta.parser_release SET is_current=false WHERE is_current")
            conn.execute(
                """UPDATE meta.parser_release
                   SET is_current=true,activated_at=clock_timestamp() WHERE parser_version=%s""",
                (parser_version,),
            )


def database_counts(database_url: str) -> dict[str, object]:
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(
            "SELECT parser_version, parse_status, count(*) FROM staging.review_parse_result GROUP BY 1,2 ORDER BY 1,2"
        ).fetchall()
        return {
            "source_records": int(conn.execute("SELECT count(*) FROM raw.source_record").fetchone()[0]),
            "review_identities": int(conn.execute("SELECT count(*) FROM staging.review_record").fetchone()[0]),
            "parse_results": {f"{v}:{s}": int(n) for v, s, n in rows},
            "products": int(conn.execute("SELECT count(*) FROM target.product_edition").fetchone()[0]),
            "reviewers": int(conn.execute("SELECT count(*) FROM target.reviewer").fetchone()[0]),
            "checkpoints": int(conn.execute("SELECT count(*) FROM meta.batch_progress").fetchone()[0]),
        }


def compact_result(result: dict[str, object], counts: dict[str, object] | None = None) -> str:
    payload = dict(result)
    if counts is not None:
        payload["database_counts"] = counts
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
