from __future__ import annotations

import hashlib
import json
import shutil
import time
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


RULE_VERSION = "logical-review-exact-v1"
PARSER_VERSION = "snap-movies-review-v3"
PIPELINE_NAME = "task8_logical_review_dedupe"


class LowDiskSpaceError(RuntimeError):
    pass


@dataclass
class DedupeStats:
    run_id: int
    rule_version: str
    parser_version: str
    scope_key: str
    scope_start_record: int
    scope_end_record: int | None
    checkpoint_before: int
    checkpoint_after: int
    processed_input: int = 0
    logical_reviews_inserted: int = 0
    mappings_inserted: int = 0
    batches_committed: int = 0
    missing_key_preserved: int = 0
    status: str = "running"
    last_free_bytes: int | None = None
    elapsed_seconds: float = 0.0


def _canonical_score(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def stable_logical_key(row: dict[str, Any]) -> tuple[str, str]:
    missing = (
        not row.get("reviewer_id")
        or row.get("review_time_utc") is None
        or row.get("review_time_raw") is None
        or row.get("score") is None
        or row.get("summary_raw") is None
        or row.get("review_text_raw") is None
    )
    if missing:
        payload = ["preserved-independent", str(row["source_review_id"])]
        eligibility = "preserved_missing_key"
    else:
        payload = [
            "exact-v1", row["asin"], row["reviewer_id"], row["review_time_raw"],
            _canonical_score(row["score"]), row["summary_raw"], row["review_text_raw"],
        ]
        eligibility = "exact_eligible"
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), eligibility


def _database_url(value: str | None) -> str:
    if not value:
        raise ValueError("database URL is required")
    return value


def _ensure_run(conn: Any, batch_id: str, rule_version: str, full: bool) -> int:
    return int(conn.execute(
        """INSERT INTO meta.etl_run(
               pipeline_name,batch_id,input_version,rule_version,status,scope_kind,is_full_source
           ) VALUES (%s,%s,%s,%s,'running',%s,%s)
           ON CONFLICT(pipeline_name,batch_id,input_version,rule_version) DO UPDATE
           SET status='running',finished_at=NULL,stats='{}'::jsonb
           RETURNING run_id""",
        (PIPELINE_NAME, batch_id, PARSER_VERSION, rule_version,
         "full_source" if full else "bounded_test", full),
    ).fetchone()["run_id"])


def _create_temp(conn: Any) -> None:
    conn.execute("""CREATE TEMP TABLE IF NOT EXISTS logical_review_input(
        source_review_id char(64),record_number bigint,product_id bigint,asin varchar(10),
        reviewer_id text,review_time_raw text,review_time_utc timestamptz,score numeric,
        summary_raw text,review_text_raw text,helpful_votes bigint,total_votes bigint,
        eligibility text,stable_key char(64)
    ) ON COMMIT DELETE ROWS""")


def _copy_batch(conn: Any, rows: list[dict[str, Any]]) -> None:
    columns = (
        "source_review_id","record_number","product_id","asin","reviewer_id",
        "review_time_raw","review_time_utc","score","summary_raw","review_text_raw",
        "helpful_votes","total_votes","eligibility","stable_key",
    )
    with conn.cursor().copy(
        "COPY logical_review_input(" + ",".join(columns) + ") FROM STDIN"
    ) as copy:
        for row in rows:
            copy.write_row(tuple(row[column] for column in columns))


def _apply_batch(conn: Any, rule_version: str, run_id: int) -> tuple[int, int]:
    internal_collision = conn.execute("""
        SELECT stable_key FROM logical_review_input GROUP BY stable_key
        HAVING count(DISTINCT jsonb_build_array(
            product_id,asin,reviewer_id,review_time_raw,score,summary_raw,review_text_raw,eligibility
        )) > 1 LIMIT 1
    """).fetchone()
    if internal_collision:
        raise RuntimeError(f"stable-key collision inside batch: {internal_collision[0]}")
    existing_collision = conn.execute("""
        SELECT t.stable_key FROM logical_review_input t
        JOIN target.logical_review l
          ON l.stable_key=t.stable_key AND l.dedupe_rule_version=%s
        WHERE (l.product_id,l.asin,l.reviewer_id,l.review_time_raw,l.score,l.summary,l.review_text,l.dedupe_eligibility)
              IS DISTINCT FROM
              (t.product_id,t.asin,t.reviewer_id,t.review_time_raw,t.score,t.summary_raw,t.review_text_raw,t.eligibility)
        LIMIT 1
    """, (rule_version,)).fetchone()
    if existing_collision:
        raise RuntimeError(f"stable-key content conflict: {existing_collision[0]}")
    mapping_conflict = conn.execute("""
        SELECT t.source_review_id FROM logical_review_input t
        JOIN target.review_mapping rm
          ON rm.source_review_id=t.source_review_id AND rm.rule_version=%s
        JOIN target.logical_review l ON l.logical_review_id=rm.logical_review_id
        WHERE l.stable_key<>t.stable_key LIMIT 1
    """, (rule_version,)).fetchone()
    if mapping_conflict:
        raise RuntimeError(f"source already maps to a different logical review: {mapping_conflict[0]}")

    inserted = conn.execute("""
        WITH helpful AS (
            SELECT stable_key,count(DISTINCT jsonb_build_array(helpful_votes,total_votes)) AS helpful_variants
            FROM logical_review_input GROUP BY stable_key
        ), ranked AS (
            SELECT t.*,h.helpful_variants,row_number() OVER(
                PARTITION BY stable_key ORDER BY record_number,source_review_id
            ) AS rn
            FROM logical_review_input t JOIN helpful h USING(stable_key)
        )
        INSERT INTO target.logical_review(
            stable_key,product_id,reviewer_id,review_time_utc,score,summary,review_text,
            dedupe_rule_version,parser_version,asin,review_time_raw,dedupe_eligibility,
            representative_source_review_id,representative_record_number,
            helpful_votes,total_votes,helpfulness_resolution,created_run_id
        )
        SELECT stable_key,product_id,reviewer_id,review_time_utc,score,summary_raw,review_text_raw,
               %s,%s,asin,review_time_raw,eligibility,source_review_id,record_number,
               CASE WHEN helpful_variants=1 THEN helpful_votes END,
               CASE WHEN helpful_variants=1 THEN total_votes END,
               CASE WHEN helpful_variants=1 THEN 'unanimous_source_value'
                    ELSE 'conflict_preserved_at_source' END,%s
        FROM ranked WHERE rn=1
        ON CONFLICT(stable_key,dedupe_rule_version) DO NOTHING
    """, (rule_version, PARSER_VERSION, run_id)).rowcount

    conn.execute("""
        WITH incoming AS (
            SELECT stable_key,count(DISTINCT jsonb_build_array(helpful_votes,total_votes)) AS variants,
                   (array_agg(DISTINCT jsonb_build_array(helpful_votes,total_votes)))[1] AS only_pair
            FROM logical_review_input GROUP BY stable_key
        ), conflicts AS (
            SELECT l.logical_review_id
            FROM target.logical_review l JOIN incoming i USING(stable_key)
            WHERE l.dedupe_rule_version=%s AND (
                l.helpfulness_resolution='conflict_preserved_at_source' OR i.variants>1 OR
                jsonb_build_array(l.helpful_votes,l.total_votes) IS DISTINCT FROM i.only_pair
            )
        )
        UPDATE target.logical_review l
        SET helpful_votes=NULL,total_votes=NULL,helpfulness_resolution='conflict_preserved_at_source'
        FROM conflicts c WHERE c.logical_review_id=l.logical_review_id
    """, (rule_version,))

    mappings = conn.execute("""
        INSERT INTO target.review_mapping(
            logical_review_id,source_review_id,mapping_reason,rule_version,source_role,evidence,mapped_run_id
        )
        SELECT l.logical_review_id,t.source_review_id,
               CASE WHEN t.source_review_id=l.representative_source_review_id THEN 'stable earliest source representative'
                    WHEN t.eligibility='exact_eligible' THEN 'all exact identity and content fields equal'
                    ELSE 'missing key field preserved as independent logical review' END,
               %s,
               CASE WHEN t.source_review_id=l.representative_source_review_id THEN 'representative'
                    WHEN t.eligibility='exact_eligible' THEN 'exact_duplicate'
                    ELSE 'preserved_independent' END,
               jsonb_build_object('record_number',t.record_number,'eligibility',t.eligibility,
                                  'helpful_votes',t.helpful_votes,'total_votes',t.total_votes),%s
        FROM logical_review_input t JOIN target.logical_review l
          ON l.stable_key=t.stable_key AND l.dedupe_rule_version=%s
        ON CONFLICT(source_review_id,rule_version) DO NOTHING
    """, (rule_version, run_id, rule_version)).rowcount
    return int(inserted), int(mappings)


def deduplicate_reviews(
    database_url: str, *, batch_id: str, scope_key: str, rule_version: str = RULE_VERSION,
    batch_size: int = 10_000, scope_start_record: int = 1,
    scope_end_record: int | None = None, full: bool = False,
    monitor_path: Path | None = None, min_free_bytes: int = 0,
    monitor_interval_batches: int = 10, fail_before_commit_batch: int | None = None,
    fail_after_commit_batch: int | None = None,
) -> dict[str, Any]:
    import psycopg
    from psycopg.rows import dict_row

    if batch_size < 1 or scope_start_record < 1:
        raise ValueError("batch_size and scope_start_record must be positive")
    started = time.monotonic()
    url = _database_url(database_url)
    with psycopg.connect(url, row_factory=dict_row) as conn:
        conn.autocommit = True
        parser = conn.execute("""SELECT coverage_kind,coverage_start_record,coverage_end_record,
                                         verified_source_eof,is_current
                                  FROM meta.parser_release WHERE parser_version=%s""",
                              (PARSER_VERSION,)).fetchone()
        if not parser or not parser["is_current"]:
            raise RuntimeError(f"parser is not active: {PARSER_VERSION}")
        if full:
            if parser["coverage_kind"] != "full" or not parser["verified_source_eof"]:
                raise RuntimeError("full dedupe requires a full verified parser release")
            scope_start_record = 1
            scope_end_record = int(parser["coverage_end_record"])
        run_id = _ensure_run(conn, batch_id, rule_version, full)
        conn.execute("""INSERT INTO meta.logical_review_release(rule_version,parser_version,status)
                        VALUES (%s,%s,'running') ON CONFLICT(rule_version) DO UPDATE
                        SET status='running',updated_at=clock_timestamp()""", (rule_version, PARSER_VERSION))
        progress = conn.execute("""
            INSERT INTO meta.logical_review_progress(
                rule_version,parser_version,scope_key,scope_start_record,scope_end_record,status,run_id
            ) VALUES (%s,%s,%s,%s,%s,'running',%s)
            ON CONFLICT(rule_version,parser_version,scope_key) DO UPDATE
            SET status='running',stop_reason=NULL,run_id=EXCLUDED.run_id,updated_at=clock_timestamp()
            WHERE meta.logical_review_progress.scope_start_record=EXCLUDED.scope_start_record
              AND meta.logical_review_progress.scope_end_record IS NOT DISTINCT FROM EXCLUDED.scope_end_record
            RETURNING committed_record_number,processed_input_count
        """, (rule_version, PARSER_VERSION, scope_key, scope_start_record, scope_end_record, run_id)).fetchone()
        if not progress:
            raise RuntimeError("checkpoint scope conflict")
        checkpoint = int(progress["committed_record_number"])
        stats = DedupeStats(run_id, rule_version, PARSER_VERSION, scope_key, scope_start_record,
                            scope_end_record, checkpoint, checkpoint)
        _create_temp(conn)
        try:
            batch_number = 0
            while True:
                if monitor_path and batch_number % monitor_interval_batches == 0:
                    stats.last_free_bytes = shutil.disk_usage(monitor_path).free
                    if stats.last_free_bytes < min_free_bytes:
                        raise LowDiskSpaceError(
                            f"free bytes {stats.last_free_bytes} below threshold {min_free_bytes}"
                        )
                rows = conn.execute("""
                    SELECT r.source_review_id,r.record_number,pe.product_id,r.asin,r.reviewer_id,
                           r.review_time_raw,r.review_time_utc,r.score,r.summary_raw,r.review_text_raw,
                           r.helpful_votes,r.total_votes
                    FROM staging.current_review_parse_result r
                    JOIN target.product_edition pe ON pe.asin=r.asin
                    WHERE r.parse_status='valid' AND r.record_number>%s AND r.record_number>=%s
                      AND (%s IS NULL OR r.record_number<=%s)
                    ORDER BY r.record_number LIMIT %s
                """, (checkpoint, scope_start_record, scope_end_record, scope_end_record, batch_size)).fetchall()
                if not rows:
                    break
                prepared: list[dict[str, Any]] = []
                for item in rows:
                    row = dict(item)
                    row["stable_key"], row["eligibility"] = stable_logical_key(row)
                    prepared.append(row)
                batch_number += 1
                with conn.transaction():
                    _copy_batch(conn, prepared)
                    logical_inserted, mappings_inserted = _apply_batch(conn, rule_version, run_id)
                    if fail_before_commit_batch == batch_number:
                        raise RuntimeError(f"intentional failure before commit batch {batch_number}")
                    checkpoint = int(prepared[-1]["record_number"])
                    conn.execute("""UPDATE meta.logical_review_progress SET
                        committed_record_number=%s,processed_input_count=processed_input_count+%s,
                        last_free_bytes=%s,run_id=%s,updated_at=clock_timestamp()
                        WHERE rule_version=%s AND parser_version=%s AND scope_key=%s""",
                        (checkpoint, len(prepared), stats.last_free_bytes, run_id,
                         rule_version, PARSER_VERSION, scope_key))
                stats.processed_input += len(prepared)
                stats.logical_reviews_inserted += logical_inserted
                stats.mappings_inserted += mappings_inserted
                stats.missing_key_preserved += sum(r["eligibility"] == "preserved_missing_key" for r in prepared)
                stats.batches_committed += 1
                stats.checkpoint_after = checkpoint
                if fail_after_commit_batch == batch_number:
                    raise RuntimeError(f"intentional failure after commit batch {batch_number}")
            completion = "full_complete" if full else "sample_complete"
            final_checkpoint = scope_end_record if full and scope_end_record is not None else checkpoint
            with conn.transaction():
                conn.execute("""UPDATE meta.logical_review_progress SET status=%s,
                    committed_record_number=%s,stop_reason=NULL,updated_at=clock_timestamp()
                    WHERE rule_version=%s AND parser_version=%s AND scope_key=%s""",
                    (completion, final_checkpoint, rule_version, PARSER_VERSION, scope_key))
                conn.execute("""UPDATE meta.logical_review_release SET status=%s,
                    scope_start_record=%s,scope_end_record=%s,updated_at=clock_timestamp()
                    WHERE rule_version=%s""",
                    ("running" if full else "sample_verified", scope_start_record, scope_end_record, rule_version))
            stats.checkpoint_after = final_checkpoint
            stats.status = completion
            stats.elapsed_seconds = round(time.monotonic() - started, 3)
            with conn.transaction():
                conn.execute("""UPDATE meta.etl_run SET status='succeeded',finished_at=clock_timestamp(),stats=%s::jsonb
                                WHERE run_id=%s""", (json.dumps(asdict(stats)), run_id))
            return asdict(stats)
        except Exception as exc:
            status = "stopped_low_disk" if isinstance(exc, LowDiskSpaceError) else "failed"
            with psycopg.connect(url) as failure:
                failure.execute("""UPDATE meta.logical_review_progress SET status=%s,stop_reason=%s,
                    last_free_bytes=%s,updated_at=clock_timestamp()
                    WHERE rule_version=%s AND parser_version=%s AND scope_key=%s""",
                    (status, str(exc), stats.last_free_bytes, rule_version, PARSER_VERSION, scope_key))
                failure.execute("""UPDATE meta.logical_review_release SET status='failed',updated_at=clock_timestamp()
                                   WHERE rule_version=%s""", (rule_version,))
                failure.execute("""UPDATE meta.etl_run SET status='failed',finished_at=clock_timestamp(),
                                   stats=%s::jsonb WHERE run_id=%s""",
                                (json.dumps({**asdict(stats), "error": str(exc)}), run_id))
            raise


def dedupe_status(database_url: str, rule_version: str = RULE_VERSION) -> dict[str, Any]:
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(_database_url(database_url), row_factory=dict_row) as conn:
        release = conn.execute("SELECT * FROM meta.logical_review_release WHERE rule_version=%s",
                               (rule_version,)).fetchone()
        progress = conn.execute("""SELECT * FROM meta.logical_review_progress
                                   WHERE rule_version=%s ORDER BY scope_key""", (rule_version,)).fetchall()
        counts = conn.execute("""SELECT
            (SELECT count(*) FROM target.logical_review WHERE dedupe_rule_version=%s) logical_reviews,
            (SELECT count(*) FROM target.review_mapping WHERE rule_version=%s) mappings,
            (SELECT count(*) FROM target.logical_review l WHERE dedupe_rule_version=%s AND
                (SELECT count(*) FROM target.review_mapping rm WHERE rm.logical_review_id=l.logical_review_id)>1)
                duplicate_groups""", (rule_version, rule_version, rule_version)).fetchone()
    return {"release": dict(release) if release else None,
            "progress": [dict(row) for row in progress], "counts": dict(counts)}


def activate_release(database_url: str, rule_version: str = RULE_VERSION) -> dict[str, Any]:
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(_database_url(database_url), row_factory=dict_row) as conn:
        parser = conn.execute("""SELECT coverage_end_record FROM meta.parser_release
                                 WHERE parser_version=%s AND is_current AND coverage_kind='full'
                                   AND verified_source_eof""", (PARSER_VERSION,)).fetchone()
        if not parser:
            raise RuntimeError("active parser is not full verified")
        progress = conn.execute("""SELECT * FROM meta.logical_review_progress
            WHERE rule_version=%s AND parser_version=%s AND scope_start_record=1
              AND scope_end_record=%s AND status='full_complete'""",
            (rule_version, PARSER_VERSION, parser["coverage_end_record"])).fetchone()
        if not progress:
            raise RuntimeError("full dedupe checkpoint is missing")
        counts = conn.execute("""WITH input AS (
              SELECT source_review_id FROM staging.current_review_parse_result WHERE parse_status='valid'
            ), mapped AS (
              SELECT source_review_id,logical_review_id FROM target.review_mapping WHERE rule_version=%s
            ) SELECT
              (SELECT count(*) FROM input) input_count,
              (SELECT count(*) FROM mapped) mapping_count,
              (SELECT count(DISTINCT source_review_id) FROM mapped) mapped_sources,
              (SELECT count(*) FROM target.logical_review WHERE dedupe_rule_version=%s) logical_count,
              (SELECT count(*) FROM mapped m LEFT JOIN input i USING(source_review_id)
                 WHERE i.source_review_id IS NULL) mappings_outside_input,
              (SELECT count(*) FROM input i LEFT JOIN mapped m USING(source_review_id)
                 WHERE m.source_review_id IS NULL) missing_mappings
        """, (rule_version, rule_version)).fetchone()
        if not (counts["input_count"] == counts["mapping_count"] == counts["mapped_sources"]
                and counts["mappings_outside_input"] == 0 and counts["missing_mappings"] == 0):
            raise RuntimeError(f"dedupe coverage validation failed: {dict(counts)}")
        duplicate_records = counts["input_count"] - counts["logical_count"]
        with conn.transaction():
            conn.execute("UPDATE meta.logical_review_release SET is_current=false WHERE is_current")
            conn.execute("""UPDATE meta.logical_review_release SET status='full_verified',is_current=true,
                input_valid_count=%s,logical_review_count=%s,mapping_count=%s,duplicate_record_count=%s,
                activated_at=clock_timestamp(),updated_at=clock_timestamp() WHERE rule_version=%s""",
                (counts["input_count"], counts["logical_count"], counts["mapping_count"],
                 duplicate_records, rule_version))
        return {**dict(counts), "duplicate_record_count": duplicate_records,
                "rule_version": rule_version, "activated": True}
