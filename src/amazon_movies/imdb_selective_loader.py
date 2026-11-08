from __future__ import annotations

import base64
import gzip
import hashlib
import json
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import psycopg

from amazon_movies.imdb_snapshot import REQUIRED_HEADERS


RULE_VERSION = "imdb-selective-integration-v1"
FILE_ORDER = (
    "title.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.crew.tsv.gz",
    "title.principals.tsv.gz",
    "name.basics.tsv.gz",
)


class InjectedFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class SelectedRow:
    record_number: int
    source_line: int
    values: dict[str, str]
    content_sha256: str
    raw_bytes: bytes


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _null(value: str | None) -> str | None:
    return None if value in (None, r"\N") else value


def _split(value: str | None) -> list[str]:
    normalized = _null(value)
    return [] if not normalized else normalized.split(",")


def _integer(value: str | None, *, minimum: int | None = None, maximum: int | None = None) -> int | None:
    normalized = _null(value)
    if normalized is None:
        return None
    try:
        parsed = int(normalized)
    except ValueError:
        return None
    if minimum is not None and parsed < minimum:
        return None
    if maximum is not None and parsed > maximum:
        return None
    return parsed


def _boolean(value: str | None) -> bool | None:
    normalized = _null(value)
    if normalized == "0":
        return False
    if normalized == "1":
        return True
    return None


def _safe_text(value: str | None) -> str | None:
    return None if value is None or "\x00" in value else value


def _normalize_title_type(value: str) -> str:
    return {
        "tvMovie": "TV movie",
        "tvSeries": "TV series",
        "tvMiniSeries": "TV mini-series",
        "tvEpisode": "TV episode",
    }.get(value, value)


def _ensure_staging_source(
    conn: psycopg.Connection[Any], table: str, where_sql: str, where_values: tuple[Any, ...],
    expected_source_record_id: int,
) -> None:
    existing = conn.execute(
        f"SELECT source_record_id FROM {table} WHERE {where_sql}", where_values
    ).fetchone()
    if not existing or existing[0] != expected_source_record_id:
        raise ValueError(
            f"staging content conflict in {table}: expected source_record_id "
            f"{expected_source_record_id}, found {None if not existing else existing[0]}"
        )


def _safe_payload(values: dict[str, str], raw_bytes: bytes) -> dict[str, Any]:
    nul_fields = [name for name, value in values.items() if "\x00" in value]
    if nul_fields:
        return {
            "fields": {name: (None if name in nul_fields else value) for name, value in values.items()},
            "nul_fields": nul_fields,
            "raw_line_base64": base64.b64encode(raw_bytes).decode("ascii"),
            "raw_encoding": "utf-8",
        }
    return {"fields": values, "raw_encoding": "utf-8"}


def _scope_digest(targets: list[dict[str, Any]], snapshot_version: str, selection_version: str) -> str:
    payload = {
        "snapshot_version": snapshot_version,
        "selection_version": selection_version,
        "targets": sorted(targets, key=lambda item: (item["tconst"], item.get("asin") or "")),
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _begin_run(conn: psycopg.Connection[Any], batch_id: str, snapshot_version: str) -> int:
    return conn.execute(
        """
        INSERT INTO meta.etl_run(pipeline_name,batch_id,input_version,rule_version,status,stats)
        VALUES ('imdb_selective_integration',%s,%s,%s,'running','{}'::jsonb)
        ON CONFLICT (pipeline_name,batch_id,input_version,rule_version) DO UPDATE SET
            status='running',finished_at=NULL,stats='{}'::jsonb,started_at=clock_timestamp()
        RETURNING run_id
        """,
        (batch_id, snapshot_version, RULE_VERSION),
    ).fetchone()[0]


def _scope_lock(conn: psycopg.Connection[Any], snapshot_version: str, scope_key: str) -> None:
    locked = conn.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s,0))",
        (f"imdb-selective:{snapshot_version}:{scope_key}",),
    ).fetchone()[0]
    if not locked:
        raise RuntimeError(f"selective IMDb scope is already running: {scope_key}")


def _finish_run(database_url: str, run_id: int, status: str, stats: dict[str, Any]) -> None:
    with psycopg.connect(database_url) as conn:
        conn.execute(
            "UPDATE meta.etl_run SET status=%s,finished_at=clock_timestamp(),stats=%s::jsonb WHERE run_id=%s",
            (status, _json(stats), run_id),
        )


def _snapshot(conn: psycopg.Connection[Any], snapshot_version: str) -> tuple[int, dict[str, dict[str, Any]]]:
    row = conn.execute(
        """SELECT snapshot_id,verification_status FROM meta.external_snapshot
           WHERE source_name='imdb_noncommercial' AND snapshot_version=%s""",
        (snapshot_version,),
    ).fetchone()
    if not row:
        raise ValueError(f"unregistered IMDb snapshot: {snapshot_version}")
    if row[1] != "fully_verified":
        raise ValueError(f"snapshot is not fully verified: {row[1]}")
    files = {
        item[0]: {
            "size_bytes": item[1], "sha256": item[2], "record_count": item[3],
            "path_hint": item[4],
        }
        for item in conn.execute(
            """SELECT file_name,size_bytes,file_sha256,uncompressed_record_count,path_hint
               FROM meta.external_snapshot_file WHERE snapshot_id=%s""",
            (row[0],),
        ).fetchall()
    }
    if set(files) != set(FILE_ORDER):
        raise ValueError(f"snapshot file set mismatch: {sorted(files)}")
    return row[0], files


def _targets(conn: psycopg.Connection[Any], scope: str, explicit_tconsts: Iterable[str]) -> tuple[list[dict[str, Any]], str]:
    if scope == "adopted-development":
        rows = conn.execute(
            """SELECT imdb_candidate_id,asin,adoption_origin,product_title_raw,rule_version
               FROM integration.adopted_development_movie_mapping ORDER BY imdb_candidate_id,asin"""
        ).fetchall()
        targets = [
            {"tconst": row[0], "asin": row[1], "adoption_origin": row[2], "product_title_raw": row[3]}
            for row in rows
        ]
        versions = sorted({row[4] for row in rows})
        return targets, "adopted-development:" + ",".join(versions)
    if scope == "explicit":
        unique = sorted(set(explicit_tconsts))
        if not unique:
            raise ValueError("explicit scope requires at least one tconst")
        if any(not value.startswith("tt") or not value[2:].isdigit() for value in unique):
            raise ValueError("invalid explicit tconst")
        return [
            {"tconst": value, "asin": None, "adoption_origin": None, "product_title_raw": None}
            for value in unique
        ], "explicit-tconst-v1"
    raise ValueError(f"unsupported scope: {scope}")


def _register_scope(
    conn: psycopg.Connection[Any], snapshot_id: int, scope_key: str, selection_version: str,
    scope_sha: str, targets: list[dict[str, Any]],
) -> None:
    existing = conn.execute(
        "SELECT scope_sha256 FROM meta.imdb_selective_scope WHERE snapshot_id=%s AND scope_key=%s",
        (snapshot_id, scope_key),
    ).fetchone()
    if existing and existing[0] != scope_sha:
        raise ValueError("scope_key already exists with a different target set")
    conn.execute(
        """INSERT INTO meta.imdb_selective_scope(
               snapshot_id,scope_key,selection_version,scope_sha256,target_title_count,status)
           VALUES (%s,%s,%s,%s,%s,'registered')
           ON CONFLICT (snapshot_id,scope_key) DO UPDATE SET updated_at=clock_timestamp()""",
        (snapshot_id, scope_key, selection_version, scope_sha, len({item["tconst"] for item in targets})),
    )
    for item in targets:
        conn.execute(
            """INSERT INTO meta.imdb_selective_scope_title(
                   snapshot_id,scope_key,tconst,asin,adoption_origin,product_title_raw)
               VALUES (%s,%s,%s,%s,%s,%s)
               ON CONFLICT (snapshot_id,scope_key,tconst) DO UPDATE SET
                   asin=EXCLUDED.asin,adoption_origin=EXCLUDED.adoption_origin,
                   product_title_raw=EXCLUDED.product_title_raw""",
            (
                snapshot_id, scope_key, item["tconst"], item.get("asin"),
                item.get("adoption_origin"), item.get("product_title_raw"),
            ),
        )
        if item.get("asin"):
            conn.execute(
                """INSERT INTO meta.imdb_selective_scope_product(
                       snapshot_id,scope_key,tconst,asin,adoption_origin,product_title_raw)
                   VALUES (%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (snapshot_id,scope_key,tconst,asin) DO UPDATE SET
                       adoption_origin=EXCLUDED.adoption_origin,
                       product_title_raw=EXCLUDED.product_title_raw""",
                (
                    snapshot_id, scope_key, item["tconst"], item["asin"],
                    item.get("adoption_origin"), item.get("product_title_raw"),
                ),
            )


def _register_source_files(
    conn: psycopg.Connection[Any], snapshot_version: str, files: dict[str, dict[str, Any]]
) -> dict[str, int]:
    ids: dict[str, int] = {}
    for name, item in files.items():
        ids[name] = conn.execute(
            """INSERT INTO meta.source_file(
                   source_name,source_uri,sha256,size_bytes,acquired_at,parser_version,immutable,
                   hash_verification_method,hash_verified_at,gzip_integrity_status)
               VALUES (%s,%s,%s,%s,clock_timestamp(),%s,true,
                       'fixed_snapshot_full_sha256',clock_timestamp(),'verified_complete')
               ON CONFLICT (sha256) DO UPDATE SET
                   parser_version=EXCLUDED.parser_version,
                   hash_verification_method=EXCLUDED.hash_verification_method,
                   hash_verified_at=EXCLUDED.hash_verified_at,
                   gzip_integrity_status=EXCLUDED.gzip_integrity_status
               RETURNING source_file_id""",
            (
                f"IMDb Non-Commercial {name}", f"https://datasets.imdbws.com/{name}",
                item["sha256"], item["size_bytes"], RULE_VERSION,
            ),
        ).fetchone()[0]
    return ids


def _quality(
    conn: psycopg.Connection[Any], run_id: int, code: str, *, entity_type: str | None = None,
    entity_key: str | None = None, source_record_id: int | None = None, details: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """INSERT INTO meta.quality_issue(
               run_id,issue_code,severity,entity_type,entity_key,source_record_id,details)
           VALUES (%s,%s,'warning',%s,%s,%s,%s::jsonb)""",
        (run_id, code, entity_type, entity_key, source_record_id, _json(details or {})),
    )


def _source_record(
    conn: psycopg.Connection[Any], *, source_file_id: int, file_name: str, path: Path,
    row: SelectedRow, run_id: int,
) -> int:
    existing = conn.execute(
        """SELECT source_record_id,content_sha256 FROM raw.source_record
           WHERE source_file_id=%s AND record_number=%s ORDER BY source_record_id""",
        (source_file_id, row.record_number),
    ).fetchall()
    if existing:
        if len(existing) != 1 or existing[0][1] != row.content_sha256:
            raise ValueError(f"content conflict at {file_name} record {row.record_number}")
        return existing[0][0]
    source_uri = f"https://datasets.imdbws.com/{file_name}#record={row.record_number}"
    source_record_id = conn.execute(
        """INSERT INTO raw.source_record(
               source_kind,source_file_id,source_uri,record_number,field_locator,content_sha256,
               snapshot_path,raw_payload,access_status)
           VALUES ('third_party_file',%s,%s,%s,%s::jsonb,%s,%s,%s::jsonb,'success')
           RETURNING source_record_id""",
        (
            source_file_id, source_uri, row.record_number,
            _json({
                "file_name": file_name, "data_record_number": row.record_number,
                "source_line": row.source_line,
                "gzip_position": "sequential_scan_from_start",
            }),
            row.content_sha256, str(path), _json(_safe_payload(row.values, row.raw_bytes)),
        ),
    ).fetchone()[0]
    if any("\x00" in value for value in row.values.values()):
        _quality(
            conn, run_id, "imdb_nul_preserved_as_base64", source_record_id=source_record_id,
            details={"file_name": file_name, "record_number": row.record_number},
        )
    return source_record_id


def _insert_staging(
    conn: psycopg.Connection[Any], snapshot_id: int, file_name: str, row: SelectedRow,
    source_record_id: int, run_id: int,
) -> None:
    value = row.values
    if file_name == "title.basics.tsv.gz":
        primary = _safe_text(value["primaryTitle"])
        original = _safe_text(value["originalTitle"])
        if primary is None or original is None:
            _quality(conn, run_id, "imdb_required_title_unwritable", source_record_id=source_record_id)
            return
        params = (
            snapshot_id, value["tconst"], value["titleType"], primary, original,
            _boolean(value["isAdult"]), _integer(value["startYear"], minimum=1800, maximum=2200),
            _integer(value["endYear"], minimum=1800, maximum=2200),
            _integer(value["runtimeMinutes"], minimum=0, maximum=2_147_483_647),
            _split(value["genres"]), source_record_id,
        )
        conn.execute(
            """INSERT INTO staging.imdb_title_basics(
                   snapshot_id,tconst,title_type,primary_title,original_title,is_adult,start_year,
                   end_year,runtime_minutes,genres,source_record_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            params,
        )
        _ensure_staging_source(
            conn, "staging.imdb_title_basics", "snapshot_id=%s AND tconst=%s",
            (snapshot_id, value["tconst"]), source_record_id,
        )
    elif file_name == "title.akas.tsv.gz":
        ordering = _integer(value["ordering"], minimum=1, maximum=2_147_483_647)
        title = _safe_text(value["title"])
        if ordering is None or title is None:
            _quality(conn, run_id, "imdb_aka_required_value_invalid", source_record_id=source_record_id)
            return
        conn.execute(
            """INSERT INTO staging.imdb_title_aka(
                   snapshot_id,title_id,ordering,title,region,language,title_types,attributes,
                   is_original_title,source_record_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (
                snapshot_id, value["titleId"], ordering, title, _null(value["region"]),
                _null(value["language"]), _split(value["types"]), _split(value["attributes"]),
                _boolean(value["isOriginalTitle"]), source_record_id,
            ),
        )
        _ensure_staging_source(
            conn, "staging.imdb_title_aka", "snapshot_id=%s AND title_id=%s AND ordering=%s",
            (snapshot_id, value["titleId"], ordering), source_record_id,
        )
    elif file_name == "title.crew.tsv.gz":
        conn.execute(
            """INSERT INTO staging.imdb_title_crew(
                   snapshot_id,tconst,director_nconsts,writer_nconsts,source_record_id)
               VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (snapshot_id, value["tconst"], _split(value["directors"]), _split(value["writers"]), source_record_id),
        )
        _ensure_staging_source(
            conn, "staging.imdb_title_crew", "snapshot_id=%s AND tconst=%s",
            (snapshot_id, value["tconst"]), source_record_id,
        )
    elif file_name == "title.principals.tsv.gz":
        ordering = _integer(value["ordering"], minimum=1, maximum=2_147_483_647)
        if ordering is None:
            _quality(conn, run_id, "imdb_principal_order_invalid", source_record_id=source_record_id)
            return
        characters_raw = _null(value["characters"])
        characters: Any = None
        if characters_raw is not None:
            try:
                characters = json.loads(characters_raw)
            except json.JSONDecodeError:
                characters = {"unparsed_raw": characters_raw}
                _quality(conn, run_id, "imdb_characters_json_invalid", source_record_id=source_record_id)
        conn.execute(
            """INSERT INTO staging.imdb_title_principal(
                   snapshot_id,tconst,ordering,nconst,category,job,characters,source_record_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT DO NOTHING""",
            (
                snapshot_id, value["tconst"], ordering, value["nconst"], value["category"],
                _null(value["job"]), _json(characters) if characters is not None else None,
                source_record_id,
            ),
        )
        _ensure_staging_source(
            conn, "staging.imdb_title_principal",
            "snapshot_id=%s AND tconst=%s AND ordering=%s",
            (snapshot_id, value["tconst"], ordering), source_record_id,
        )
    elif file_name == "name.basics.tsv.gz":
        primary_name = _safe_text(value["primaryName"])
        if primary_name is None:
            _quality(conn, run_id, "imdb_primary_name_unwritable", source_record_id=source_record_id)
            return
        conn.execute(
            """INSERT INTO staging.imdb_name_basics(
                   snapshot_id,nconst,primary_name,birth_year,death_year,primary_professions,
                   known_for_tconsts,source_record_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (
                snapshot_id, value["nconst"], primary_name,
                _integer(value["birthYear"], minimum=1700, maximum=2200),
                _integer(value["deathYear"], minimum=1700, maximum=2200),
                _split(value["primaryProfession"]), _split(value["knownForTitles"]), source_record_id,
            ),
        )
        _ensure_staging_source(
            conn, "staging.imdb_name_basics", "snapshot_id=%s AND nconst=%s",
            (snapshot_id, value["nconst"]), source_record_id,
        )


def _target_keys(conn: psycopg.Connection[Any], snapshot_id: int, scope_key: str, file_name: str) -> set[str]:
    if file_name != "name.basics.tsv.gz":
        return {
            row[0] for row in conn.execute(
                "SELECT tconst FROM meta.imdb_selective_scope_title WHERE snapshot_id=%s AND scope_key=%s",
                (snapshot_id, scope_key),
            ).fetchall()
        }
    return {
        row[0] for row in conn.execute(
            """
            WITH scoped AS (
                SELECT tconst FROM meta.imdb_selective_scope_title
                WHERE snapshot_id=%s AND scope_key=%s
            )
            SELECT unnest(c.director_nconsts) FROM staging.imdb_title_crew c JOIN scoped USING(tconst)
             WHERE c.snapshot_id=%s
            UNION SELECT unnest(c.writer_nconsts) FROM staging.imdb_title_crew c JOIN scoped USING(tconst)
             WHERE c.snapshot_id=%s
            UNION SELECT p.nconst FROM staging.imdb_title_principal p JOIN scoped USING(tconst)
             WHERE p.snapshot_id=%s
            """,
            (snapshot_id, scope_key, snapshot_id, snapshot_id, snapshot_id),
        ).fetchall()
    }


def _scan_file(
    conn: psycopg.Connection[Any], *, snapshot_id: int, scope_key: str, scope_sha: str,
    file_name: str, path: Path, source_file_id: int, run_id: int, batch_size: int,
    fail_before_commit_batch: int | None, fail_after_commit_batch: int | None,
    batch_counter: list[int], progress_every: int,
) -> dict[str, int | bool]:
    progress = conn.execute(
        """SELECT committed_record_number,next_record_number,selected_record_count,reached_eof,scope_sha256
           FROM meta.imdb_selective_progress
           WHERE snapshot_id=%s AND scope_key=%s AND file_name=%s""",
        (snapshot_id, scope_key, file_name),
    ).fetchone()
    if progress and progress[4] != scope_sha:
        raise ValueError(f"checkpoint scope hash conflict for {file_name}")
    if progress and progress[3]:
        return {
            "selected": progress[2], "reached_eof": True, "resume_discarded": 0,
            "committed_record_number": progress[0],
        }
    if not progress:
        conn.execute(
            """INSERT INTO meta.imdb_selective_progress(
                   snapshot_id,scope_key,file_name,scope_sha256,status,run_id)
               VALUES (%s,%s,%s,%s,'pending',%s)""",
            (snapshot_id, scope_key, file_name, scope_sha, run_id),
        )
        conn.commit()
        start_record, selected_total = 1, 0
    else:
        start_record, selected_total = progress[1], progress[2]
    targets = _target_keys(conn, snapshot_id, scope_key, file_name)
    if not targets:
        raise ValueError(f"no filter keys available for {file_name}")
    expected_header = list(REQUIRED_HEADERS[file_name])
    pending: list[SelectedRow] = []
    last_record = 0
    processed_since_commit = 0
    resume_discarded = 0
    with gzip.open(path, "rb") as stream:
        header = stream.readline().decode("utf-8", errors="strict").rstrip("\r\n").split("\t")
        if header != expected_header:
            raise ValueError(f"header changed for {file_name}")
        for record_number, raw_line_with_newline in enumerate(stream, start=1):
            last_record = record_number
            if record_number < start_record:
                resume_discarded += 1
                continue
            raw_line = raw_line_with_newline.rstrip(b"\r\n")
            first = raw_line.split(b"\t", 1)[0].decode("utf-8", errors="strict")
            if first in targets:
                text = raw_line.decode("utf-8", errors="strict")
                values = text.split("\t")
                if len(values) != len(header):
                    raise ValueError(
                        f"TSV column count mismatch in {file_name} record {record_number}: "
                        f"expected {len(header)}, got {len(values)}"
                    )
                pending.append(
                    SelectedRow(
                        record_number, record_number + 1, dict(zip(header, values)),
                        hashlib.sha256(raw_line).hexdigest(), raw_line,
                    )
                )
            processed_since_commit += 1
            if progress_every and record_number % progress_every == 0:
                print(
                    _json({
                        "file": file_name, "record": record_number, "selected_committed": selected_total,
                        "resume_discarded": resume_discarded,
                    }),
                    flush=True,
                )
            if processed_since_commit >= batch_size:
                for item in pending:
                    source_record_id = _source_record(
                        conn, source_file_id=source_file_id, file_name=file_name, path=path,
                        row=item, run_id=run_id,
                    )
                    _insert_staging(conn, snapshot_id, file_name, item, source_record_id, run_id)
                new_selected = selected_total + len(pending)
                conn.execute(
                    """UPDATE meta.imdb_selective_progress SET
                           committed_record_number=%s,next_record_number=%s,selected_record_count=%s,
                           reached_eof=false,status='running',run_id=%s,updated_at=clock_timestamp()
                       WHERE snapshot_id=%s AND scope_key=%s AND file_name=%s""",
                    (record_number, record_number + 1, new_selected, run_id, snapshot_id, scope_key, file_name),
                )
                batch_counter[0] += 1
                if fail_before_commit_batch == batch_counter[0]:
                    conn.rollback()
                    raise InjectedFailure(f"injected before commit batch {batch_counter[0]}")
                conn.commit()
                selected_total = new_selected
                pending.clear()
                processed_since_commit = 0
                if fail_after_commit_batch == batch_counter[0]:
                    raise InjectedFailure(f"injected after commit batch {batch_counter[0]}")
    for item in pending:
        source_record_id = _source_record(
            conn, source_file_id=source_file_id, file_name=file_name, path=path, row=item, run_id=run_id,
        )
        _insert_staging(conn, snapshot_id, file_name, item, source_record_id, run_id)
    selected_total += len(pending)
    conn.execute(
        """UPDATE meta.imdb_selective_progress SET
               committed_record_number=%s,next_record_number=%s,selected_record_count=%s,
               reached_eof=true,status='verified_complete',run_id=%s,updated_at=clock_timestamp()
           WHERE snapshot_id=%s AND scope_key=%s AND file_name=%s""",
        (last_record, last_record + 1, selected_total, run_id, snapshot_id, scope_key, file_name),
    )
    batch_counter[0] += 1
    if fail_before_commit_batch == batch_counter[0]:
        conn.rollback()
        raise InjectedFailure(f"injected before commit batch {batch_counter[0]}")
    conn.commit()
    if fail_after_commit_batch == batch_counter[0]:
        raise InjectedFailure(f"injected after commit batch {batch_counter[0]}")
    return {
        "selected": selected_total, "reached_eof": True, "resume_discarded": resume_discarded,
        "committed_record_number": last_record,
    }


def _assertion(
    conn: psycopg.Connection[Any], *, entity_type: str, entity_key: str, field_name: str,
    raw_value: Any, normalized_value: Any, semantics: str, source_record_id: int,
    locator: dict[str, Any], run_id: int,
) -> int:
    existing = conn.execute(
        """SELECT assertion_id FROM integration.field_assertion
           WHERE entity_type=%s AND entity_key=%s AND field_name=%s AND source_record_id=%s
             AND extraction_rule_version=%s ORDER BY assertion_id LIMIT 1""",
        (entity_type, entity_key, field_name, source_record_id, RULE_VERSION),
    ).fetchone()
    if existing:
        return existing[0]
    return conn.execute(
        """INSERT INTO integration.field_assertion(
               entity_type,entity_key,field_name,raw_value,normalized_value,value_semantics,
               source_record_id,field_locator,extraction_rule_version,confidence,quality_status,run_id)
           VALUES (%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s::jsonb,%s,1.0,'observed',%s)
           RETURNING assertion_id""",
        (
            entity_type, entity_key, field_name, _json(raw_value), _json(normalized_value), semantics,
            source_record_id, _json(locator), RULE_VERSION, run_id,
        ),
    ).fetchone()[0]


def _select_field(
    conn: psycopg.Connection[Any], *, entity_type: str, entity_key: str, field_name: str,
    assertion_id: int, selection_version: str, reason: str, run_id: int,
    decision_status: str = "selected",
) -> None:
    conn.execute(
        """INSERT INTO integration.field_selection(
               entity_type,entity_key,field_name,selection_version,selected_assertion_id,
               selected_assertion_ids,decision_status,decision_reason,run_id)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (entity_type,entity_key,field_name,selection_version) DO UPDATE SET
               selected_assertion_id=EXCLUDED.selected_assertion_id,
               selected_assertion_ids=EXCLUDED.selected_assertion_ids,
               decision_status=EXCLUDED.decision_status,decision_reason=EXCLUDED.decision_reason,
               run_id=EXCLUDED.run_id,decided_at=clock_timestamp()""",
        (
            entity_type, entity_key, field_name, selection_version,
            assertion_id if decision_status == "selected" else None,
            [assertion_id], decision_status, reason, run_id,
        ),
    )


def _person(
    conn: psycopg.Connection[Any], snapshot_id: int, nconst: str, run_id: int,
    selection_version: str,
) -> tuple[int, str, int] | None:
    name = conn.execute(
        """SELECT primary_name,source_record_id FROM staging.imdb_name_basics
           WHERE snapshot_id=%s AND nconst=%s""",
        (snapshot_id, nconst),
    ).fetchone()
    if not name:
        _quality(conn, run_id, "imdb_name_missing", entity_type="person", entity_key=f"imdb:{nconst}")
        return None
    stable_key = f"imdb:{nconst}"
    existing = conn.execute(
        "SELECT person_id,canonical_name FROM target.person WHERE stable_key=%s", (stable_key,)
    ).fetchone()
    if existing and existing[1] != name[0]:
        _quality(
            conn, run_id, "imdb_person_name_conflict", entity_type="person", entity_key=stable_key,
            source_record_id=name[1], details={"existing": existing[1], "snapshot": name[0]},
        )
        person_id = existing[0]
    else:
        person_id = conn.execute(
            """INSERT INTO target.person(stable_key,canonical_name,status)
               VALUES (%s,%s,'verified') ON CONFLICT (stable_key) DO UPDATE SET status='verified'
               RETURNING person_id""",
            (stable_key, name[0]),
        ).fetchone()[0]
    conn.execute(
        """INSERT INTO integration.person_external_identifier(
               person_id,source_name,external_id,snapshot_id,source_record_id,status)
           VALUES (%s,'imdb',%s,%s,%s,'adopted') ON CONFLICT DO NOTHING""",
        (person_id, nconst, snapshot_id, name[1]),
    )
    alias_normalized = unicodedata.normalize("NFKC", name[0]).casefold().strip()
    conn.execute(
        """INSERT INTO target.person_alias(
               person_id,alias_raw,alias_normalized,source_record_id)
           VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (person_id, name[0], alias_normalized, name[1]),
    )
    assertion_id = _assertion(
        conn, entity_type="person", entity_key=stable_key, field_name="canonical_name",
        raw_value=name[0], normalized_value=name[0], semantics="imdb_name_basics_primary_name",
        source_record_id=name[1], locator={"column": "primaryName"}, run_id=run_id,
    )
    _select_field(
        conn, entity_type="person", entity_key=stable_key, field_name="canonical_name",
        assertion_id=assertion_id, selection_version=selection_version,
        reason="IMDb nconst fixes identity; primaryName selected from the same fixed snapshot",
        run_id=run_id,
    )
    return person_id, name[0], name[1]


def _person_name_key(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _reconcile_suggestion_people(
    conn: psycopg.Connection[Any], snapshot_id: int, scope_key: str, run_id: int,
    selection_version: str,
) -> int:
    merged = 0
    movies = conn.execute(
        """SELECT m.movie_id,m.stable_key
           FROM meta.imdb_selective_scope_title s
           JOIN target.movie m ON m.stable_key='imdb:' || s.tconst
           WHERE s.snapshot_id=%s AND s.scope_key=%s""",
        (snapshot_id, scope_key),
    ).fetchall()
    for movie_id, movie_key in movies:
        official = conn.execute(
            """SELECT DISTINCT p.person_id,p.stable_key,p.canonical_name,e.relationship_source_record_id
               FROM integration.movie_person_source_evidence e
               JOIN target.person p USING(person_id)
               WHERE e.movie_id=%s AND e.snapshot_id=%s""",
            (movie_id, snapshot_id),
        ).fetchall()
        by_name: dict[str, list[Any]] = defaultdict(list)
        for row in official:
            by_name[_person_name_key(row[2])].append(row)
        suggestions = conn.execute(
            """SELECT DISTINCT p.person_id,p.stable_key,p.canonical_name,mp.source_record_id
               FROM target.movie_person mp JOIN target.person p USING(person_id)
               WHERE mp.movie_id=%s AND p.stable_key LIKE 'imdb-suggestion-name:%%'""",
            (movie_id,),
        ).fetchall()
        for old_person_id, old_key, old_name, old_source in suggestions:
            candidates = by_name.get(_person_name_key(old_name), [])
            if len(candidates) == 1:
                new_person_id, new_key, new_name, fixed_source = candidates[0]
                evidence = {
                    "facts": [
                        {"type": "normalized_name_exact", "left": old_name, "right": new_name},
                        {
                            "type": "same_verified_movie", "movie_key": movie_key,
                            "suggestion_source_record_id": old_source,
                            "fixed_snapshot_relationship_source_record_id": fixed_source,
                        },
                    ],
                    "reason": "unique exact-name nconst credit on the same verified tconst; not name-only",
                }
                conn.execute(
                    """INSERT INTO integration.person_match_candidate(
                           left_person_key,imdb_nconst,snapshot_id,rule_version,match_status,
                           score_breakdown,evidence,conflict_points,run_id)
                       VALUES (%s,%s,%s,%s,'rule_verified',%s::jsonb,%s::jsonb,'[]'::jsonb,%s)
                       ON CONFLICT (left_person_key,imdb_nconst,snapshot_id,rule_version) DO UPDATE SET
                           match_status=EXCLUDED.match_status,score_breakdown=EXCLUDED.score_breakdown,
                           evidence=EXCLUDED.evidence,conflict_points=EXCLUDED.conflict_points,
                           run_id=EXCLUDED.run_id,decided_at=clock_timestamp()""",
                    (
                        old_key, new_key.removeprefix("imdb:"), snapshot_id, RULE_VERSION,
                        _json({"exact_name": 1, "same_verified_tconst": 1}), _json(evidence), run_id,
                    ),
                )
                conn.execute(
                    """INSERT INTO integration.merge_decision(
                           entity_type,left_entity_key,right_entity_key,decision,decision_origin,
                           rule_version,confidence,evidence,run_id)
                       VALUES ('person',%s,%s,'merge','rule',%s,NULL,%s::jsonb,%s)
                       ON CONFLICT (entity_type,left_entity_key,right_entity_key,rule_version) DO UPDATE SET
                           evidence=EXCLUDED.evidence,run_id=EXCLUDED.run_id,decided_at=clock_timestamp()""",
                    (old_key, new_key, selection_version, _json(evidence), run_id),
                )
                conn.execute("UPDATE target.person SET status='retired' WHERE person_id=%s", (old_person_id,))
                merged += 1
            elif len(candidates) > 1:
                for candidate in candidates:
                    conn.execute(
                        """INSERT INTO integration.person_match_candidate(
                               left_person_key,imdb_nconst,snapshot_id,rule_version,match_status,
                               score_breakdown,evidence,conflict_points,run_id)
                           VALUES (%s,%s,%s,%s,'needs_review_ambiguous',%s::jsonb,%s::jsonb,%s::jsonb,%s)
                           ON CONFLICT (left_person_key,imdb_nconst,snapshot_id,rule_version) DO UPDATE SET
                               match_status=EXCLUDED.match_status,evidence=EXCLUDED.evidence,
                               conflict_points=EXCLUDED.conflict_points,run_id=EXCLUDED.run_id,
                               decided_at=clock_timestamp()""",
                        (
                            old_key, candidate[1].removeprefix("imdb:"), snapshot_id, RULE_VERSION,
                            _json({"exact_name": 1, "same_verified_tconst": 1}),
                            _json({"movie_key": movie_key, "left_name": old_name, "right_name": candidate[2]}),
                            _json([{"type": "multiple_nconsts_with_same_name_on_title"}]), run_id,
                        ),
                    )
                conn.execute(
                    """INSERT INTO integration.review_queue(
                           queue_type,entity_key,candidate_key,reason_code,evidence,run_id)
                       VALUES ('person_match',%s,NULL,'multiple_same_name_nconst_on_verified_title',%s::jsonb,%s)
                       ON CONFLICT DO NOTHING""",
                    (old_key, _json({"movie_key": movie_key, "candidate_count": len(candidates)}), run_id),
                )
            else:
                conn.execute(
                    """INSERT INTO integration.review_queue(
                           queue_type,entity_key,candidate_key,reason_code,evidence,run_id)
                       VALUES ('person_match',%s,NULL,'no_exact_nconst_on_verified_title',%s::jsonb,%s)
                       ON CONFLICT DO NOTHING""",
                    (old_key, _json({"movie_key": movie_key, "name": old_name}), run_id),
                )
    return merged


def _materialize(
    conn: psycopg.Connection[Any], snapshot_id: int, scope_key: str, snapshot_version: str, run_id: int,
) -> dict[str, int]:
    selection_version = f"{RULE_VERSION}:{snapshot_version}"
    movies = 0
    people: set[int] = set()
    facts: set[tuple[int, int, str]] = set()
    missing_titles = 0
    missing_names_before = conn.execute(
        "SELECT count(*) FROM meta.quality_issue WHERE run_id=%s AND issue_code='imdb_name_missing'", (run_id,)
    ).fetchone()[0]
    targets = conn.execute(
        """SELECT s.tconst,
                  b.title_type,b.primary_title,b.original_title,b.start_year,b.source_record_id
           FROM meta.imdb_selective_scope_title s
           LEFT JOIN staging.imdb_title_basics b
             ON b.snapshot_id=s.snapshot_id AND b.tconst=s.tconst
           WHERE s.snapshot_id=%s AND s.scope_key=%s ORDER BY s.tconst""",
        (snapshot_id, scope_key),
    ).fetchall()
    for target in targets:
        tconst, title_type, primary_title, original_title, start_year, title_source = target
        if title_source is None:
            missing_titles += 1
            _quality(conn, run_id, "imdb_title_missing", entity_type="movie", entity_key=f"imdb:{tconst}")
            continue
        stable_key = f"imdb:{tconst}"
        normalized_title_type = _normalize_title_type(title_type)
        existing = conn.execute(
            """SELECT movie_id,canonical_title,original_title,release_year,release_type
               FROM target.movie WHERE stable_key=%s""",
            (stable_key,),
        ).fetchone()
        if existing:
            movie_id = existing[0]
            for field, old, new in (
                ("canonical_title", existing[1], primary_title), ("original_title", existing[2], original_title),
                ("release_year", existing[3], start_year),
                ("release_type", existing[4], normalized_title_type),
            ):
                if old is not None and new is not None and str(old) != str(new):
                    _quality(
                        conn, run_id, "imdb_movie_field_conflict", entity_type="movie", entity_key=stable_key,
                        source_record_id=title_source, details={"field": field, "existing": old, "snapshot": new},
                    )
            conn.execute(
                """UPDATE target.movie SET
                       canonical_title=coalesce(canonical_title,%s),
                       original_title=coalesce(original_title,%s),
                       release_year=coalesce(release_year,%s),
                       release_precision=coalesce(release_precision,%s),
                       release_type=coalesce(release_type,%s),
                       status='verified'
                   WHERE movie_id=%s""",
                (
                    primary_title, original_title, start_year,
                    "year" if start_year is not None else "unknown", normalized_title_type, movie_id,
                ),
            )
        else:
            movie_id = conn.execute(
                """INSERT INTO target.movie(
                       stable_key,canonical_title,original_title,release_year,release_precision,release_type,status)
                   VALUES (%s,%s,%s,%s,%s,%s,'verified') RETURNING movie_id""",
                (
                    stable_key, primary_title, original_title, start_year,
                    "year" if start_year is not None else "unknown", normalized_title_type,
                ),
            ).fetchone()[0]
        movies += 1
        conn.execute(
            """INSERT INTO integration.movie_external_identifier(
                   movie_id,source_name,external_id,snapshot_id,source_record_id,status)
               VALUES (%s,'imdb',%s,%s,%s,'adopted') ON CONFLICT DO NOTHING""",
            (movie_id, tconst, snapshot_id, title_source),
        )
        for field_name, raw_value, semantics, column in (
            ("canonical_title", primary_title, "imdb_title_basics_primary_title", "primaryTitle"),
            ("original_title", original_title, "imdb_title_basics_original_title", "originalTitle"),
            ("release_year", start_year, "imdb_title_basics_start_year", "startYear"),
            ("release_type", normalized_title_type, "imdb_title_basics_title_type", "titleType"),
        ):
            assertion_id = _assertion(
                conn, entity_type="movie", entity_key=stable_key, field_name=field_name,
                raw_value=(title_type if field_name == "release_type" else raw_value),
                normalized_value=raw_value, semantics=semantics,
                source_record_id=title_source, locator={"column": column}, run_id=run_id,
            )
            _select_field(
                conn, entity_type="movie", entity_key=stable_key, field_name=field_name,
                assertion_id=assertion_id, selection_version=selection_version,
                reason="selected from fully verified fixed IMDb snapshot for an adopted tconst",
                run_id=run_id,
            )
        products = conn.execute(
            """SELECT asin,adoption_origin,product_title_raw
               FROM meta.imdb_selective_scope_product
               WHERE snapshot_id=%s AND scope_key=%s AND tconst=%s ORDER BY asin""",
            (snapshot_id, scope_key, tconst),
        ).fetchall()
        for asin, adoption_origin, product_title in products:
            product = conn.execute("SELECT product_id FROM target.product_edition WHERE asin=%s", (asin,)).fetchone()
            if not product:
                _quality(conn, run_id, "adopted_product_missing", entity_type="product", entity_key=asin)
            else:
                origin = "human_label" if adoption_origin == "human_confirmed" else "rule"
                decision_id = conn.execute(
                    """INSERT INTO integration.merge_decision(
                           entity_type,left_entity_key,right_entity_key,decision,decision_origin,
                           rule_version,confidence,evidence,run_id)
                       VALUES ('product_movie',%s,%s,'merge',%s,%s,NULL,%s::jsonb,%s)
                       ON CONFLICT (entity_type,left_entity_key,right_entity_key,rule_version) DO UPDATE SET
                           evidence=EXCLUDED.evidence,run_id=EXCLUDED.run_id
                       RETURNING merge_decision_id""",
                    (
                        asin, stable_key, origin, selection_version,
                        _json({
                            "adoption_origin": adoption_origin, "product_title_raw": product_title,
                            "snapshot_version": snapshot_version, "title_source_record_id": title_source,
                        }), run_id,
                    ),
                ).fetchone()[0]
                conn.execute(
                    """INSERT INTO target.product_movie(
                           product_id,movie_id,relationship_type,decision_id,mapping_status)
                       VALUES (%s,%s,'single_work',%s,%s)
                       ON CONFLICT (product_id,movie_id,relationship_type) DO UPDATE SET
                           decision_id=EXCLUDED.decision_id,mapping_status=EXCLUDED.mapping_status""",
                    (product[0], movie_id, decision_id, "manual" if origin == "human_label" else "verified"),
                )

        evidence: list[dict[str, Any]] = []
        crew = conn.execute(
            """SELECT director_nconsts,writer_nconsts,source_record_id
               FROM staging.imdb_title_crew WHERE snapshot_id=%s AND tconst=%s""",
            (snapshot_id, tconst),
        ).fetchone()
        if crew:
            evidence.extend(
                {"nconst": nconst, "role": "director", "kind": "crew_director", "source": crew[2],
                 "order": None, "category": "director", "job": None, "characters": None}
                for nconst in crew[0]
            )
            evidence.extend(
                {"nconst": nconst, "role": "writer", "kind": "crew_writer", "source": crew[2],
                 "order": None, "category": "writer", "job": None, "characters": None}
                for nconst in crew[1]
            )
        for principal in conn.execute(
            """SELECT nconst,ordering,category,job,characters,source_record_id
               FROM staging.imdb_title_principal WHERE snapshot_id=%s AND tconst=%s ORDER BY ordering""",
            (snapshot_id, tconst),
        ).fetchall():
            role = (
                "cast" if principal[2] in {"actor", "actress", "self"}
                else "director" if principal[2] == "director"
                else "writer" if principal[2] == "writer"
                else "other"
            )
            evidence.append(
                {"nconst": principal[0], "role": role, "kind": "principal_credit", "source": principal[5],
                 "order": principal[1], "category": principal[2], "job": principal[3],
                 "characters": principal[4]},
            )
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for item in evidence:
            grouped[(item["nconst"], item["role"])].append(item)
        for (nconst, role), items in grouped.items():
            person = _person(conn, snapshot_id, nconst, run_id, selection_version)
            if not person:
                continue
            person_id, primary_name, name_source = person
            people.add(person_id)
            selected = sorted(
                items,
                key=lambda item: (
                    0 if item["kind"] in {"crew_director", "crew_writer"} else 1,
                    item["order"] if item["order"] is not None else 1_000_000,
                    item["source"],
                ),
            )[0]
            for item in items:
                is_selected = item is selected
                conn.execute(
                    """INSERT INTO integration.movie_person_source_evidence(
                           movie_id,person_id,role_type,snapshot_id,relationship_source_record_id,
                           name_source_record_id,evidence_kind,principal_order,imdb_category,job_raw,
                           characters_raw,primary_name_raw,extraction_rule_version,selection_status,
                           selection_reason,run_id)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)
                       ON CONFLICT (
                           movie_id,person_id,role_type,snapshot_id,
                           relationship_source_record_id,extraction_rule_version
                       ) DO UPDATE SET run_id=EXCLUDED.run_id""",
                    (
                        movie_id, person_id, role, snapshot_id, item["source"], name_source,
                        item["kind"], item["order"], item["category"], item["job"],
                        _json(item["characters"]) if item["characters"] is not None else None,
                        primary_name, RULE_VERSION, "selected" if is_selected else "supporting",
                        (
                            "crew directors/writers are selected for those roles; principal ordering is selected "
                            "for cast/other credits; IMDb principals are not treated as complete cast or starring"
                        ), run_id,
                    ),
                )
            conn.execute(
                """INSERT INTO target.movie_person(
                       movie_id,person_id,role_type,credited_as,billing_order,source_record_id)
                   VALUES (%s,%s,%s,NULL,%s,%s) ON CONFLICT DO NOTHING""",
                (
                    movie_id, person_id, role,
                    selected["order"] if selected["kind"] == "principal_credit" else None,
                    selected["source"],
                ),
            )
            facts.add((movie_id, person_id, role))
    person_merges = _reconcile_suggestion_people(
        conn, snapshot_id, scope_key, run_id, selection_version
    )
    missing_names_after = conn.execute(
        "SELECT count(*) FROM meta.quality_issue WHERE run_id=%s AND issue_code='imdb_name_missing'", (run_id,)
    ).fetchone()[0]
    return {
        "movies": movies, "persons": len(people), "relationship_facts": len(facts),
        "person_merges": person_merges,
        "missing_titles": missing_titles, "missing_names": missing_names_after - missing_names_before,
    }


def load_selective_snapshot(
    database_url: str, *, snapshot_dir: Path | str, snapshot_version: str, scope: str,
    scope_key: str, batch_id: str, explicit_tconsts: Iterable[str] = (),
    batch_size: int = 500_000, progress_every: int = 5_000_000,
    fail_before_commit_batch: int | None = None, fail_after_commit_batch: int | None = None,
) -> dict[str, Any]:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    snapshot_dir = Path(snapshot_dir).resolve()
    run_id = 0
    snapshot_id = 0
    try:
        with psycopg.connect(database_url) as conn:
            _scope_lock(conn, snapshot_version, scope_key)
            snapshot_id, files = _snapshot(conn, snapshot_version)
            for file_name, item in files.items():
                path = snapshot_dir / file_name
                if not path.is_file() or path.stat().st_size != item["size_bytes"]:
                    raise ValueError(f"snapshot file missing or size changed: {path}")
            targets, selection_version = _targets(conn, scope, explicit_tconsts)
            scope_sha = _scope_digest(targets, snapshot_version, selection_version)
            run_id = _begin_run(conn, batch_id, snapshot_version)
            _register_scope(conn, snapshot_id, scope_key, selection_version, scope_sha, targets)
            source_files = _register_source_files(conn, snapshot_version, files)
            conn.execute(
                """UPDATE meta.imdb_selective_scope SET status='loading',updated_at=clock_timestamp()
                   WHERE snapshot_id=%s AND scope_key=%s""",
                (snapshot_id, scope_key),
            )
            conn.commit()
            batch_counter = [0]
            file_stats: dict[str, Any] = {}
            for file_name in FILE_ORDER:
                file_stats[file_name] = _scan_file(
                    conn, snapshot_id=snapshot_id, scope_key=scope_key, scope_sha=scope_sha,
                    file_name=file_name, path=snapshot_dir / file_name,
                    source_file_id=source_files[file_name], run_id=run_id, batch_size=batch_size,
                    fail_before_commit_batch=fail_before_commit_batch,
                    fail_after_commit_batch=fail_after_commit_batch,
                    batch_counter=batch_counter, progress_every=progress_every,
                )
                if file_stats[file_name]["committed_record_number"] != files[file_name]["record_count"]:
                    raise ValueError(
                        f"record count changed for {file_name}: scanned "
                        f"{file_stats[file_name]['committed_record_number']}, "
                        f"registered {files[file_name]['record_count']}"
                    )
            conn.execute(
                """UPDATE meta.imdb_selective_scope SET status='loaded',updated_at=clock_timestamp()
                   WHERE snapshot_id=%s AND scope_key=%s""",
                (snapshot_id, scope_key),
            )
            materialized = _materialize(conn, snapshot_id, scope_key, snapshot_version, run_id)
            conn.execute(
                """UPDATE meta.imdb_selective_scope SET status='materialized',updated_at=clock_timestamp()
                   WHERE snapshot_id=%s AND scope_key=%s""",
                (snapshot_id, scope_key),
            )
            conn.commit()
            stats = {
                "run_id": run_id, "snapshot_id": snapshot_id, "snapshot_version": snapshot_version,
                "scope_key": scope_key, "scope_sha256": scope_sha, "target_titles": len(targets),
                "files": file_stats, **materialized,
                "resume_strategy": "scan_each_gzip_from_start_and_discard_through_committed_record",
            }
        _finish_run(database_url, run_id, "succeeded", stats)
        return stats
    except Exception as exc:
        if snapshot_id:
            with psycopg.connect(database_url) as failure_conn:
                failure_conn.execute(
                    """UPDATE meta.imdb_selective_scope SET status='failed',updated_at=clock_timestamp()
                       WHERE snapshot_id=%s AND scope_key=%s""",
                    (snapshot_id, scope_key),
                )
        if run_id:
            _finish_run(database_url, run_id, "failed", {"error_type": type(exc).__name__, "error": str(exc)})
        raise


def load_evidence_bundle(
    database_url: str, *, evidence_file: Path | str, snapshot_version: str, scope_key: str, batch_id: str,
) -> dict[str, Any]:
    evidence = json.loads(Path(evidence_file).read_text(encoding="utf-8"))
    tconst = evidence["tconst"]
    run_id = 0
    snapshot_id = 0
    try:
        with psycopg.connect(database_url) as conn:
            _scope_lock(conn, snapshot_version, scope_key)
            snapshot_id, files = _snapshot(conn, snapshot_version)
            targets = [{"tconst": tconst, "asin": None, "adoption_origin": None, "product_title_raw": None}]
            selection_version = "explicit-evidence-bundle-v1"
            scope_sha = _scope_digest(targets, snapshot_version, selection_version)
            run_id = _begin_run(conn, batch_id, snapshot_version)
            _register_scope(conn, snapshot_id, scope_key, selection_version, scope_sha, targets)
            source_files = _register_source_files(conn, snapshot_version, files)
            sections: list[tuple[str, list[dict[str, Any]]]] = [
                ("title.basics.tsv.gz", [evidence["title"]] if evidence.get("title") else []),
                ("title.akas.tsv.gz", evidence.get("aliases", [])),
                ("title.crew.tsv.gz", [evidence["crew"]] if evidence.get("crew") else []),
                ("title.principals.tsv.gz", evidence.get("principals", [])),
                ("name.basics.tsv.gz", list(evidence.get("names", {}).values())),
            ]
            selected_counts: dict[str, int] = {}
            for file_name, rows in sections:
                selected_counts[file_name] = len(rows)
                path = Path(files[file_name]["path_hint"])
                header = REQUIRED_HEADERS[file_name]
                for values_with_meta in rows:
                    values = {name: str(values_with_meta[name]) for name in header}
                    content = "\t".join(values[name] for name in header).encode("utf-8")
                    expected_hash = values_with_meta["raw_line_sha256"]
                    if hashlib.sha256(content).hexdigest() != expected_hash:
                        raise ValueError(f"evidence bundle line hash mismatch: {file_name}")
                    row = SelectedRow(
                        record_number=int(values_with_meta["source_line"]) - 1,
                        source_line=int(values_with_meta["source_line"]), values=values,
                        content_sha256=expected_hash, raw_bytes=content,
                    )
                    source_record_id = _source_record(
                        conn, source_file_id=source_files[file_name], file_name=file_name,
                        path=path, row=row, run_id=run_id,
                    )
                    _insert_staging(conn, snapshot_id, file_name, row, source_record_id, run_id)
            materialized = _materialize(conn, snapshot_id, scope_key, snapshot_version, run_id)
            conn.execute(
                """UPDATE meta.imdb_selective_scope SET status='materialized',updated_at=clock_timestamp()
                   WHERE snapshot_id=%s AND scope_key=%s""",
                (snapshot_id, scope_key),
            )
            conn.commit()
            stats = {
                "run_id": run_id, "snapshot_id": snapshot_id, "snapshot_version": snapshot_version,
                "scope_key": scope_key, "target_titles": 1, "selected_records": selected_counts,
                **materialized, "source": "previously verified real evidence bundle",
            }
        _finish_run(database_url, run_id, "succeeded", stats)
        return stats
    except Exception as exc:
        if snapshot_id:
            with psycopg.connect(database_url) as failure_conn:
                failure_conn.execute(
                    """UPDATE meta.imdb_selective_scope SET status='failed',updated_at=clock_timestamp()
                       WHERE snapshot_id=%s AND scope_key=%s""",
                    (snapshot_id, scope_key),
                )
        if run_id:
            _finish_run(database_url, run_id, "failed", {"error_type": type(exc).__name__, "error": str(exc)})
        raise


def scope_status(database_url: str, snapshot_version: str, scope_key: str) -> dict[str, Any]:
    with psycopg.connect(database_url) as conn:
        snapshot = conn.execute(
            "SELECT snapshot_id FROM meta.external_snapshot WHERE snapshot_version=%s", (snapshot_version,)
        ).fetchone()
        if not snapshot:
            raise ValueError("unknown snapshot")
        scope = conn.execute(
            """SELECT status,target_title_count,scope_sha256,updated_at FROM meta.imdb_selective_scope
               WHERE snapshot_id=%s AND scope_key=%s""",
            (snapshot[0], scope_key),
        ).fetchone()
        progress = conn.execute(
            """SELECT file_name,status,committed_record_number,next_record_number,
                      selected_record_count,reached_eof,updated_at
               FROM meta.imdb_selective_progress WHERE snapshot_id=%s AND scope_key=%s
               ORDER BY file_name""",
            (snapshot[0], scope_key),
        ).fetchall()
    return {
        "snapshot_version": snapshot_version, "scope_key": scope_key,
        "scope": None if not scope else {
            "status": scope[0], "target_title_count": scope[1], "scope_sha256": scope[2],
            "updated_at": scope[3].isoformat(),
        },
        "files": [
            {
                "file_name": row[0], "status": row[1], "committed_record_number": row[2],
                "next_record_number": row[3], "selected_record_count": row[4],
                "reached_eof": row[5], "updated_at": row[6].isoformat(),
            }
            for row in progress
        ],
    }
