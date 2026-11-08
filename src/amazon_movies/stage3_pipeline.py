from __future__ import annotations

import gzip
import csv
import hashlib
import html
import json
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable

import psycopg


UCSD_URL = (
    "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_v2/"
    "metaFiles2/meta_Movies_and_TV.json.gz"
)
RULE_VERSION = "stage3-product-movie-v2"
USER_AGENT = "Agent/amazon-movie-course-etl/0.2 (bounded academic validation; no access-control bypass)"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_title(value: str | None) -> str:
    text = html.unescape(value or "")
    text = re.sub(r"\[(?:vhs|dvd|blu-ray|4k)[^]]*\]", " ", text, flags=re.I)
    text = re.sub(r"\b(?:vhs|dvd|blu-ray|4k uhd)\b", " ", text, flags=re.I)
    text = re.sub(r"[^a-z0-9]+", " ", text.casefold())
    return " ".join(text.split())


def classify_product(record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    title = html.unescape(str(record.get("title") or ""))
    categories = [str(v) for v in record.get("category") or []]
    description = " ".join(str(v) for v in record.get("description") or [])
    joined = f"{title} {' '.join(categories)} {description}".casefold()
    matched: list[str] = []
    if re.search(r"\b(?:\d+[- ]?(?:movie|film|disc|pack)|collection|anthology|box set)\b", joined):
        matched.append("collection_token")
        return "collection", {"rule": "collection_token", "matched": matched}
    if re.search(r"\b(?:complete series|season [0-9]+|tv series|episode)\b", joined):
        matched.append("series_token")
        return "series", {"rule": "series_token", "matched": matched}
    if any(token in joined for token in ("aerobics", "workout", "instructional", "bible memory music videos")):
        matched.append("non_movie_program_token")
        return "non_movie", {"rule": "non_movie_program_token", "matched": matched}
    narrative = len(description) >= 120
    category_movie = "Movies" in categories
    if category_movie and narrative:
        return "movie", {
            "rule": "movie_category_plus_narrative_description",
            "matched": ["category:Movies", "description_length>=120"],
        }
    return "pending", {
        "rule": "insufficient_product_evidence",
        "matched": ["category:Movies"] if category_movie else [],
    }


def _open(url: str, timeout: int = 60):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.8"}),
        timeout=timeout,
    )


def _run(conn: psycopg.Connection[Any], pipeline: str, batch_id: str, input_version: str) -> int:
    row = conn.execute(
        """
        INSERT INTO meta.etl_run(pipeline_name,batch_id,input_version,rule_version,status)
        VALUES (%s,%s,%s,%s,'running')
        ON CONFLICT (pipeline_name,batch_id,input_version,rule_version) DO UPDATE
        SET status='running', finished_at=NULL
        RETURNING run_id
        """,
        (pipeline, batch_id, input_version, RULE_VERSION),
    ).fetchone()
    assert row
    return int(row[0])


def _finish(conn: psycopg.Connection[Any], run_id: int, stats: dict[str, Any]) -> None:
    conn.execute(
        "UPDATE meta.etl_run SET status='succeeded',finished_at=clock_timestamp(),stats=%s::jsonb WHERE run_id=%s",
        (_json(stats), run_id),
    )


def _source_record(
    conn: psycopg.Connection[Any], *, source_uri: str, record_number: int | None,
    payload: bytes, locator: dict[str, Any], raw_payload: Any,
    http_status: int | None = None, access_status: str | None = None,
    snapshot_path: str | None = None,
) -> int:
    digest = _sha(payload)
    row = conn.execute(
        """
        INSERT INTO raw.source_record(
            source_kind,source_uri,record_number,field_locator,content_sha256,snapshot_path,
            fetched_at,http_status,access_status,raw_payload
        ) VALUES ('third_party_file',%s,%s,%s::jsonb,%s,%s,clock_timestamp(),%s,%s,%s::jsonb)
        ON CONFLICT DO NOTHING RETURNING source_record_id
        """,
        (source_uri, record_number, _json(locator), digest, snapshot_path, http_status, access_status, _json(raw_payload)),
    ).fetchone()
    if row:
        return int(row[0])
    existing = conn.execute(
        """SELECT source_record_id FROM raw.source_record
           WHERE source_file_id IS NULL AND record_number IS NOT DISTINCT FROM %s
             AND source_uri=%s AND content_sha256=%s""",
        (record_number, source_uri, digest),
    ).fetchone()
    assert existing
    return int(existing[0])


def _assertion(
    conn: psycopg.Connection[Any], *, entity_type: str, entity_key: str, field_name: str,
    raw_value: Any, normalized_value: Any, semantics: str, source_record_id: int | None,
    locator: dict[str, Any], quality_status: str, confidence: float, run_id: int,
) -> int:
    row = conn.execute(
        """
        INSERT INTO integration.field_assertion(
            entity_type,entity_key,field_name,raw_value,normalized_value,value_semantics,
            source_record_id,field_locator,extraction_rule_version,confidence,quality_status,run_id
        ) SELECT %s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s::jsonb,%s,%s,%s,%s
        WHERE NOT EXISTS (
            SELECT 1 FROM integration.field_assertion WHERE entity_type=%s AND entity_key=%s
              AND field_name=%s AND source_record_id IS NOT DISTINCT FROM %s
              AND extraction_rule_version=%s AND normalized_value IS NOT DISTINCT FROM %s::jsonb
        ) RETURNING assertion_id
        """,
        (entity_type, entity_key, field_name, _json(raw_value), _json(normalized_value), semantics,
         source_record_id, _json(locator), RULE_VERSION, confidence, quality_status, run_id,
         entity_type, entity_key, field_name, source_record_id, RULE_VERSION, _json(normalized_value)),
    ).fetchone()
    if row:
        return int(row[0])
    existing = conn.execute(
        """SELECT assertion_id FROM integration.field_assertion
           WHERE entity_type=%s AND entity_key=%s AND field_name=%s
             AND source_record_id IS NOT DISTINCT FROM %s AND extraction_rule_version=%s
             AND normalized_value IS NOT DISTINCT FROM %s::jsonb ORDER BY assertion_id LIMIT 1""",
        (entity_type, entity_key, field_name, source_record_id, RULE_VERSION, _json(normalized_value)),
    ).fetchone()
    assert existing
    return int(existing[0])


def load_ucsd_sample(
    database_url: str, *, asins: Iterable[str], max_records: int, batch_id: str,
    source_url: str = UCSD_URL,
) -> dict[str, Any]:
    targets = {asin.upper() for asin in asins}
    found: dict[str, tuple[int, bytes, dict[str, Any]]] = {}
    with _open(source_url) as response:
        headers = {k.lower(): v for k, v in response.headers.items()}
        source_version = "etag=" + headers.get("etag", "unknown") + ";last-modified=" + headers.get(
            "last-modified", "unknown"
        )
        with gzip.GzipFile(fileobj=response) as stream:
            scanned = 0
            for scanned, line in enumerate(stream, 1):
                if scanned > max_records:
                    scanned = max_records
                    break
                record = json.loads(line)
                asin = str(record.get("asin") or "").upper()
                if asin in targets:
                    found[asin] = (scanned, line.rstrip(b"\r\n"), record)
                if targets <= found.keys():
                    break

    with psycopg.connect(database_url) as conn:
        run_id = _run(conn, "stage3_ucsd_metadata", batch_id, source_version)
        for asin, (record_number, payload, record) in found.items():
            classification, basis = classify_product(record)
            source_record_id = _source_record(
                conn,
                source_uri=source_url,
                record_number=record_number,
                payload=payload,
                locator={
                    "format": "gzip_jsonl",
                    "record_number": record_number,
                    "source_version": source_version,
                    "content_length": headers.get("content-length"),
                },
                raw_payload=record,
                http_status=200,
                access_status="success",
            )
            title_raw = str(record.get("title") or "") or None
            conn.execute(
                """
                INSERT INTO staging.product_metadata_record(
                    asin,source_name,source_version,source_record_id,source_record_number,
                    title_raw,title_normalized,description_raw,categories_raw,brand_raw,source_date_raw,
                    proposed_classification,classification_basis,run_id
                ) VALUES (%s,'ucsd_movies_tv_2018',%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s::jsonb,%s)
                ON CONFLICT (asin,source_name,source_version,source_record_number) DO NOTHING
                """,
                (
                    asin, source_version, source_record_id, record_number, title_raw,
                    normalize_title(title_raw), _json(record.get("description") or []),
                    _json(record.get("category") or []), str(record.get("brand") or "") or None,
                    str(record.get("date") or "") or None, classification, _json(basis), run_id,
                ),
            )
            conn.execute(
                """UPDATE target.product_edition SET classification=%s
                   WHERE asin=%s AND classification='pending' AND %s <> 'pending'""",
                (classification, asin, classification),
            )
            conn.execute(
                """
                INSERT INTO integration.field_assertion(
                    entity_type,entity_key,field_name,raw_value,normalized_value,value_semantics,
                    source_record_id,field_locator,extraction_rule_version,confidence,quality_status,run_id
                ) SELECT 'product',%s,'title',%s::jsonb,%s::jsonb,'product_listing_title',
                    %s,%s::jsonb,%s,1.0,'observed',%s
                WHERE NOT EXISTS (
                    SELECT 1 FROM integration.field_assertion WHERE entity_type='product'
                    AND entity_key=%s AND field_name='title' AND source_record_id=%s
                    AND extraction_rule_version=%s
                )
                """,
                (asin, _json(title_raw), _json(normalize_title(title_raw)), source_record_id,
                 _json({"json_path": "$.title", "record_number": record_number}), RULE_VERSION,
                 run_id, asin, source_record_id, RULE_VERSION),
            )
            if classification == "pending":
                conn.execute(
                    """
                    INSERT INTO integration.review_queue(queue_type,entity_key,reason_code,evidence,run_id)
                    VALUES ('product_classification',%s,'insufficient_offline_evidence',%s::jsonb,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (asin, _json({"source_record_id": source_record_id, "basis": basis}), run_id),
                )
        stats = {
            "requested_asins": len(targets), "scanned_records": scanned, "matched_asins": len(found),
            "missing_asins": sorted(targets - found.keys()), "source_version": source_version,
            "bounded": True, "full_source_complete": False,
        }
        _finish(conn, run_id, stats)
    return {"run_id": run_id, **stats, "matched": sorted(found)}


def _fetch_json_evidence(conn: psycopg.Connection[Any], url: str) -> tuple[int, dict[str, Any]]:
    with _open(url) as response:
        payload = response.read(2_000_000)
        status = int(response.status)
        record = json.loads(payload)
    source_record_id = _source_record(
        conn, source_uri=url, record_number=None, payload=payload,
        locator={"format": "json", "bounded_max_bytes": 2_000_000}, raw_payload=record,
        http_status=status, access_status="success",
    )
    return source_record_id, record


def verify_imdb_candidate(
    database_url: str, *, asin: str, tconst: str, suggestion_url: str,
    support_url: str, batch_id: str,
) -> dict[str, Any]:
    with psycopg.connect(database_url) as conn:
        run_id = _run(conn, "stage3_movie_candidate", batch_id, tconst)
        imdb_source_id, suggestion = _fetch_json_evidence(conn, suggestion_url)
        support_source_id, support = _fetch_json_evidence(conn, support_url)
        candidates = [item for item in suggestion.get("d", []) if item.get("id") == tconst]
        if not candidates:
            raise ValueError(f"IMDb suggestion response does not contain {tconst}")
        candidate = candidates[0]
        product = conn.execute(
            """SELECT title_raw,title_normalized,description_raw,source_record_id
               FROM staging.product_metadata_record WHERE asin=%s
               ORDER BY created_at DESC LIMIT 1""",
            (asin,),
        ).fetchone()
        if not product:
            raise ValueError(f"no offline product evidence for {asin}")
        product_title, normalized_product_title, descriptions, product_source_id = product
        candidate_title = str(candidate.get("l") or "")
        title_similarity = SequenceMatcher(None, normalized_product_title, normalize_title(candidate_title)).ratio()
        product_description = " ".join(descriptions or [])
        support_extract = str(support.get("extract") or "")
        description_similarity = SequenceMatcher(
            None, normalize_title(product_description), normalize_title(support_extract)
        ).ratio() if support_extract else 0.0
        year = candidate.get("y")
        people = [v.strip() for v in str(candidate.get("s") or "").split(",") if v.strip()]
        support_year_match = re.search(r"\b(18|19|20)\d{2}\b", support_extract)
        support_year = int(support_year_match.group(0)) if support_year_match else None
        director_match = re.search(r"directed by ([A-Z][A-Za-z .'-]+?)(?: and|,|\.)", support_extract)
        support_director = director_match.group(1).strip() if director_match else None
        corroborating = [
            {"fact": "normalized_title", "value": candidate_title, "source_record_id": imdb_source_id},
            {"fact": "release_year", "value": year, "source_record_id": imdb_source_id},
            {"fact": "title_type", "value": candidate.get("q"), "source_record_id": imdb_source_id},
            {"fact": "principal_names", "value": people, "source_record_id": imdb_source_id},
            {"fact": "support_summary", "value": support_extract, "source_record_id": support_source_id},
            {"fact": "support_year", "value": support_year, "source_record_id": support_source_id},
            {"fact": "support_director", "value": support_director, "source_record_id": support_source_id},
        ]
        verified = (
            title_similarity >= 0.98 and year is not None and support_year == year
            and bool(people) and bool(support_director)
        )
        status = "verified" if verified else "review"
        reason = (
            "normalized product title exact; IMDb and support source agree on year; IMDb supplies principal names; support source supplies director"
            if verified else
            "candidate lacks the configured multi-fact threshold; no automatic merge"
        )
        conn.execute(
            """
            INSERT INTO integration.product_movie_candidate(
                asin,candidate_key,source_record_id,candidate_title,candidate_year,candidate_type,
                title_similarity,description_similarity,corroborating_facts,decision_status,
                decision_reason,rule_version,run_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s)
            ON CONFLICT (asin,candidate_key,rule_version) DO UPDATE SET
                source_record_id=EXCLUDED.source_record_id,candidate_title=EXCLUDED.candidate_title,
                candidate_year=EXCLUDED.candidate_year,candidate_type=EXCLUDED.candidate_type,
                title_similarity=EXCLUDED.title_similarity,description_similarity=EXCLUDED.description_similarity,
                corroborating_facts=EXCLUDED.corroborating_facts,decision_status=EXCLUDED.decision_status,
                decision_reason=EXCLUDED.decision_reason,run_id=EXCLUDED.run_id
            """,
            (asin, f"imdb:{tconst}", imdb_source_id, candidate_title, year, candidate.get("q"),
             title_similarity, description_similarity, _json(corroborating), status, reason,
             RULE_VERSION, run_id),
        )
        candidate_key = f"imdb:{tconst}"
        title_assertion = _assertion(
            conn, entity_type="movie_candidate", entity_key=candidate_key, field_name="title",
            raw_value=candidate_title, normalized_value=normalize_title(candidate_title),
            semantics="imdb_suggestion_title", source_record_id=imdb_source_id,
            locator={"json_path": "$.d[?id].l"}, quality_status="observed", confidence=1.0, run_id=run_id,
        )
        imdb_year_assertion = _assertion(
            conn, entity_type="movie_candidate", entity_key=candidate_key, field_name="release_year",
            raw_value=year, normalized_value=year, semantics="imdb_suggestion_year",
            source_record_id=imdb_source_id, locator={"json_path": "$.d[?id].y"},
            quality_status="observed", confidence=1.0, run_id=run_id,
        )
        support_year_assertion = _assertion(
            conn, entity_type="movie_candidate", entity_key=candidate_key, field_name="release_year",
            raw_value=support_extract, normalized_value=support_year, semantics="year_extracted_from_support_summary",
            source_record_id=support_source_id, locator={"json_path": "$.extract", "regex": "four_digit_year"},
            quality_status="observed", confidence=0.95, run_id=run_id,
        )
        director_assertion = _assertion(
            conn, entity_type="movie_candidate", entity_key=candidate_key, field_name="director",
            raw_value=support_extract, normalized_value=support_director,
            semantics="director_extracted_from_support_summary", source_record_id=support_source_id,
            locator={"json_path": "$.extract", "regex": "directed by NAME"},
            quality_status="observed", confidence=0.95, run_id=run_id,
        )
        people_assertion = _assertion(
            conn, entity_type="movie_candidate", entity_key=candidate_key, field_name="principal_names",
            raw_value=candidate.get("s"), normalized_value=people, semantics="imdb_suggestion_principal_names",
            source_record_id=imdb_source_id, locator={"json_path": "$.d[?id].s"},
            quality_status="observed", confidence=0.9, run_id=run_id,
        )
        if verified:
            movie_id = conn.execute(
                """
                INSERT INTO target.movie(stable_key,canonical_title,release_year,release_precision,release_type,status)
                VALUES (%s,%s,%s,'year',%s,'verified')
                ON CONFLICT (stable_key) DO UPDATE SET
                    canonical_title=EXCLUDED.canonical_title, release_year=EXCLUDED.release_year,
                    release_precision=EXCLUDED.release_precision, release_type=EXCLUDED.release_type,
                    status='verified'
                RETURNING movie_id
                """,
                (f"imdb:{tconst}", candidate_title, year, candidate.get("q")),
            ).fetchone()[0]
            decision_id = conn.execute(
                """
                INSERT INTO integration.merge_decision(
                    entity_type,left_entity_key,right_entity_key,decision,decision_origin,
                    rule_version,confidence,evidence,run_id
                ) VALUES ('product_movie',%s,%s,'merge','rule',%s,%s,%s::jsonb,%s)
                ON CONFLICT (entity_type,left_entity_key,right_entity_key,rule_version) DO UPDATE
                SET evidence=EXCLUDED.evidence, run_id=EXCLUDED.run_id
                RETURNING merge_decision_id
                """,
                (asin, f"imdb:{tconst}", RULE_VERSION, min(0.99999, (title_similarity + description_similarity) / 2),
                 _json({"facts": corroborating, "reason": reason, "product_source_record_id": product_source_id}), run_id),
            ).fetchone()[0]
            product_id = conn.execute("SELECT product_id FROM target.product_edition WHERE asin=%s", (asin,)).fetchone()[0]
            conn.execute(
                """
                INSERT INTO target.product_movie(product_id,movie_id,relationship_type,decision_id,mapping_status)
                VALUES (%s,%s,'single_work',%s,'verified')
                ON CONFLICT (product_id,movie_id,relationship_type) DO UPDATE
                SET decision_id=EXCLUDED.decision_id,mapping_status='verified'
                """,
                (product_id, movie_id, decision_id),
            )
            selected_title = _assertion(
                conn, entity_type="movie", entity_key=candidate_key, field_name="canonical_title",
                raw_value=None, normalized_value=candidate_title, semantics="selected_canonical_title",
                source_record_id=None, locator={"operation": "normalize_and_compare_titles"},
                quality_status="inferred", confidence=0.99, run_id=run_id,
            )
            product_title_assertion = conn.execute(
                """SELECT assertion_id FROM integration.field_assertion
                   WHERE entity_type='product' AND entity_key=%s AND field_name='title'
                     AND source_record_id=%s
                   ORDER BY assertion_id LIMIT 1""",
                (asin, product_source_id),
            ).fetchone()[0]
            for input_assertion, operation in (
                (product_title_assertion, "strip_carrier_and_normalize_product_title"),
                (title_assertion, "normalize_candidate_title"),
            ):
                conn.execute(
                    """INSERT INTO integration.assertion_dependency(
                           output_assertion_id,input_assertion_id,operation_type,rule_id,run_id)
                       VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (selected_title, input_assertion, operation, RULE_VERSION, run_id),
                )
            conn.execute(
                """
                INSERT INTO integration.field_selection(
                    entity_type,entity_key,field_name,selection_version,selected_assertion_id,
                    selected_assertion_ids,decision_status,decision_reason,run_id
                ) VALUES ('movie',%s,'canonical_title',%s,%s,%s,'selected',%s,%s)
                ON CONFLICT (entity_type,entity_key,field_name,selection_version) DO UPDATE
                SET selected_assertion_id=EXCLUDED.selected_assertion_id,
                    selected_assertion_ids=EXCLUDED.selected_assertion_ids,
                    decision_status=EXCLUDED.decision_status,decision_reason=EXCLUDED.decision_reason,
                    run_id=EXCLUDED.run_id,decided_at=clock_timestamp()
                """,
                (candidate_key, RULE_VERSION, selected_title, [title_assertion], reason, run_id),
            )
            conn.execute(
                """
                INSERT INTO integration.field_selection(
                    entity_type,entity_key,field_name,selection_version,selected_assertion_id,
                    selected_assertion_ids,decision_status,decision_reason,run_id
                ) VALUES ('movie',%s,'release_year',%s,%s,%s,'selected',%s,%s)
                ON CONFLICT (entity_type,entity_key,field_name,selection_version) DO UPDATE
                SET selected_assertion_id=EXCLUDED.selected_assertion_id,
                    selected_assertion_ids=EXCLUDED.selected_assertion_ids,
                    decision_status=EXCLUDED.decision_status,decision_reason=EXCLUDED.decision_reason,
                    run_id=EXCLUDED.run_id,decided_at=clock_timestamp()
                """,
                (candidate_key, RULE_VERSION, imdb_year_assertion,
                 [imdb_year_assertion, support_year_assertion], reason, run_id),
            )
            for order, name in enumerate(people, 1):
                person_key = "imdb-suggestion-name:" + normalize_title(name).replace(" ", "-")
                person_id = conn.execute(
                    """
                    INSERT INTO target.person(stable_key,canonical_name,status)
                    VALUES (%s,%s,'candidate')
                    ON CONFLICT (stable_key) DO UPDATE SET canonical_name=EXCLUDED.canonical_name
                    RETURNING person_id
                    """,
                    (person_key, name),
                ).fetchone()[0]
                conn.execute(
                    """
                    INSERT INTO target.movie_person(movie_id,person_id,role_type,credited_as,billing_order,source_record_id)
                    VALUES (%s,%s,'starring',%s,%s,%s) ON CONFLICT DO NOTHING
                    """,
                    (movie_id, person_id, name, order, imdb_source_id),
                )
            conn.execute(
                """UPDATE integration.review_queue SET status='resolved',resolved_at=clock_timestamp()
                   WHERE queue_type='product_movie' AND entity_key=%s AND candidate_key=%s
                     AND reason_code='multi_fact_threshold_not_met' AND status='open'""",
                (asin, f"imdb:{tconst}"),
            )
        else:
            conn.execute(
                """
                INSERT INTO integration.review_queue(queue_type,entity_key,candidate_key,reason_code,evidence,run_id)
                VALUES ('product_movie',%s,%s,'multi_fact_threshold_not_met',%s::jsonb,%s)
                ON CONFLICT DO NOTHING
                """,
                (asin, f"imdb:{tconst}", _json({"facts": corroborating, "reason": reason}), run_id),
            )
        stats = {
            "asin": asin, "candidate": f"imdb:{tconst}", "decision": status,
            "title_similarity": round(title_similarity, 5),
            "description_similarity": round(description_similarity, 5), "people": people,
            "support_year": support_year, "support_director": support_director,
        }
        _finish(conn, run_id, stats)
    return {"run_id": run_id, **stats}


def sample_report(database_url: str, asins: Iterable[str]) -> dict[str, Any]:
    target = list(asins)
    with psycopg.connect(database_url) as conn:
        offline = conn.execute(
            "SELECT count(DISTINCT asin) FROM staging.product_metadata_record WHERE asin=ANY(%s)", (target,)
        ).fetchone()[0]
        page = conn.execute(
            """SELECT status,count(*) FROM staging.product_harvest WHERE asin=ANY(%s)
               GROUP BY status ORDER BY status""", (target,)
        ).fetchall()
        review = conn.execute(
            "SELECT queue_type,count(*) FROM integration.review_queue WHERE status='open' GROUP BY queue_type"
        ).fetchall()
    return {
        "requested_products": len(target), "offline_matched": offline,
        "offline_coverage": offline / len(target) if target else None,
        "page_outcomes": dict(page), "open_review_queue": dict(review),
    }


def load_ucsd_full_file(
    database_url: str, *, metadata_file: Path, product_list_file: Path,
    source_sha256: str, batch_id: str, batch_size: int = 5000,
    partition_key: str = "ucsd-movies-tv-full-v1",
) -> dict[str, Any]:
    """Scan a local immutable gzip to clean EOF with transactional source-record checkpoints.

    Recovery deliberately decompresses from the start and skips through the committed record;
    a gzip compressed byte offset is never treated as a safe random-access point.
    """
    if batch_size < 100:
        raise ValueError("batch_size must be at least 100")
    source_sha256 = source_sha256.lower()
    if not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
        raise ValueError("source_sha256 must be 64 lowercase hexadecimal characters")
    actual_sha256 = _sha_file(metadata_file)
    if actual_sha256 != source_sha256:
        raise ValueError(f"metadata SHA-256 mismatch: expected {source_sha256}, actual {actual_sha256}")
    with product_list_file.open("r", encoding="utf-8", newline="") as stream:
        targets = {row["asin"].upper() for row in csv.DictReader(stream, delimiter="\t")}
    source_uri = metadata_file.resolve().as_uri()
    input_version = f"sha256:{source_sha256}"
    with psycopg.connect(database_url) as conn:
        run_id = _run(conn, "stage3_ucsd_metadata_full", batch_id, input_version)
        conn.execute(
            """
            INSERT INTO meta.source_file(source_name,source_uri,sha256,size_bytes,parser_version)
            VALUES ('ucsd_movies_tv_2018',%s,%s,%s,%s)
            ON CONFLICT (sha256) DO NOTHING
            """,
            (source_uri, source_sha256, metadata_file.stat().st_size, RULE_VERSION),
        )
        progress = conn.execute(
            """SELECT next_record_number,reached_eof FROM meta.product_metadata_progress
               WHERE source_sha256=%s AND partition_key=%s""",
            (source_sha256, partition_key),
        ).fetchone()
        resume_record = int(progress[0]) if progress else 1
        if progress and progress[1]:
            matched = conn.execute(
                "SELECT count(DISTINCT asin) FROM staging.product_metadata_record WHERE source_version=%s",
                (input_version,),
            ).fetchone()[0]
            _finish(conn, run_id, {"already_complete": True, "matched_asins": matched})
            return {"run_id": run_id, "already_complete": True, "matched_asins": matched}
        conn.execute(
            """
            INSERT INTO meta.product_metadata_progress(
                source_sha256,partition_key,committed_record_number,next_record_number,status,run_id)
            VALUES (%s,%s,%s,%s,'running',%s)
            ON CONFLICT (source_sha256,partition_key) DO UPDATE
            SET status='running',run_id=EXCLUDED.run_id,updated_at=clock_timestamp()
            """,
            (source_sha256, partition_key, resume_record - 1, resume_record, run_id),
        )
        conn.commit()

        last_committed = resume_record - 1
        scanned = 0
        try:
            with gzip.open(metadata_file, "rb") as stream:
                for scanned, line in enumerate(stream, 1):
                    if scanned < resume_record:
                        continue
                    record = json.loads(line)
                    asin = str(record.get("asin") or "").upper()
                    if asin in targets:
                        classification, basis = classify_product(record)
                        payload = line.rstrip(b"\r\n")
                        source_record_id = _source_record(
                            conn, source_uri=source_uri, record_number=scanned, payload=payload,
                            locator={"format": "gzip_jsonl", "record_number": scanned,
                                     "source_version": input_version}, raw_payload=record,
                            access_status="success",
                        )
                        title_raw = str(record.get("title") or "") or None
                        conn.execute(
                            """
                            INSERT INTO staging.product_metadata_record(
                                asin,source_name,source_version,source_record_id,source_record_number,
                                title_raw,title_normalized,description_raw,categories_raw,brand_raw,
                                source_date_raw,proposed_classification,classification_basis,run_id
                            ) VALUES (%s,'ucsd_movies_tv_2018',%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,
                                      %s,%s,%s,%s::jsonb,%s)
                            ON CONFLICT (asin,source_name,source_version,source_record_number) DO NOTHING
                            """,
                            (asin, input_version, source_record_id, scanned, title_raw,
                             normalize_title(title_raw), _json(record.get("description") or []),
                             _json(record.get("category") or []), str(record.get("brand") or "") or None,
                             str(record.get("date") or "") or None, classification, _json(basis), run_id),
                        )
                        conn.execute(
                            """UPDATE target.product_edition SET classification=%s
                               WHERE asin=%s AND classification='pending' AND %s <> 'pending'""",
                            (classification, asin, classification),
                        )
                        _assertion(
                            conn, entity_type="product", entity_key=asin, field_name="title",
                            raw_value=title_raw, normalized_value=normalize_title(title_raw),
                            semantics="product_listing_title", source_record_id=source_record_id,
                            locator={"json_path": "$.title", "record_number": scanned},
                            quality_status="observed", confidence=1.0, run_id=run_id,
                        )
                        if classification == "pending":
                            conn.execute(
                                """INSERT INTO integration.review_queue(
                                       queue_type,entity_key,reason_code,evidence,run_id)
                                   VALUES ('product_classification',%s,'insufficient_offline_evidence',%s::jsonb,%s)
                                   ON CONFLICT DO NOTHING""",
                                (asin, _json({"source_record_id": source_record_id, "basis": basis}), run_id),
                            )
                    if scanned % batch_size == 0:
                        conn.execute(
                            """UPDATE meta.product_metadata_progress SET committed_record_number=%s,
                               next_record_number=%s,status='running',run_id=%s,updated_at=clock_timestamp()
                               WHERE source_sha256=%s AND partition_key=%s""",
                            (scanned, scanned + 1, run_id, source_sha256, partition_key),
                        )
                        conn.commit()
                        last_committed = scanned
            conn.execute(
                """UPDATE meta.product_metadata_progress SET committed_record_number=%s,
                   next_record_number=%s,reached_eof=true,status='verified_complete',run_id=%s,
                   updated_at=clock_timestamp() WHERE source_sha256=%s AND partition_key=%s""",
                (scanned, scanned + 1, run_id, source_sha256, partition_key),
            )
            matched = conn.execute(
                "SELECT count(DISTINCT asin) FROM staging.product_metadata_record WHERE source_version=%s",
                (input_version,),
            ).fetchone()[0]
            stats = {
                "source_records": scanned, "matched_asins": matched, "target_asins": len(targets),
                "reached_eof": True, "resume_record": resume_record,
                "resume_strategy": "scan_from_start_to_committed_record",
            }
            _finish(conn, run_id, stats)
            return {"run_id": run_id, **stats}
        except Exception:
            conn.rollback()
            with psycopg.connect(database_url) as failure_conn:
                failure_conn.execute(
                    """UPDATE meta.etl_run SET status='failed',finished_at=clock_timestamp(),
                       stats=%s::jsonb WHERE run_id=%s""",
                    (_json({"last_committed_record": last_committed,
                            "resume_strategy": "scan_from_start_to_committed_record"}), run_id),
                )
                failure_conn.execute(
                    """UPDATE meta.product_metadata_progress SET status='failed',updated_at=clock_timestamp()
                       WHERE source_sha256=%s AND partition_key=%s""",
                    (source_sha256, partition_key),
                )
            raise
