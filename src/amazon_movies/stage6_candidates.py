from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable

import psycopg

from amazon_movies.imdb_selective_loader import (
    RULE_VERSION as SOURCE_RULE_VERSION,
    SelectedRow,
    _insert_staging,
    _register_source_files,
    _snapshot,
    _source_record,
)
from amazon_movies.imdb_snapshot import REQUIRED_HEADERS
from amazon_movies.stage4_v3 import title_forms


RULE_VERSION = "stage6-local-imdb-candidate-range-v1"
SCAN_FILES = ("title.akas.tsv.gz", "title.basics.tsv.gz")
STOP_TOKENS = {
    "the", "and", "for", "with", "from", "movie", "film", "dvd", "vhs", "blu", "ray",
    "edition", "special", "complete", "collection", "season", "volume", "part",
}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _tokens(value: str) -> set[str]:
    return {token for token in value.split() if len(token) >= 4 and token not in STOP_TOKENS and not token.isdigit()}


def _match_level(similarity: float, exact: bool) -> str:
    if exact:
        return "exact"
    if similarity >= 0.95:
        return "strong"
    if similarity >= 0.85:
        return "close"
    return "blocked"


def candidate_matches(
    source_title: str,
    products_by_exact: dict[str, list[dict[str, Any]]],
    products_by_token: dict[str, list[dict[str, Any]]],
    *,
    minimum_similarity: float = 0.82,
) -> list[tuple[dict[str, Any], float, str]]:
    normalized = title_forms(source_title)["full_normalized"]
    if not normalized:
        return []
    candidates: dict[str, dict[str, Any]] = {
        item["asin"]: item for item in products_by_exact.get(normalized, [])
    }
    for token in _tokens(normalized):
        for item in products_by_token.get(token, []):
            candidates[item["asin"]] = item
    output = []
    for item in candidates.values():
        similarity = SequenceMatcher(None, item["normalized"], normalized).ratio()
        exact = item["normalized"] == normalized
        if exact or similarity >= minimum_similarity:
            output.append((item, similarity, _match_level(similarity, exact)))
    return sorted(output, key=lambda x: (-x[1], x[0]["asin"]))


def _scope_products(conn: psycopg.Connection[Any], exclusions: dict[str, Any]) -> list[dict[str, Any]]:
    excluded = set(exclusions.get("development_asins", []))
    excluded.update(exclusions.get("movie_cross_version_development_asins", []))
    rows = conn.execute(
        """
        SELECT DISTINCT ON (p.asin) p.asin,p.title_raw,p.title_normalized,
               p.source_record_id,p.source_record_number
        FROM staging.product_metadata_record p
        JOIN target.product_edition pe ON pe.asin=p.asin
        WHERE p.proposed_classification='movie' AND p.title_raw IS NOT NULL
          AND NOT (p.asin=ANY(%s))
        ORDER BY p.asin,p.product_metadata_id DESC
        """, (sorted(excluded) or [""],)
    ).fetchall()
    return [
        {
            "asin": row[0], "title_raw": row[1],
            "normalized": title_forms(row[1])["full_normalized"],
            "source_record_id": row[3], "source_record_number": row[4],
        }
        for row in rows if title_forms(row[1])["full_normalized"]
    ]


def _scope_sha(snapshot_version: str, products: list[dict[str, Any]], exclusions: dict[str, Any]) -> str:
    payload = {
        "snapshot_version": snapshot_version, "rule_version": RULE_VERSION,
        "products": [(p["asin"], p["normalized"], p["source_record_id"]) for p in products],
        "development_exclusion_manifest_sha256": exclusions.get("manifest_sha256"),
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _begin_run(conn: psycopg.Connection[Any], batch_id: str, snapshot_version: str) -> int:
    return conn.execute(
        """INSERT INTO meta.etl_run(pipeline_name,batch_id,input_version,rule_version,status,stats)
           VALUES ('stage6_candidate_range',%s,%s,%s,'running','{}'::jsonb)
           ON CONFLICT (pipeline_name,batch_id,input_version,rule_version) DO UPDATE SET
             status='running',finished_at=NULL,stats='{}'::jsonb,started_at=clock_timestamp()
           RETURNING run_id""", (batch_id, snapshot_version, RULE_VERSION)
    ).fetchone()[0]


def _register_scope(
    conn: psycopg.Connection[Any], snapshot_id: int, scope_key: str, scope_sha: str,
    products: list[dict[str, Any]], exclusions: dict[str, Any], random_seed: int,
) -> None:
    existing = conn.execute(
        "SELECT scope_sha256 FROM meta.stage6_candidate_scope WHERE snapshot_id=%s AND scope_key=%s",
        (snapshot_id, scope_key),
    ).fetchone()
    if existing and existing[0] != scope_sha:
        raise ValueError("scope_key already exists with a different product/exclusion set")
    conn.execute(
        """INSERT INTO meta.stage6_candidate_scope(
             snapshot_id,scope_key,rule_version,scope_sha256,random_seed,target_product_count,
             development_exclusion,status)
           VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,'registered')
           ON CONFLICT (snapshot_id,scope_key) DO UPDATE SET updated_at=clock_timestamp()""",
        (snapshot_id, scope_key, RULE_VERSION, scope_sha, random_seed, len(products), _json(exclusions)),
    )
    for product in products:
        conn.execute(
            """INSERT INTO meta.stage6_candidate_scope_product(
                 snapshot_id,scope_key,asin,product_title_raw,product_title_normalized,
                 product_source_record_id,product_source_record_number)
               VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (snapshot_id, scope_key, product["asin"], product["title_raw"], product["normalized"],
             product["source_record_id"], product["source_record_number"]),
        )


def _indexes(products: Iterable[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    exact: dict[str, list[dict[str, Any]]] = defaultdict(list)
    token: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for product in products:
        exact[product["normalized"]].append(product)
        for value in _tokens(product["normalized"]):
            token[value].append(product)
    return exact, token


def _upsert_candidate(
    conn: psycopg.Connection[Any], *, snapshot_id: int, scope_key: str, asin: str,
    tconst: str, similarity: float, level: str, evidence: dict[str, Any], run_id: int,
    basics: dict[str, Any] | None = None, basics_source_record_id: int | None = None,
) -> None:
    conn.execute(
        """INSERT INTO integration.stage6_movie_candidate_cache(
             snapshot_id,scope_key,asin,tconst,best_title_similarity,title_match_level,
             matched_titles,primary_title,original_title,title_type,start_year,genres,
             basics_source_record_id,type_compatibility,rule_version,run_id)
           VALUES (%s,%s,%s,%s,%s,%s,jsonb_build_array(%s::jsonb),%s,%s,%s,%s,%s,%s,
                   %s,%s,%s)
           ON CONFLICT (snapshot_id,scope_key,asin,tconst) DO UPDATE SET
             best_title_similarity=GREATEST(integration.stage6_movie_candidate_cache.best_title_similarity,EXCLUDED.best_title_similarity),
             title_match_level=CASE
               WHEN EXCLUDED.best_title_similarity > integration.stage6_movie_candidate_cache.best_title_similarity
               THEN EXCLUDED.title_match_level ELSE integration.stage6_movie_candidate_cache.title_match_level END,
             matched_titles=integration.stage6_movie_candidate_cache.matched_titles || EXCLUDED.matched_titles,
             primary_title=COALESCE(EXCLUDED.primary_title,integration.stage6_movie_candidate_cache.primary_title),
             original_title=COALESCE(EXCLUDED.original_title,integration.stage6_movie_candidate_cache.original_title),
             title_type=COALESCE(EXCLUDED.title_type,integration.stage6_movie_candidate_cache.title_type),
             start_year=COALESCE(EXCLUDED.start_year,integration.stage6_movie_candidate_cache.start_year),
             genres=CASE WHEN cardinality(EXCLUDED.genres)>0 THEN EXCLUDED.genres ELSE integration.stage6_movie_candidate_cache.genres END,
             basics_source_record_id=COALESCE(EXCLUDED.basics_source_record_id,integration.stage6_movie_candidate_cache.basics_source_record_id),
             type_compatibility=CASE WHEN EXCLUDED.title_type IS NOT NULL THEN EXCLUDED.type_compatibility ELSE integration.stage6_movie_candidate_cache.type_compatibility END,
             updated_at=clock_timestamp(),run_id=EXCLUDED.run_id""",
        (
            snapshot_id, scope_key, asin, tconst, round(similarity, 5), level, _json(evidence),
            None if not basics else basics.get("primaryTitle"),
            None if not basics else basics.get("originalTitle"),
            None if not basics else basics.get("titleType"),
            None if not basics or basics.get("startYear") in (None, r"\N") else int(basics["startYear"]),
            [] if not basics or basics.get("genres") == r"\N" else basics["genres"].split(","),
            basics_source_record_id,
            "unknown" if not basics else _type_compatibility(basics.get("titleType")),
            RULE_VERSION, run_id,
        ),
    )


def _type_compatibility(title_type: str | None) -> str:
    if title_type in {"movie", "tvMovie", "short"}:
        return "compatible"
    if title_type in {"video"}:
        return "unknown"
    if title_type is None:
        return "unknown"
    return "conflict"


def _scan_file(
    conn: psycopg.Connection[Any], *, snapshot_id: int, scope_key: str, scope_sha: str,
    file_name: str, path: Path, source_file_id: int, run_id: int, products: list[dict[str, Any]],
    batch_size: int, progress_every: int,
) -> dict[str, Any]:
    progress = conn.execute(
        """SELECT next_record_number,matched_record_count,reached_eof,scope_sha256
           FROM meta.stage6_candidate_progress
           WHERE snapshot_id=%s AND scope_key=%s AND file_name=%s""",
        (snapshot_id, scope_key, file_name),
    ).fetchone()
    if progress and progress[3] != scope_sha:
        raise ValueError(f"checkpoint scope conflict for {file_name}")
    if progress and progress[2]:
        return {"file_name": file_name, "already_complete": True, "matched_records": progress[1]}
    start_record = progress[0] if progress else 1
    matched_total = progress[1] if progress else 0
    if not progress:
        conn.execute(
            """INSERT INTO meta.stage6_candidate_progress(
                 snapshot_id,scope_key,file_name,scope_sha256,status,run_id)
               VALUES (%s,%s,%s,%s,'pending',%s)""",
            (snapshot_id, scope_key, file_name, scope_sha, run_id),
        )
        conn.commit()
    exact, token = _indexes(products)
    known_tconsts = set()
    if file_name == "title.basics.tsv.gz":
        known_tconsts = {
            row[0] for row in conn.execute(
                """SELECT DISTINCT tconst FROM integration.stage6_movie_candidate_cache
                   WHERE snapshot_id=%s AND scope_key=%s""", (snapshot_id, scope_key)
            ).fetchall()
        }
    header_expected = list(REQUIRED_HEADERS[file_name])
    last_record = start_record - 1
    matched_pending = 0
    with gzip.open(path, "rb") as stream:
        header = stream.readline().decode("utf-8", errors="strict").rstrip("\r\n").split("\t")
        if header != header_expected:
            raise ValueError(f"header changed for {file_name}")
        for record_number, raw_with_newline in enumerate(stream, 1):
            last_record = record_number
            if record_number < start_record:
                continue
            raw = raw_with_newline.rstrip(b"\r\n")
            text = raw.decode("utf-8", errors="strict")
            values_list = text.split("\t")
            if len(values_list) != len(header):
                raise ValueError(f"TSV column count mismatch in {file_name} record {record_number}")
            values = dict(zip(header, values_list, strict=True))
            title_values = [values["title"]] if file_name == "title.akas.tsv.gz" else [values["primaryTitle"], values["originalTitle"]]
            matches: dict[str, tuple[dict[str, Any], float, str, str]] = {}
            for source_title in title_values:
                for product, similarity, level in candidate_matches(source_title, exact, token):
                    previous = matches.get(product["asin"])
                    if previous is None or similarity > previous[1]:
                        matches[product["asin"]] = (product, similarity, level, source_title)
            tconst = values["titleId"] if file_name == "title.akas.tsv.gz" else values["tconst"]
            should_store_basics = file_name == "title.basics.tsv.gz" and tconst in known_tconsts
            if matches or should_store_basics:
                row = SelectedRow(record_number, record_number + 1, values, hashlib.sha256(raw).hexdigest(), raw)
                source_record_id = _source_record(
                    conn, source_file_id=source_file_id, file_name=file_name, path=path, row=row, run_id=run_id,
                )
                _insert_staging(conn, snapshot_id, file_name, row, source_record_id, run_id)
                matched_pending += 1
                matched_total += 1
                for product, similarity, level, source_title in matches.values():
                    _upsert_candidate(
                        conn, snapshot_id=snapshot_id, scope_key=scope_key, asin=product["asin"],
                        tconst=tconst, similarity=similarity, level=level,
                        evidence={
                            "file_name": file_name, "record_number": record_number,
                            "source_record_id": source_record_id, "source_title_raw": source_title,
                            "source_title_normalized": title_forms(source_title)["full_normalized"],
                            "product_title_normalized": product["normalized"], "similarity": round(similarity, 5),
                        },
                        run_id=run_id, basics=values if file_name == "title.basics.tsv.gz" else None,
                        basics_source_record_id=source_record_id if file_name == "title.basics.tsv.gz" else None,
                    )
                if should_store_basics:
                    update_year = None if values["startYear"] == r"\N" else int(values["startYear"])
                    update_genres = [] if values["genres"] == r"\N" else values["genres"].split(",")
                    conn.execute(
                        """UPDATE integration.stage6_movie_candidate_cache SET
                             primary_title=%s,original_title=%s,title_type=%s,
                             start_year=%s,genres=%s,
                             basics_source_record_id=%s,type_compatibility=%s,run_id=%s,updated_at=clock_timestamp()
                           WHERE snapshot_id=%s AND scope_key=%s AND tconst=%s""",
                        (values["primaryTitle"], values["originalTitle"], values["titleType"],
                         update_year, update_genres,
                         source_record_id, _type_compatibility(values["titleType"]), run_id,
                         snapshot_id, scope_key, tconst),
                    )
            if record_number % batch_size == 0:
                conn.execute(
                    """UPDATE meta.stage6_candidate_progress SET committed_record_number=%s,
                         next_record_number=%s,matched_record_count=%s,status='running',run_id=%s,
                         updated_at=clock_timestamp()
                       WHERE snapshot_id=%s AND scope_key=%s AND file_name=%s""",
                    (record_number, record_number + 1, matched_total, run_id, snapshot_id, scope_key, file_name),
                )
                conn.commit()
                matched_pending = 0
            if progress_every and record_number % progress_every == 0:
                print(_json({"file_name": file_name, "scanned": record_number, "matched_records": matched_total}), flush=True)
    conn.execute(
        """UPDATE meta.stage6_candidate_progress SET committed_record_number=%s,next_record_number=%s,
             matched_record_count=%s,reached_eof=true,status='verified_complete',run_id=%s,
             updated_at=clock_timestamp()
           WHERE snapshot_id=%s AND scope_key=%s AND file_name=%s""",
        (last_record, last_record + 1, matched_total, run_id, snapshot_id, scope_key, file_name),
    )
    conn.commit()
    return {"file_name": file_name, "matched_records": matched_total, "reached_eof": True}


def _rank_shortlist(conn: psycopg.Connection[Any], snapshot_id: int, scope_key: str, limit_per_product: int) -> int:
    conn.execute(
        "UPDATE integration.stage6_movie_candidate_cache SET shortlist_rank=NULL WHERE snapshot_id=%s AND scope_key=%s",
        (snapshot_id, scope_key),
    )
    conn.execute(
        """WITH ranked AS (
             SELECT snapshot_id,scope_key,asin,tconst,
                    row_number() OVER (PARTITION BY asin ORDER BY
                      CASE title_match_level WHEN 'exact' THEN 0 WHEN 'strong' THEN 1 WHEN 'close' THEN 2 ELSE 3 END,
                      CASE type_compatibility WHEN 'compatible' THEN 0 WHEN 'unknown' THEN 1 ELSE 2 END,
                      best_title_similarity DESC,tconst) AS rn
             FROM integration.stage6_movie_candidate_cache
             WHERE snapshot_id=%s AND scope_key=%s AND primary_title IS NOT NULL
           )
           UPDATE integration.stage6_movie_candidate_cache c SET shortlist_rank=r.rn
           FROM ranked r WHERE c.snapshot_id=r.snapshot_id AND c.scope_key=r.scope_key
             AND c.asin=r.asin AND c.tconst=r.tconst AND r.rn<=%s""",
        (snapshot_id, scope_key, limit_per_product),
    )
    return conn.execute(
        "SELECT count(*) FROM integration.stage6_movie_candidate_cache WHERE snapshot_id=%s AND scope_key=%s AND shortlist_rank IS NOT NULL",
        (snapshot_id, scope_key),
    ).fetchone()[0]


def scan_candidate_range(
    database_url: str, *, snapshot_dir: Path, snapshot_version: str, exclusions_file: Path,
    scope_key: str, batch_id: str, random_seed: int = 6022026, batch_size: int = 500_000,
    progress_every: int = 5_000_000, limit_per_product: int = 5,
    minimum_products: int = 100,
) -> dict[str, Any]:
    exclusions = json.loads(exclusions_file.read_text(encoding="utf-8"))
    with psycopg.connect(database_url) as conn:
        snapshot_id, files = _snapshot(conn, snapshot_version)
        products = _scope_products(conn, exclusions)
        if len(products) < minimum_products:
            raise ValueError(f"too few non-development movie products for evaluation range: {len(products)}")
        scope_sha = _scope_sha(snapshot_version, products, exclusions)
        run_id = _begin_run(conn, batch_id, snapshot_version)
        lock_key = f"stage6-candidate:{snapshot_version}:{scope_key}"
        if not conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (lock_key,)).fetchone()[0]:
            raise RuntimeError(f"candidate scope already running: {scope_key}")
        _register_scope(conn, snapshot_id, scope_key, scope_sha, products, exclusions, random_seed)
        source_files = _register_source_files(conn, snapshot_version, files)
        conn.execute(
            "UPDATE meta.stage6_candidate_scope SET status='scanning',updated_at=clock_timestamp() WHERE snapshot_id=%s AND scope_key=%s",
            (snapshot_id, scope_key),
        )
        conn.commit()
        results = []
        try:
            for file_name in SCAN_FILES:
                results.append(_scan_file(
                    conn, snapshot_id=snapshot_id, scope_key=scope_key, scope_sha=scope_sha,
                    file_name=file_name, path=snapshot_dir / file_name,
                    source_file_id=source_files[file_name], run_id=run_id, products=products,
                    batch_size=batch_size, progress_every=progress_every,
                ))
            shortlisted = _rank_shortlist(conn, snapshot_id, scope_key, limit_per_product)
            candidate_rows = conn.execute(
                "SELECT count(*) FROM integration.stage6_movie_candidate_cache WHERE snapshot_id=%s AND scope_key=%s",
                (snapshot_id, scope_key),
            ).fetchone()[0]
            covered = conn.execute(
                "SELECT count(DISTINCT asin) FROM integration.stage6_movie_candidate_cache WHERE snapshot_id=%s AND scope_key=%s",
                (snapshot_id, scope_key),
            ).fetchone()[0]
            stats = {
                "snapshot_id": snapshot_id, "scope_key": scope_key, "target_products": len(products),
                "candidate_rows": candidate_rows, "covered_products": covered,
                "shortlisted_pairs": shortlisted, "files": results,
                "resume_strategy": "scan_gzip_from_start_to_committed_record",
            }
            conn.execute(
                "UPDATE meta.stage6_candidate_scope SET status='candidates_ready',updated_at=clock_timestamp() WHERE snapshot_id=%s AND scope_key=%s",
                (snapshot_id, scope_key),
            )
            conn.execute(
                "UPDATE meta.etl_run SET status='succeeded',finished_at=clock_timestamp(),stats=%s::jsonb WHERE run_id=%s",
                (_json(stats), run_id),
            )
            conn.commit()
            return {"run_id": run_id, **stats}
        except Exception:
            conn.rollback()
            with psycopg.connect(database_url) as failure:
                failure.execute(
                    "UPDATE meta.stage6_candidate_scope SET status='failed',updated_at=clock_timestamp() WHERE snapshot_id=%s AND scope_key=%s",
                    (snapshot_id, scope_key),
                )
                failure.execute(
                    "UPDATE meta.etl_run SET status='failed',finished_at=clock_timestamp() WHERE run_id=%s",
                    (run_id,),
                )
            raise


def candidate_status(database_url: str, snapshot_version: str, scope_key: str) -> dict[str, Any]:
    with psycopg.connect(database_url) as conn:
        scope = conn.execute(
            """SELECT s.snapshot_id,s.status,s.target_product_count,s.scope_sha256,s.random_seed
               FROM meta.stage6_candidate_scope s JOIN meta.external_snapshot x USING(snapshot_id)
               WHERE x.snapshot_version=%s AND s.scope_key=%s""", (snapshot_version, scope_key)
        ).fetchone()
        if not scope:
            return {"scope_key": scope_key, "status": "not_registered"}
        progress = [
            {"file_name": r[0], "committed_record_number": r[1], "matched_record_count": r[2], "reached_eof": r[3], "status": r[4]}
            for r in conn.execute(
                """SELECT file_name,committed_record_number,matched_record_count,reached_eof,status
                   FROM meta.stage6_candidate_progress WHERE snapshot_id=%s AND scope_key=%s ORDER BY file_name""",
                (scope[0], scope_key),
            ).fetchall()
        ]
        counts = conn.execute(
            """SELECT count(*),count(DISTINCT asin),count(*) FILTER (WHERE shortlist_rank IS NOT NULL)
               FROM integration.stage6_movie_candidate_cache WHERE snapshot_id=%s AND scope_key=%s""",
            (scope[0], scope_key),
        ).fetchone()
    return {
        "snapshot_id": scope[0], "scope_key": scope_key, "status": scope[1],
        "target_products": scope[2], "scope_sha256": scope[3], "random_seed": scope[4],
        "candidate_rows": counts[0], "covered_products": counts[1], "shortlisted_pairs": counts[2],
        "progress": progress,
    }


def export_shortlist_tconsts(database_url: str, snapshot_version: str, scope_key: str, output: Path) -> dict[str, Any]:
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(
            """SELECT DISTINCT c.tconst FROM integration.stage6_movie_candidate_cache c
               JOIN meta.external_snapshot x USING(snapshot_id)
               WHERE x.snapshot_version=%s AND c.scope_key=%s AND c.shortlist_rank IS NOT NULL
               ORDER BY c.tconst""", (snapshot_version, scope_key)
        ).fetchall()
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(f"{row[0]}\n" for row in rows), encoding="utf-8")
    return {"scope_key": scope_key, "tconst_count": len(rows), "output": str(output), "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
