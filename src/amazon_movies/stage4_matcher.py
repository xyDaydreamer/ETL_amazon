from __future__ import annotations

import csv
import html
import json
import re
import time
import urllib.parse
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import psycopg

from amazon_movies.stage3_pipeline import USER_AGENT, _finish, _json, _open, _run, _source_record, normalize_title


RULE_VERSION = "stage4-first50-movie-match-v1"
SOURCE_VERSION = "sha256:b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9"
NEGATIVE_TITLE_TOKENS = (
    "official review", "best of ufc", "aquarium", "hd moods", "bluscenes",
    "workout", "aerobics", "travel video", "scenic", "puppy power pack",
)
PACKAGING_TOKENS = re.compile(
    r"\s*(?:\[(?:blu-ray|dvd|vhs)[^]]*\]|\b(?:blu-ray|dvd|vhs|widescreen|special edition|collector'?s edition)\b)\s*",
    re.I,
)
YEAR_RE = re.compile(r"(?<!\d)((?:18|19|20)\d{2})(?!\d)")


def query_title(value: str) -> str:
    text = html.unescape(value).strip()
    text = PACKAGING_TOKENS.sub(" ", text)
    return " ".join(text.split()).strip(" -:,")


def _meaningful_brand(value: str | None) -> str | None:
    brand = html.unescape(value or "").strip()
    if not brand or brand.casefold() in {"none", "learn more", "artist not provided", "various"}:
        return None
    return brand


def _years(text: str | None) -> list[int]:
    return sorted({int(v) for v in YEAR_RE.findall(text or "")})


def _person_in_text(person: str, text: str) -> bool:
    needle = normalize_title(person)
    haystack = normalize_title(text)
    return bool(needle and len(needle) >= 5 and re.search(rf"(?:^| ){re.escape(needle)}(?: |$)", haystack))


def select_development_sample(database_url: str, *, batch_id: str, limit: int = 50) -> dict[str, Any]:
    if not 1 <= limit <= 50:
        raise ValueError("limit must be between 1 and 50")
    with psycopg.connect(database_url) as conn:
        run_id = _run(conn, "stage4_movie_development_selection", batch_id, SOURCE_VERSION)
        rows = conn.execute(
            """
            SELECT DISTINCT ON (m.asin)
                   m.asin,m.title_raw,m.description_raw,m.brand_raw,m.source_date_raw,
                   m.source_record_id,m.source_record_number
            FROM staging.product_metadata_record m
            WHERE m.source_version=%s AND m.proposed_classification='movie'
            ORDER BY m.asin,m.source_record_number
            """,
            (SOURCE_VERSION,),
        ).fetchall()
        scored: list[dict[str, Any]] = []
        for asin, title, descriptions, brand_raw, source_date, source_record_id, record_number in rows:
            title = title or ""
            cleaned = query_title(title)
            description = " ".join(descriptions or [])
            brand = _meaningful_brand(brand_raw)
            lowered = f"{cleaned} {description}".casefold()
            negative = [token for token in NEGATIVE_TITLE_TOKENS if token in lowered]
            if re.search(r"\s/\s", cleaned) or re.search(r"\b(?:double feature|triple feature|[234]-movie)\b", lowered):
                negative.append("multi_title_or_collection_marker")
            if not cleaned or negative:
                continue
            brand_in_description = bool(brand and _person_in_text(brand, description))
            narrative = len(description) >= 180
            film_language = bool(re.search(r"\b(?:film|movie|stars?|starring|director|cast)\b", description, re.I))
            title_years = _years(cleaned)
            score = (4 if brand_in_description else 0) + (2 if brand else 0) + (2 if narrative else 0)
            score += (2 if film_language else 0) + (1 if title_years else 0)
            if score < 4:
                continue
            scored.append({
                "asin": asin, "title": title, "query_title": cleaned, "description": description,
                "brand": brand, "source_date": source_date, "source_record_id": source_record_id,
                "record_number": record_number, "title_years": title_years, "score": score,
                "selection_evidence": {
                    "brand": brand, "brand_appears_in_description": brand_in_description,
                    "narrative_description": narrative, "film_language": film_language,
                    "title_years": title_years, "source_date_semantics": "product_listing_or_release_not_movie_release",
                },
            })
        scored.sort(key=lambda v: (-v["score"], v["asin"]))
        selected: list[dict[str, Any]] = []
        seen_titles: set[str] = set()
        for item in scored:
            title_key = normalize_title(item["query_title"])
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            selected.append(item)
            if len(selected) >= limit:
                break
        for rank, item in enumerate(selected, 1):
            conn.execute(
                """
                INSERT INTO integration.movie_match_development_sample(
                    asin,sample_rank,selection_score,product_source_record_id,product_title_raw,
                    query_title,product_release_date_raw,product_year_evidence,selection_evidence,
                    sample_role,selection_rule_version,run_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,'development',%s,%s)
                ON CONFLICT (asin) DO UPDATE SET
                    sample_rank=EXCLUDED.sample_rank,
                    selection_score=EXCLUDED.selection_score,
                    product_source_record_id=EXCLUDED.product_source_record_id,
                    product_title_raw=EXCLUDED.product_title_raw,
                    query_title=EXCLUDED.query_title,
                    product_release_date_raw=EXCLUDED.product_release_date_raw,
                    product_year_evidence=EXCLUDED.product_year_evidence,
                    selection_evidence=EXCLUDED.selection_evidence,
                    sample_role=EXCLUDED.sample_role,
                    selection_rule_version=EXCLUDED.selection_rule_version,
                    run_id=EXCLUDED.run_id,
                    selected_at=clock_timestamp()
                """,
                (item["asin"], rank, item["score"], item["source_record_id"], item["title"],
                 item["query_title"], item["source_date"], _json(item["title_years"]),
                 _json(item["selection_evidence"]), RULE_VERSION, run_id),
            )
        stats = {"eligible": len(scored), "selected": len(selected), "limit": limit, "sample_role": "development"}
        _finish(conn, run_id, stats)
    return {"run_id": run_id, **stats}


def _suggestion_url(title: str) -> str:
    slug = urllib.parse.quote(title, safe="")
    return f"https://v2.sg.media-imdb.com/suggestion/x/{slug}.json"


def _candidate_pool(payload: dict[str, Any], query: str) -> list[dict[str, Any]]:
    pool = []
    for item in payload.get("d", []):
        candidate_id = str(item.get("id") or "")
        if not candidate_id.startswith("tt"):
            continue
        title = str(item.get("l") or "")
        similarity = SequenceMatcher(None, normalize_title(query), normalize_title(title)).ratio()
        pool.append({
            "id": candidate_id, "title": title, "year": item.get("y"), "type": item.get("q"),
            "principals": [v.strip() for v in str(item.get("s") or "").split(",") if v.strip()],
            "similarity": round(similarity, 5), "rank": item.get("rank"),
        })
    pool.sort(key=lambda v: (-v["similarity"], v["rank"] if isinstance(v["rank"], int) else 10**12, v["id"]))
    return pool[:5]


def generate_imdb_candidates(database_url: str, *, batch_id: str, limit: int = 50) -> dict[str, Any]:
    with psycopg.connect(database_url) as conn:
        run_id = _run(conn, "stage4_imdb_candidate_generation", batch_id, RULE_VERSION)
        samples = conn.execute(
            """
            SELECT s.asin,s.product_title_raw,s.query_title,s.product_release_date_raw,
                   s.product_year_evidence,s.selection_evidence,s.product_source_record_id,
                   r.raw_payload
            FROM integration.movie_match_development_sample s
            JOIN raw.source_record r ON r.source_record_id=s.product_source_record_id
            WHERE s.selection_rule_version=%s ORDER BY s.sample_rank LIMIT %s
            """,
            (RULE_VERSION, limit),
        ).fetchall()
        counts = {"rule_verified": 0, "needs_review": 0, "no_candidate": 0, "fetch_failed": 0}
        for asin, product_title, query, product_date, product_years, selection, product_source_id, raw_product in samples:
            url = _suggestion_url(query)
            source_id: int | None = None
            pool: list[dict[str, Any]] = []
            fetch_error: str | None = None
            try:
                with _open(url, timeout=30) as response:
                    response_bytes = response.read(1_000_000)
                    payload = json.loads(response_bytes)
                    source_id = _source_record(
                        conn, source_uri=url, record_number=None, payload=response_bytes,
                        locator={"format": "json", "query_title": query, "bounded_max_bytes": 1_000_000},
                        raw_payload=payload, http_status=int(response.status), access_status="success",
                    )
                    pool = _candidate_pool(payload, query)
            except Exception as exc:
                fetch_error = f"{exc.__class__.__name__}: {str(exc)[:200]}"
            finally:
                # This is a bounded development-sample lookup, but still avoid
                # issuing back-to-back requests to the public suggestion host.
                time.sleep(0.25)

            if fetch_error:
                status, candidate_key, question = "fetch_failed", "fetch_failed", "IMDb候选请求失败，是否稍后重试？"
                best = None
                matched, conflicts = [], [fetch_error]
            elif not pool:
                status, candidate_key, question = "no_candidate", "no_candidate", "IMDb建议源未返回标题候选，是否人工检索？"
                best = None
                matched, conflicts = [], ["no IMDb title candidate returned"]
            else:
                best = pool[0]
                candidate_key = f"imdb:{best['id']}"
                description = " ".join(str(v) for v in (raw_product.get("description") or []))
                brand = _meaningful_brand(str(raw_product.get("brand") or ""))
                principal_hits = [p for p in best["principals"] if _person_in_text(p, description)]
                brand_hit = bool(brand and any(normalize_title(brand) == normalize_title(p) for p in best["principals"]))
                matched = []
                conflicts = []
                if best["similarity"] >= 0.98:
                    matched.append("normalized_title_exact_or_near_exact")
                else:
                    conflicts.append(f"title_similarity={best['similarity']}")
                if principal_hits:
                    matched.append("IMDb principal name appears in UCSD description: " + ", ".join(principal_hits))
                if brand_hit:
                    matched.append("UCSD brand equals IMDb principal: " + str(brand))
                title_years = [int(v) for v in product_years or []]
                if title_years and best["year"] in title_years:
                    matched.append("ambiguous year token in product title agrees with IMDb year (corroboration only)")
                elif title_years and best["year"] not in title_years:
                    conflicts.append(f"title_years={title_years} vs imdb_year={best['year']}")
                if product_date:
                    matched.append("product date retained separately and not used as movie release evidence")
                # A year embedded in a product title may be part of the film's
                # name, and the listing date is an edition/product date.  Only
                # independent person evidence can promote a title candidate to
                # rule_verified in this first development batch.
                strong_fact = bool(principal_hits or brand_hit)
                if best["similarity"] >= 0.98 and strong_fact and not conflicts:
                    status = "rule_verified"
                    question = "规则证据已满足；请仅在独立人工评价集中另行判断，不把本开发样本当评价标签。"
                else:
                    status = "needs_review"
                    question = "当前是否只有标题证据，或年份/人物存在歧义？请核对导演、演职员或可靠外部ID。"
            counts[status] += 1
            conn.execute(
                """
                INSERT INTO integration.movie_match_candidate_result(
                    asin,candidate_key,candidate_source_record_id,imdb_candidate_id,imdb_title,imdb_year,
                    imdb_title_type,imdb_principal_names,title_similarity,matched_facts,conflict_points,
                    match_status,review_question,candidate_pool,rule_version,run_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s::jsonb,%s,%s,%s::jsonb,%s,%s)
                ON CONFLICT (asin,rule_version) DO UPDATE SET
                    candidate_key=EXCLUDED.candidate_key,candidate_source_record_id=EXCLUDED.candidate_source_record_id,
                    imdb_candidate_id=EXCLUDED.imdb_candidate_id,imdb_title=EXCLUDED.imdb_title,
                    imdb_year=EXCLUDED.imdb_year,imdb_title_type=EXCLUDED.imdb_title_type,
                    imdb_principal_names=EXCLUDED.imdb_principal_names,title_similarity=EXCLUDED.title_similarity,
                    matched_facts=EXCLUDED.matched_facts,conflict_points=EXCLUDED.conflict_points,
                    match_status=EXCLUDED.match_status,review_question=EXCLUDED.review_question,
                    candidate_pool=EXCLUDED.candidate_pool,run_id=EXCLUDED.run_id,decided_at=clock_timestamp()
                """,
                (asin, candidate_key, source_id, best["id"] if best else None, best["title"] if best else None,
                 best["year"] if best else None, best["type"] if best else None,
                 _json(best["principals"] if best else []), best["similarity"] if best else None,
                 _json(matched), _json(conflicts), status, question, _json(pool), RULE_VERSION, run_id),
            )
        stats = {"selected": len(samples), **counts, "sample_role": "development"}
        _finish(conn, run_id, stats)
    return {"run_id": run_id, **stats}


def export_results(
    database_url: str,
    output: Path,
    *,
    rule_version: str = "stage4-first50-movie-match-v3.1-evidence-extraction",
) -> dict[str, Any]:
    output.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(
            """
            SELECT s.sample_rank,s.asin,s.product_source_record_id,pr.record_number,
                   s.product_title_raw,s.query_title,s.product_release_date_raw,
                   s.product_year_evidence,
                   jsonb_build_object(
                       'source_kind',pr.source_kind,
                       'record_number',pr.record_number,
                       'content_sha256',pr.content_sha256,
                       'brand_raw',pr.raw_payload->'brand',
                       'description_excerpt',left(coalesce(pr.raw_payload->>'description',''),500),
                       'locator',pr.field_locator
                   ) AS ucsd_product_evidence,
                   s.selection_evidence,c.candidate_source_record_id,cr.source_uri,
                   c.imdb_candidate_id,c.imdb_title,c.imdb_year,c.imdb_title_type,
                   c.imdb_principal_names,c.title_similarity,c.matched_facts,c.conflict_points,
                   c.match_status,c.review_question,c.candidate_pool,c.confirmation_paths,
                   c.review_reason_code,c.score_breakdown,s.sample_role,
                   s.selection_rule_version,c.rule_version,
                   h.label_scope,h.imdb_candidate_id,h.human_label,h.reason,h.evidence,
                   h.annotator,h.labeled_at,coalesce(ha.labels,'[]'::jsonb)
            FROM integration.movie_match_development_sample s
            JOIN raw.source_record pr ON pr.source_record_id=s.product_source_record_id
            LEFT JOIN integration.movie_match_candidate_result c
              ON c.asin=s.asin AND c.rule_version=%s
            LEFT JOIN raw.source_record cr ON cr.source_record_id=c.candidate_source_record_id
            LEFT JOIN LATERAL (
                SELECT label_scope,imdb_candidate_id,human_label,reason,evidence,annotator,labeled_at
                FROM integration.movie_match_human_label h
                WHERE h.asin=s.asin
                  AND (h.label_scope<>'candidate' OR h.imdb_candidate_id=c.imdb_candidate_id)
                ORDER BY h.labeled_at DESC,h.label_id DESC LIMIT 1
            ) h ON true
            LEFT JOIN LATERAL (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'label_id',h2.label_id,
                        'rule_version',h2.rule_version,
                        'scope',h2.label_scope,
                        'imdb_candidate_id',h2.imdb_candidate_id,
                        'label',h2.human_label,
                        'reason',h2.reason,
                        'evidence',h2.evidence,
                        'annotator',h2.annotator,
                        'labeled_at',h2.labeled_at,
                        'import_batch',h2.import_batch
                    ) ORDER BY h2.labeled_at,h2.label_id
                ) AS labels
                FROM integration.movie_match_human_label h2
                WHERE h2.asin=s.asin
            ) ha ON true
            WHERE s.selection_rule_version=%s ORDER BY s.sample_rank
            """,
            (rule_version, RULE_VERSION),
        ).fetchall()
    columns = [
        "sample_rank","asin","ucsd_source_record_id","ucsd_source_record_number",
        "product_title_raw","query_title","product_release_date_raw","product_year_evidence",
        "ucsd_product_evidence","selection_evidence","imdb_source_record_id","imdb_source_uri",
        "imdb_candidate_id","imdb_title","imdb_year","imdb_title_type","imdb_principal_names",
        "title_similarity","matched_facts","conflict_points","match_status","review_question",
        "candidate_pool","confirmation_paths","review_reason_code","score_breakdown",
        "sample_role","selection_rule_version","match_rule_version",
        "human_label_scope","human_label_candidate_id","human_label","human_reason",
        "human_evidence","human_annotator","human_labeled_at","human_note",
        "human_labels_json",
    ]
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            values = [_json(value) if isinstance(value, (dict, list)) else ("" if value is None else value) for value in row]
            writer.writerow([*values[:-1], "", values[-1]])
    counts: dict[str, int] = {}
    status_index = columns.index("match_status")
    for row in rows:
        status = row[status_index] or "not_run"
        counts[status] = counts.get(status, 0) + 1
    return {"output": str(output), "rows": len(rows), "status_counts": counts}
