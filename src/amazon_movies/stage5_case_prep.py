from __future__ import annotations

import csv
import hashlib
import html
import json
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from amazon_movies.stage4_v3 import title_evidence


RULE_VERSION = "stage5-development-case-prep-v1"


def _name_key(value: str) -> str:
    value = "".join(
        char for char in unicodedata.normalize("NFKD", value).casefold()
        if not unicodedata.combining(char)
    )
    return " ".join(re.findall(r"[^\W_]+", value, flags=re.UNICODE))


def _sha(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _write_tsv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(row.get(key), ensure_ascii=False) if isinstance(row.get(key), (dict, list)) else row.get(key, "") for key in fields})


def _write_html(path: Path, title: str, rows: list[dict[str, Any]]) -> None:
    cards = []
    for number, row in enumerate(rows, 1):
        links = []
        for link in row.get("source_links", []):
            links.append(f'<li><a href="{html.escape(link["url"], quote=True)}" target="_blank" rel="noreferrer">{html.escape(link["label"])}</a></li>')
        display = {k: v for k, v in row.items() if k not in {"source_links", "human_label", "human_reason"}}
        cards.append(
            f"<article><h2>#{number}</h2><pre>{html.escape(json.dumps(display, ensure_ascii=False, indent=2))}</pre>"
            f"<h3>来源</h3><ul>{''.join(links)}</ul><p><b>人工答案：</b>留空；正式评价集不得复用本页。</p></article>"
        )
    path.write_text(
        "<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>" + html.escape(title) +
        "</title><style>body{font-family:system-ui;max-width:1100px;margin:auto;padding:24px}article{border:1px solid #bbb;padding:16px;margin:18px 0}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f6f8fa;padding:12px}</style>" +
        f"<h1>{html.escape(title)}</h1><p>development 材料；程序仅生成候选，不预填人工答案，不计入独立评价集。</p>" +
        "".join(cards) + "</html>", encoding="utf-8"
    )


def prepare(database_url: str, output_dir: Path, *, scope_key: str = "stage5-adopted-38-v1") -> dict[str, Any]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        people = conn.execute(
            """
            SELECT DISTINCT nb.nconst,nb.primary_name,nb.birth_year,nb.death_year,
                   nb.primary_professions,nb.known_for_tconsts,nb.source_record_id,
                   sr.record_number,sr.content_sha256,sr.field_locator,
                   coalesce(array_agg(DISTINCT st.tconst) FILTER (WHERE st.tconst IS NOT NULL),'{}') AS scope_tconsts
            FROM staging.imdb_name_basics nb
            JOIN raw.source_record sr ON sr.source_record_id=nb.source_record_id
            JOIN integration.person_external_identifier pei
              ON pei.snapshot_id=nb.snapshot_id AND pei.external_id=nb.nconst AND pei.source_name='imdb'
            JOIN integration.movie_person_source_evidence e
              ON e.snapshot_id=nb.snapshot_id AND e.person_id=pei.person_id
            JOIN integration.movie_external_identifier mei
              ON mei.snapshot_id=e.snapshot_id AND mei.movie_id=e.movie_id AND mei.source_name='imdb'
            JOIN meta.imdb_selective_scope_title st
              ON st.snapshot_id=e.snapshot_id AND st.scope_key=%s AND st.tconst=mei.external_id
            WHERE nb.snapshot_id=1
            GROUP BY nb.nconst,nb.primary_name,nb.birth_year,nb.death_year,
                     nb.primary_professions,nb.known_for_tconsts,nb.source_record_id,
                     sr.record_number,sr.content_sha256,sr.field_locator
            ORDER BY nb.nconst
            """, (scope_key,)
        ).fetchall()
        pair_candidates: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        for index, left in enumerate(people):
            left_key = _name_key(left["primary_name"])
            for right in people[index + 1:]:
                similarity = SequenceMatcher(None, left_key, _name_key(right["primary_name"])).ratio()
                pair_candidates.append((similarity, left, right))
        pair_candidates.sort(key=lambda item: (-item[0], item[1]["nconst"], item[2]["nconst"]))
        person_rows = []
        for similarity, left, right in pair_candidates[:10]:
            person_rows.append({
                "case_role": "development_candidate",
                "left_nconst": left["nconst"], "left_name": left["primary_name"],
                "right_nconst": right["nconst"], "right_name": right["primary_name"],
                "name_similarity": round(similarity, 5),
                "left_context": {"birth_year": left["birth_year"], "death_year": left["death_year"], "professions": left["primary_professions"], "known_for": left["known_for_tconsts"], "scope_tconsts": left["scope_tconsts"]},
                "right_context": {"birth_year": right["birth_year"], "death_year": right["death_year"], "professions": right["primary_professions"], "known_for": right["known_for_tconsts"], "scope_tconsts": right["scope_tconsts"]},
                "left_source": {"source_record_id": left["source_record_id"], "record_number": left["record_number"], "content_sha256": left["content_sha256"], "field_locator": left["field_locator"]},
                "right_source": {"source_record_id": right["source_record_id"], "record_number": right["record_number"], "content_sha256": right["content_sha256"], "field_locator": right["field_locator"]},
                "program_note": "similar_name_candidate_only; distinct nconsts are not merged by name",
                "source_links": [{"label": f"IMDb {left['nconst']}", "url": f"https://www.imdb.com/name/{left['nconst']}/"}, {"label": f"IMDb {right['nconst']}", "url": f"https://www.imdb.com/name/{right['nconst']}/"}],
                "human_label": "", "human_reason": "",
            })

        verified_merges = conn.execute(
            """SELECT left_person_key,imdb_nconst,match_status,evidence,conflict_points
               FROM integration.person_match_candidate
               WHERE snapshot_id=1 AND match_status='rule_verified'
               ORDER BY left_person_key,imdb_nconst"""
        ).fetchall()

        products = conn.execute(
            """SELECT DISTINCT ON (p.asin) p.asin,p.title_raw,p.source_record_id,p.source_record_number,
                      sr.content_sha256,sr.field_locator
               FROM staging.product_metadata_record p
               JOIN raw.source_record sr ON sr.source_record_id=p.source_record_id
               LEFT JOIN integration.movie_match_development_sample d ON d.asin=p.asin
               WHERE p.proposed_classification='movie' AND d.asin IS NULL
               ORDER BY p.asin,p.product_metadata_id DESC"""
        ).fetchall()
        titles = conn.execute(
            """SELECT st.asin,st.tconst,st.product_title_raw,b.primary_title,b.original_title,
                      b.start_year,b.title_type,b.source_record_id AS title_source_record_id,
                      br.record_number AS title_record_number,br.content_sha256 AS title_content_sha256,
                      coalesce(array_agg(DISTINCT a.title) FILTER (WHERE a.title IS NOT NULL),'{}') AS aliases
               FROM meta.imdb_selective_scope_title st
               JOIN staging.imdb_title_basics b ON b.snapshot_id=st.snapshot_id AND b.tconst=st.tconst
               JOIN raw.source_record br ON br.source_record_id=b.source_record_id
               LEFT JOIN staging.imdb_title_aka a ON a.snapshot_id=st.snapshot_id AND a.title_id=st.tconst
               WHERE st.snapshot_id=1 AND st.scope_key=%s
               GROUP BY st.asin,st.tconst,st.product_title_raw,b.primary_title,b.original_title,
                        b.start_year,b.title_type,b.source_record_id,br.record_number,br.content_sha256
               ORDER BY st.tconst""", (scope_key,)
        ).fetchall()
        movie_candidates: list[tuple[float, dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for product in products:
            if not product["title_raw"]:
                continue
            for work in titles:
                comparisons = [("primary", work["primary_title"])]
                comparisons.extend(("aka", alias) for alias in work["aliases"] if alias)
                evaluated = [
                    (kind, candidate_title, title_evidence(product["title_raw"], candidate_title))
                    for kind, candidate_title in comparisons
                ]
                matched_kind, matched_title, evidence = max(
                    evaluated,
                    key=lambda item: (float(item[2]["similarity"]), bool(item[2]["T_exact"])),
                )
                score = float(evidence["similarity"])
                evidence = {**evidence, "matched_title_kind": matched_kind, "matched_title": matched_title}
                movie_candidates.append((score, product, work, evidence))
        movie_candidates.sort(key=lambda item: (-item[0], item[1]["asin"], item[2]["tconst"]))
        movie_rows = []
        used_products: set[str] = set()
        for score, product, work, evidence in movie_candidates:
            if score < 0.85 or product["asin"] in used_products:
                continue
            used_products.add(product["asin"])
            movie_rows.append({
                "case_role": "development_cross_version_candidate",
                "candidate_asin": product["asin"], "candidate_product_title": product["title_raw"],
                "existing_edition_asin": work["asin"], "candidate_tconst": work["tconst"],
                "imdb_primary_title": work["primary_title"], "imdb_original_title": work["original_title"],
                "imdb_year": work["start_year"], "imdb_type": work["title_type"],
                "title_similarity": round(score, 5), "title_evidence": evidence,
                "product_source": {"source_record_id": product["source_record_id"], "record_number": product["source_record_number"], "content_sha256": product["content_sha256"], "field_locator": product["field_locator"]},
                "work_source": {"source_record_id": work["title_source_record_id"], "record_number": work["title_record_number"], "content_sha256": work["title_content_sha256"]},
                "program_note": "candidate_only; title agreement alone does not prove an edition-to-work merge",
                "source_links": [{"label": f"Amazon {product['asin']}", "url": f"https://www.amazon.com/dp/{product['asin']}"}, {"label": f"IMDb {work['tconst']}", "url": f"https://www.imdb.com/title/{work['tconst']}/"}],
                "human_label": "", "human_reason": "",
            })
            if len(movie_rows) >= 10:
                break

        development_asins = [r["asin"] for r in conn.execute("SELECT asin FROM integration.movie_match_development_sample ORDER BY asin").fetchall()]
        development_tconsts = [r["id"] for r in conn.execute(
            """SELECT DISTINCT candidate->>'id' AS id
               FROM integration.movie_match_candidate_result r,
                    LATERAL jsonb_array_elements(r.candidate_pool) candidate
               WHERE r.rule_version='stage4-first50-movie-match-v3.1-evidence-extraction'
                 AND candidate->>'id' ~ '^tt[0-9]+$' ORDER BY id"""
        ).fetchall()]
        development_nconsts = [r["external_id"] for r in conn.execute(
            """SELECT DISTINCT pei.external_id
               FROM integration.person_external_identifier pei
               JOIN integration.movie_person_source_evidence e
                 ON e.snapshot_id=pei.snapshot_id AND e.person_id=pei.person_id
               WHERE pei.snapshot_id=1 AND pei.source_name='imdb'
               ORDER BY pei.external_id"""
        ).fetchall()]

    person_fields = list(person_rows[0].keys()) if person_rows else []
    movie_fields = list(movie_rows[0].keys()) if movie_rows else []
    _write_tsv(output_dir / "person_disambiguation_development.tsv", person_rows, person_fields)
    _write_html(output_dir / "person_disambiguation_development.html", "人物消歧开发候选", person_rows)
    _write_tsv(output_dir / "movie_cross_version_development.tsv", movie_rows, movie_fields)
    _write_html(output_dir / "movie_cross_version_development.html", "电影跨版本合并开发候选", movie_rows)
    exclusions = {
        "rule_version": RULE_VERSION,
        "purpose": "exclude all Stage 4/5 development entities from independent evaluation",
        "development_asins": development_asins,
        "development_candidate_tconsts": development_tconsts,
        "development_scope_nconsts": development_nconsts,
        "movie_cross_version_development_asins": sorted(r["candidate_asin"] for r in movie_rows),
        "person_development_pairs": [[r["left_nconst"], r["right_nconst"]] for r in person_rows],
    }
    exclusions["manifest_sha256"] = _sha(exclusions)
    (output_dir / "development_exclusions.json").write_text(json.dumps(exclusions, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    manifest = {
        "rule_version": RULE_VERSION,
        "scope_key": scope_key,
        "person_scope_count": len(people),
        "person_development_candidates": len(person_rows),
        "existing_verified_person_merges": len(verified_merges),
        "movie_cross_version_candidates": len(movie_rows),
        "development_asin_exclusions": len(development_asins),
        "development_tconst_exclusions": len(development_tconsts),
        "development_nconst_exclusions": len(development_nconsts),
        "human_answers_prefilled": False,
        "independent_evaluation_created": False,
        "files": {},
    }
    for path in sorted(output_dir.iterdir()):
        if path.name != "case_prep_manifest.json" and path.is_file():
            manifest["files"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (output_dir / "case_prep_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest
