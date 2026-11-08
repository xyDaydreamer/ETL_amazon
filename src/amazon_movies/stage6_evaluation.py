from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from amazon_movies.stage4_v3 import title_evidence


SAMPLING_VERSION = "stage6-independent-evaluation-sampling-v2-development-person-exclusion"
PREDICTION_VERSION = "stage6-evidence-baseline-v1"


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _name_norm(value: str) -> str:
    value = "".join(c for c in unicodedata.normalize("NFKD", value).casefold() if not unicodedata.combining(c))
    return " ".join(re.findall(r"[^\W_]+", value, flags=re.UNICODE))


def _stable_order(key: str, seed: int) -> str:
    return hashlib.sha256(f"{seed}\0{key}".encode()).hexdigest()


def _stratified(rows: list[dict[str, Any]], size: int, seed: int) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(row["stratum"], []).append(row)
    for values in groups.values():
        values.sort(key=lambda item: _stable_order(item["pair_key"], seed))
    selected: list[dict[str, Any]] = []
    active = sorted(groups)
    while active and len(selected) < size:
        next_active = []
        for name in active:
            if groups[name] and len(selected) < size:
                selected.append(groups[name].pop(0))
            if groups[name]:
                next_active.append(name)
        active = next_active
    return selected


def _product_text(row: dict[str, Any]) -> str:
    descriptions = row["description_raw"] if isinstance(row["description_raw"], list) else []
    return "\n".join([row.get("brand_raw") or "", *[str(v) for v in descriptions]])


def _snippet(text: str, name: str, radius: int = 120) -> str | None:
    normalized_text = _name_norm(text)
    normalized_name = _name_norm(name)
    if not normalized_name or normalized_name not in normalized_text:
        return None
    # Return bounded original evidence; normalization is only used to find candidate mentions.
    lowered = text.casefold()
    first_token = name.split()[0].casefold()
    position = lowered.find(first_token)
    if position < 0:
        return text[: radius * 2]
    return text[max(0, position - radius): position + len(name) + radius]


def prepare_manifests(
    database_url: str, *, snapshot_version: str, candidate_scope_key: str,
    selective_scope_key: str, exclusions_file: Path, output_dir: Path,
    movie_pairs: int = 100, person_pairs: int = 100, random_seed: int = 6022026,
) -> dict[str, Any]:
    exclusions = json.loads(exclusions_file.read_text(encoding="utf-8"))
    excluded_person_keys = set(exclusions.get("development_scope_nconsts", []))
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        scope = conn.execute(
            """SELECT c.snapshot_id,c.status,c.random_seed
               FROM meta.stage6_candidate_scope c JOIN meta.external_snapshot x USING(snapshot_id)
               WHERE x.snapshot_version=%s AND c.scope_key=%s""",
            (snapshot_version, candidate_scope_key),
        ).fetchone()
        if not scope or scope["status"] != "candidates_ready":
            raise ValueError("candidate scope must be candidates_ready")
        selective = conn.execute(
            """SELECT status FROM meta.imdb_selective_scope
               WHERE snapshot_id=%s AND scope_key=%s""", (scope["snapshot_id"], selective_scope_key)
        ).fetchone()
        if not selective or selective["status"] != "materialized":
            raise ValueError("shortlisted IMDb evidence scope must be materialized before evaluation preparation")
        missing = conn.execute(
            """SELECT count(DISTINCT c.tconst)
               FROM integration.stage6_movie_candidate_cache c
               LEFT JOIN meta.imdb_selective_scope_title st
                 ON st.snapshot_id=c.snapshot_id AND st.scope_key=%s AND st.tconst=c.tconst
               WHERE c.snapshot_id=%s AND c.scope_key=%s AND c.shortlist_rank IS NOT NULL
                 AND st.tconst IS NULL""", (selective_scope_key, scope["snapshot_id"], candidate_scope_key)
        ).fetchone()["count"]
        if missing:
            raise ValueError(f"selective evidence scope misses {missing} shortlisted tconsts")
        rows = conn.execute(
            """SELECT c.*,p.product_title_raw,p.product_source_record_id,p.product_source_record_number,
                      pm.description_raw,pm.brand_raw,pm.source_date_raw
               FROM integration.stage6_movie_candidate_cache c
               JOIN meta.stage6_candidate_scope_product p
                 ON p.snapshot_id=c.snapshot_id AND p.scope_key=c.scope_key AND p.asin=c.asin
               JOIN LATERAL (
                 SELECT description_raw,brand_raw,source_date_raw FROM staging.product_metadata_record
                 WHERE asin=c.asin ORDER BY product_metadata_id DESC LIMIT 1
               ) pm ON true
               WHERE c.snapshot_id=%s AND c.scope_key=%s AND c.shortlist_rank IS NOT NULL
               ORDER BY c.asin,c.shortlist_rank""", (scope["snapshot_id"], candidate_scope_key)
        ).fetchall()
        people = conn.execute(
            """SELECT DISTINCT p.tconst,p.nconst,n.primary_name,n.birth_year,n.death_year,
                      n.primary_professions,n.known_for_tconsts,n.source_record_id,
                      p.category,p.job,p.characters,p.ordering
               FROM staging.imdb_title_principal p
               JOIN staging.imdb_name_basics n USING(snapshot_id,nconst)
               JOIN integration.stage6_movie_candidate_cache c
                 ON c.snapshot_id=p.snapshot_id AND c.tconst=p.tconst
               WHERE c.snapshot_id=%s AND c.scope_key=%s AND c.shortlist_rank IS NOT NULL""",
            (scope["snapshot_id"], candidate_scope_key),
        ).fetchall()
    people_by_title: dict[str, list[dict[str, Any]]] = {}
    all_people: dict[str, dict[str, Any]] = {}
    for person in people:
        if person["nconst"] in excluded_person_keys:
            continue
        people_by_title.setdefault(person["tconst"], []).append(person)
        all_people[person["nconst"]] = person

    movie_candidates = []
    mention_candidates: dict[tuple[int, str], dict[str, Any]] = {}
    for row in rows:
        text = _product_text(row)
        hits = []
        for person in people_by_title.get(row["tconst"], []):
            snippet = _snippet(text, person["primary_name"])
            if snippet is not None:
                hit = {"nconst": person["nconst"], "name": person["primary_name"], "snippet": snippet,
                       "product_source_record_id": row["product_source_record_id"], "category": person["category"]}
                hits.append(hit)
                mention_candidates[(row["product_source_record_id"], person["nconst"])] = {**hit, "asin": row["asin"], "tconst": row["tconst"], "product_title": row["product_title_raw"]}
        hard_conflict = row["type_compatibility"] == "conflict"
        strong = row["title_match_level"] in {"exact", "strong"}
        prediction = "different_entity" if hard_conflict else ("same_entity" if strong and hits else "needs_review")
        stratum = "predicted_same" if prediction == "same_entity" else ("type_or_title_conflict" if prediction == "different_entity" else "insufficient_or_ambiguous")
        movie_candidates.append({
            "pair_key": f"{row['asin']}|{row['tconst']}", "stratum": stratum,
            "left_entity_key": row["asin"], "right_entity_key": row["tconst"],
            "left_display": {"asin": row["asin"], "product_title": row["product_title_raw"], "brand": row["brand_raw"], "product_date_raw": row["source_date_raw"]},
            "right_display": {"tconst": row["tconst"], "title": row["primary_title"], "original_title": row["original_title"], "year": row["start_year"], "type": row["title_type"], "genres": row["genres"]},
            "evidence_display": {"title_similarity": float(row["best_title_similarity"]), "title_match_level": row["title_match_level"], "type_compatibility": row["type_compatibility"], "matched_titles": row["matched_titles"], "product_person_mentions": hits, "warning": "product date is not treated as work release year"},
            "source_links": [{"label": f"Amazon {row['asin']}", "url": f"https://www.amazon.com/dp/{row['asin']}"}, {"label": f"IMDb {row['tconst']}", "url": f"https://www.imdb.com/title/{row['tconst']}/"}],
            "algorithm_prediction": {"prediction": prediction, "evidence": {"hard_type_conflict": hard_conflict, "strong_title": strong, "person_hit_count": len(hits)}},
        })
    selected_movies = _stratified(movie_candidates, movie_pairs, random_seed)
    if len(selected_movies) < movie_pairs:
        raise ValueError(f"only {len(selected_movies)} independent movie pairs available; require {movie_pairs}")

    person_candidates = []
    person_values = list(all_people.values())
    required_mentions = (person_pairs + 1) // 2
    selected_mentions = sorted(
        mention_candidates.values(),
        key=lambda item: _stable_order(
            f"{item['product_source_record_id']}|{item['nconst']}|{item['tconst']}", random_seed + 1
        ),
    )[:required_mentions]
    for mention in selected_mentions:
        true_person = all_people[mention["nconst"]]
        left_key = f"ucsd-credit:{mention['product_source_record_id']}:{_name_norm(mention['name'])}"
        base = {
            "left_display": {"raw_name": mention["name"], "asin": mention["asin"], "product_title": mention["product_title"], "source_record_id": mention["product_source_record_id"], "context": mention["snippet"]},
            "source_links": [{"label": f"Amazon {mention['asin']}", "url": f"https://www.amazon.com/dp/{mention['asin']}"}],
        }
        person_candidates.append({
            "pair_key": f"{left_key}|{true_person['nconst']}", "stratum": "exact_name_with_work_context",
            "left_entity_key": left_key, "right_entity_key": true_person["nconst"], **base,
            "right_display": {"nconst": true_person["nconst"], "primary_name": true_person["primary_name"], "birth_year": true_person["birth_year"], "death_year": true_person["death_year"], "professions": true_person["primary_professions"], "known_for": true_person["known_for_tconsts"]},
            "evidence_display": {"shared_candidate_work": mention["tconst"], "credit_category": mention["category"], "identity_warning": "name agreement is candidate evidence, not identity proof"},
            "source_links": [*base["source_links"], {"label": f"IMDb {true_person['nconst']}", "url": f"https://www.imdb.com/name/{true_person['nconst']}/"}],
            "algorithm_prediction": {"prediction": "same_entity", "evidence": {"name_exact": True, "shared_work_context": mention["tconst"]}},
        })
        alternatives = (p for p in person_values if p["nconst"] != true_person["nconst"])
        other = min(
            alternatives,
            key=lambda p: (
                -SequenceMatcher(None, _name_norm(mention["name"]), _name_norm(p["primary_name"])).ratio(),
                p["nconst"],
            ),
            default=None,
        )
        if other is not None:
            similarity = SequenceMatcher(None, _name_norm(mention["name"]), _name_norm(other["primary_name"])).ratio()
            person_candidates.append({
                "pair_key": f"{left_key}|{other['nconst']}", "stratum": "near_name_or_same_name_negative",
                "left_entity_key": left_key, "right_entity_key": other["nconst"], **base,
                "right_display": {"nconst": other["nconst"], "primary_name": other["primary_name"], "birth_year": other["birth_year"], "death_year": other["death_year"], "professions": other["primary_professions"], "known_for": other["known_for_tconsts"]},
                "evidence_display": {"name_similarity": round(similarity, 5), "candidate_work": mention["tconst"], "conflict": "right person is not credited to the candidate work in the loaded principal evidence"},
                "source_links": [*base["source_links"], {"label": f"IMDb {other['nconst']}", "url": f"https://www.imdb.com/name/{other['nconst']}/"}],
                "algorithm_prediction": {"prediction": "different_entity", "evidence": {"name_similarity": round(similarity, 5), "shared_work": False}},
            })
    selected_people = _stratified(person_candidates, person_pairs, random_seed + 1)
    if len(selected_people) < person_pairs:
        raise ValueError(f"only {len(selected_people)} independent person pairs available; require {person_pairs}")

    excluded_entities = sorted(set(exclusions.get("development_candidate_tconsts", [])) | set(exclusions.get("development_scope_nconsts", [])))
    common = {
        "snapshot_id": scope["snapshot_id"], "sampling_rule_version": SAMPLING_VERSION,
        "prediction_rule_version": PREDICTION_VERSION,
        "development_exclusion": {"entity_keys": excluded_entities, "left_entity_keys": exclusions.get("development_asins", [])},
    }
    movie_payload = {**common, "dataset_key": f"stage6-movie-eval-{candidate_scope_key}-v2", "entity_type": "movie", "pairs": selected_movies}
    person_payload = {**common, "dataset_key": f"stage6-person-eval-{candidate_scope_key}-v2", "entity_type": "person", "pairs": selected_people}
    outputs = {}
    for name, payload in (("movie_evaluation_input.json", movie_payload), ("person_evaluation_input.json", person_payload)):
        path = output_dir / name
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        outputs[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "sampling_version": SAMPLING_VERSION, "prediction_version": PREDICTION_VERSION,
        "random_seed": random_seed, "movie_pairs": len(selected_movies), "person_pairs": len(selected_people),
        "human_answers_prefilled": False, "predictions_stored_separately": True,
        "source_limitations": ["IMDb principals are not a complete cast", "UCSD descriptions may omit credits", "product dates are not work release years"],
        "files": outputs,
    }
    (output_dir / "evaluation_preparation_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest
