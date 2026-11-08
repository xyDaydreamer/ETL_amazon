from __future__ import annotations

import re
from typing import Any

import psycopg

from amazon_movies.stage3_pipeline import _finish, _json, _run, normalize_title
from amazon_movies.stage4_matcher import (
    SOURCE_VERSION,
    _candidate_pool,
    _meaningful_brand,
)


SELECTION_RULE_VERSION = "stage4-first50-movie-match-v1"
PREVIOUS_RULE_VERSION = "stage4-first50-movie-match-v1"
RULE_VERSION = "stage4-first50-movie-match-v2.1-cached-pool"

PRODUCT_CLASS_RULES = {
    "sports_highlight": "UFC/fight/knockout/highlight or compilation language; not treated as a film",
    "documentary": "explicit documentary/non-fiction language",
    "television_series": "season/episodes/TV series/complete series language",
    "television_movie": "explicit made-for-TV/television movie language",
    "direct_release_film": "explicit direct-to-video/home-video language",
    "single_movie": "default single-work film class after collection and series exclusions",
}

TYPE_CLASSES = {
    "feature": "movie",
    "TV movie": "television_movie",
    "video": "direct_release_or_home_video",
    "TV series": "television_series",
    "TV mini-series": "television_series",
    "short": "short_film",
    "musicVideo": "music_video",
    "podcastSeries": "podcast_series",
    "video game": "video_game",
}


def classify_product_intent(title: str, descriptions: list[str]) -> tuple[str, list[str]]:
    # Product-class evidence must come from the title and the first substantive
    # synopsis. Later descriptions often contain editorial comparisons such as
    # "like a TV movie", which describe neither the product nor the work type.
    primary_description = next((value for value in descriptions if value.strip()), "")
    text = " ".join([title, primary_description[:2000]]).casefold()
    reasons: list[str] = []
    if re.search(r"\b(?:ufc|knockouts?|fight highlights?|sports highlights?|tkos?)\b", text):
        reasons.append(PRODUCT_CLASS_RULES["sports_highlight"])
        return "sports_highlight", reasons
    if re.search(r"\b(?:documentary|non-fiction|nonfiction|docudrama)\b", text):
        reasons.append(PRODUCT_CLASS_RULES["documentary"])
        return "documentary", reasons
    if re.search(r"\b(?:complete series|tv series|television series|season \d+|episodes?)\b", text):
        reasons.append(PRODUCT_CLASS_RULES["television_series"])
        return "television_series", reasons
    if re.search(r"\b(?:made[- ]for[- ]tv|television movie|tv movie)\b", text):
        reasons.append(PRODUCT_CLASS_RULES["television_movie"])
        return "television_movie", reasons
    if re.search(r"\b(?:direct[- ]to[- ]video|straight[- ]to[- ]video|home video release)\b", text):
        reasons.append(PRODUCT_CLASS_RULES["direct_release_film"])
        return "direct_release_film", reasons
    reasons.append(PRODUCT_CLASS_RULES["single_movie"])
    return "single_movie", reasons


def candidate_type_evidence(product_class: str, imdb_type: str | None) -> dict[str, Any]:
    candidate_class = TYPE_CLASSES.get(imdb_type or "", "unknown")
    if product_class == "sports_highlight":
        compatibility = "conflict"
        reason = "product evidence identifies a sports-highlight compilation, not a movie work"
    elif product_class == "television_series":
        compatibility = "compatible" if candidate_class == "television_series" else "conflict"
        reason = "series product requires a series or mini-series candidate"
    elif product_class == "television_movie":
        compatibility = "compatible" if candidate_class == "television_movie" else "conflict"
        reason = "explicit television-movie evidence requires TV movie type"
    elif product_class == "direct_release_film":
        compatibility = "compatible" if candidate_class == "direct_release_or_home_video" else "conflict"
        reason = "explicit direct-release evidence requires video type"
    elif product_class == "documentary":
        compatibility = "compatible" if candidate_class in {
            "movie", "television_movie", "direct_release_or_home_video"
        } else "conflict"
        reason = "IMDb suggestion type is a container type; documentary genre remains product-side evidence"
    else:
        compatibility = "compatible" if candidate_class in {
            "movie", "television_movie", "direct_release_or_home_video"
        } else "conflict"
        reason = "single-film products accept feature, TV movie, or direct/home-video film candidates"
    return {
        "imdb_type": imdb_type,
        "candidate_class": candidate_class,
        "product_class": product_class,
        "compatibility": compatibility,
        "reason": reason,
    }


def distinctive_number_tokens(title: str) -> list[str]:
    return re.findall(r"(?<!\d)(?:[1-9]|[1-9]\d|[1-9]\d{2})(?!\d)", title)


def _name_locations(
    name: str, brand: str | None, descriptions: list[str], source_record_id: int
) -> list[dict[str, Any]]:
    locations: list[dict[str, Any]] = []
    fields: list[tuple[str, str]] = []
    if brand:
        fields.append(("raw_payload.brand", brand))
    fields.extend((f"raw_payload.description[{index}]", value) for index, value in enumerate(descriptions))
    pattern = re.compile(re.escape(name), re.IGNORECASE)
    for field_path, value in fields:
        for match in pattern.finditer(value):
            left = max(0, match.start() - 90)
            right = min(len(value), match.end() + 90)
            locations.append({
                "source_record_id": source_record_id,
                "field_path": field_path,
                "char_start": match.start(),
                "char_end": match.end(),
                "snippet_start": left,
                "snippet_end": right,
                "snippet": value[left:right],
            })
    return locations


def evaluate_candidate(
    candidate: dict[str, Any], *, query_title: str, product_class: str,
    brand: str | None, descriptions: list[str], source_record_id: int,
) -> dict[str, Any]:
    similarity = float(candidate.get("similarity") or 0)
    query_numbers = distinctive_number_tokens(query_title)
    candidate_numbers = distinctive_number_tokens(str(candidate.get("title") or ""))
    number_conflict = bool(query_numbers and query_numbers != candidate_numbers)
    title_exact = normalize_title(query_title) == normalize_title(str(candidate.get("title") or ""))
    type_evidence = candidate_type_evidence(product_class, candidate.get("type"))

    person_matches: list[dict[str, Any]] = []
    seen_people: set[str] = set()
    for person in candidate.get("principals") or []:
        person_key = normalize_title(str(person))
        if not person_key or person_key in seen_people:
            continue
        seen_people.add(person_key)
        locations = _name_locations(str(person), brand, descriptions, source_record_id)
        if locations:
            person_matches.append({
                "name": person,
                "normalized_name": person_key,
                "locations": locations,
                "unique_person_evidence": True,
            })

    conflicts: list[str] = []
    if similarity < 0.98:
        conflicts.append(f"title_similarity={similarity:.5f}")
    if number_conflict:
        conflicts.append(f"distinctive_number_mismatch product={query_numbers} candidate={candidate_numbers}")
    if type_evidence["compatibility"] == "conflict":
        conflicts.append(
            f"type_conflict product={product_class} imdb={candidate.get('type')}"
        )
    if not person_matches:
        conflicts.append("no candidate principal located in product brand or description")

    sufficient = bool(
        similarity >= 0.98
        and not number_conflict
        and type_evidence["compatibility"] == "compatible"
        and person_matches
    )
    evidence_score = (
        (40 if title_exact else 25 if similarity >= 0.98 else 10 if similarity >= 0.90 else 0)
        + min(30, 15 * len(person_matches))
        + (10 if type_evidence["compatibility"] == "compatible" else -30)
        + (-30 if number_conflict else 0)
    )
    return {
        **candidate,
        "title_evidence": {
            "query_title": query_title,
            "normalized_exact": title_exact,
            "similarity": similarity,
            "product_number_tokens": query_numbers,
            "candidate_number_tokens": candidate_numbers,
            "number_conflict": number_conflict,
        },
        "type_evidence": type_evidence,
        "person_evidence": person_matches,
        "unique_person_match_count": len(person_matches),
        "conflicts": conflicts,
        "sufficient": sufficient,
        "evidence_score": evidence_score,
        "rank_used_only_after_evidence": True,
    }


def decide_candidate(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    if not evaluations:
        return {
            "status": "no_candidate", "best": None,
            "question": "缓存响应没有IMDb标题候选。请人工检索并填写human_label与可靠外部ID。",
            "reason": "cached response contained no title candidate",
        }
    ordered = sorted(
        evaluations,
        key=lambda value: (
            -int(bool(value["sufficient"])),
            -int(value["evidence_score"]),
            -float(value["similarity"]),
            value["rank"] if isinstance(value.get("rank"), int) else 10**12,
            value["id"],
        ),
    )
    sufficient = [value for value in ordered if value["sufficient"]]
    best = ordered[0]
    if len(sufficient) == 1:
        best = sufficient[0]
        return {
            "status": "rule_verified", "best": best,
            "question": (
                f"程序选择{best['id']}（{best['title']}，{best.get('year')}，{best.get('type')}）。"
                "请填写human_label=correct_match/incorrect_match/uncertain，并记录核对依据；开发样本允许修改。"
            ),
            "reason": "exact title, compatible work type, and at least one unique cross-source person",
        }
    if len(sufficient) > 1:
        ids = ", ".join(value["id"] for value in sufficient)
        return {
            "status": "needs_review_ambiguous", "best": best,
            "question": f"多个缓存候选均有充分证据（{ids}）。请用年份、导演或可靠外部ID选择并填写human_label。",
            "reason": "multiple candidates satisfy the full evidence rule",
        }
    viable = [
        value for value in ordered
        if value["type_evidence"]["compatibility"] != "conflict"
        and not value["title_evidence"]["number_conflict"]
        and float(value["similarity"]) >= 0.80
    ]
    if not viable:
        conflict_summary = "; ".join(best["conflicts"][:3])
        return {
            "status": "no_suitable_candidate", "best": best,
            "question": (
                f"缓存最佳候选{best['id']}（{best['title']}）仍有冲突：{conflict_summary}。"
                "请人工检索正确IMDb ID，或标记该商品不是单部电影。"
            ),
            "reason": "all cached candidates have a hard title/number/type conflict",
        }
    best = viable[0]
    missing = []
    if not best["person_evidence"]:
        missing.append("可定位人物交叉证据")
    if float(best["similarity"]) < 0.98:
        missing.append("足够精确的标题一致性")
    return {
        "status": "needs_review_insufficient", "best": best,
        "question": (
            f"当前最佳缓存候选为{best['id']}（{best['title']}，{best.get('year')}，{best.get('type')}），"
            f"但缺少{'、'.join(missing) or '可靠区分证据'}。请核对年份、导演、演职员或外部ID并填写human_label。"
        ),
        "reason": "a plausible cached candidate exists but evidence is insufficient",
    }


def reevaluate_cached_candidates(database_url: str, *, batch_id: str, limit: int = 50) -> dict[str, Any]:
    if not 1 <= limit <= 50:
        raise ValueError("limit must be between 1 and 50")
    with psycopg.connect(database_url) as conn:
        run_id = _run(conn, "stage4_cached_candidate_reevaluation", batch_id, RULE_VERSION)
        rows = conn.execute(
            """
            SELECT s.asin,s.product_title_raw,s.query_title,s.product_source_record_id,
                   pr.raw_payload,c.candidate_source_record_id,cr.raw_payload
            FROM integration.movie_match_development_sample s
            JOIN raw.source_record pr ON pr.source_record_id=s.product_source_record_id
            JOIN integration.movie_match_candidate_result c
              ON c.asin=s.asin AND c.rule_version=%s
            LEFT JOIN raw.source_record cr ON cr.source_record_id=c.candidate_source_record_id
            WHERE s.selection_rule_version=%s
            ORDER BY s.sample_rank LIMIT %s
            """,
            (PREVIOUS_RULE_VERSION, SELECTION_RULE_VERSION, limit),
        ).fetchall()
        counts = {
            "rule_verified": 0,
            "needs_review_ambiguous": 0,
            "needs_review_insufficient": 0,
            "no_suitable_candidate": 0,
            "no_candidate": 0,
        }
        for asin, product_title, query_title, product_source_id, product_raw, candidate_source_id, cached_raw in rows:
            descriptions = [str(value) for value in (product_raw.get("description") or [])]
            brand = _meaningful_brand(str(product_raw.get("brand") or ""))
            product_class, class_reasons = classify_product_intent(product_title, descriptions)
            pool = _candidate_pool(cached_raw or {}, query_title)
            evaluations = [
                evaluate_candidate(
                    candidate, query_title=query_title, product_class=product_class,
                    brand=brand, descriptions=descriptions, source_record_id=product_source_id,
                )
                for candidate in pool
            ]
            decision = decide_candidate(evaluations)
            best = decision["best"]
            counts[decision["status"]] += 1
            matched_facts: list[dict[str, Any]] = [{
                "kind": "product_classification",
                "class": product_class,
                "reasons": class_reasons,
                "source_record_id": product_source_id,
                "evidence_scope": "product title plus first substantive description",
            }]
            conflicts: list[str] = []
            if best:
                matched_facts.extend([
                    {"kind": "title", **best["title_evidence"]},
                    {"kind": "type", **best["type_evidence"]},
                    *({"kind": "person", **value} for value in best["person_evidence"]),
                ])
                conflicts = list(best["conflicts"])
            conflicts.append(f"decision_reason={decision['reason']}")
            enriched_pool = [{
                **value,
                "product_classification": {"class": product_class, "reasons": class_reasons},
            } for value in sorted(
                evaluations,
                key=lambda value: (
                    -int(bool(value["sufficient"])), -int(value["evidence_score"]),
                    -float(value["similarity"]),
                    value["rank"] if isinstance(value.get("rank"), int) else 10**12,
                ),
            )]
            conn.execute(
                """
                INSERT INTO integration.movie_match_candidate_result(
                    asin,candidate_key,candidate_source_record_id,imdb_candidate_id,imdb_title,
                    imdb_year,imdb_title_type,imdb_principal_names,title_similarity,matched_facts,
                    conflict_points,match_status,review_question,candidate_pool,rule_version,run_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s::jsonb,%s,%s,%s::jsonb,%s,%s)
                ON CONFLICT (asin,rule_version) DO UPDATE SET
                    candidate_key=EXCLUDED.candidate_key,
                    candidate_source_record_id=EXCLUDED.candidate_source_record_id,
                    imdb_candidate_id=EXCLUDED.imdb_candidate_id,
                    imdb_title=EXCLUDED.imdb_title,
                    imdb_year=EXCLUDED.imdb_year,
                    imdb_title_type=EXCLUDED.imdb_title_type,
                    imdb_principal_names=EXCLUDED.imdb_principal_names,
                    title_similarity=EXCLUDED.title_similarity,
                    matched_facts=EXCLUDED.matched_facts,
                    conflict_points=EXCLUDED.conflict_points,
                    match_status=EXCLUDED.match_status,
                    review_question=EXCLUDED.review_question,
                    candidate_pool=EXCLUDED.candidate_pool,
                    run_id=EXCLUDED.run_id,
                    decided_at=clock_timestamp()
                """,
                (
                    asin, f"imdb:{best['id']}" if best else "no_candidate", candidate_source_id,
                    best["id"] if best else None, best["title"] if best else None,
                    best.get("year") if best else None, best.get("type") if best else None,
                    _json(best.get("principals") or [] if best else []),
                    best.get("similarity") if best else None, _json(matched_facts), _json(conflicts),
                    decision["status"], decision["question"], _json(enriched_pool), RULE_VERSION, run_id,
                ),
            )
        stats = {"selected": len(rows), **counts, "source": "cached_only", "previous_rule_version": PREVIOUS_RULE_VERSION}
        _finish(conn, run_id, stats)
    return {"run_id": run_id, "rule_version": RULE_VERSION, **stats}

