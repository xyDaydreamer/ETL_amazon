from __future__ import annotations

import csv
import hashlib
import html
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable

import psycopg

from amazon_movies.stage3_pipeline import _finish, _json, _run
from amazon_movies.stage4_matcher import _meaningful_brand


RULE_VERSION = "stage4-first50-movie-match-v3.1-evidence-extraction"
SELECTION_RULE_VERSION = "stage4-first50-movie-match-v1"
CACHE_RULE_VERSION = "stage4-first50-movie-match-v1"
DISPLAY_LIMIT = 10

TITLE_STRONG_THRESHOLD = 0.95
TITLE_CLOSE_THRESHOLD = 0.85
AMBIGUITY_SCORE_MARGIN = 10
MAX_EXPLICIT_SCORE = 40
MAX_BRAND_SCORE = 20
MENTION_SCORE = 5

GENERIC_MAIN_TITLES = {
    "the chosen one", "the lost city", "castle rock", "middle of nowhere",
    "the help", "the proposal", "the open road",
}

PACKAGING_BRACKET_RE = re.compile(
    r"[\[(]\s*(?:dvd|blu[ -]?ray|vhs|4k(?:\s+uhd)?|uhd|widescreen|"
    r"special edition|collector'?s edition|collectors edition)\s*[\])]",
    re.IGNORECASE,
)
TRAILING_CARRIER_RE = re.compile(r"\s+(?:dvd|blu[ -]?ray|vhs|4k(?:\s+uhd)?)\s*$", re.IGNORECASE)
TRAILING_EDITION_RE = re.compile(
    r"\s+(?:[-–—:]\s*)?(?:special edition|collector'?s edition|collectors edition|"
    r"widescreen edition)\s*$",
    re.IGNORECASE,
)
SUBTITLE_RE = re.compile(r"\s+(?:[-–—])\s+|:\s+")

EXPLICIT_BEFORE_RE = re.compile(
    r"(?:\bstarring\b|\bstars?\b|\bcast\b|\bdirected\s+by\b|\bdirector\b|主演|导演|饰演)",
    re.IGNORECASE,
)
DIRECTOR_RE = re.compile(r"(?:\bdirected\s+by\b|\bdirector\b|导演)", re.IGNORECASE)
OTHER_WORK_RE = re.compile(
    r"(?:known\s+for|from\s+(?:the\s+)?(?:film|movie|series)|other\s+works?|"
    r"previous\s+(?:film|movie)|--\s*$|review(?:er)?|comparison)",
    re.IGNORECASE,
)

ROMAN_VALUES = {"i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6, "vii": 7, "viii": 8, "ix": 9, "x": 10}


def _collapse_unicode(value: str) -> str:
    value = unicodedata.normalize("NFKC", html.unescape(value or "")).casefold()
    output: list[str] = []
    previous_space = True
    for char in value:
        if char.isalnum():
            output.append(char)
            previous_space = False
        elif not previous_space:
            output.append(" ")
            previous_space = True
    return "".join(output).strip()


def _accent_fold_with_map(value: str) -> tuple[str, list[int]]:
    chars: list[str] = []
    positions: list[int] = []
    previous_space = True
    for index, original in enumerate(unicodedata.normalize("NFC", value)):
        expanded = unicodedata.normalize("NFKD", original)
        pieces = [piece.casefold() for piece in expanded if not unicodedata.combining(piece)]
        for piece in "".join(pieces):
            if piece.isalnum():
                chars.append(piece)
                positions.append(index)
                previous_space = False
            elif not previous_space:
                chars.append(" ")
                positions.append(index)
                previous_space = True
    while chars and chars[-1] == " ":
        chars.pop()
        positions.pop()
    return "".join(chars), positions


def strip_packaging_markers(value: str) -> tuple[str, list[str]]:
    text = unicodedata.normalize("NFKC", html.unescape(value or "")).strip()
    removed: list[str] = []
    for pattern in (PACKAGING_BRACKET_RE, TRAILING_CARRIER_RE, TRAILING_EDITION_RE):
        while True:
            match = pattern.search(text)
            if not match:
                break
            removed.append(match.group(0).strip())
            text = f"{text[:match.start()]} {text[match.end():]}".strip()
    return " ".join(text.split()), removed


def title_forms(value: str) -> dict[str, Any]:
    stripped, removed = strip_packaging_markers(value)
    match = SUBTITLE_RE.search(stripped)
    if match:
        main_raw = stripped[:match.start()].strip()
        subtitle_raw = stripped[match.end():].strip() or None
    else:
        main_raw = stripped
        subtitle_raw = None
    full_normalized = _collapse_unicode(stripped)
    main_normalized = _collapse_unicode(main_raw)
    subtitle_normalized = _collapse_unicode(subtitle_raw or "") or None
    main_tokens = main_normalized.split()
    main_is_generic = (
        len(main_tokens) <= 1
        or len(main_normalized) < 8
        or main_normalized in GENERIC_MAIN_TITLES
    )
    return {
        "raw": value,
        "stripped": stripped,
        "removed_markers": removed,
        "full_normalized": full_normalized,
        "main_raw": main_raw,
        "main_normalized": main_normalized,
        "subtitle_raw": subtitle_raw,
        "subtitle_normalized": subtitle_normalized,
        "main_is_generic_or_short": main_is_generic,
    }


def title_evidence(product_title: str, candidate_title: str, aliases: Iterable[str] = ()) -> dict[str, Any]:
    product = title_forms(product_title)
    candidate = title_forms(candidate_title)
    similarity = SequenceMatcher(None, product["full_normalized"], candidate["full_normalized"]).ratio()
    verified_alias_inputs = [title_forms(value)["full_normalized"] for value in aliases]
    alias_exact = candidate["full_normalized"] in verified_alias_inputs
    exact = product["full_normalized"] == candidate["full_normalized"] or alias_exact
    product_without_article = re.sub(r"^(?:the|a|an)\s+", "", product["full_normalized"])
    candidate_without_article = re.sub(r"^(?:the|a|an)\s+", "", candidate["full_normalized"])
    leading_article_variant = bool(
        product["full_normalized"] != candidate["full_normalized"]
        and product_without_article
        and product_without_article == candidate_without_article
    )
    main_subtitle_match = bool(
        product["main_normalized"]
        and product["main_normalized"] == candidate["main_normalized"]
        and product["full_normalized"] != candidate["full_normalized"]
        and (product["subtitle_normalized"] or candidate["subtitle_normalized"])
        and not product["main_is_generic_or_short"]
    )
    strong = bool(
        exact or similarity >= TITLE_STRONG_THRESHOLD
        or main_subtitle_match or leading_article_variant
    )
    close = bool(strong or similarity >= TITLE_CLOSE_THRESHOLD)
    if exact:
        level = "T_exact"
    elif strong:
        level = "T_strong"
    elif close:
        level = "T_close"
    else:
        level = "T_weak"
    return {
        "product": product,
        "candidate": candidate,
        "sequence_matcher_input_product": product["full_normalized"],
        "sequence_matcher_input_candidate": candidate["full_normalized"],
        "similarity": round(similarity, 5),
        "verified_aliases": list(aliases),
        "alias_exact": alias_exact,
        "leading_article_variant": leading_article_variant,
        "main_subtitle_match": main_subtitle_match,
        "main_title_route_blocked": bool(
            product["main_normalized"] == candidate["main_normalized"]
            and product["full_normalized"] != candidate["full_normalized"]
            and product["main_is_generic_or_short"]
        ),
        "level": level,
        "T_exact": exact,
        "T_strong": strong,
        "T_close": close,
    }


def _number_tokens(value: str) -> dict[str, Any]:
    stripped, _ = strip_packaging_markers(value)
    normalized = unicodedata.normalize("NFKC", stripped).casefold()
    ignored: list[str] = []
    for pattern in (
        r"\b(?:3d|4k|8k|1080p|720p|2160p)\b",
        r"\b(?:region|code|disc|disk)\s*[0-9ivx]+\b",
    ):
        for match in re.finditer(pattern, normalized, re.IGNORECASE):
            ignored.append(match.group(0))
        normalized = re.sub(pattern, " ", normalized, flags=re.IGNORECASE)
    definite: list[int] = []
    for token in re.findall(r"(?<!\w)(?:[1-9]\d{0,2}|[ivx]+)(?!\w)", normalized, re.IGNORECASE):
        lowered = token.casefold()
        if lowered.isdigit():
            definite.append(int(lowered))
        elif lowered in ROMAN_VALUES and (lowered != "i" or normalized.rstrip().endswith(" i")):
            definite.append(ROMAN_VALUES[lowered])
    return {"definite": definite, "ignored_release_markers": ignored, "warnings": []}


def number_evidence(product_title: str, candidate_title: str) -> dict[str, Any]:
    product = _number_tokens(product_title)
    candidate = _number_tokens(candidate_title)
    conflict = product["definite"] != candidate["definite"] and bool(
        product["definite"] or candidate["definite"]
    )
    return {"product": product, "candidate": candidate, "hard_conflict": conflict}


def classify_product_type(title: str, descriptions: list[str]) -> dict[str, Any]:
    primary = next((value for value in descriptions if value.strip()), "")[:2500]
    text = f"{title} {primary}".casefold()
    sports_context = bool(
        re.search(r"\b(?:ufc|sports?|fights?|matches?|tournaments?)\b", text)
        and re.search(r"\b(?:highlights?|compilation|collection|multiple|over\s+\d+|finishes|tkos?)\b", text)
    )
    rules = [
        ("sports_highlight", sports_context, "explicit event/highlight or multiple-bout context"),
        ("collection", bool(re.search(r"\b(?:collection|anthology|box set|double feature|triple feature)\b", text)), "collection marker"),
        ("television_series", bool(re.search(r"\b(?:complete series|tv series|television series|season\s+\d+|episodes?)\b", text)), "series marker"),
        ("music_video", bool(re.search(r"\b(?:music video|concert video)\b", text)), "music-video marker"),
        ("documentary", bool(re.search(r"\b(?:documentary|non-fiction|nonfiction|docudrama)\b", text)), "documentary marker"),
        ("television_movie", bool(re.search(r"\b(?:made[- ]for[- ]tv|television movie|tv movie)\b", text)), "television-movie marker"),
        ("direct_release_film", bool(re.search(r"\b(?:direct[- ]to[- ]video|straight[- ]to[- ]video|home video release)\b", text)), "direct-release marker"),
        ("movie", bool(re.search(r"\b(?:this|the|a)\s+(?:\w+\s+){0,3}(?:film|movie)\b", text)), "explicit current-work film/movie wording"),
    ]
    for product_type, matched, reason in rules:
        if matched:
            return {"class": product_type, "basis": reason, "source_scope": "title+first_description", "primary_description": primary}
    return {"class": "unknown", "basis": "no explicit work-type marker", "source_scope": "title+first_description", "primary_description": primary}


def type_evidence(product_type: str, imdb_type: str | None) -> dict[str, Any]:
    imdb_class = {
        "feature": "movie", "TV movie": "television_movie", "video": "video_unknown",
        "TV series": "television_series", "TV mini-series": "television_series",
        "short": "short", "musicVideo": "music_video",
    }.get(imdb_type or "", "unknown")
    if product_type in {"sports_highlight", "collection", "television_series", "music_video"}:
        compatibility = "conflict"
    elif product_type == "television_movie":
        compatibility = "compatible" if imdb_class == "television_movie" else ("unknown" if imdb_class == "unknown" else "conflict")
    elif product_type == "direct_release_film":
        compatibility = "compatible" if imdb_class == "video_unknown" else ("unknown" if imdb_class == "unknown" else "conflict")
    elif product_type in {"movie", "documentary"}:
        if imdb_class in {"movie", "television_movie"}:
            compatibility = "compatible"
        elif imdb_class in {"video_unknown", "unknown"}:
            compatibility = "unknown"
        else:
            compatibility = "conflict"
    else:
        compatibility = "unknown" if imdb_class in {"movie", "television_movie", "video_unknown", "unknown"} else "conflict"
    return {"product_type": product_type, "imdb_type_raw": imdb_type, "imdb_type_class": imdb_class, "compatibility": compatibility}


def _find_name_occurrences(name: str, value: str) -> list[tuple[int, int, str]]:
    folded_value, mapping = _accent_fold_with_map(value)
    folded_name, _ = _accent_fold_with_map(name)
    if not folded_name:
        return []
    occurrences: list[tuple[int, int, str]] = []
    for match in re.finditer(rf"(?<!\w){re.escape(folded_name)}(?!\w)", folded_value):
        start = mapping[match.start()]
        end = mapping[match.end() - 1] + 1
        occurrences.append((start, end, folded_name))
    return occurrences


def _inside_parentheses(value: str, start: int, end: int) -> bool:
    left = value.rfind("(", max(0, start - 100), start)
    right = value.find(")", end, min(len(value), end + 100))
    return left >= 0 and right >= 0 and value.rfind(")", left, start) < left


def _followed_by_actor_parentheses(value: str, end: int) -> bool:
    return bool(re.match(r"\s*\([^)]{2,80}\)", value[end:]))


def _plain_context(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", html.unescape(value))
    return " ".join(without_tags.split())


def classify_person_occurrence(value: str, start: int, end: int, *, field_path: str) -> tuple[str, str | None, str]:
    if field_path == "raw_payload.brand":
        return "brand_support", None, "brand field fully matched candidate name"
    before = value[max(0, start - 120):start]
    after = value[end:min(len(value), end + 120)]
    context = f"{before} {value[start:end]} {after}"
    plain_before = _plain_context(before)
    plain_after = _plain_context(after)
    if OTHER_WORK_RE.search(plain_before[-100:]):
        return "other_work_reference", None, "near other-work/reviewer attribution language"
    if (
        re.search(r"\b(?:actor|actress|performer|host|presenter)\s*$", plain_before, re.IGNORECASE)
        or re.search(r"\bfollows?\s+(?:the\s+)?host\s*$", plain_before, re.IGNORECASE)
    ):
        role = "host" if re.search(r"\bhost\s*$", plain_before, re.IGNORECASE) else "actor"
        return "explicit_credit", role, "explicit actor/host credit immediately before name"
    if re.search(
        r"^\s*(?:\([^)]{0,100}\)\s*)?(?:stars?|starring)\b",
        plain_after,
        re.IGNORECASE,
    ):
        return "explicit_credit", "actor", "name followed by starring credit"
    if re.search(
        r"^.{0,110}\b(?:is|are)\s+top-billed\b",
        plain_after,
        re.IGNORECASE,
    ):
        return "explicit_credit", "actor", "name occurs in a top-billed cast statement"
    if re.search(
        r"^.{0,90}\b(?:puts?\s+in|delivers?)\b.{0,45}\bperformance\b",
        plain_after,
        re.IGNORECASE,
    ) or re.search(
        r"^.{0,100}\bdeserves\b.{0,80}\bportrayal\b",
        plain_after,
        re.IGNORECASE,
    ):
        return "explicit_credit", "actor", "current-work performance/portrayal statement"
    if (
        re.search(r"\bjoins?\b[^.!?]{0,60}$", plain_before, re.IGNORECASE)
        and re.search(r"^.{0,120}\b(?:film|movie|drama)\b", plain_after, re.IGNORECASE)
    ):
        return "explicit_credit", "actor", "joins another performer in the current work"
    first_break = re.search(r"<br\s*/?>", value, re.IGNORECASE)
    if (
        first_break and end <= first_break.start() and start < 180
        and "," in value[:first_break.start()]
        and re.search(r"item\s+type", value[first_break.end():first_break.end() + 100], re.IGNORECASE)
    ):
        return "explicit_credit", "actor", "leading cast list before structured item metadata"
    if _followed_by_actor_parentheses(value, end):
        return "character_or_subject", None, "name precedes a parenthesized performer"
    if _inside_parentheses(value, start, end):
        return "explicit_credit", "actor", "name appears in role-name (performer-name) syntax"
    if DIRECTOR_RE.search(plain_before[-80:]) or re.search(r"\b(?:directs?|directed)\b", plain_after[:50], re.IGNORECASE):
        return "explicit_credit", "director", "director credit context"
    if EXPLICIT_BEFORE_RE.search(plain_before[-100:]) or re.search(r"^\s*(?:stars?|starring)\b", plain_after, re.IGNORECASE):
        return "explicit_credit", "actor", "starring/cast credit context"
    if re.search(r"\b(?:historical figure|subject|about|portrays?)\b", context, re.IGNORECASE):
        return "character_or_subject", None, "subject/character context"
    return "mention_only", None, "name present but work-credit semantics are not reliable"


def person_evidence(name: str, brand: str | None, descriptions: list[str], source_record_id: int) -> dict[str, Any] | None:
    locations: list[dict[str, Any]] = []
    fields: list[tuple[str, str]] = []
    if brand:
        fields.append(("raw_payload.brand", brand))
    fields.extend((f"raw_payload.description[{index}]", text) for index, text in enumerate(descriptions))
    for field_path, value in fields:
        if field_path == "raw_payload.brand":
            folded_brand, _ = _accent_fold_with_map(value)
            folded_name, _ = _accent_fold_with_map(name)
            if folded_brand != folded_name:
                continue
        for start, end, variant in _find_name_occurrences(name, value):
            classification, role, basis = classify_person_occurrence(value, start, end, field_path=field_path)
            left, right = max(0, start - 100), min(len(value), end + 100)
            locations.append({
                "source_record_id": source_record_id, "field_path": field_path,
                "char_start": start, "char_end": end, "snippet_start": left, "snippet_end": right,
                "snippet": value[left:right], "classification": classification,
                "credit_role": role, "classification_basis": basis,
                "matched_variant": variant, "variant_rule": "NFKD accent fold + casefold + punctuation/space collapse + full-name boundary",
            })
    if not locations:
        return None
    classes = {location["classification"] for location in locations}
    if "explicit_credit" in classes:
        effective = "explicit_credit"
    elif "brand_support" in classes:
        effective = "brand_support"
    elif "mention_only" in classes:
        effective = "mention_only"
    elif "character_or_subject" in classes:
        effective = "character_or_subject"
    else:
        effective = "other_work_reference"
    roles = sorted({location["credit_role"] for location in locations if location["credit_role"]})
    return {"name": name, "identity_key": _accent_fold_with_map(name)[0], "effective_class": effective, "credit_roles": roles, "locations": locations}


def year_evidence(candidate_year: int | None, reliable_product_years: list[dict[str, Any]]) -> dict[str, Any]:
    if not reliable_product_years or candidate_year is None:
        return {"status": "missing", "score": 0, "hard_conflict": False, "candidate_year": candidate_year, "reliable_product_years": reliable_product_years}
    years = [int(item["year"]) for item in reliable_product_years]
    difference = min(abs(candidate_year - year) for year in years)
    if difference == 0:
        status, score, hard = "match", 10, False
    elif difference == 1:
        status, score, hard = "one_year_difference", 0, False
    else:
        status, score, hard = "source_conflict", 0, True
    return {"status": status, "score": score, "hard_conflict": hard, "candidate_year": candidate_year, "reliable_product_years": reliable_product_years, "minimum_difference": difference}


def extract_all_candidates(payload: dict[str, Any], product_title: str) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for position, item in enumerate(payload.get("d") or []):
        candidate_id = str(item.get("id") or "")
        if not candidate_id.startswith("tt"):
            continue
        principals = [part.strip() for part in str(item.get("s") or "").split(",") if part.strip()]
        variant = {"response_position": position, "title": item.get("l"), "year": item.get("y"), "type": item.get("q"), "rank": item.get("rank"), "principals_raw": item.get("s")}
        if candidate_id not in by_id:
            by_id[candidate_id] = {
                "id": candidate_id, "title": str(item.get("l") or ""), "year": item.get("y"),
                "type": item.get("q"), "rank": item.get("rank"), "principals": principals,
                "raw_variants": [variant],
            }
        else:
            existing = by_id[candidate_id]
            existing["raw_variants"].append(variant)
            existing["principals"] = list(dict.fromkeys([*existing["principals"], *principals]))
            ranks = [rank for rank in (existing.get("rank"), item.get("rank")) if isinstance(rank, int)]
            existing["rank"] = min(ranks) if ranks else None
    return list(by_id.values())


def evaluate_candidate(
    candidate: dict[str, Any], *, product_title: str, product_type: dict[str, Any],
    brand: str | None, descriptions: list[str], source_record_id: int,
    reliable_product_years: list[dict[str, Any]] | None = None,
    verified_aliases: list[str] | None = None,
    external_id_mapping: dict[str, Any] | None = None,
) -> dict[str, Any]:
    title = title_evidence(product_title, candidate["title"], verified_aliases or [])
    numbers = number_evidence(product_title, candidate["title"])
    type_result = type_evidence(product_type["class"], candidate.get("type"))
    year = year_evidence(candidate.get("year"), reliable_product_years or [])
    people: list[dict[str, Any]] = []
    seen: set[str] = set()
    for name in candidate.get("principals") or []:
        evidence = person_evidence(str(name), brand, descriptions, source_record_id)
        if evidence and evidence["identity_key"] not in seen:
            seen.add(evidence["identity_key"])
            people.append(evidence)
    explicit = [person for person in people if person["effective_class"] == "explicit_credit"]
    brand_only = [person for person in people if person["effective_class"] == "brand_support"]
    mentions = [person for person in people if person["effective_class"] == "mention_only"]
    directors = [person for person in explicit if "director" in person["credit_roles"]]
    title_score = 40 if title["T_exact"] else 30 if title["T_strong"] else 15 if title["T_close"] else 0
    breakdown = {
        "title": title_score,
        "explicit_credit": min(MAX_EXPLICIT_SCORE, 20 * len(explicit)),
        "brand_support": min(MAX_BRAND_SCORE, 10 * len(brand_only)),
        "mention_only": MENTION_SCORE if mentions else 0,
        "reliable_year_match": year["score"],
        "type_compatible": 5 if type_result["compatibility"] == "compatible" else 0,
    }
    score = sum(breakdown.values())
    hard_conflicts: list[str] = []
    if numbers["hard_conflict"]:
        hard_conflicts.append("sequel_or_installment_number_conflict")
    if type_result["compatibility"] == "conflict":
        hard_conflicts.append("work_type_conflict")
    if year["hard_conflict"]:
        hard_conflicts.append("reliable_release_year_conflict_gt_1")
    paths: list[str] = []
    if external_id_mapping and external_id_mapping.get("candidate_id") == candidate["id"] and external_id_mapping.get("verified"):
        paths.append("A_verified_external_id_mapping")
    if title["T_exact"] and explicit:
        paths.append("B_T_exact_plus_explicit_credit")
    if title["T_exact"] and year["status"] == "match" and brand_only:
        paths.append("C_T_exact_year_match_brand_support")
    if title["T_strong"] and len(explicit) >= 2:
        paths.append("D_T_strong_two_explicit_credits")
    if title["T_strong"] and year["status"] == "match" and directors:
        paths.append("E_T_strong_year_match_director")
    eligible = bool(paths and not hard_conflicts)
    return {
        **candidate, "title_evidence": title, "number_evidence": numbers,
        "product_type_evidence": product_type, "type_evidence": type_result,
        "year_evidence": year, "person_evidence": people,
        "person_counts": {"explicit_credit": len(explicit), "brand_support": len(brand_only), "mention_only": len(mentions), "director": len(directors)},
        "score_breakdown": breakdown, "evidence_score": score,
        "hard_conflicts": hard_conflicts, "confirmation_paths": paths,
        "auto_confirmation_eligible": eligible,
    }


def _sort_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -int(bool(candidate["auto_confirmation_eligible"])),
        int(bool(candidate["hard_conflicts"])),
        -int(candidate["evidence_score"]),
        -int(candidate["title_evidence"]["T_exact"]),
        -int(candidate["title_evidence"]["T_strong"]),
        -float(candidate["title_evidence"]["similarity"]),
        candidate["rank"] if isinstance(candidate.get("rank"), int) else 10**12,
        candidate["id"],
    )


def decide(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    if not evaluations:
        return {"status": "no_candidate", "best": None, "reason_code": "expand_search", "question": "缓存响应没有tt候选；需要扩大检索并记录新的来源证据。"}
    ordered = sorted(evaluations, key=_sort_key)
    eligible = [candidate for candidate in ordered if candidate["auto_confirmation_eligible"]]
    best = ordered[0]
    if len(eligible) > 1:
        ids = ", ".join(candidate["id"] for candidate in eligible)
        return {"status": "needs_review_ambiguous", "best": best, "reason_code": "multiple_credible_works", "question": f"多个不同IMDb ID具备自动确认资格：{ids}；需要年份、导演或外部ID区分，不能用rank消歧。"}
    if len(eligible) == 1:
        best = eligible[0]
        near_rivals = [candidate for candidate in ordered if candidate["id"] != best["id"] and not candidate["hard_conflicts"] and candidate["title_evidence"]["T_strong"] and candidate["person_counts"]["explicit_credit"] >= 1 and abs(best["evidence_score"] - candidate["evidence_score"]) <= AMBIGUITY_SCORE_MARGIN]
        if near_rivals:
            ids = ", ".join([best["id"], *[candidate["id"] for candidate in near_rivals]])
            return {"status": "needs_review_ambiguous", "best": best, "reason_code": "multiple_credible_works", "question": f"最佳候选与可信竞争候选分差≤{AMBIGUITY_SCORE_MARGIN}：{ids}；需要额外来源区分。"}
        return {"status": "rule_verified", "best": best, "reason_code": "single_eligible_candidate", "question": f"程序通过路径{','.join(best['confirmation_paths'])}选择{best['id']}；请人工核对并绑定该IMDb ID填写标签。"}
    plausible = [candidate for candidate in ordered if candidate["title_evidence"]["T_close"] and not candidate["hard_conflicts"]]
    if plausible:
        best = plausible[0]
        missing: list[str] = []
        if not best["title_evidence"]["T_strong"]:
            missing.append("T_strong标题证据")
        if not best["person_counts"]["explicit_credit"]:
            missing.append("明确本作品人物署名")
        if best["year_evidence"]["status"] == "missing":
            missing.append("可靠作品年份")
        return {"status": "needs_review_insufficient", "best": best, "reason_code": "evidence_missing", "question": f"候选{best['id']}至少T_close且无硬冲突，但缺少{'、'.join(missing) or '自动确认路径所需证据'}。"}
    category_problem = any(candidate["product_type_evidence"]["class"] in {"sports_highlight", "collection", "television_series", "music_video"} for candidate in ordered)
    return {"status": "no_suitable_candidate", "best": best, "reason_code": "product_category_review" if category_problem else "expand_search", "question": "商品类别待确认，缓存候选均有硬冲突。" if category_problem else "缓存候选均为硬冲突或低于T_close；需要扩大检索。"}


def reevaluate_cached(database_url: str, *, batch_id: str, limit: int = 50) -> dict[str, Any]:
    if not 1 <= limit <= 50:
        raise ValueError("limit must be between 1 and 50")
    with psycopg.connect(database_url) as conn:
        run_id = _run(conn, "stage4_v3_cached_evidence", batch_id, RULE_VERSION)
        rows = conn.execute(
            """
            SELECT s.asin,s.product_title_raw,s.product_source_record_id,pr.raw_payload,
                   c.candidate_source_record_id,cr.raw_payload
            FROM integration.movie_match_development_sample s
            JOIN raw.source_record pr ON pr.source_record_id=s.product_source_record_id
            JOIN integration.movie_match_candidate_result c ON c.asin=s.asin AND c.rule_version=%s
            LEFT JOIN raw.source_record cr ON cr.source_record_id=c.candidate_source_record_id
            WHERE s.selection_rule_version=%s ORDER BY s.sample_rank LIMIT %s
            """,
            (CACHE_RULE_VERSION, SELECTION_RULE_VERSION, limit),
        ).fetchall()
        counts = {status: 0 for status in ("rule_verified", "needs_review_ambiguous", "needs_review_insufficient", "no_suitable_candidate", "no_candidate")}
        total_candidates = 0
        for asin, product_title, product_source_id, product_raw, candidate_source_id, cached_raw in rows:
            descriptions = [str(value) for value in (product_raw.get("description") or [])]
            brand = _meaningful_brand(str(product_raw.get("brand") or ""))
            product_type = classify_product_type(product_title, descriptions)
            raw_candidates = extract_all_candidates(cached_raw or {}, product_title)
            total_candidates += len(raw_candidates)
            evaluations = [evaluate_candidate(candidate, product_title=product_title, product_type=product_type, brand=brand, descriptions=descriptions, source_record_id=product_source_id) for candidate in raw_candidates]
            ordered = sorted(evaluations, key=_sort_key)
            for display_rank, candidate in enumerate(ordered, 1):
                candidate["evaluated_rank"] = display_rank
                candidate["shown_in_display_top"] = display_rank <= DISPLAY_LIMIT
            decision = decide(ordered)
            best = decision["best"]
            counts[decision["status"]] += 1
            selected_facts = [] if not best else [
                {"kind": "title", **best["title_evidence"]},
                {"kind": "number", **best["number_evidence"]},
                {"kind": "type", **best["type_evidence"]},
                {"kind": "year", **best["year_evidence"]},
                *({"kind": "person", **person} for person in best["person_evidence"]),
            ]
            conn.execute(
                """
                INSERT INTO integration.movie_match_candidate_result(
                    asin,candidate_key,candidate_source_record_id,imdb_candidate_id,imdb_title,
                    imdb_year,imdb_title_type,imdb_principal_names,title_similarity,matched_facts,
                    conflict_points,match_status,review_question,candidate_pool,rule_version,run_id,
                    confirmation_paths,review_reason_code,score_breakdown
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s::jsonb,%s,%s,%s::jsonb,%s,%s,%s::jsonb,%s,%s::jsonb)
                ON CONFLICT (asin,rule_version) DO UPDATE SET
                    candidate_key=EXCLUDED.candidate_key,candidate_source_record_id=EXCLUDED.candidate_source_record_id,
                    imdb_candidate_id=EXCLUDED.imdb_candidate_id,imdb_title=EXCLUDED.imdb_title,
                    imdb_year=EXCLUDED.imdb_year,imdb_title_type=EXCLUDED.imdb_title_type,
                    imdb_principal_names=EXCLUDED.imdb_principal_names,title_similarity=EXCLUDED.title_similarity,
                    matched_facts=EXCLUDED.matched_facts,conflict_points=EXCLUDED.conflict_points,
                    match_status=EXCLUDED.match_status,review_question=EXCLUDED.review_question,
                    candidate_pool=EXCLUDED.candidate_pool,run_id=EXCLUDED.run_id,
                    confirmation_paths=EXCLUDED.confirmation_paths,review_reason_code=EXCLUDED.review_reason_code,
                    score_breakdown=EXCLUDED.score_breakdown,decided_at=clock_timestamp()
                """,
                (
                    asin, f"imdb:{best['id']}" if best else "no_candidate", candidate_source_id,
                    best["id"] if best else None, best["title"] if best else None,
                    best.get("year") if best else None, best.get("type") if best else None,
                    _json(best.get("principals", []) if best else []),
                    best["title_evidence"]["similarity"] if best else None,
                    _json(selected_facts), _json(best["hard_conflicts"] if best else []),
                    decision["status"], decision["question"], _json(ordered), RULE_VERSION, run_id,
                    _json(best["confirmation_paths"] if best else []), decision["reason_code"],
                    _json(best["score_breakdown"] if best else {}),
                ),
            )
        stats = {"selected": len(rows), "evaluated_candidates": total_candidates, "source": "cached_only", **counts}
        _finish(conn, run_id, stats)
    return {"run_id": run_id, "rule_version": RULE_VERSION, **stats}


LABEL_VALUES = {"correct_match", "incorrect_match", "uncertain", "no_match_in_pool", "not_single_movie"}


def resolve_label_binding(row: dict[str, str], row_number: int) -> tuple[str, str | None, str]:
    label = (row.get("human_label") or "").strip()
    if label not in LABEL_VALUES:
        raise ValueError(f"row {row_number}: unsupported human_label {label!r}")
    explicit_candidate_id = (row.get("human_label_candidate_id") or "").strip() or None
    predicted_candidate_id = (row.get("imdb_candidate_id") or "").strip() or None
    requested_scope = (row.get("human_label_scope") or "").strip() or None
    if label == "no_match_in_pool":
        scope, candidate_id = "pool", None
    elif label == "not_single_movie":
        scope, candidate_id = "product", None
    else:
        scope = requested_scope or "candidate"
        candidate_id = explicit_candidate_id or predicted_candidate_id
        if scope != "candidate":
            raise ValueError(
                f"row {row_number}: {label} must bind to one candidate; "
                "use no_match_in_pool for an absent correct work"
            )
        if not candidate_id:
            raise ValueError(f"row {row_number}: candidate label requires human_label_candidate_id")
    if requested_scope and requested_scope != scope:
        raise ValueError(
            f"row {row_number}: human_label_scope {requested_scope!r} conflicts with {label!r}"
        )
    return scope, candidate_id, label


def import_labels(database_url: str, path: Path, *, import_batch: str) -> dict[str, int]:
    inserted = 0
    skipped_blank = 0
    with path.open("r", encoding="utf-8-sig", newline="") as stream, psycopg.connect(database_url) as conn:
        for row_number, row in enumerate(csv.DictReader(stream, delimiter="\t"), 2):
            label = (row.get("human_label") or "").strip()
            if not label:
                skipped_blank += 1
                continue
            scope, candidate_id, label = resolve_label_binding(row, row_number)
            asin = (row.get("asin") or "").strip()
            rule_version = (row.get("match_rule_version") or RULE_VERSION).strip()
            reason = (row.get("human_reason") or row.get("human_note") or "").strip()
            annotator = (row.get("human_annotator") or "").strip()
            labeled_at_text = (row.get("human_labeled_at") or "").strip()
            if not reason or not annotator or not labeled_at_text:
                raise ValueError(f"row {row_number}: human_reason, human_annotator and human_labeled_at are required")
            labeled_at = datetime.fromisoformat(labeled_at_text.replace("Z", "+00:00"))
            evidence_text = (row.get("human_evidence") or "{}").strip() or "{}"
            evidence = json.loads(evidence_text)
            result_exists = conn.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM integration.movie_match_candidate_result
                    WHERE asin=%s AND rule_version=%s
                )
                """,
                (asin, rule_version),
            ).fetchone()[0]
            if not result_exists:
                raise ValueError(
                    f"row {row_number}: no algorithm result for ASIN {asin!r} and rule {rule_version!r}"
                )
            if scope == "candidate":
                candidate_exists = conn.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM integration.movie_match_candidate_result r,
                             jsonb_array_elements(r.candidate_pool) candidate
                        WHERE r.asin=%s AND r.rule_version=%s
                          AND candidate->>'id'=%s
                    )
                    """,
                    (asin, rule_version, candidate_id),
                ).fetchone()[0]
                if not candidate_exists:
                    raise ValueError(
                        f"row {row_number}: candidate {candidate_id!r} is not in the saved pool for {asin!r}"
                    )
            identity = "|".join([asin, candidate_id or "", rule_version, scope, label, reason, annotator, labeled_at.isoformat(), _json(evidence)])
            label_hash = hashlib.sha256(identity.encode("utf-8")).hexdigest()
            result = conn.execute(
                """
                INSERT INTO integration.movie_match_human_label(
                    label_hash,asin,imdb_candidate_id,rule_version,label_scope,human_label,
                    reason,evidence,annotator,labeled_at,import_batch
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)
                ON CONFLICT (label_hash) DO NOTHING
                RETURNING label_id
                """,
                (label_hash, asin, candidate_id, rule_version, scope, label, reason, _json(evidence), annotator, labeled_at, import_batch),
            ).fetchone()
            inserted += int(result is not None)
    return {"inserted": inserted, "skipped_blank": skipped_blank}
