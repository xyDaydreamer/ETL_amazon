from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


RULE_VERSION = "stage7-submission-case-inventory-v2"
CROSS_VERSION_RULE_VERSION = "stage7-cross-product-version-v1"


CROSS_VERSION_SPECS = [
    {
        "case_id": "movie-cross-version-01",
        "tconst": "tt1183252",
        "asins": ["B001EBO90G", "B001L67A3M"],
        "version_difference": (
            "two separately identified Blu-ray product editions; B001EBO90G says import/Region All while "
            "the retained specification text also reports Region 2, so the region inconsistency is preserved rather than resolved"
        ),
    },
    {
        "case_id": "movie-cross-version-02",
        "tconst": "tt0090863",
        "asins": ["6300276740", "B001GS7AZS"],
        "version_difference": "VHS edition versus Blu-ray/25th-anniversary edition",
    },
    {
        "case_id": "movie-cross-version-03",
        "tconst": "tt0439630",
        "asins": ["B000BY9VO6", "B0014HST4W"],
        "version_difference": "DVD/DTS listing versus import Region-All Blu-ray edition",
    },
    {
        "case_id": "movie-cross-version-04",
        "tconst": "tt0052948",
        "asins": ["6302098424", "B00007JMD8"],
        "version_difference": "explicit VHS edition versus a separate format-unspecified 20th Century Fox home-video listing",
    },
    {
        "case_id": "movie-cross-version-05",
        "tconst": "tt0327850",
        "asins": ["B00016MSVM", "B0009NJFPO"],
        "version_difference": "factory-sealed VHS edition versus a separate original-case Universal home-video listing",
    },
]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream, delimiter="\t"))


def _norm(value: str) -> str:
    folded = "".join(
        char for char in unicodedata.normalize("NFKD", value).casefold()
        if not unicodedata.combining(char)
    )
    return " ".join(re.findall(r"[^\W_]+", folded, flags=re.UNICODE))


def _title_key(value: str) -> str:
    return _norm(re.sub(r"\b(?:vhs|dvd|blu[ -]?ray|4k uhd)\b", " ", value, flags=re.IGNORECASE))


def _evidence_snippet(text: str, needle: str, radius: int = 110) -> dict[str, Any] | None:
    offset = text.casefold().find(needle.casefold())
    if offset < 0:
        return None
    return {
        "needle": needle,
        "char_start": offset,
        "char_end": offset + len(needle),
        "snippet": text[max(0, offset - radius):offset + len(needle) + radius],
    }


def _record_cross_version_cases(conn: psycopg.Connection[Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    run_id = conn.execute(
        """INSERT INTO meta.etl_run(pipeline_name,batch_id,input_version,rule_version,status,stats)
           VALUES ('stage7_cross_product_version','stage7-cross-product-version-v1',
                   'imdb-noncommercial-2026-11-05',%s,'running','{}'::jsonb)
           ON CONFLICT (pipeline_name,batch_id,input_version,rule_version) DO UPDATE SET
             status='running',finished_at=NULL
           RETURNING run_id""",
        (CROSS_VERSION_RULE_VERSION,),
    ).fetchone()["run_id"]
    cases: list[dict[str, Any]] = []
    inserted_or_updated = 0
    development_asins = [asin for spec in CROSS_VERSION_SPECS for asin in spec["asins"]]
    evaluation_overlap = conn.execute(
        """SELECT count(*) AS count
           FROM evaluation.entity_pair ep
           JOIN evaluation.dataset d USING(evaluation_dataset_id)
           WHERE (d.entity_type='movie' AND ep.left_entity_key = ANY(%s))
              OR (d.entity_type='person' AND ep.left_display->>'asin' = ANY(%s))""",
        (development_asins, development_asins),
    ).fetchone()["count"]
    if evaluation_overlap:
        raise ValueError(f"cross-version development ASIN overlaps formal evaluation: {evaluation_overlap}")
    for spec in CROSS_VERSION_SPECS:
        tconst = spec["tconst"]
        work = conn.execute(
            """SELECT b.primary_title,b.original_title,b.start_year,b.title_type,b.source_record_id,
                      sr.record_number,sr.content_sha256,sr.field_locator,m.movie_id,m.stable_key
               FROM staging.imdb_title_basics b
               JOIN raw.source_record sr ON sr.source_record_id=b.source_record_id
               JOIN target.movie m ON m.stable_key='imdb:'||b.tconst
               WHERE b.snapshot_id=1 AND b.tconst=%s""",
            (tconst,),
        ).fetchone()
        if not work:
            raise ValueError(f"fixed-snapshot work is not materialized: {tconst}")
        principals = conn.execute(
            """SELECT n.primary_name,p.category,p.nconst,p.source_record_id
               FROM staging.imdb_title_principal p
               JOIN staging.imdb_name_basics n USING(snapshot_id,nconst)
               WHERE p.snapshot_id=1 AND p.tconst=%s ORDER BY p.ordering""",
            (tconst,),
        ).fetchall()
        products = []
        for asin in spec["asins"]:
            product = conn.execute(
                """SELECT pm.asin,pm.title_raw,pm.brand_raw,pm.description_raw,pm.categories_raw,
                          pm.source_record_id,pm.source_record_number,sr.content_sha256,sr.field_locator,
                          pe.product_id
                   FROM staging.product_metadata_record pm
                   JOIN raw.source_record sr USING(source_record_id)
                   JOIN target.product_edition pe USING(asin)
                   WHERE pm.asin=%s ORDER BY pm.product_metadata_id DESC LIMIT 1""",
                (asin,),
            ).fetchone()
            if not product:
                raise ValueError(f"product evidence is missing: {asin}")
            if _title_key(product["title_raw"] or "") != _title_key(work["primary_title"]):
                raise ValueError(f"product title is not compatible with fixed-snapshot title: {asin}/{tconst}")
            product_text = " ".join(
                [product["brand_raw"] or ""] + [str(value) for value in product["description_raw"]]
            )
            person_hits = []
            normalized_text = _norm(product_text)
            for principal in principals:
                normalized_name = _norm(principal["primary_name"])
                if normalized_name and re.search(rf"(?<!\w){re.escape(normalized_name)}(?!\w)", normalized_text):
                    location = _evidence_snippet(product_text, principal["primary_name"])
                    person_hits.append({
                        "name": principal["primary_name"], "nconst": principal["nconst"],
                        "category": principal["category"], "imdb_source_record_id": principal["source_record_id"],
                        "product_excerpt": location,
                    })
            # Some product spellings are stable variants (for example JeeJa Yanin / Yanin Vismitananda).
            # They are not counted unless another exact fixed-snapshot credit is present in the same product record.
            if not person_hits:
                raise ValueError(f"no independently located IMDb principal in product evidence: {asin}/{tconst}")
            products.append({**dict(product), "person_hits": person_hits})

        evidence = {
            "case_id": spec["case_id"],
            "sample_role": "development",
            "rule_version": CROSS_VERSION_RULE_VERSION,
            "work": {
                "tconst": tconst, "primary_title": work["primary_title"],
                "original_title": work["original_title"], "start_year": work["start_year"],
                "title_type": work["title_type"], "source_record_id": work["source_record_id"],
                "record_number": work["record_number"], "content_sha256": work["content_sha256"],
                "field_locator": work["field_locator"],
            },
            "version_difference": spec["version_difference"],
            "products": [
                {
                    "asin": product["asin"], "title_raw": product["title_raw"],
                    "brand_raw": product["brand_raw"], "categories_raw": product["categories_raw"],
                    "source_record_id": product["source_record_id"],
                    "source_record_number": product["source_record_number"],
                    "content_sha256": product["content_sha256"], "field_locator": product["field_locator"],
                    "person_hits": product["person_hits"],
                }
                for product in products
            ],
            "selection_reason": (
                "Each distinct ASIN has a title compatible with the fixed-snapshot work and at least one independently "
                "located principal credit; edition/format evidence distinguishes the product records. Title alone was not used."
            ),
        }
        decision_ids = []
        for product in products:
            decision_id = conn.execute(
                """INSERT INTO integration.merge_decision(
                       entity_type,left_entity_key,right_entity_key,decision,decision_origin,
                       rule_version,confidence,evidence,run_id)
                   VALUES ('product_movie',%s,%s,'merge','rule',%s,NULL,%s::jsonb,%s)
                   ON CONFLICT (entity_type,left_entity_key,right_entity_key,rule_version) DO UPDATE SET
                     evidence=EXCLUDED.evidence,run_id=EXCLUDED.run_id
                   RETURNING merge_decision_id""",
                (product["asin"], work["stable_key"], CROSS_VERSION_RULE_VERSION, _json(evidence), run_id),
            ).fetchone()["merge_decision_id"]
            conn.execute(
                """INSERT INTO target.product_movie(product_id,movie_id,relationship_type,decision_id,mapping_status)
                   VALUES (%s,%s,'single_work',%s,'verified')
                   ON CONFLICT (product_id,movie_id,relationship_type) DO UPDATE SET
                     decision_id=EXCLUDED.decision_id,mapping_status=EXCLUDED.mapping_status""",
                (product["product_id"], work["movie_id"], decision_id),
            )
            decision_ids.append(decision_id)
            inserted_or_updated += 1
        cases.append({
            "case_id": spec["case_id"], "category": "movie_cross_version_merge",
            "status": "verified_merge", "left_id": ",".join(spec["asins"]),
            "left_name": " | ".join(product["title_raw"] for product in products),
            "right_id": tconst, "right_name": work["primary_title"],
            "decision_reason": evidence["selection_reason"], "evidence": evidence,
            "source_record_ids": [product["source_record_id"] for product in products] + [work["source_record_id"]],
            "source_links": [
                *[{"label": f"Amazon {asin}", "url": f"https://www.amazon.com/dp/{asin}"} for asin in spec["asins"]],
                {"label": f"IMDb {tconst}", "url": f"https://www.imdb.com/title/{tconst}/"},
            ],
            "sample_role": "development", "rule_version": CROSS_VERSION_RULE_VERSION,
        })

    # Rechecked existing candidate: these are the 2010 remake and the 1973 original, not two editions.
    crazies_evidence = {
        "case_id": "movie-cross-version-rejected-crazies",
        "asins": ["B0021L8UXK", "B002VRNJQ2"], "candidate_tconst": "tt0455407",
        "reason": (
            "B0021L8UXK names Timothy Olyphant, Breck Eisner and the Ogden Marsh story of the 2010 remake; "
            "B002VRNJQ2 names Lane Carroll, George Romero and Evans City from the 1973 original."
        ),
        "sample_role": "development", "rule_version": CROSS_VERSION_RULE_VERSION,
    }
    conn.execute(
        """INSERT INTO integration.merge_decision(
               entity_type,left_entity_key,right_entity_key,decision,decision_origin,rule_version,confidence,evidence,run_id)
           VALUES ('product_movie','B002VRNJQ2','imdb:tt0455407','do_not_merge','rule',%s,NULL,%s::jsonb,%s)
           ON CONFLICT (entity_type,left_entity_key,right_entity_key,rule_version) DO UPDATE SET
             evidence=EXCLUDED.evidence,run_id=EXCLUDED.run_id""",
        (CROSS_VERSION_RULE_VERSION, _json(crazies_evidence), run_id),
    )
    verified_versions = conn.execute(
        """SELECT m.stable_key,m.canonical_title,m.release_year,
                  array_agg(pe.asin ORDER BY pe.asin) AS asins,
                  array_agg(pm.mapping_status ORDER BY pe.asin) AS mapping_statuses,
                  count(*) AS edition_count
           FROM target.movie m
           JOIN target.product_movie pm USING(movie_id)
           JOIN target.product_edition pe USING(product_id)
           WHERE m.stable_key = ANY(%s)
           GROUP BY m.stable_key,m.canonical_title,m.release_year ORDER BY m.stable_key""",
        ([f"imdb:{spec['tconst']}" for spec in CROSS_VERSION_SPECS],),
    ).fetchall()
    orphan_count = conn.execute(
        """SELECT count(*) AS count
           FROM target.product_movie pm
           LEFT JOIN target.product_edition pe USING(product_id)
           LEFT JOIN target.movie m USING(movie_id)
           LEFT JOIN integration.merge_decision md ON md.merge_decision_id=pm.decision_id
           WHERE pe.product_id IS NULL OR m.movie_id IS NULL OR md.merge_decision_id IS NULL"""
    ).fetchone()["count"]
    if len(verified_versions) != len(CROSS_VERSION_SPECS) or any(row["edition_count"] < 2 for row in verified_versions):
        raise ValueError("all-product-versions verification did not return two editions for every confirmed group")
    if orphan_count:
        raise ValueError(f"orphan product-movie mapping detected: {orphan_count}")
    stats = {
        "verified_groups": len(cases), "product_mappings_recorded": inserted_or_updated,
        "rejected_candidate_groups": 1, "verified_version_query": [dict(row) for row in verified_versions],
        "orphan_product_movie_mappings": orphan_count, "formal_evaluation_overlap": evaluation_overlap,
    }
    conn.execute(
        "UPDATE meta.etl_run SET status='succeeded',finished_at=clock_timestamp(),stats=%s::jsonb WHERE run_id=%s",
        (_json(stats), run_id),
    )
    return cases, {"run_id": run_id, **stats}


def _find_location(product: dict[str, Any], raw_name: str) -> dict[str, Any]:
    fields = [("raw_payload.brand", product.get("brand_raw") or "")]
    fields.extend(
        (f"raw_payload.description[{index}]", str(value))
        for index, value in enumerate(product.get("description_raw") or [])
    )
    needle = raw_name.casefold()
    for field_path, text in fields:
        offset = text.casefold().find(needle)
        if offset >= 0:
            return {
                "field_path": field_path,
                "char_start": offset,
                "char_end": offset + len(raw_name),
                "snippet": text[max(0, offset - 80): offset + len(raw_name) + 80],
            }
    raise ValueError(f"raw person spelling not found for {product['asin']}: {raw_name}")


def build_case_inventory(
    database_url: str,
    *,
    stage4_tsv: Path,
    stage5_case_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cases: list[dict[str, Any]] = []
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        cross_version_cases, cross_version_run = _record_cross_version_cases(conn)
        cases.extend(cross_version_cases)
        existing = conn.execute(
            """SELECT md.merge_decision_id,md.left_entity_key,md.right_entity_key,
                      md.evidence,p.canonical_name,pei.external_id,
                      pei.source_record_id AS name_source_record_id
               FROM integration.merge_decision md
               JOIN target.person p ON p.stable_key=md.right_entity_key
               JOIN integration.person_external_identifier pei
                 ON pei.person_id=p.person_id AND pei.source_name='imdb'
               WHERE md.entity_type='person' AND md.decision='merge'
               ORDER BY md.merge_decision_id"""
        ).fetchall()
        for index, row in enumerate(existing, start=1):
            cases.append({
                "case_id": f"person-merge-{index:02d}",
                "category": "person_alias_merge",
                "status": "verified_merge",
                "left_id": row["left_entity_key"],
                "left_name": row["evidence"]["facts"][0]["left"],
                "right_id": row["external_id"],
                "right_name": row["canonical_name"],
                "decision_reason": row["evidence"]["reason"],
                "evidence": row["evidence"],
                "source_record_ids": [
                    fact.get("suggestion_source_record_id")
                    for fact in row["evidence"]["facts"]
                    if fact.get("suggestion_source_record_id")
                ] + [row["name_source_record_id"]],
                "source_links": [{"label": f"IMDb {row['external_id']}", "url": f"https://www.imdb.com/name/{row['external_id']}/"}],
                "sample_role": "development",
                "rule_version": RULE_VERSION,
            })

        variant_specs = [
            ("B0039USJBK", "tt0365675", "nm0777788", "MATTHIAS SCHWEIGHOFER"),
            ("B002PSLXPG", "tt1153690", "nm2867570", "Micheal J. Smith, Sr."),
            ("B0031DDG9U", "tt1277737", "nm0013037", "Shorheh Aghdashloo"),
        ]
        for asin, tconst, nconst, raw_name in variant_specs:
            adopted = conn.execute(
                """SELECT adoption_status,adopted_imdb_candidate_id
                   FROM integration.unified_development_movie_mapping_decision
                   WHERE asin=%s""",
                (asin,),
            ).fetchone()
            if not adopted or adopted["adopted_imdb_candidate_id"] != tconst:
                raise ValueError(f"development product is not adopted to expected work: {asin} -> {tconst}")
            product = conn.execute(
                """SELECT asin,title_raw,brand_raw,description_raw,source_record_id,source_record_number
                   FROM staging.product_metadata_record WHERE asin=%s
                   ORDER BY product_metadata_id DESC LIMIT 1""",
                (asin,),
            ).fetchone()
            person = conn.execute(
                """SELECT n.primary_name,p.category,p.characters,
                          p.source_record_id AS relationship_source_record_id,
                          n.source_record_id AS name_source_record_id
                   FROM staging.imdb_title_principal p
                   JOIN staging.imdb_name_basics n USING(snapshot_id,nconst)
                   WHERE p.snapshot_id=1 AND p.tconst=%s AND p.nconst=%s""",
                (tconst, nconst),
            ).fetchone()
            if not person:
                raise ValueError(f"fixed snapshot person credit missing: {tconst}/{nconst}")
            location = _find_location(product, raw_name)
            cases.append({
                "case_id": f"person-merge-{len([c for c in cases if c['category']=='person_alias_merge']) + 1:02d}",
                "category": "person_alias_merge",
                "status": "verified_merge",
                "left_id": f"ucsd:{product['source_record_id']}:{location['field_path']}:{location['char_start']}",
                "left_name": raw_name,
                "right_id": nconst,
                "right_name": person["primary_name"],
                "decision_reason": (
                    "UCSD explicitly credits the name on an adopted product-to-work mapping; the fixed IMDb snapshot "
                    "credits the nconst on the same tconst and the spelling difference is a bounded accent, punctuation, or transposition variant."
                ),
                "evidence": {
                    "asin": asin,
                    "tconst": tconst,
                    "adoption_status": adopted["adoption_status"],
                    "ucsd_location": location,
                    "imdb_category": person["category"],
                    "imdb_characters": person["characters"],
                },
                "source_record_ids": [
                    product["source_record_id"], person["relationship_source_record_id"], person["name_source_record_id"]
                ],
                "source_links": [
                    {"label": f"Amazon {asin}", "url": f"https://www.amazon.com/dp/{asin}"},
                    {"label": f"IMDb title {tconst}", "url": f"https://www.imdb.com/title/{tconst}/"},
                    {"label": f"IMDb person {nconst}", "url": f"https://www.imdb.com/name/{nconst}/"},
                ],
                "sample_role": "development",
                "rule_version": RULE_VERSION,
            })

    person_rows = _read_tsv(stage5_case_dir / "person_disambiguation_development.tsv")
    chosen_person_pairs = {
        frozenset(("nm2864782", "nm2866628")),
        frozenset(("nm0005811", "nm0406334")),
        frozenset(("nm0000140", "nm0205157")),
        frozenset(("nm0316078", "nm0316079")),
        frozenset(("nm0567756", "nm1222820")),
    }
    for row in person_rows:
        pair = frozenset((row["left_nconst"], row["right_nconst"]))
        if pair not in chosen_person_pairs:
            continue
        left_context = json.loads(row["left_context"])
        right_context = json.loads(row["right_context"])
        if pair == frozenset(("nm2864782", "nm2866628")):
            reason = "The fixed snapshot has two separately credited nconsts on the same work with distinct full names; they are simultaneous people, not spelling aliases."
        else:
            reason = "Distinct full names plus independent life/work/profession evidence support keeping the fixed-snapshot people separate; nconst inequality alone is not the reason."
        cases.append({
            "case_id": f"person-nonmerge-{len([c for c in cases if c['category']=='person_near_name_nonmerge']) + 1:02d}",
            "category": "person_near_name_nonmerge",
            "status": "verified_do_not_merge",
            "left_id": row["left_nconst"], "left_name": row["left_name"],
            "right_id": row["right_nconst"], "right_name": row["right_name"],
            "decision_reason": reason,
            "evidence": {"name_similarity": row["name_similarity"], "left_context": left_context, "right_context": right_context},
            "source_record_ids": [json.loads(row["left_source"])["source_record_id"], json.loads(row["right_source"])["source_record_id"]],
            "source_links": json.loads(row["source_links"]),
            "sample_role": "development", "rule_version": RULE_VERSION,
        })

    stage4_rows = {row["asin"]: row for row in _read_tsv(stage4_tsv)}
    movie_specs = [
        ("B0046MOVD0", "tt0432021", "human incorrect_match: a different Resident Evil installment"),
        ("B003L1ZWG2", "tt4520988", "hard sequel/installment-number conflict: Frozen versus Frozen II"),
        ("B000RGN2IY", "tt1204979", "hard sequel/installment-number conflict: Vacancy versus Vacancy 2"),
        ("B000PGTNCK", "tt6548228", "hard work-type conflict: single movie product versus television series"),
        ("B0021L8UXK", "tt1707815", "hard work-type conflict: movie product versus television series"),
    ]
    for asin, candidate_id, reason in movie_specs:
        row = stage4_rows[asin]
        pool = json.loads(row["candidate_pool"])
        candidate = next((item for item in pool if item.get("id") == candidate_id), None)
        if not candidate:
            raise ValueError(f"candidate not present in frozen Stage 4 pool: {asin}/{candidate_id}")
        if asin == "B0046MOVD0":
            labels = json.loads(row["human_labels_json"])
            rejected = any(
                label.get("imdb_candidate_id") == candidate_id and label.get("label") == "incorrect_match"
                for label in labels
            )
            if not rejected:
                raise ValueError("Resident Evil negative human decision is missing")
        elif not candidate.get("hard_conflicts"):
            raise ValueError(f"expected hard conflict is missing: {asin}/{candidate_id}")
        cases.append({
            "case_id": f"movie-nonmerge-{len([c for c in cases if c['category']=='movie_confusing_nonmerge']) + 1:02d}",
            "category": "movie_confusing_nonmerge",
            "status": "verified_do_not_merge",
            "left_id": asin, "left_name": row["product_title_raw"],
            "right_id": candidate_id, "right_name": candidate.get("title"),
            "decision_reason": reason,
            "evidence": {
                "candidate_year": candidate.get("year"), "candidate_type": candidate.get("type"),
                "hard_conflicts": candidate.get("hard_conflicts"), "title_evidence": candidate.get("title_evidence"),
                "human_decision_used": asin == "B0046MOVD0",
            },
            "source_record_ids": [row["ucsd_source_record_id"], row["imdb_source_record_id"]],
            "source_links": [
                {"label": f"Amazon {asin}", "url": f"https://www.amazon.com/dp/{asin}"},
                {"label": f"IMDb {candidate_id}", "url": f"https://www.imdb.com/title/{candidate_id}/"},
            ],
            "sample_role": "development", "rule_version": RULE_VERSION,
        })

    for index, row in enumerate(_read_tsv(stage5_case_dir / "movie_cross_version_development.tsv"), start=1):
        if row["candidate_tconst"] == "tt1183252":
            continue
        status = "verified_do_not_merge" if row["candidate_tconst"] == "tt0455407" else "candidate_evidence_insufficient"
        reason = (
            "The two ASINs describe different works: the 2010 Breck Eisner/Timothy Olyphant remake and the 1973 George Romero/Lane Carroll original."
            if status == "verified_do_not_merge"
            else "Title agreement alone does not prove that this ASIN is another edition of the adopted work; person/version evidence is still required."
        )
        cases.append({
            "case_id": f"movie-cross-version-candidate-{index:02d}",
            "category": "movie_cross_version_rejected" if status == "verified_do_not_merge" else "movie_cross_version_merge",
            "status": status,
            "left_id": row["candidate_asin"], "left_name": row["candidate_product_title"],
            "right_id": row["candidate_tconst"], "right_name": row["imdb_primary_title"],
            "decision_reason": reason,
            "evidence": {"existing_edition_asin": row["existing_edition_asin"], "title_similarity": row["title_similarity"], "title_evidence": json.loads(row["title_evidence"])},
            "source_record_ids": [json.loads(row["product_source"])["source_record_id"], json.loads(row["work_source"])["source_record_id"]],
            "source_links": json.loads(row["source_links"]),
            "sample_role": "development", "rule_version": RULE_VERSION,
        })

    categories = [
        "person_alias_merge", "person_near_name_nonmerge",
        "movie_cross_version_merge", "movie_confusing_nonmerge",
    ]
    summary = {}
    for category in categories:
        selected = [case for case in cases if case["category"] == category]
        verified = sum(case["status"].startswith("verified") for case in selected)
        candidates = len(selected) - verified
        summary[category] = {
            "required": 5,
            "verified": verified,
            "candidate_only": candidates,
            "missing_verified": max(0, 5 - verified),
        }

    fields = [
        "case_id", "category", "status", "left_id", "left_name", "right_id", "right_name",
        "decision_reason", "evidence", "source_record_ids", "source_links", "sample_role", "rule_version",
    ]
    tsv_path = output_dir / "case_inventory.tsv"
    with tsv_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for case in cases:
            writer.writerow({
                **case,
                "evidence": _json(case["evidence"]),
                "source_record_ids": _json(case["source_record_ids"]),
                "source_links": _json(case["source_links"]),
            })
    result = {
        "rule_version": RULE_VERSION,
        "summary": summary,
        "case_count": len(cases),
        "inputs": {
            "stage4_tsv": str(stage4_tsv.resolve()), "stage4_tsv_sha256": _sha(stage4_tsv),
            "stage5_person_candidates_sha256": _sha(stage5_case_dir / "person_disambiguation_development.tsv"),
            "stage5_movie_candidates_sha256": _sha(stage5_case_dir / "movie_cross_version_development.tsv"),
        },
        "output_tsv": str(tsv_path),
        "output_tsv_sha256": _sha(tsv_path),
        "cross_version_database_run": cross_version_run,
        "limitations": [
            "The Crazies same-title pair is explicitly rejected because it joins a remake to the original work.",
            "Chocolate's contradictory region text is retained as a source conflict; the work merge relies on multiple located credits and shared detailed specifications, not that region claim.",
            "Formal evaluation pairs are not reused as development cases.",
        ],
    }
    (output_dir / "case_inventory.json").write_text(_json(result) + "\n", encoding="utf-8")
    (output_dir / "cross_version_verification.json").write_text(
        _json(cross_version_run) + "\n", encoding="utf-8"
    )
    return result
