from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from amazon_movies.imdb_selective_loader import _normalize_title_type
from amazon_movies.stage4_v3 import (
    classify_product_type,
    decide,
    evaluate_candidate,
)


RULE_VERSION = "p2-movie-expansion-v1-stage4-evidence"
SOURCE_NAME = "ucsd_movies_tv_2018"
SOURCE_VERSION = "sha256:b7362b6c0bf92a6c47fbbb7855aea2bffd45042f6f10af73bc3afa3d8716c3e9"
SNAPSHOT_VERSION = "imdb-noncommercial-2026-11-05"
STAGE4_RULE_VERSION = "stage4-first50-movie-match-v3.1-evidence-extraction"
STAGE6_SCOPE_KEY = "stage6-independent-candidates-v1"


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _run(conn: psycopg.Connection[Any], pipeline: str, batch_id: str, input_version: str) -> int:
    return int(conn.execute(
        """INSERT INTO meta.etl_run(pipeline_name,batch_id,input_version,rule_version,status,stats)
           VALUES (%s,%s,%s,%s,'running','{}'::jsonb)
           ON CONFLICT (pipeline_name,batch_id,input_version,rule_version) DO UPDATE SET
             status='running',started_at=clock_timestamp(),finished_at=NULL,stats='{}'::jsonb
           RETURNING run_id""",
        (pipeline, batch_id, input_version, RULE_VERSION),
    ).fetchone()["run_id"])


def _finish(conn: psycopg.Connection[Any], run_id: int, stats: dict[str, Any]) -> None:
    conn.execute(
        "UPDATE meta.etl_run SET status='succeeded',finished_at=clock_timestamp(),stats=%s::jsonb WHERE run_id=%s",
        (_canonical(stats), run_id),
    )


def _completed_run(
    conn: psycopg.Connection[Any], pipeline: str, batch_id: str, input_version: str
) -> dict[str, Any] | None:
    row = conn.execute(
        """SELECT run_id,stats FROM meta.etl_run
           WHERE pipeline_name=%s AND batch_id=%s AND input_version=%s AND rule_version=%s
             AND status='succeeded'""",
        (pipeline, batch_id, input_version, RULE_VERSION),
    ).fetchone()
    if not row:
        return None
    stats = dict(row["stats"] or {})
    return {"run_id": row["run_id"], **stats, "idempotent_replay": True, "applied_this_execution": 0}


def _fail(database_url: str, run_id: int) -> None:
    with psycopg.connect(database_url) as conn:
        conn.execute(
            "UPDATE meta.etl_run SET status='failed',finished_at=clock_timestamp() WHERE run_id=%s",
            (run_id,),
        )


def _snapshot_id(conn: psycopg.Connection[Any], snapshot_version: str) -> int:
    row = conn.execute(
        "SELECT snapshot_id FROM meta.external_snapshot WHERE snapshot_version=%s AND verification_status='fully_verified'",
        (snapshot_version,),
    ).fetchone()
    if not row:
        raise ValueError(f"fully verified snapshot not found: {snapshot_version}")
    return int(row["snapshot_id"])


def _base_rows(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    return list(conn.execute(
        """WITH latest AS (
             SELECT DISTINCT ON (p.asin)
                    p.asin,p.source_record_id AS product_source_record_id,
                    p.source_record_number AS product_source_record_number,
                    p.title_raw AS product_title_raw,
                    pe.product_id
             FROM staging.product_metadata_record p
             JOIN target.product_edition pe USING(asin)
             WHERE p.source_name=%s AND p.source_version=%s
               AND p.proposed_classification='movie' AND p.title_raw IS NOT NULL
             ORDER BY p.asin,p.product_metadata_id DESC
           ), adopted AS (
             SELECT pe.asin,array_agg(DISTINCT pm.movie_id ORDER BY pm.movie_id) movie_ids
             FROM target.product_movie pm JOIN target.product_edition pe USING(product_id)
             WHERE pm.relationship_type='single_work'
               AND pm.mapping_status IN ('verified','manual','predicted')
             GROUP BY pe.asin
           ), stage4 AS (
             SELECT DISTINCT asin FROM integration.movie_match_candidate_result
             WHERE rule_version=%s
           ), stage6 AS (
             SELECT p.asin,count(c.tconst)::integer candidate_count
             FROM meta.stage6_candidate_scope_product p
             JOIN meta.stage6_candidate_scope s USING(snapshot_id,scope_key)
             LEFT JOIN integration.stage6_movie_candidate_cache c
               ON c.snapshot_id=p.snapshot_id AND c.scope_key=p.scope_key AND c.asin=p.asin
             WHERE p.scope_key=%s AND s.status='candidates_ready'
               AND NOT EXISTS (
                 SELECT 1 FROM meta.stage6_candidate_progress g
                 WHERE g.snapshot_id=s.snapshot_id AND g.scope_key=s.scope_key
                   AND (g.status<>'verified_complete' OR NOT g.reached_eof)
               )
             GROUP BY p.asin
           )
           SELECT l.*,coalesce(a.movie_ids,ARRAY[]::bigint[]) baseline_movie_ids,
                  a.asin IS NOT NULL baseline_adopted,
                  d.asin IS NOT NULL stage4_scanned,
                  s.asin IS NOT NULL stage6_scanned,
                  coalesce(s.candidate_count,0) stage6_candidate_count
           FROM latest l LEFT JOIN adopted a USING(asin)
           LEFT JOIN stage4 d USING(asin) LEFT JOIN stage6 s USING(asin)
           ORDER BY l.asin""",
        (SOURCE_NAME, SOURCE_VERSION, STAGE4_RULE_VERSION, STAGE6_SCOPE_KEY),
    ).fetchall())


def register_scope(
    database_url: str, *, scope_key: str, batch_id: str,
    output: Path, snapshot_version: str = SNAPSHOT_VERSION,
) -> dict[str, Any]:
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        snapshot_id = _snapshot_id(conn, snapshot_version)
        rows = _base_rows(conn)
        if len(rows) != 433 or len({row["asin"] for row in rows}) != 433:
            raise ValueError(f"fixed movie-product baseline must contain 433 unique ASINs, got {len(rows)}")
        scope_payload = [
            (row["asin"], row["product_source_record_id"], row["product_title_raw"])
            for row in rows
        ]
        scope_sha = _sha({"source_version": SOURCE_VERSION, "snapshot": snapshot_version, "products": scope_payload})
        baseline_adopted = sum(bool(row["baseline_adopted"]) for row in rows)
        baseline_movies = len({movie_id for row in rows for movie_id in row["baseline_movie_ids"]})
        existing = conn.execute(
            "SELECT scope_sha256 FROM meta.p2_movie_scope WHERE scope_key=%s", (scope_key,)
        ).fetchone()
        if existing and existing["scope_sha256"] != scope_sha:
            raise ValueError("scope_key already exists with a different 433-ASIN baseline")
        run_id = _run(conn, "p2_movie_scope_register", batch_id, scope_sha)
        conn.execute(
            """INSERT INTO meta.p2_movie_scope(
                 scope_key,source_name,source_version,snapshot_id,candidate_scope_key,
                 rule_version,scope_sha256,asin_count,baseline_adopted_asin_count,
                 baseline_adopted_movie_count,status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'registered')
               ON CONFLICT (scope_key) DO UPDATE SET updated_at=clock_timestamp()""",
            (scope_key, SOURCE_NAME, SOURCE_VERSION, snapshot_id, STAGE6_SCOPE_KEY,
             RULE_VERSION, scope_sha, len(rows), baseline_adopted, baseline_movies),
        )
        for row in rows:
            conn.execute(
                """INSERT INTO meta.p2_movie_scope_product(
                     scope_key,asin,product_id,product_source_record_id,
                     product_source_record_number,product_title_raw,baseline_adopted,
                     baseline_movie_ids,stage4_scanned,stage6_scanned,stage6_candidate_count)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (scope_key,asin) DO UPDATE SET
                     product_id=EXCLUDED.product_id,
                     product_source_record_id=EXCLUDED.product_source_record_id,
                     product_source_record_number=EXCLUDED.product_source_record_number,
                     product_title_raw=EXCLUDED.product_title_raw,
                     stage4_scanned=EXCLUDED.stage4_scanned,
                     stage6_scanned=EXCLUDED.stage6_scanned,
                     stage6_candidate_count=EXCLUDED.stage6_candidate_count""",
                (scope_key, row["asin"], row["product_id"], row["product_source_record_id"],
                 row["product_source_record_number"], row["product_title_raw"],
                 row["baseline_adopted"], row["baseline_movie_ids"], row["stage4_scanned"],
                 row["stage6_scanned"], row["stage6_candidate_count"]),
            )
        frozen_scope = conn.execute(
            "SELECT * FROM meta.p2_movie_scope WHERE scope_key=%s", (scope_key,)
        ).fetchone()
        rows = list(conn.execute(
            "SELECT * FROM meta.p2_movie_scope_product WHERE scope_key=%s ORDER BY asin", (scope_key,)
        ).fetchall())
        baseline_adopted = int(frozen_scope["baseline_adopted_asin_count"])
        baseline_movies = int(frozen_scope["baseline_adopted_movie_count"])
        conn.execute(
            """INSERT INTO meta.p2_movie_progress(scope_key,phase,processed_count,status,run_id,details)
               VALUES (%s,'register',%s,'completed',%s,%s::jsonb)
               ON CONFLICT (scope_key,phase) DO UPDATE SET processed_count=EXCLUDED.processed_count,
                 status='completed',run_id=EXCLUDED.run_id,details=EXCLUDED.details,
                 updated_at=clock_timestamp()""",
            (scope_key, len(rows), run_id, _canonical({"scope_sha256": scope_sha})),
        )
        stats = {
            "scope_key": scope_key, "scope_sha256": scope_sha, "asin_count": len(rows),
            "baseline_adopted_asins": baseline_adopted,
            "baseline_adopted_movies": baseline_movies,
            "remaining_asins": int(frozen_scope["asin_count"]) - baseline_adopted,
            "stage6_scope_coverage": sum(bool(row["stage6_scanned"]) for row in rows),
            "collection_member_asins": conn.execute(
                """SELECT count(DISTINCT pe.asin) FROM target.product_movie pm
                   JOIN target.product_edition pe USING(product_id)
                   WHERE pm.relationship_type='collection_member'
                     AND pm.mapping_status IN ('verified','manual','predicted')"""
            ).fetchone()["count"],
        }
        _finish(conn, run_id, stats)
        conn.commit()

    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow([
            "asin", "product_title_raw", "product_source_record_id", "product_source_record_number",
            "baseline_adopted", "baseline_movie_ids", "stage4_scanned", "stage6_scanned",
            "stage6_candidate_count",
        ])
        for row in rows:
            writer.writerow([
                row["asin"], row["product_title_raw"], row["product_source_record_id"],
                row["product_source_record_number"], str(row["baseline_adopted"]).lower(),
                ",".join(str(v) for v in row["baseline_movie_ids"]),
                str(row["stage4_scanned"]).lower(), str(row["stage6_scanned"]).lower(),
                row["stage6_candidate_count"],
            ])
    stats.update({"run_id": run_id, "output": str(output), "output_sha256": hashlib.sha256(output.read_bytes()).hexdigest()})
    return stats


def _stage4_decisions(conn: psycopg.Connection[Any]) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """SELECT u.*,r.candidate_pool
           FROM integration.unified_development_movie_mapping_decision u
           JOIN integration.movie_match_candidate_result r
             ON r.asin=u.asin AND r.rule_version=u.rule_version"""
    ).fetchall()
    return {row["asin"]: dict(row) for row in rows}


def _candidate_rows(conn: psycopg.Connection[Any], scope_key: str) -> list[dict[str, Any]]:
    return list(conn.execute(
        """SELECT c.*,sp.product_title_raw,sp.product_source_record_id,
                  pm.description_raw,pm.brand_raw,pm.source_date_raw
           FROM integration.stage6_movie_candidate_cache c
           JOIN meta.p2_movie_scope s ON s.snapshot_id=c.snapshot_id
           JOIN meta.p2_movie_scope_product sp ON sp.scope_key=s.scope_key AND sp.asin=c.asin
           JOIN LATERAL (
             SELECT description_raw,brand_raw,source_date_raw
             FROM staging.product_metadata_record
             WHERE asin=c.asin AND source_name=%s AND source_version=%s
             ORDER BY product_metadata_id DESC LIMIT 1
           ) pm ON true
           WHERE s.scope_key=%s AND c.scope_key=s.candidate_scope_key
             AND c.shortlist_rank IS NOT NULL
           ORDER BY c.asin,c.shortlist_rank,c.tconst""",
        (SOURCE_NAME, SOURCE_VERSION, scope_key),
    ).fetchall())


def _credit_evidence(
    conn: psycopg.Connection[Any], snapshot_id: int, tconsts: list[str]
) -> dict[str, list[dict[str, Any]]]:
    if not tconsts:
        return {}
    rows = conn.execute(
        """WITH credits AS (
             SELECT p.tconst,p.nconst,p.category AS imdb_role,p.source_record_id AS credit_source_record_id,
                    p.ordering,n.primary_name,n.source_record_id AS name_source_record_id
             FROM staging.imdb_title_principal p
             JOIN staging.imdb_name_basics n USING(snapshot_id,nconst)
             WHERE p.snapshot_id=%s AND p.tconst=ANY(%s)
             UNION ALL
             SELECT c.tconst,n.nconst,'director',c.source_record_id,NULL,n.primary_name,n.source_record_id
             FROM staging.imdb_title_crew c
             CROSS JOIN LATERAL unnest(c.director_nconsts) d(nconst)
             JOIN staging.imdb_name_basics n ON n.snapshot_id=c.snapshot_id AND n.nconst=d.nconst
             WHERE c.snapshot_id=%s AND c.tconst=ANY(%s)
             UNION ALL
             SELECT c.tconst,n.nconst,'writer',c.source_record_id,NULL,n.primary_name,n.source_record_id
             FROM staging.imdb_title_crew c
             CROSS JOIN LATERAL unnest(c.writer_nconsts) w(nconst)
             JOIN staging.imdb_name_basics n ON n.snapshot_id=c.snapshot_id AND n.nconst=w.nconst
             WHERE c.snapshot_id=%s AND c.tconst=ANY(%s)
           ) SELECT DISTINCT * FROM credits ORDER BY tconst,ordering NULLS LAST,nconst""",
        (snapshot_id, tconsts, snapshot_id, tconsts, snapshot_id, tconsts),
    ).fetchall()
    result: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(row["tconst"], []).append(dict(row))
    return result


def _matched_title(row: dict[str, Any]) -> tuple[str, int]:
    values = row["matched_titles"] if isinstance(row["matched_titles"], list) else []
    values = sorted(
        values,
        key=lambda item: (-float(item.get("similarity") or 0), int(item.get("record_number") or 10**18)),
    )
    if values:
        return str(values[0].get("source_title_raw") or row["primary_title"] or ""), int(values[0]["source_record_id"])
    return str(row["primary_title"] or row["original_title"] or ""), int(row["basics_source_record_id"])


def _insert_immutable_candidate(
    conn: psycopg.Connection[Any], *, scope_key: str, asin: str, tconst: str,
    shortlist_rank: int, product_source_record_id: int, work_source_record_id: int,
    evaluation: dict[str, Any], evidence_sha: str, run_id: int,
) -> bool:
    inserted = conn.execute(
        """INSERT INTO integration.p2_movie_candidate_evaluation(
             scope_key,asin,tconst,rule_version,shortlist_rank,product_source_record_id,
             work_source_record_id,title_evidence,type_evidence,year_evidence,
             person_evidence,evaluation_payload,score_breakdown,evidence_score,
             hard_conflicts,confirmation_paths,auto_confirmation_eligible,evidence_sha256,run_id)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,
                   %s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s)
           ON CONFLICT DO NOTHING RETURNING 1""",
        (scope_key, asin, tconst, RULE_VERSION, shortlist_rank, product_source_record_id,
         work_source_record_id, _canonical(evaluation["title_evidence"]),
         _canonical(evaluation["type_evidence"]), _canonical(evaluation["year_evidence"]),
         _canonical(evaluation["person_evidence"]), _canonical(evaluation),
         _canonical(evaluation["score_breakdown"]), evaluation["evidence_score"],
         evaluation["hard_conflicts"], evaluation["confirmation_paths"],
         evaluation["auto_confirmation_eligible"], evidence_sha, run_id),
    ).fetchone()
    if inserted:
        return True
    existing = conn.execute(
        """SELECT evidence_sha256 FROM integration.p2_movie_candidate_evaluation
           WHERE scope_key=%s AND asin=%s AND tconst=%s AND rule_version=%s""",
        (scope_key, asin, tconst, RULE_VERSION),
    ).fetchone()
    if not existing or existing["evidence_sha256"] != evidence_sha:
        raise ValueError(f"P2 candidate content conflict: {asin}/{tconst}/{RULE_VERSION}")
    return False


def _insert_immutable_decision(
    conn: psycopg.Connection[Any], *, scope_key: str, asin: str, status: str,
    selected_tconst: str | None, reason_code: str, reason: str,
    evidence: dict[str, Any], run_id: int,
) -> bool:
    digest = _sha(evidence)
    inserted = conn.execute(
        """INSERT INTO integration.p2_movie_decision(
             scope_key,asin,rule_version,decision_status,selected_tconst,reason_code,
             decision_reason,evidence,evidence_sha256,run_id)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)
           ON CONFLICT DO NOTHING RETURNING 1""",
        (scope_key, asin, RULE_VERSION, status, selected_tconst, reason_code,
         reason, _canonical(evidence), digest, run_id),
    ).fetchone()
    if inserted:
        return True
    existing = conn.execute(
        """SELECT evidence_sha256 FROM integration.p2_movie_decision
           WHERE scope_key=%s AND asin=%s AND rule_version=%s""",
        (scope_key, asin, RULE_VERSION),
    ).fetchone()
    if not existing or existing["evidence_sha256"] != digest:
        raise ValueError(f"P2 decision content conflict: {asin}/{RULE_VERSION}")
    return False


def evaluate_existing_candidates(
    database_url: str, *, scope_key: str, batch_id: str,
) -> dict[str, Any]:
    run_id = 0
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as conn:
            scope = conn.execute("SELECT * FROM meta.p2_movie_scope WHERE scope_key=%s", (scope_key,)).fetchone()
            if not scope:
                raise ValueError(f"P2 scope is not registered: {scope_key}")
            run_id = _run(conn, "p2_movie_evaluate", batch_id, scope["scope_sha256"])
            conn.execute(
                """INSERT INTO meta.p2_movie_progress(scope_key,phase,status,run_id)
                   VALUES (%s,'evaluate','running',%s)
                   ON CONFLICT (scope_key,phase) DO UPDATE SET status='running',run_id=EXCLUDED.run_id,
                     updated_at=clock_timestamp()""", (scope_key, run_id),
            )
            products = {row["asin"]: dict(row) for row in conn.execute(
                "SELECT * FROM meta.p2_movie_scope_product WHERE scope_key=%s ORDER BY asin", (scope_key,)
            ).fetchall()}
            stage4 = _stage4_decisions(conn)
            candidate_rows = _candidate_rows(conn, scope_key)
            by_asin: dict[str, list[dict[str, Any]]] = {}
            for row in candidate_rows:
                by_asin.setdefault(row["asin"], []).append(dict(row))
            credits = _credit_evidence(conn, int(scope["snapshot_id"]), sorted({r["tconst"] for r in candidate_rows}))
            inserted_candidates = 0
            inserted_decisions = 0
            decision_counts: dict[str, int] = {}
            for index, (asin, product) in enumerate(products.items(), start=1):
                if product["baseline_adopted"]:
                    evidence = {"baseline_movie_ids": product["baseline_movie_ids"], "basis": "current adopted single_work mapping at scope registration"}
                    status, selected, code, reason = "existing_adopted", None, "baseline_adopted", "ASIN already had an adopted single_work relationship when the fixed 433-ASIN scope was registered."
                elif asin in stage4:
                    saved = stage4[asin]
                    pool = saved["candidate_pool"] if isinstance(saved["candidate_pool"], list) else []
                    match_status = saved["match_status"]
                    if saved["human_not_single_movie"] or saved["human_no_match_in_pool"]:
                        status, code = "explicit_mismatch", "saved_human_exclusion"
                    elif match_status == "no_candidate" or not pool:
                        status, code = "no_candidate", "stage4_empty_candidate_pool"
                    elif match_status == "no_suitable_candidate":
                        status, code = "explicit_mismatch", "stage4_candidates_conflicted"
                    else:
                        status, code = "needs_review", "stage4_frozen_review_queue"
                    selected = None
                    reason = "Frozen Stage 4 decision is preserved; P2 does not recalculate or overwrite the development sample."
                    evidence = {
                        "stage4_rule_version": saved["rule_version"], "stage4_match_status": match_status,
                        "adoption_status": saved["adoption_status"], "review_reason_code": saved["review_reason_code"],
                        "candidate_count": len(pool), "human_not_single_movie": saved["human_not_single_movie"],
                        "human_no_match_in_pool": saved["human_no_match_in_pool"],
                    }
                elif by_asin.get(asin):
                    evaluations: list[dict[str, Any]] = []
                    for candidate_row in by_asin[asin]:
                        matched_title, matched_title_source_id = _matched_title(candidate_row)
                        candidate_credits = credits.get(candidate_row["tconst"], [])
                        candidate = {
                            "id": candidate_row["tconst"], "title": matched_title,
                            "year": candidate_row["start_year"],
                            "type": _normalize_title_type(candidate_row["title_type"] or ""),
                            "rank": candidate_row["shortlist_rank"],
                            "principals": list(dict.fromkeys(c["primary_name"] for c in candidate_credits)),
                            "primary_title": candidate_row["primary_title"],
                            "original_title": candidate_row["original_title"],
                            "matched_title_source_record_id": matched_title_source_id,
                            "basics_source_record_id": candidate_row["basics_source_record_id"],
                        }
                        descriptions = candidate_row["description_raw"] if isinstance(candidate_row["description_raw"], list) else []
                        product_type = classify_product_type(product["product_title_raw"], [str(v) for v in descriptions])
                        evaluation = evaluate_candidate(
                            candidate, product_title=product["product_title_raw"], product_type=product_type,
                            brand=candidate_row["brand_raw"], descriptions=[str(v) for v in descriptions],
                            source_record_id=product["product_source_record_id"], reliable_product_years=[],
                        )
                        credit_by_name: dict[str, list[dict[str, Any]]] = {}
                        for credit in candidate_credits:
                            credit_by_name.setdefault(credit["primary_name"], []).append(credit)
                        for person in evaluation["person_evidence"]:
                            person["candidate_credit_sources"] = credit_by_name.get(person["name"], [])
                        evaluation["candidate_source_chain"] = {
                            "product_source_record_id": product["product_source_record_id"],
                            "matched_title_source_record_id": matched_title_source_id,
                            "basics_source_record_id": candidate_row["basics_source_record_id"],
                            "snapshot_version": SNAPSHOT_VERSION,
                        }
                        digest = _sha(evaluation)
                        inserted_candidates += int(_insert_immutable_candidate(
                            conn, scope_key=scope_key, asin=asin, tconst=candidate_row["tconst"],
                            shortlist_rank=candidate_row["shortlist_rank"],
                            product_source_record_id=product["product_source_record_id"],
                            work_source_record_id=candidate_row["basics_source_record_id"],
                            evaluation=evaluation, evidence_sha=digest, run_id=run_id,
                        ))
                        evaluations.append(evaluation)
                    result = decide(evaluations)
                    selected = None if not result["best"] else result["best"]["id"]
                    status = {
                        "rule_verified": "rule_verified_pending",
                        "needs_review_ambiguous": "needs_review",
                        "needs_review_insufficient": "needs_review",
                        "no_suitable_candidate": "explicit_mismatch",
                        "no_candidate": "no_candidate",
                    }[result["status"]]
                    code = result["reason_code"]
                    reason = result["question"]
                    evidence = {
                        "algorithm_status": result["status"], "candidate_count": len(evaluations),
                        "selected_tconst": selected,
                        "ordered_candidates": sorted(
                            [{"tconst": e["id"], "score": e["evidence_score"],
                              "paths": e["confirmation_paths"], "hard_conflicts": e["hard_conflicts"],
                              "title_level": e["title_evidence"]["level"]} for e in evaluations],
                            key=lambda item: (-bool(item["paths"]), -item["score"], item["tconst"]),
                        ),
                    }
                else:
                    status, selected, code = "no_candidate", None, "no_cached_candidate"
                    reason = "No cached Stage 4 or Stage 6 candidate exists for this fixed-scope ASIN."
                    evidence = {"stage4_scanned": product["stage4_scanned"], "stage6_scanned": product["stage6_scanned"]}
                inserted_decisions += int(_insert_immutable_decision(
                    conn, scope_key=scope_key, asin=asin, status=status, selected_tconst=selected,
                    reason_code=code, reason=reason, evidence=evidence, run_id=run_id,
                ))
                decision_counts[status] = decision_counts.get(status, 0) + 1
                if index % 100 == 0:
                    conn.execute(
                        """UPDATE meta.p2_movie_progress SET committed_asin=%s,processed_count=%s,
                             details=%s::jsonb,updated_at=clock_timestamp()
                           WHERE scope_key=%s AND phase='evaluate'""",
                        (asin, index, _canonical(decision_counts), scope_key),
                    )
            if sum(decision_counts.values()) != 433:
                raise ValueError(f"P2 decisions do not balance to 433: {decision_counts}")
            conn.execute(
                """UPDATE meta.p2_movie_progress SET committed_asin=%s,processed_count=%s,status='completed',
                     details=%s::jsonb,updated_at=clock_timestamp()
                   WHERE scope_key=%s AND phase='evaluate'""",
                (max(products), len(products), _canonical(decision_counts), scope_key),
            )
            has_adoption = conn.execute(
                """SELECT EXISTS(SELECT 1 FROM integration.p2_movie_decision
                   WHERE scope_key=%s AND rule_version=%s AND decision_status='newly_adopted')""",
                (scope_key, RULE_VERSION),
            ).fetchone()["exists"]
            conn.execute(
                "UPDATE meta.p2_movie_scope SET status=%s,updated_at=clock_timestamp() WHERE scope_key=%s",
                ("bounded_applied" if has_adoption else "evaluated", scope_key),
            )
            stats = {"scope_key": scope_key, "products": len(products), "candidate_rows": len(candidate_rows),
                     "candidate_evaluations_inserted": inserted_candidates,
                     "decisions_inserted": inserted_decisions, "decision_counts": decision_counts}
            _finish(conn, run_id, stats)
            conn.commit()
            return {"run_id": run_id, **stats}
    except Exception:
        if run_id:
            _fail(database_url, run_id)
        raise


def apply_verified(
    database_url: str, *, scope_key: str, batch_id: str, limit: int = 5,
) -> dict[str, Any]:
    if limit < 1:
        raise ValueError("limit must be at least 1 for a bounded adoption batch")
    run_id = 0
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as conn:
            scope = conn.execute("SELECT * FROM meta.p2_movie_scope WHERE scope_key=%s FOR UPDATE", (scope_key,)).fetchone()
            if not scope or scope["status"] not in {"evaluated", "bounded_applied"}:
                raise ValueError("P2 scope must be evaluated before adoption")
            replay = _completed_run(conn, "p2_movie_apply", batch_id, scope["scope_sha256"])
            if replay is not None:
                return replay
            run_id = _run(conn, "p2_movie_apply", batch_id, scope["scope_sha256"])
            rows = conn.execute(
                """SELECT d.*,sp.product_id,sp.product_source_record_id,e.work_source_record_id,
                          e.evaluation_payload,e.evidence_score,e.confirmation_paths,e.hard_conflicts,
                          e.evidence_sha256,
                          m.movie_id,m.stable_key
                   FROM integration.p2_movie_decision d
                   JOIN meta.p2_movie_scope_product sp USING(scope_key,asin)
                   JOIN integration.p2_movie_candidate_evaluation e
                     ON e.scope_key=d.scope_key AND e.asin=d.asin AND e.tconst=d.selected_tconst
                    AND e.rule_version=d.rule_version
                   JOIN target.movie m ON m.stable_key='imdb:'||d.selected_tconst
                   WHERE d.scope_key=%s AND d.rule_version=%s
                     AND d.decision_status='rule_verified_pending'
                   ORDER BY d.asin LIMIT %s FOR UPDATE OF d""",
                (scope_key, RULE_VERSION, limit),
            ).fetchall()
            applied = 0
            links = 0
            for row in rows:
                if row["hard_conflicts"] or not row["confirmation_paths"]:
                    raise ValueError(f"ineligible P2 adoption reached apply phase: {row['asin']}")
                existing = conn.execute(
                    """SELECT m.stable_key FROM target.product_movie pm JOIN target.movie m USING(movie_id)
                       WHERE pm.product_id=%s AND pm.relationship_type='single_work'
                         AND pm.mapping_status IN ('verified','manual','predicted')""",
                    (row["product_id"],),
                ).fetchall()
                if existing:
                    raise ValueError(f"ASIN already has a current single_work mapping: {row['asin']} -> {existing}")
                evaluation = row["evaluation_payload"]
                source_ids = {
                    int(row["product_source_record_id"]), int(row["work_source_record_id"]),
                    int(evaluation["candidate_source_chain"]["matched_title_source_record_id"]),
                }
                for person in evaluation.get("person_evidence", []):
                    for credit in person.get("candidate_credit_sources", []):
                        source_ids.add(int(credit["credit_source_record_id"]))
                        source_ids.add(int(credit["name_source_record_id"]))
                decision_evidence = {
                    "product_source_record_id": row["product_source_record_id"],
                    "title_source_record_id": evaluation["candidate_source_chain"]["matched_title_source_record_id"],
                    "work": {"tconst": row["selected_tconst"], "source_record_id": row["work_source_record_id"]},
                    "confirmation_paths": row["confirmation_paths"], "evidence_score": row["evidence_score"],
                    "source_record_ids": sorted(source_ids), "candidate_evaluation_sha256": row["evidence_sha256"],
                    "selection_reason": row["decision_reason"], "sample_role": "p2_expansion",
                }
                decision_id = conn.execute(
                    """INSERT INTO integration.merge_decision(
                         entity_type,left_entity_key,right_entity_key,decision,decision_origin,
                         rule_version,confidence,evidence,run_id)
                       VALUES ('product_movie',%s,%s,'merge','rule',%s,NULL,%s::jsonb,%s)
                       ON CONFLICT (entity_type,left_entity_key,right_entity_key,rule_version) DO UPDATE SET
                         evidence=EXCLUDED.evidence,run_id=EXCLUDED.run_id
                       RETURNING merge_decision_id""",
                    (row["asin"], row["stable_key"], RULE_VERSION, _canonical(decision_evidence), run_id),
                ).fetchone()["merge_decision_id"]
                conn.execute(
                    """INSERT INTO target.product_movie(product_id,movie_id,relationship_type,decision_id,mapping_status)
                       VALUES (%s,%s,'single_work',%s,'verified')
                       ON CONFLICT (product_id,movie_id,relationship_type) DO UPDATE SET
                         decision_id=EXCLUDED.decision_id,mapping_status=EXCLUDED.mapping_status""",
                    (row["product_id"], row["movie_id"], decision_id),
                )
                conn.execute("UPDATE target.movie SET status='verified' WHERE movie_id=%s AND status='candidate'", (row["movie_id"],))
                relationship_key = f"{row['product_id']}:{row['movie_id']}:single_work"
                for source_id in sorted(source_ids):
                    links += conn.execute(
                        """INSERT INTO integration.relationship_source_link(
                             relationship_type,relationship_key,source_record_id,evidence_role,field_names,
                             rule_version,selection_reason)
                           VALUES ('product_movie',%s,%s,'p2_multi_fact_source',
                                   ARRAY['product_id','movie_id','relationship_type','mapping_status'],%s,%s)
                           ON CONFLICT DO NOTHING""",
                        (relationship_key, source_id, RULE_VERSION, row["decision_reason"]),
                    ).rowcount
                conn.execute(
                    """UPDATE integration.p2_movie_decision SET decision_status='newly_adopted',run_id=%s,
                         decided_at=clock_timestamp() WHERE scope_key=%s AND asin=%s AND rule_version=%s""",
                    (run_id, scope_key, row["asin"], RULE_VERSION),
                )
                applied += 1
            remaining = conn.execute(
                """SELECT count(*) FROM integration.p2_movie_decision
                   WHERE scope_key=%s AND rule_version=%s AND decision_status='rule_verified_pending'""",
                (scope_key, RULE_VERSION),
            ).fetchone()["count"]
            reconciliation = {
                row["reconciliation_status"]: row["count"] for row in conn.execute(
                    """SELECT reconciliation_status,count(*) FROM integration.p2_movie_reconciliation
                       WHERE scope_key=%s GROUP BY reconciliation_status""", (scope_key,)
                ).fetchall()
            }
            if sum(reconciliation.values()) != 433:
                raise ValueError(f"P2 reconciliation does not balance to 433: {reconciliation}")
            conflicts = conn.execute(
                """SELECT count(*) FROM (
                     SELECT pe.asin FROM target.product_movie pm JOIN target.product_edition pe USING(product_id)
                     JOIN meta.p2_movie_scope_product sp ON sp.scope_key=%s AND sp.asin=pe.asin
                     WHERE pm.relationship_type='single_work' AND pm.mapping_status IN ('verified','manual','predicted')
                     GROUP BY pe.asin HAVING count(DISTINCT pm.movie_id)>1
                   ) q""", (scope_key,)
            ).fetchone()["count"]
            if conflicts:
                raise ValueError(f"multiple current single_work mappings detected for {conflicts} ASINs")
            status = "complete" if remaining == 0 else "bounded_applied"
            conn.execute("UPDATE meta.p2_movie_scope SET status=%s,updated_at=clock_timestamp() WHERE scope_key=%s", (status, scope_key))
            conn.execute(
                """INSERT INTO meta.p2_movie_progress(scope_key,phase,committed_asin,processed_count,status,run_id,details)
                   VALUES (%s,'apply',%s,%s,'completed',%s,%s::jsonb)
                   ON CONFLICT (scope_key,phase) DO UPDATE SET committed_asin=EXCLUDED.committed_asin,
                     processed_count=meta.p2_movie_progress.processed_count+EXCLUDED.processed_count,
                     status='completed',run_id=EXCLUDED.run_id,details=EXCLUDED.details,
                     updated_at=clock_timestamp()""",
                (scope_key, rows[-1]["asin"] if rows else None, applied, run_id,
                 _canonical({"remaining_verified_pending": remaining, "reconciliation": reconciliation})),
            )
            stats = {"scope_key": scope_key, "applied": applied, "relationship_source_links_inserted": links,
                     "remaining_verified_pending": remaining, "single_work_conflict_asins": conflicts,
                     "reconciliation": reconciliation}
            _finish(conn, run_id, stats)
            conn.commit()
            return {"run_id": run_id, **stats}
    except Exception:
        if run_id:
            _fail(database_url, run_id)
        raise


def status(database_url: str, *, scope_key: str) -> dict[str, Any]:
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        scope = conn.execute("SELECT * FROM meta.p2_movie_scope WHERE scope_key=%s", (scope_key,)).fetchone()
        if not scope:
            return {"scope_key": scope_key, "status": "not_registered"}
        counts = {row["reconciliation_status"]: row["count"] for row in conn.execute(
            """SELECT reconciliation_status,count(*) FROM integration.p2_movie_reconciliation
               WHERE scope_key=%s GROUP BY reconciliation_status""", (scope_key,)
        ).fetchall()}
        decisions = {row["decision_status"]: row["count"] for row in conn.execute(
            """SELECT decision_status,count(*) FROM integration.p2_movie_decision
               WHERE scope_key=%s AND rule_version=%s GROUP BY decision_status""",
            (scope_key, RULE_VERSION),
        ).fetchall()}
        adopted = conn.execute(
            """SELECT count(DISTINCT pe.asin) asin_count,count(DISTINCT pm.movie_id) movie_count
               FROM target.product_movie pm JOIN target.product_edition pe USING(product_id)
               JOIN meta.p2_movie_scope_product sp ON sp.scope_key=%s AND sp.asin=pe.asin
               WHERE pm.relationship_type='single_work' AND pm.mapping_status IN ('verified','manual','predicted')""",
            (scope_key,),
        ).fetchone()
        progress = [dict(row) for row in conn.execute(
            "SELECT * FROM meta.p2_movie_progress WHERE scope_key=%s ORDER BY phase", (scope_key,)
        ).fetchall()]
    total = sum(counts.values())
    return {
        "scope": dict(scope), "reconciliation": counts, "decision_status": decisions,
        "reconciliation_total": total, "adopted_asins_current": adopted["asin_count"],
        "adopted_movies_current": adopted["movie_count"],
        "adoption_coverage_percent": round(100 * adopted["asin_count"] / 433, 4),
        "progress": progress,
    }


def export_reconciliation(database_url: str, *, scope_key: str, output: Path) -> dict[str, Any]:
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            """SELECT * FROM integration.p2_movie_reconciliation
               WHERE scope_key=%s ORDER BY asin""", (scope_key,)
        ).fetchall()
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow([
            "asin", "product_title_raw", "reconciliation_status", "selected_tconst",
            "reason_code", "decision_reason", "stage4_scanned", "stage6_scanned",
            "stage6_candidate_count", "rule_version",
        ])
        for row in rows:
            writer.writerow([row[name] for name in (
                "asin", "product_title_raw", "reconciliation_status", "selected_tconst",
                "reason_code", "decision_reason", "stage4_scanned", "stage6_scanned",
                "stage6_candidate_count", "rule_version",
            )])
    return {"scope_key": scope_key, "rows": len(rows), "output": str(output),
            "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
