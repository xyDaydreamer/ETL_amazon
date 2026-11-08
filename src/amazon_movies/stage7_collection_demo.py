from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg.rows import dict_row


RULE_VERSION = "stage7-john-wayne-collection-v1"
ASIN = "B001PO54OM"
MEMBERS = [
    ("tt0068421", "The Cowboys", 1972),
    ("tt0040369", "Fort Apache", 1948),
    ("tt0053221", "Rio Bravo", 1959),
    ("tt0049730", "The Searchers", 1956),
]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def materialize_collection_demo(database_url: str) -> dict[str, Any]:
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        product = conn.execute(
            """SELECT pm.asin,pm.title_raw,pm.description_raw,pm.categories_raw,
                      pm.source_record_id,pm.source_record_number,sr.content_sha256,sr.field_locator,
                      pe.product_id
               FROM staging.product_metadata_record pm
               JOIN raw.source_record sr USING(source_record_id)
               JOIN target.product_edition pe USING(asin)
               WHERE pm.asin=%s ORDER BY pm.product_metadata_id DESC LIMIT 1""",
            (ASIN,),
        ).fetchone()
        if not product:
            raise ValueError(f"collection product evidence missing: {ASIN}")
        description = " ".join(str(value) for value in product["description_raw"])
        review_count = conn.execute(
            "SELECT count(*) AS count FROM staging.current_review_parse_result WHERE asin=%s",
            (ASIN,),
        ).fetchone()["count"]
        if review_count <= 0:
            raise ValueError("collection demo must have non-zero real review evidence")

        run_id = conn.execute(
            """INSERT INTO meta.etl_run(pipeline_name,batch_id,input_version,rule_version,status,stats)
               VALUES ('stage7_collection_demo',%s,'imdb-noncommercial-2026-11-05',%s,'running','{}'::jsonb)
               ON CONFLICT (pipeline_name,batch_id,input_version,rule_version) DO UPDATE SET
                 status='running',finished_at=NULL
               RETURNING run_id""",
            (RULE_VERSION, RULE_VERSION),
        ).fetchone()["run_id"]

        member_results = []
        for tconst, expected_title, expected_year in MEMBERS:
            work = conn.execute(
                """SELECT b.tconst,b.primary_title,b.start_year,b.title_type,b.source_record_id,
                          sr.record_number,sr.content_sha256,sr.field_locator,m.movie_id,m.stable_key
                   FROM staging.imdb_title_basics b
                   JOIN raw.source_record sr ON sr.source_record_id=b.source_record_id
                   JOIN target.movie m ON m.stable_key='imdb:'||b.tconst
                   WHERE b.snapshot_id=1 AND b.tconst=%s""",
                (tconst,),
            ).fetchone()
            if not work or work["primary_title"] != expected_title or work["start_year"] != expected_year:
                raise ValueError(f"fixed IMDb work mismatch: {tconst}")
            if expected_title.casefold() not in description.casefold():
                raise ValueError(f"product description does not explicitly list member title: {expected_title}")
            people = conn.execute(
                """SELECT DISTINCT n.nconst,n.primary_name,p.category,p.source_record_id
                   FROM staging.imdb_title_principal p
                   JOIN staging.imdb_name_basics n USING(snapshot_id,nconst)
                   WHERE p.snapshot_id=1 AND p.tconst=%s
                     AND position(lower(n.primary_name) in lower(%s)) > 0
                   ORDER BY n.primary_name""",
                (tconst, description),
            ).fetchall()
            if not people:
                raise ValueError(f"no product-to-work principal cross evidence: {tconst}")
            evidence = {
                "sample_role": "fixed_offline_acceptance",
                "asin": ASIN,
                "product_title_raw": product["title_raw"],
                "product_source_record_id": product["source_record_id"],
                "product_source_record_number": product["source_record_number"],
                "product_content_sha256": product["content_sha256"],
                "product_field_locator": product["field_locator"],
                "member": {
                    "tconst": tconst, "title": work["primary_title"], "year": work["start_year"],
                    "title_type": work["title_type"], "source_record_id": work["source_record_id"],
                    "record_number": work["record_number"], "content_sha256": work["content_sha256"],
                    "field_locator": work["field_locator"],
                },
                "matching_principals": [dict(person) for person in people],
                "decision_reason": (
                    "UCSD explicitly lists this work as one disc/member of the four-film product; "
                    "the fixed IMDb snapshot agrees on title/year/type and at least one principal named in the product description."
                ),
                "review_policy": "reviews remain on the collection product and are not copied to member movies",
            }
            decision_id = conn.execute(
                """INSERT INTO integration.merge_decision(
                       entity_type,left_entity_key,right_entity_key,decision,decision_origin,
                       rule_version,confidence,evidence,run_id)
                   VALUES ('product_movie',%s,%s,'merge','rule',%s,NULL,%s::jsonb,%s)
                   ON CONFLICT (entity_type,left_entity_key,right_entity_key,rule_version) DO UPDATE SET
                     evidence=EXCLUDED.evidence,run_id=EXCLUDED.run_id
                   RETURNING merge_decision_id""",
                (ASIN, work["stable_key"], RULE_VERSION, _json(evidence), run_id),
            ).fetchone()["merge_decision_id"]
            conn.execute(
                """INSERT INTO target.product_movie(product_id,movie_id,relationship_type,decision_id,mapping_status)
                   VALUES (%s,%s,'collection_member',%s,'verified')
                   ON CONFLICT (product_id,movie_id,relationship_type) DO UPDATE SET
                     decision_id=EXCLUDED.decision_id,mapping_status=EXCLUDED.mapping_status""",
                (product["product_id"], work["movie_id"], decision_id),
            )
            member_results.append({
                "tconst": tconst, "title": work["primary_title"], "year": work["start_year"],
                "title_source_record_id": work["source_record_id"],
                "matching_principals": [person["primary_name"] for person in people],
            })

        attribution = conn.execute(
            """SELECT movie_id,relationship_type,product_source_review_count,
                      product_logical_review_count,attributed_logical_review_count,attribution_reason
               FROM target.product_movie_review_attribution WHERE asin=%s ORDER BY movie_id""",
            (ASIN,),
        ).fetchall()
        if len(attribution) != len(MEMBERS):
            raise ValueError(f"expected {len(MEMBERS)} collection-member policy rows, got {len(attribution)}")
        if any(row["attributed_logical_review_count"] != 0 for row in attribution):
            raise ValueError("collection reviews were incorrectly attributed to member movies")
        stats = {
            "asin": ASIN, "product_source_record_id": product["source_record_id"],
            "product_review_count": review_count, "collection_member_count": len(member_results),
            "member_reviews_attributed": sum(row["attributed_logical_review_count"] for row in attribution),
            "members": member_results, "attribution": [dict(row) for row in attribution],
        }
        conn.execute(
            "UPDATE meta.etl_run SET status='succeeded',finished_at=clock_timestamp(),stats=%s::jsonb WHERE run_id=%s",
            (_json(stats), run_id),
        )
        return {"run_id": run_id, "rule_version": RULE_VERSION, **stats}

