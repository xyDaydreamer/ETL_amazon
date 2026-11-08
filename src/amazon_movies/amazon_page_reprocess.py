"""Idempotently link already saved valid Amazon HTML fields to provenance."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

import psycopg

from amazon_movies.stage3_crawler import assess_product_response
from amazon_movies.stage3_pipeline import _finish, _json, _run


RULE = "amazon-saved-html-fields-v1"


def reprocess(database_url: str) -> dict[str, int]:
    checked = valid = inserted = invalid = 0
    with psycopg.connect(database_url) as conn:
        run_id = _run(conn, "amazon_saved_page_field_extraction", "saved-valid-pages", RULE)
        rows = conn.execute(
            """SELECT DISTINCT ON (a.asin,s.source_record_id)
                      a.asin,a.http_status,a.final_url,s.source_record_id,s.snapshot_path,s.content_sha256
               FROM staging.product_harvest_attempt a
               JOIN raw.source_record s ON s.source_record_id=a.source_record_id
               WHERE a.outcome='success' AND a.cache_hit IS FALSE
               ORDER BY a.asin,s.source_record_id,a.finished_at"""
        ).fetchall()
        for asin, status, final_url, source_id, location, digest in rows:
            checked += 1
            if not location or not Path(location).is_file():
                invalid += 1
                continue
            body = Path(location).read_bytes()
            if hashlib.sha256(body).hexdigest() != digest:
                invalid += 1
                continue
            result = assess_product_response(
                target_asin=asin, status=status, final_url=final_url, body=body)
            if result.outcome != "success" or not result.product_title:
                invalid += 1
                continue
            valid += 1
            for field, value, locator in (
                ("title", result.product_title,
                 {"selectors": ["#productTitle", "#title span", "meta[property=og:title]", "jsonld:Product.name"]}),
                ("asin", asin,
                 {"selectors": ["input[name=ASIN]", "input[id=ASIN]", "link[rel=canonical]", "jsonld:Product.sku"]}),
            ):
                row = conn.execute(
                    """INSERT INTO integration.field_assertion(
                         entity_type,entity_key,field_name,raw_value,normalized_value,
                         value_semantics,source_record_id,field_locator,extraction_rule_version,
                         confidence,quality_status,run_id)
                       SELECT 'product',%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s::jsonb,%s,1.0,'observed',%s
                       WHERE NOT EXISTS (
                         SELECT 1 FROM integration.field_assertion WHERE entity_type='product'
                         AND entity_key=%s AND field_name=%s AND source_record_id=%s
                         AND extraction_rule_version=%s) RETURNING assertion_id""",
                    (asin, field, _json(value), _json(value), "amazon_live_html_observation",
                     source_id, _json({"source_record_id": source_id, "snapshot_sha256": digest,
                                       "candidate_locators": locator["selectors"]}), RULE, run_id,
                     asin, field, source_id, RULE),
                ).fetchone()
                inserted += row is not None
        summary = {"checked_saved_responses": checked, "valid": valid,
                   "invalid_or_missing": invalid, "assertions_inserted": inserted}
        _finish(conn, run_id, summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("AIRFLOW_CONN_AMAZON_MOVIES_DB"))
    args = parser.parse_args()
    if not args.database_url:
        parser.error("database URL missing")
    print(json.dumps(reprocess(args.database_url), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
