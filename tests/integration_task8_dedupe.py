from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import psycopg

from amazon_movies.logical_review_dedupe import deduplicate_reviews, dedupe_status


def seed(url: str, records: int = 10_000) -> dict[str, int]:
    with psycopg.connect(url) as conn:
        conn.execute("TRUNCATE target.review_mapping,target.logical_review,meta.logical_review_progress,meta.logical_review_release CASCADE")
        conn.execute("TRUNCATE staging.review_parse_result,staging.review_record,raw.source_record,target.reviewer,target.product_edition,meta.source_file,meta.etl_run CASCADE")
        run_id = conn.execute("""INSERT INTO meta.etl_run(
            pipeline_name,batch_id,input_version,rule_version,status,scope_kind,is_full_source
        ) VALUES ('task8_seed','task8-seed','constructed-v1','constructed-v1','succeeded','bounded_test',false)
        RETURNING run_id""").fetchone()[0]
        source_file_id = conn.execute("""INSERT INTO meta.source_file(
            source_name,source_uri,sha256,size_bytes,parser_version,hash_verification_method,gzip_integrity_status
        ) VALUES ('task8-constructed','constructed://task8',repeat('f',64),0,'snap-movies-review-v3',
                  'constructed','verified_complete') RETURNING source_file_id""").fetchone()[0]
        conn.execute("UPDATE meta.parser_release SET is_current=false WHERE is_current")
        conn.execute("""INSERT INTO meta.parser_release(
            parser_version,is_current,description,coverage_kind,coverage_start_record,
            coverage_end_record,verified_source_eof,activated_at
        ) VALUES ('snap-movies-review-v3',true,'Task 8 constructed parser','full',1,%s,true,clock_timestamp())
        ON CONFLICT(parser_version) DO UPDATE SET is_current=true,coverage_kind='full',
            coverage_start_record=1,coverage_end_record=EXCLUDED.coverage_end_record,
            verified_source_eof=true,activated_at=clock_timestamp()""", (records,))
        conn.execute("""INSERT INTO target.product_edition(asin,classification,created_run_id)
                        VALUES ('B000000001','movie',%s),('B000000002','movie',%s)""", (run_id, run_id))
        conn.execute("""INSERT INTO target.reviewer(reviewer_id)
                        SELECT 'user-'||g FROM generate_series(1,%s) g""", (records,))
        conn.execute("""INSERT INTO raw.source_record(
            source_kind,source_file_id,record_number,field_locator,content_sha256,raw_payload
        ) SELECT 'review_file',%s,g,jsonb_build_object('record_number',g),
                 lpad(to_hex(g),64,'0'),jsonb_build_object('constructed',true)
          FROM generate_series(1,%s) g""", (source_file_id, records))
        conn.execute("""INSERT INTO staging.review_record(
            source_review_id,source_record_id,record_number,content_sha256
        ) SELECT lpad(to_hex(r.record_number),64,'0'),r.source_record_id,r.record_number,r.content_sha256
          FROM raw.source_record r WHERE r.source_file_id=%s""", (source_file_id,))
        conn.execute("""INSERT INTO staging.review_parse_result(
            source_review_id,source_record_id,asin,reviewer_id,profile_name_raw,
            helpful_votes,total_votes,score,review_unix_seconds,review_time_utc,
            summary_raw,review_text_raw,parse_status,parse_issues,run_id,
            asin_raw,reviewer_id_raw,helpfulness_raw,score_raw,review_time_raw,
            content_sha256,parser_version,record_number,asin_status
        )
        SELECT lpad(to_hex(g),64,'0'),r.source_record_id,
          CASE WHEN g=3 THEN 'B000000002' ELSE 'B000000001' END,
          CASE WHEN g=4 THEN NULL WHEN g IN (8001,9001) THEN
               CASE WHEN g=8001 THEN 'user-1' ELSE 'user-6' END ELSE 'user-'||g END,
          'Profile '||g,
          CASE WHEN g=8001 THEN 2 ELSE 1 END,
          CASE WHEN g=8001 THEN 3 ELSE 2 END,
          CASE WHEN g=10 THEN NULL ELSE 5.0 END,
          CASE WHEN g=5 THEN NULL WHEN g=8001 THEN 100 WHEN g=9001 THEN 600 ELSE g*100 END,
          CASE WHEN g=5 THEN NULL WHEN g=8001 THEN to_timestamp(100) WHEN g=9001 THEN to_timestamp(600)
               ELSE to_timestamp(g*100) END,
          CASE WHEN g IN (1,6,8001,9001) THEN 'same' ELSE 'summary-'||g END,
          CASE WHEN g=8001 THEN 'body' WHEN g=9001 THEN 'body-6'
               WHEN g=2 THEN 'body!' WHEN g=3 THEN 'body'
               WHEN g=1 THEN 'body' WHEN g=6 THEN 'body-6'
               ELSE repeat('x',512)||g END,
          'valid','[]'::jsonb,%s,
          CASE WHEN g=3 THEN 'B000000002' ELSE 'B000000001' END,
          CASE WHEN g=4 THEN NULL WHEN g=8001 THEN 'user-1' WHEN g=9001 THEN 'user-6' ELSE 'user-'||g END,
          CASE WHEN g=8001 THEN '2/3' ELSE '1/2' END,
          CASE WHEN g=10 THEN NULL ELSE '5.0' END,
          CASE WHEN g=5 THEN NULL WHEN g=8001 THEN '100' WHEN g=9001 THEN '600' ELSE (g*100)::text END,
          lpad(to_hex(g),64,'0'),'snap-movies-review-v3',g,'usable'
        FROM generate_series(1,%s) g JOIN raw.source_record r
          ON r.source_file_id=%s AND r.record_number=g""", (run_id, records, source_file_id))
    return {"records": records}


def counts(url: str, rule: str) -> dict[str, int]:
    with psycopg.connect(url) as conn:
        row = conn.execute("""SELECT
          (SELECT count(*) FROM target.logical_review WHERE dedupe_rule_version=%s),
          (SELECT count(*) FROM target.review_mapping WHERE rule_version=%s),
          (SELECT count(*) FROM target.logical_review l WHERE dedupe_rule_version=%s AND
             helpfulness_resolution='conflict_preserved_at_source'),
          (SELECT count(*) FROM target.logical_review WHERE dedupe_rule_version=%s AND
             dedupe_eligibility='preserved_missing_key')""", (rule,rule,rule,rule)).fetchone()
        return {"logical": row[0], "mappings": row[1], "helpfulness_conflicts": row[2],
                "missing_key_preserved": row[3]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--records", type=int, default=10_000)
    parser.add_argument("--output")
    args = parser.parse_args()
    before = None
    with psycopg.connect(args.database_url) as conn:
        before = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    seed(args.database_url, args.records)
    with psycopg.connect(args.database_url) as conn:
        seeded = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]

    started = time.monotonic()
    baseline = deduplicate_reviews(
        args.database_url,batch_id="task8-baseline",scope_key="sample-10000",
        rule_version="task8-test-baseline-v1",batch_size=1000,scope_end_record=args.records,
    )
    elapsed = time.monotonic()-started
    baseline_counts = counts(args.database_url,"task8-test-baseline-v1")
    rerun = deduplicate_reviews(
        args.database_url,batch_id="task8-baseline",scope_key="sample-10000",
        rule_version="task8-test-baseline-v1",batch_size=1000,scope_end_record=args.records,
    )
    rerun_counts = counts(args.database_url,"task8-test-baseline-v1")

    pre_failed = False
    try:
        deduplicate_reviews(
            args.database_url,batch_id="task8-precommit",scope_key="sample-10000",
            rule_version="task8-test-precommit-v1",batch_size=1000,scope_end_record=args.records,
            fail_before_commit_batch=3,
        )
    except RuntimeError:
        pre_failed = True
    pre_checkpoint = dedupe_status(args.database_url,"task8-test-precommit-v1")["progress"][0]["committed_record_number"]
    pre_resume = deduplicate_reviews(
        args.database_url,batch_id="task8-precommit",scope_key="sample-10000",
        rule_version="task8-test-precommit-v1",batch_size=1000,scope_end_record=args.records,
    )

    post_failed = False
    try:
        deduplicate_reviews(
            args.database_url,batch_id="task8-postcommit",scope_key="sample-10000",
            rule_version="task8-test-postcommit-v1",batch_size=1000,scope_end_record=args.records,
            fail_after_commit_batch=3,
        )
    except RuntimeError:
        post_failed = True
    post_checkpoint = dedupe_status(args.database_url,"task8-test-postcommit-v1")["progress"][0]["committed_record_number"]
    post_resume = deduplicate_reviews(
        args.database_url,batch_id="task8-postcommit",scope_key="sample-10000",
        rule_version="task8-test-postcommit-v1",batch_size=1000,scope_end_record=args.records,
    )
    with psycopg.connect(args.database_url) as conn:
        after = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        cross_asin = conn.execute("""SELECT count(DISTINCT l.logical_review_id)
            FROM target.logical_review l JOIN target.review_mapping rm USING(logical_review_id)
            WHERE l.dedupe_rule_version='task8-test-baseline-v1'
              AND rm.source_review_id IN (lpad(to_hex(1),64,'0'),lpad(to_hex(3),64,'0'))""").fetchone()[0]
    expected_logical = args.records-2
    assertions = {
        "baseline_counts": baseline_counts == {"logical": expected_logical,"mappings": args.records,
            "helpfulness_conflicts": 1,"missing_key_preserved": 3},
        "rerun_idempotent": rerun_counts == baseline_counts and rerun["logical_reviews_inserted"] == 0
            and rerun["mappings_inserted"] == 0,
        "precommit_recovery": pre_failed and pre_checkpoint == 2000
            and counts(args.database_url,"task8-test-precommit-v1")["logical"] == expected_logical,
        "postcommit_recovery": post_failed and post_checkpoint == 3000
            and counts(args.database_url,"task8-test-postcommit-v1")["logical"] == expected_logical,
        "cross_asin_not_merged": cross_asin == 2,
        "mapping_balance": pre_resume["checkpoint_after"] == args.records
            and post_resume["checkpoint_after"] == args.records,
    }
    result = {
        "records": args.records,"expected_logical": expected_logical,"duplicate_records": 2,
        "elapsed_seconds_baseline": round(elapsed,3),"records_per_second": round(args.records/elapsed,1),
        "database_bytes_before": before,"database_bytes_seeded": seeded,"database_bytes_after": after,
        "dedupe_growth_bytes_three_versions": after-seeded,"baseline": baseline,
        "rerun": rerun,"precommit_checkpoint": pre_checkpoint,"postcommit_checkpoint": post_checkpoint,
        "assertions": assertions,"all_passed": all(assertions.values()),
    }
    rendered = json.dumps(result,indent=2,default=str) + "\n"
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(rendered,encoding="utf-8")
    print(rendered,end="")
    return 0 if result["all_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
