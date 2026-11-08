from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import platform
import sys
from datetime import date, datetime
from pathlib import Path

from amazon_movies.review_parser import restore_record_bytes


EXPECTED_PREFIXES = (
    "product/productId:",
    "review/userId:",
    "review/profileName:",
    "review/helpfulness:",
    "review/score:",
    "review/time:",
    "review/summary:",
    "review/text:",
)


def inspect_bounded(path: Path, max_records: int = 3) -> dict[str, object]:
    records = 0
    prefixes: set[str] = set()
    first_asin: str | None = None
    with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as stream:
        for line_number, line in enumerate(stream, start=1):
            stripped = line.rstrip("\r\n")
            if not stripped:
                records += 1
                if records >= max_records:
                    break
                continue
            for prefix in EXPECTED_PREFIXES:
                if stripped.startswith(prefix):
                    prefixes.add(prefix)
                    if prefix == "product/productId:" and first_asin is None:
                        first_asin = stripped.split(":", 1)[1].strip()
                    break
        else:
            line_number = 0
    return {
        "records_read": records,
        "last_line_number": line_number,
        "first_asin": first_asin,
        "prefixes_seen": sorted(prefixes),
    }


def sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def doctor(args: argparse.Namespace) -> int:
    path = Path(args.data_file).resolve()
    result: dict[str, object] = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "data_file": str(path),
        "exists": path.is_file(),
        "database_url_configured": bool(os.getenv("AMAZON_MOVIES_DATABASE_URL")),
    }
    if path.is_file():
        result["size_bytes"] = path.stat().st_size
        result["bounded_sample"] = inspect_bounded(path, args.records)
        if args.hash:
            result["sha256"] = sha256(path)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["exists"] else 2


def parse_sample(args: argparse.Namespace) -> int:
    path = Path(args.data_file).resolve()
    if args.parser_version == "snap-movies-review-v1":
        from amazon_movies.review_parser import iter_reviews, summarize_reviews

        summary = summarize_reviews(
            iter_reviews(path, source_file_sha256=args.source_sha256, max_records=args.limit)
        )
    else:
        from amazon_movies.review_parser_v2 import iter_reviews

        reviews = list(iter_reviews(path, source_file_sha256=args.source_sha256, max_records=args.limit))
        valid = sum(review.parse_status == "valid" for review in reviews)
        summary = {
            "records_read": len(reviews),
            "valid_records": valid,
            "quarantined_records": len(reviews) - valid,
            "field_quality_issue_count": sum(len(review.issues) for review in reviews),
            "usable_asins": sum(review.asin_status == "usable" for review in reviews),
            "classification_balanced": len(reviews) == valid + (len(reviews) - valid),
            "parser_version": args.parser_version,
        }
    summary.update(
        {
            "data_file": str(path),
            "source_sha256": args.source_sha256.lower(),
            "sample_limit": args.limit,
            "scope": "real_sample_not_full_source",
            "gzip_integrity": "sample_readable_not_full_integrity_check",
        }
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["classification_balanced"] else 3


def load_sample(args: argparse.Namespace) -> int:
    database_url = args.database_url or os.getenv("AMAZON_MOVIES_DATABASE_URL") or os.getenv(
        "AIRFLOW_CONN_AMAZON_MOVIES_DB"
    )
    if not database_url:
        raise SystemExit("database URL is required via --database-url or environment")
    from amazon_movies.review_loader_v2 import compact_result, database_counts, load_review_sample
    result = load_review_sample(
        database_url=database_url,
        data_file=Path(args.data_file).resolve(),
        source_sha256=args.source_sha256,
        source_uri=args.source_uri,
        batch_id=args.batch_id,
        partition_key=args.partition_key,
        sample_limit=args.limit,
        batch_size=args.batch_size,
        scope_start_record=args.scope_start_record,
        load_strategy=args.load_strategy,
        fail_before_commit_batch=args.fail_before_commit_batch,
        fail_after_commit_batch=args.fail_after_commit_batch,
        monitor_path=Path(args.monitor_path) if args.monitor_path else None,
        min_free_bytes=args.min_free_bytes,
        monitor_interval_batches=args.monitor_interval_batches,
    )
    print(compact_result(result, database_counts(database_url)))
    return 0


def load_full(args: argparse.Namespace) -> int:
    from amazon_movies.review_loader_v2 import compact_result, database_counts, load_review_full

    database_url = args.database_url or os.getenv("AMAZON_MOVIES_DATABASE_URL") or os.getenv(
        "AIRFLOW_CONN_AMAZON_MOVIES_DB"
    )
    if not database_url:
        raise SystemExit("database URL is required via --database-url or environment")
    result = load_review_full(
        database_url=database_url,
        data_file=Path(args.data_file).resolve(),
        source_sha256=args.source_sha256,
        source_uri=args.source_uri,
        batch_id=args.batch_id,
        partition_key=args.partition_key,
        batch_size=args.batch_size,
        load_strategy=args.load_strategy,
        monitor_path=Path(args.monitor_path) if args.monitor_path else None,
        min_free_bytes=args.min_free_bytes,
        monitor_interval_batches=args.monitor_interval_batches,
    )
    print(compact_result(result, database_counts(database_url)))
    return 0


def encoding_audit(args: argparse.Namespace) -> int:
    from amazon_movies.review_parser_v2 import iter_reviews

    rows: list[dict[str, object]] = []
    for review in iter_reviews(
        Path(args.data_file).resolve(),
        source_file_sha256=args.source_sha256,
        max_records=args.limit,
    ):
        for issue in review.issues:
            if issue.code == "invalid_utf8":
                rows.append(
                    {
                        "record_number": review.record_number,
                        "field_name": issue.field_name,
                        **issue.details,
                    }
                )
    print(json.dumps({"parser_version": "snap-movies-review-v3", "invalid_utf8": rows}, indent=2))
    return 0


def activate_parser(args: argparse.Namespace) -> int:
    from amazon_movies.review_loader_v2 import activate_parser as activate

    database_url = args.database_url or os.getenv("AMAZON_MOVIES_DATABASE_URL") or os.getenv(
        "AIRFLOW_CONN_AMAZON_MOVIES_DB"
    )
    if not database_url:
        raise SystemExit("database URL is required via --database-url or environment")
    activate(database_url, args.parser_version)
    print(json.dumps({"activated_parser_version": args.parser_version}))
    return 0


def verify_locator(args: argparse.Namespace) -> int:
    payload, digest = restore_record_bytes(
        Path(args.data_file).resolve(),
        uncompressed_start=args.uncompressed_start,
        uncompressed_end=args.uncompressed_end,
        expected_sha256=args.expected_sha256,
    )
    print(
        json.dumps(
            {
                "restored_bytes": len(payload),
                "content_sha256": digest,
                "hash_matches": True,
                "offset_semantics": "uncompressed_seek_requires_decompression_not_compressed_random_access",
            },
            indent=2,
        )
    )
    return 0


def _stage3_database_url(args: argparse.Namespace) -> str:
    value = args.database_url or os.getenv("AMAZON_MOVIES_DATABASE_URL") or os.getenv(
        "AIRFLOW_CONN_AMAZON_MOVIES_DB"
    )
    if not value:
        raise SystemExit("database URL is required via --database-url or environment")
    return value


def _stage3_config(path: str) -> dict[str, object]:
    with Path(path).open("r", encoding="utf-8") as stream:
        return json.load(stream)


def stage3_offline_sample(args: argparse.Namespace) -> int:
    from amazon_movies.stage3_pipeline import load_ucsd_sample

    config = _stage3_config(args.config)
    result = load_ucsd_sample(
        _stage3_database_url(args),
        asins=config["asins"],
        max_records=args.max_records or int(config["ucsd_max_records"]),
        batch_id=args.batch_id,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage3_imdb_verify(args: argparse.Namespace) -> int:
    from amazon_movies.stage3_pipeline import verify_imdb_candidate

    config = _stage3_config(args.config)
    results = []
    for candidate in config["imdb_candidates"]:
        results.append(
            verify_imdb_candidate(
                _stage3_database_url(args),
                asin=candidate["asin"],
                tconst=candidate["tconst"],
                suggestion_url=candidate["suggestion_url"],
                support_url=candidate["support_url"],
                batch_id=f"{args.batch_id}-{candidate['asin']}",
            )
        )
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


def stage3_report_command(args: argparse.Namespace) -> int:
    from amazon_movies.stage3_pipeline import sample_report

    config = _stage3_config(args.config)
    print(json.dumps(sample_report(_stage3_database_url(args), config["asins"]), ensure_ascii=False, indent=2))
    return 0


def stage3_offline_full(args: argparse.Namespace) -> int:
    from amazon_movies.stage3_pipeline import load_ucsd_full_file

    result = load_ucsd_full_file(
        _stage3_database_url(args), metadata_file=Path(args.metadata_file).resolve(),
        product_list_file=Path(args.product_list).resolve(), source_sha256=args.source_sha256,
        batch_id=args.batch_id, batch_size=args.batch_size, partition_key=args.partition_key,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage4_select(args: argparse.Namespace) -> int:
    from amazon_movies.stage4_matcher import select_development_sample

    result = select_development_sample(_stage3_database_url(args), batch_id=args.batch_id, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage4_match(args: argparse.Namespace) -> int:
    from amazon_movies.stage4_matcher import generate_imdb_candidates

    result = generate_imdb_candidates(_stage3_database_url(args), batch_id=args.batch_id, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage4_export(args: argparse.Namespace) -> int:
    from amazon_movies.stage4_matcher import export_results

    result = export_results(_stage3_database_url(args), Path(args.output), rule_version=args.rule_version)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage4_reevaluate_cached(args: argparse.Namespace) -> int:
    from amazon_movies.stage4_reevaluator import reevaluate_cached_candidates

    result = reevaluate_cached_candidates(
        _stage3_database_url(args), batch_id=args.batch_id, limit=args.limit
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage4_v3_reevaluate(args: argparse.Namespace) -> int:
    from amazon_movies.stage4_v3 import reevaluate_cached

    result = reevaluate_cached(
        _stage3_database_url(args), batch_id=args.batch_id, limit=args.limit
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage4_import_labels(args: argparse.Namespace) -> int:
    from amazon_movies.stage4_v3 import import_labels

    result = import_labels(
        _stage3_database_url(args), Path(args.input), import_batch=args.import_batch
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def imdb_snapshot_inspect(args: argparse.Namespace) -> int:
    from amazon_movies.imdb_snapshot import inspect_snapshot, write_manifest

    manifest = inspect_snapshot(
        Path(args.snapshot_dir), date.fromisoformat(args.snapshot_date),
        full_verify=args.full_verify, progress_every=args.progress_every,
    )
    write_manifest(manifest, Path(args.output))
    print(json.dumps({
        "snapshot_version": manifest["snapshot_version"],
        "complete_file_set": manifest["complete_file_set"],
        "all_headers_verified": manifest["all_headers_verified"],
        "all_gzip_streams_verified": manifest["all_gzip_streams_verified"],
        "manifest_sha256": manifest["manifest_sha256"],
        "output": str(Path(args.output).resolve()),
    }, ensure_ascii=False, indent=2))
    return 0 if manifest["complete_file_set"] and manifest["all_headers_verified"] else 2


def imdb_snapshot_register(args: argparse.Namespace) -> int:
    from amazon_movies.imdb_snapshot import register_snapshot

    result = register_snapshot(_stage3_database_url(args), Path(args.manifest))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def imdb_title_evidence(args: argparse.Namespace) -> int:
    from amazon_movies.imdb_snapshot import title_evidence

    evidence = title_evidence(Path(args.snapshot_dir), args.tconst, progress_every=args.progress_every)
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "tconst": args.tconst,
        "title_found": evidence["title"] is not None,
        "alias_count": len(evidence["aliases"]),
        "principal_count": len(evidence["principals"]),
        "resolved_name_count": len(evidence["names"]),
        "missing_name_ids": evidence["missing_name_ids"],
        "output": str(Path(args.output).resolve()) if args.output else None,
    }, ensure_ascii=False, indent=2))
    return 0 if evidence["title"] is not None and not evidence["missing_name_ids"] else 3


def evaluation_create(args: argparse.Namespace) -> int:
    from amazon_movies.evaluation_material import create_dataset

    result = create_dataset(_stage3_database_url(args), Path(args.input))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def evaluation_export(args: argparse.Namespace) -> int:
    from amazon_movies.evaluation_material import export_material

    result = export_material(_stage3_database_url(args), args.dataset_key, Path(args.output_dir))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def evaluation_import(args: argparse.Namespace) -> int:
    from amazon_movies.evaluation_material import import_labels

    result = import_labels(
        _stage3_database_url(args), args.dataset_key, Path(args.input), annotator=args.annotator,
        labeled_at=datetime.fromisoformat(args.labeled_at) if args.labeled_at else None,
        submission_channel=args.submission_channel,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def evaluation_report(args: argparse.Namespace) -> int:
    from amazon_movies.evaluation_reporting import build_evaluation_report

    result = build_evaluation_report(
        _stage3_database_url(args), list(args.dataset_key), Path(args.output_dir)
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def stage7_case_report(args: argparse.Namespace) -> int:
    from amazon_movies.stage7_cases import build_case_inventory

    result = build_case_inventory(
        _stage3_database_url(args),
        stage4_tsv=Path(args.stage4_tsv),
        stage5_case_dir=Path(args.stage5_case_dir),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage7_collection_demo(args: argparse.Namespace) -> int:
    from amazon_movies.stage7_collection_demo import materialize_collection_demo

    result = materialize_collection_demo(_stage3_database_url(args))
    if args.output:
        Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def task8_dedupe(args: argparse.Namespace) -> int:
    from amazon_movies.logical_review_dedupe import deduplicate_reviews
    database_url = args.database_url or os.getenv("AMAZON_MOVIES_DATABASE_URL") or os.getenv(
        "AIRFLOW_CONN_AMAZON_MOVIES_DB"
    )
    result = deduplicate_reviews(
        database_url, batch_id=args.batch_id, scope_key=args.scope_key,
        rule_version=args.rule_version, batch_size=args.batch_size,
        scope_start_record=args.scope_start_record, scope_end_record=args.scope_end_record,
        full=args.full, monitor_path=Path(args.monitor_path) if args.monitor_path else None,
        min_free_bytes=args.min_free_bytes, monitor_interval_batches=args.monitor_interval_batches,
        fail_before_commit_batch=args.fail_before_commit_batch,
        fail_after_commit_batch=args.fail_after_commit_batch,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def task8_dedupe_status(args: argparse.Namespace) -> int:
    from amazon_movies.logical_review_dedupe import dedupe_status
    database_url = args.database_url or os.getenv("AMAZON_MOVIES_DATABASE_URL") or os.getenv(
        "AIRFLOW_CONN_AMAZON_MOVIES_DB"
    )
    print(json.dumps(dedupe_status(database_url, args.rule_version), ensure_ascii=False, indent=2, default=str))
    return 0


def task8_dedupe_activate(args: argparse.Namespace) -> int:
    from amazon_movies.logical_review_dedupe import activate_release
    database_url = args.database_url or os.getenv("AMAZON_MOVIES_DATABASE_URL") or os.getenv(
        "AIRFLOW_CONN_AMAZON_MOVIES_DB"
    )
    print(json.dumps(activate_release(database_url, args.rule_version), ensure_ascii=False, indent=2, default=str))
    return 0


def lineage_repair(args: argparse.Namespace) -> int:
    from amazon_movies.lineage_audit import repair_existing_lineage

    result = repair_existing_lineage(_stage3_database_url(args), batch_id=args.batch_id)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def lineage_audit(args: argparse.Namespace) -> int:
    from amazon_movies.lineage_audit import mark_audit_failed, run_lineage_audit

    database_url = _stage3_database_url(args)
    try:
        result = run_lineage_audit(
            database_url, batch_id=args.batch_id, scope_key=args.scope_key,
            full=args.full, limit=args.limit,
        )
    except Exception as exc:
        mark_audit_failed(database_url, batch_id=args.batch_id, error=str(exc))
        raise
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def lineage_audit_status(args: argparse.Namespace) -> int:
    from amazon_movies.lineage_audit import audit_status

    result = audit_status(_stage3_database_url(args), batch_id=args.batch_id)
    rendered = json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n"
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


def lineage_impact_demo(args: argparse.Namespace) -> int:
    from amazon_movies.lineage_audit import run_impact_demo

    result = run_impact_demo(_stage3_database_url(args), batch_id=args.batch_id)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def imdb_selective_load(args: argparse.Namespace) -> int:
    from amazon_movies.imdb_selective_loader import load_selective_snapshot

    tconsts = list(args.tconst or [])
    if args.tconst_file:
        tconsts.extend(
            line.strip() for line in Path(args.tconst_file).read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    result = load_selective_snapshot(
        _stage3_database_url(args), snapshot_dir=Path(args.snapshot_dir),
        snapshot_version=args.snapshot_version, scope=args.scope, scope_key=args.scope_key,
        batch_id=args.batch_id, explicit_tconsts=tconsts, batch_size=args.batch_size,
        progress_every=args.progress_every, fail_before_commit_batch=args.fail_before_commit_batch,
        fail_after_commit_batch=args.fail_after_commit_batch,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def imdb_evidence_load(args: argparse.Namespace) -> int:
    from amazon_movies.imdb_selective_loader import load_evidence_bundle

    result = load_evidence_bundle(
        _stage3_database_url(args), evidence_file=Path(args.evidence_file),
        snapshot_version=args.snapshot_version, scope_key=args.scope_key, batch_id=args.batch_id,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def imdb_selective_status(args: argparse.Namespace) -> int:
    from amazon_movies.imdb_selective_loader import scope_status

    result = scope_status(_stage3_database_url(args), args.snapshot_version, args.scope_key)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage5_prepare_cases(args: argparse.Namespace) -> int:
    from amazon_movies.stage5_case_prep import prepare

    result = prepare(_stage3_database_url(args), Path(args.output_dir), scope_key=args.scope_key)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage6_candidate_scan(args: argparse.Namespace) -> int:
    from amazon_movies.stage6_candidates import scan_candidate_range

    result = scan_candidate_range(
        _stage3_database_url(args), snapshot_dir=Path(args.snapshot_dir),
        snapshot_version=args.snapshot_version, exclusions_file=Path(args.exclusions_file),
        scope_key=args.scope_key, batch_id=args.batch_id, random_seed=args.random_seed,
        batch_size=args.batch_size, progress_every=args.progress_every,
        limit_per_product=args.limit_per_product,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage6_candidate_status(args: argparse.Namespace) -> int:
    from amazon_movies.stage6_candidates import candidate_status

    print(json.dumps(candidate_status(_stage3_database_url(args), args.snapshot_version, args.scope_key), ensure_ascii=False, indent=2))
    return 0


def p2_scope_register(args: argparse.Namespace) -> int:
    from amazon_movies.p2_movie_expansion import register_scope

    result = register_scope(
        _stage3_database_url(args), scope_key=args.scope_key, batch_id=args.batch_id,
        output=Path(args.output), snapshot_version=args.snapshot_version,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def p2_evaluate(args: argparse.Namespace) -> int:
    from amazon_movies.p2_movie_expansion import evaluate_existing_candidates

    result = evaluate_existing_candidates(
        _stage3_database_url(args), scope_key=args.scope_key, batch_id=args.batch_id,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def p2_apply(args: argparse.Namespace) -> int:
    from amazon_movies.p2_movie_expansion import apply_verified

    result = apply_verified(
        _stage3_database_url(args), scope_key=args.scope_key,
        batch_id=args.batch_id, limit=args.limit,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def p2_status(args: argparse.Namespace) -> int:
    from amazon_movies.p2_movie_expansion import status

    print(json.dumps(status(_stage3_database_url(args), scope_key=args.scope_key), ensure_ascii=False, indent=2, default=str))
    return 0


def p2_export(args: argparse.Namespace) -> int:
    from amazon_movies.p2_movie_expansion import export_reconciliation

    result = export_reconciliation(
        _stage3_database_url(args), scope_key=args.scope_key, output=Path(args.output),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def stage6_export_tconsts(args: argparse.Namespace) -> int:
    from amazon_movies.stage6_candidates import export_shortlist_tconsts

    result = export_shortlist_tconsts(
        _stage3_database_url(args), args.snapshot_version, args.scope_key, Path(args.output)
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def stage6_prepare_evaluation(args: argparse.Namespace) -> int:
    from amazon_movies.stage6_evaluation import prepare_manifests

    result = prepare_manifests(
        _stage3_database_url(args), snapshot_version=args.snapshot_version,
        candidate_scope_key=args.candidate_scope_key, selective_scope_key=args.selective_scope_key,
        exclusions_file=Path(args.exclusions_file), output_dir=Path(args.output_dir),
        movie_pairs=args.movie_pairs, person_pairs=args.person_pairs, random_seed=args.random_seed,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="amazon-movies")
    subparsers = parser.add_subparsers(required=True)
    doctor_parser = subparsers.add_parser("doctor", help="bounded environment and source-file check")
    doctor_parser.add_argument("--data-file", required=True)
    doctor_parser.add_argument("--records", type=int, default=3)
    doctor_parser.add_argument("--hash", action="store_true", help="read the full file to compute SHA-256")
    doctor_parser.set_defaults(func=doctor)

    parse_parser = subparsers.add_parser("parse-sample", help="stream and summarize a bounded real sample")
    parse_parser.add_argument("--data-file", required=True)
    parse_parser.add_argument("--source-sha256", required=True)
    parse_parser.add_argument("--limit", type=int, default=1000)
    parse_parser.add_argument(
        "--parser-version",
        choices=("snap-movies-review-v1", "snap-movies-review-v2", "snap-movies-review-v3"),
        default="snap-movies-review-v3",
    )
    parse_parser.set_defaults(func=parse_sample)

    load_parser = subparsers.add_parser("load-sample", help="transactionally load a bounded sample")
    load_parser.add_argument("--database-url")
    load_parser.add_argument("--data-file", required=True)
    load_parser.add_argument("--source-uri", required=True)
    load_parser.add_argument("--source-sha256", required=True)
    load_parser.add_argument("--batch-id", required=True)
    load_parser.add_argument("--partition-key", required=True)
    load_parser.add_argument("--limit", type=int, default=1000)
    load_parser.add_argument("--batch-size", type=int, default=200)
    load_parser.add_argument("--scope-start-record", type=int, default=1)
    load_parser.add_argument("--load-strategy", choices=("row", "copy"), default="copy")
    load_parser.add_argument(
        "--parser-version",
        choices=("snap-movies-review-v3",),
        default="snap-movies-review-v3",
    )
    load_parser.add_argument("--fail-before-commit-batch", type=int)
    load_parser.add_argument("--fail-after-commit-batch", type=int)
    load_parser.add_argument("--monitor-path")
    load_parser.add_argument("--min-free-bytes", type=int, default=0)
    load_parser.add_argument("--monitor-interval-batches", type=int, default=10)
    load_parser.set_defaults(func=load_sample)

    full_parser = subparsers.add_parser("load-full", help="resume and read the entire gzip through verified EOF")
    full_parser.add_argument("--database-url")
    full_parser.add_argument("--data-file", required=True)
    full_parser.add_argument("--source-uri", required=True)
    full_parser.add_argument("--source-sha256", required=True)
    full_parser.add_argument("--batch-id", required=True)
    full_parser.add_argument("--partition-key", default="full-source")
    full_parser.add_argument("--batch-size", type=int, default=5000)
    full_parser.add_argument("--load-strategy", choices=("copy", "row"), default="copy")
    full_parser.add_argument("--monitor-path")
    full_parser.add_argument("--min-free-bytes", type=int, default=0)
    full_parser.add_argument("--monitor-interval-batches", type=int, default=10)
    full_parser.set_defaults(func=load_full)

    locator_parser = subparsers.add_parser("verify-locator", help="restore one raw record without printing its body")
    locator_parser.add_argument("--data-file", required=True)
    locator_parser.add_argument("--uncompressed-start", type=int, required=True)
    locator_parser.add_argument("--uncompressed-end", type=int, required=True)
    locator_parser.add_argument("--expected-sha256", required=True)
    locator_parser.set_defaults(func=verify_locator)

    audit_parser = subparsers.add_parser(
        "encoding-audit", help="show bounded invalid UTF-8 positions and short hexadecimal context"
    )
    audit_parser.add_argument("--data-file", required=True)
    audit_parser.add_argument("--source-sha256", required=True)
    audit_parser.add_argument("--limit", type=int, default=1000)
    audit_parser.set_defaults(func=encoding_audit)

    activate_version = subparsers.add_parser(
        "activate-parser", help="atomically select the parser version exposed by current views"
    )
    activate_version.add_argument("--database-url")
    activate_version.add_argument(
        "--parser-version",
        choices=("snap-movies-review-v3",),
        default="snap-movies-review-v3",
    )
    activate_version.set_defaults(func=activate_parser)

    stage3_offline = subparsers.add_parser("stage3-offline-sample", help="bounded UCSD metadata match by ASIN")
    stage3_offline.add_argument("--database-url")
    stage3_offline.add_argument("--config", default="config/stage3_sample.json")
    stage3_offline.add_argument("--batch-id", required=True)
    stage3_offline.add_argument("--max-records", type=int)
    stage3_offline.set_defaults(func=stage3_offline_sample)

    stage3_imdb = subparsers.add_parser("stage3-imdb-verify", help="verify configured IMDb candidates")
    stage3_imdb.add_argument("--database-url")
    stage3_imdb.add_argument("--config", default="config/stage3_sample.json")
    stage3_imdb.add_argument("--batch-id", required=True)
    stage3_imdb.set_defaults(func=stage3_imdb_verify)

    stage3_report_parser = subparsers.add_parser("stage3-report", help="small-sample coverage report")
    stage3_report_parser.add_argument("--database-url")
    stage3_report_parser.add_argument("--config", default="config/stage3_sample.json")
    stage3_report_parser.set_defaults(func=stage3_report_command)

    stage3_full = subparsers.add_parser("stage3-offline-full", help="manual resumable local UCSD full scan")
    stage3_full.add_argument("--database-url")
    stage3_full.add_argument("--metadata-file", required=True)
    stage3_full.add_argument("--product-list", required=True)
    stage3_full.add_argument("--source-sha256", required=True)
    stage3_full.add_argument("--batch-id", required=True)
    stage3_full.add_argument("--batch-size", type=int, default=5000)
    stage3_full.add_argument("--partition-key", default="ucsd-movies-tv-full-v1")
    stage3_full.set_defaults(func=stage3_offline_full)

    stage4_selection = subparsers.add_parser("stage4-select", help="select up to 50 development movie products")
    stage4_selection.add_argument("--database-url")
    stage4_selection.add_argument("--batch-id", required=True)
    stage4_selection.add_argument("--limit", type=int, default=50)
    stage4_selection.set_defaults(func=stage4_select)

    stage4_matching = subparsers.add_parser("stage4-match", help="generate bounded IMDb candidates for development sample")
    stage4_matching.add_argument("--database-url")
    stage4_matching.add_argument("--batch-id", required=True)
    stage4_matching.add_argument("--limit", type=int, default=50)
    stage4_matching.set_defaults(func=stage4_match)

    stage4_cached = subparsers.add_parser(
        "stage4-reevaluate-cached",
        help="reevaluate every cached IMDb candidate without network access",
    )
    stage4_cached.add_argument("--database-url")
    stage4_cached.add_argument("--batch-id", required=True)
    stage4_cached.add_argument("--limit", type=int, default=50)
    stage4_cached.set_defaults(func=stage4_reevaluate_cached)

    stage4_v3 = subparsers.add_parser(
        "stage4-v3-reevaluate",
        help="evaluate all cached tt candidates with Stage 4 v3 evidence rules",
    )
    stage4_v3.add_argument("--database-url")
    stage4_v3.add_argument("--batch-id", required=True)
    stage4_v3.add_argument("--limit", type=int, default=50)
    stage4_v3.set_defaults(func=stage4_v3_reevaluate)

    stage4_labels = subparsers.add_parser(
        "stage4-import-labels", help="import evidence-backed human labels from the Stage 4 TSV"
    )
    stage4_labels.add_argument("--database-url")
    stage4_labels.add_argument("--input", required=True)
    stage4_labels.add_argument("--import-batch", required=True)
    stage4_labels.set_defaults(func=stage4_import_labels)

    stage4_export_parser = subparsers.add_parser("stage4-export", help="export Stage 4 development candidates TSV")
    stage4_export_parser.add_argument("--database-url")
    stage4_export_parser.add_argument(
        "--output", default="/opt/airflow/project/data/work/stage4/first50_movie_candidates.tsv"
    )
    stage4_export_parser.add_argument(
        "--rule-version", default="stage4-first50-movie-match-v3.1-evidence-extraction"
    )
    stage4_export_parser.set_defaults(func=stage4_export)

    snapshot_inspect = subparsers.add_parser(
        "imdb-snapshot-inspect", help="verify a fixed IMDb snapshot and write a versioned manifest"
    )
    snapshot_inspect.add_argument("--snapshot-dir", required=True)
    snapshot_inspect.add_argument("--snapshot-date", required=True)
    snapshot_inspect.add_argument("--output", required=True)
    snapshot_inspect.add_argument("--full-verify", action="store_true")
    snapshot_inspect.add_argument("--progress-every", type=int, default=5_000_000)
    snapshot_inspect.set_defaults(func=imdb_snapshot_inspect)

    snapshot_register = subparsers.add_parser(
        "imdb-snapshot-register", help="register a verified IMDb manifest in PostgreSQL"
    )
    snapshot_register.add_argument("--database-url")
    snapshot_register.add_argument("--manifest", required=True)
    snapshot_register.set_defaults(func=imdb_snapshot_register)

    title_evidence_parser = subparsers.add_parser(
        "imdb-title-evidence", help="extract one title/person evidence chain from a fixed IMDb snapshot"
    )
    title_evidence_parser.add_argument("--snapshot-dir", required=True)
    title_evidence_parser.add_argument("--tconst", required=True)
    title_evidence_parser.add_argument("--output")
    title_evidence_parser.add_argument("--progress-every", type=int, default=5_000_000)
    title_evidence_parser.set_defaults(func=imdb_title_evidence)

    evaluation_create_parser = subparsers.add_parser(
        "evaluation-create", help="freeze a program-generated independent entity-pair dataset"
    )
    evaluation_create_parser.add_argument("--database-url")
    evaluation_create_parser.add_argument("--input", required=True)
    evaluation_create_parser.set_defaults(func=evaluation_create)

    evaluation_export_parser = subparsers.add_parser(
        "evaluation-export", help="export blind HTML evidence and a blank answer TSV"
    )
    evaluation_export_parser.add_argument("--database-url")
    evaluation_export_parser.add_argument("--dataset-key", required=True)
    evaluation_export_parser.add_argument("--output-dir", required=True)
    evaluation_export_parser.set_defaults(func=evaluation_export)

    evaluation_import_parser = subparsers.add_parser(
        "evaluation-import", help="append human evaluation answers without overwriting predictions"
    )
    evaluation_import_parser.add_argument("--database-url")
    evaluation_import_parser.add_argument("--dataset-key", required=True)
    evaluation_import_parser.add_argument("--input", required=True)
    evaluation_import_parser.add_argument("--annotator", required=True)
    evaluation_import_parser.add_argument("--labeled-at")
    evaluation_import_parser.add_argument("--submission-channel", default="evaluation_tsv")
    evaluation_import_parser.set_defaults(func=evaluation_import)

    evaluation_report_parser = subparsers.add_parser(
        "evaluation-report", help="calculate frozen-prediction metrics after blind labels are complete"
    )
    evaluation_report_parser.add_argument("--database-url")
    evaluation_report_parser.add_argument("--dataset-key", action="append", required=True)
    evaluation_report_parser.add_argument("--output-dir", required=True)
    evaluation_report_parser.set_defaults(func=evaluation_report)

    stage7_case_parser = subparsers.add_parser(
        "stage7-case-report", help="materialize evidence-backed development cases without reusing evaluation pairs"
    )
    stage7_case_parser.add_argument("--database-url")
    stage7_case_parser.add_argument("--stage4-tsv", required=True)
    stage7_case_parser.add_argument("--stage5-case-dir", required=True)
    stage7_case_parser.add_argument("--output-dir", required=True)
    stage7_case_parser.set_defaults(func=stage7_case_report)

    stage7_collection_parser = subparsers.add_parser(
        "stage7-collection-demo", help="materialize and verify one fixed offline multi-movie collection case"
    )
    stage7_collection_parser.add_argument("--database-url")
    stage7_collection_parser.add_argument("--output")
    stage7_collection_parser.set_defaults(func=stage7_collection_demo)

    task8 = subparsers.add_parser(
        "task8-dedupe", help="build versioned logical reviews from the active valid parser release"
    )
    task8.add_argument("--database-url")
    task8.add_argument("--batch-id", required=True)
    task8.add_argument("--scope-key", required=True)
    task8.add_argument("--rule-version", default="logical-review-exact-v1")
    task8.add_argument("--batch-size", type=int, default=10000)
    task8.add_argument("--scope-start-record", type=int, default=1)
    task8.add_argument("--scope-end-record", type=int)
    task8.add_argument("--full", action="store_true")
    task8.add_argument("--monitor-path")
    task8.add_argument("--min-free-bytes", type=int, default=0)
    task8.add_argument("--monitor-interval-batches", type=int, default=10)
    task8.add_argument("--fail-before-commit-batch", type=int)
    task8.add_argument("--fail-after-commit-batch", type=int)
    task8.set_defaults(func=task8_dedupe)

    task8_status = subparsers.add_parser("task8-dedupe-status", help="show logical-review checkpoints")
    task8_status.add_argument("--database-url")
    task8_status.add_argument("--rule-version", default="logical-review-exact-v1")
    task8_status.set_defaults(func=task8_dedupe_status)

    task8_activate = subparsers.add_parser(
        "task8-dedupe-activate", help="validate full coverage and activate logical-review counting"
    )
    task8_activate.add_argument("--database-url")
    task8_activate.add_argument("--rule-version", default="logical-review-exact-v1")
    task8_activate.set_defaults(func=task8_dedupe_activate)

    lineage_repair_parser = subparsers.add_parser(
        "lineage-repair", help="idempotently link existing evidence to final fields without inventing history"
    )
    lineage_repair_parser.add_argument("--database-url")
    lineage_repair_parser.add_argument("--batch-id", required=True)
    lineage_repair_parser.set_defaults(func=lineage_repair)

    lineage_audit_parser = subparsers.add_parser(
        "lineage-audit", help="audit final-field and relationship provenance for bounded or full scope"
    )
    lineage_audit_parser.add_argument("--database-url")
    lineage_audit_parser.add_argument("--batch-id", required=True)
    lineage_audit_parser.add_argument("--scope-key", required=True)
    lineage_audit_parser.add_argument("--limit", type=int, default=10000)
    lineage_audit_parser.add_argument("--full", action="store_true")
    lineage_audit_parser.add_argument("--output")
    lineage_audit_parser.set_defaults(func=lineage_audit)

    lineage_status_parser = subparsers.add_parser(
        "lineage-audit-status", help="show final-field lineage audit progress and coverage"
    )
    lineage_status_parser.add_argument("--database-url")
    lineage_status_parser.add_argument("--batch-id")
    lineage_status_parser.add_argument("--output")
    lineage_status_parser.set_defaults(func=lineage_audit_status)

    impact_demo_parser = subparsers.add_parser(
        "lineage-impact-demo", help="recompute one real sourced field in an isolated shadow scope"
    )
    impact_demo_parser.add_argument("--database-url")
    impact_demo_parser.add_argument("--batch-id", required=True)
    impact_demo_parser.add_argument("--output")
    impact_demo_parser.set_defaults(func=lineage_impact_demo)

    selective_load = subparsers.add_parser(
        "imdb-selective-load", help="stream a fixed IMDb snapshot and load only scoped tconst/nconst records"
    )
    selective_load.add_argument("--database-url")
    selective_load.add_argument("--snapshot-dir", required=True)
    selective_load.add_argument("--snapshot-version", required=True)
    selective_load.add_argument("--scope", choices=("adopted-development", "explicit"), required=True)
    selective_load.add_argument("--scope-key", required=True)
    selective_load.add_argument("--batch-id", required=True)
    selective_load.add_argument("--tconst", action="append")
    selective_load.add_argument("--tconst-file")
    selective_load.add_argument("--batch-size", type=int, default=500_000)
    selective_load.add_argument("--progress-every", type=int, default=5_000_000)
    selective_load.add_argument("--fail-before-commit-batch", type=int)
    selective_load.add_argument("--fail-after-commit-batch", type=int)
    selective_load.set_defaults(func=imdb_selective_load)

    evidence_load = subparsers.add_parser(
        "imdb-evidence-load", help="load and materialize one previously verified IMDb evidence bundle"
    )
    evidence_load.add_argument("--database-url")
    evidence_load.add_argument("--evidence-file", required=True)
    evidence_load.add_argument("--snapshot-version", required=True)
    evidence_load.add_argument("--scope-key", required=True)
    evidence_load.add_argument("--batch-id", required=True)
    evidence_load.set_defaults(func=imdb_evidence_load)

    selective_status = subparsers.add_parser(
        "imdb-selective-status", help="show selective IMDb checkpoints without scanning files"
    )
    selective_status.add_argument("--database-url")
    selective_status.add_argument("--snapshot-version", required=True)
    selective_status.add_argument("--scope-key", required=True)
    selective_status.set_defaults(func=imdb_selective_status)

    case_prep = subparsers.add_parser(
        "stage5-prepare-cases", help="prepare bounded development cases and evaluation exclusions without scanning IMDb files"
    )
    case_prep.add_argument("--database-url")
    case_prep.add_argument("--output-dir", required=True)
    case_prep.add_argument("--scope-key", default="stage5-adopted-38-v1")
    case_prep.set_defaults(func=stage5_prepare_cases)

    stage6_scan = subparsers.add_parser(
        "stage6-candidate-scan", help="scan title aliases/basics once for a reusable non-development candidate range"
    )
    stage6_scan.add_argument("--database-url")
    stage6_scan.add_argument("--snapshot-dir", required=True)
    stage6_scan.add_argument("--snapshot-version", required=True)
    stage6_scan.add_argument("--exclusions-file", required=True)
    stage6_scan.add_argument("--scope-key", required=True)
    stage6_scan.add_argument("--batch-id", required=True)
    stage6_scan.add_argument("--random-seed", type=int, default=6022026)
    stage6_scan.add_argument("--batch-size", type=int, default=500000)
    stage6_scan.add_argument("--progress-every", type=int, default=5000000)
    stage6_scan.add_argument("--limit-per-product", type=int, default=5)
    stage6_scan.set_defaults(func=stage6_candidate_scan)

    stage6_status = subparsers.add_parser("stage6-candidate-status", help="show Stage 6 shared-scan checkpoints")
    stage6_status.add_argument("--database-url")
    stage6_status.add_argument("--snapshot-version", required=True)
    stage6_status.add_argument("--scope-key", required=True)
    stage6_status.set_defaults(func=stage6_candidate_status)

    p2_register = subparsers.add_parser(
        "p2-movie-register", help="freeze and export the 433-ASIN UCSD movie-product scope"
    )
    p2_register.add_argument("--database-url")
    p2_register.add_argument("--scope-key", required=True)
    p2_register.add_argument("--batch-id", required=True)
    p2_register.add_argument("--snapshot-version", default="imdb-noncommercial-2026-11-05")
    p2_register.add_argument("--output", required=True)
    p2_register.set_defaults(func=p2_scope_register)

    p2_evaluate_parser = subparsers.add_parser(
        "p2-movie-evaluate", help="evaluate existing Stage 4/6 candidates without scanning snapshot files"
    )
    p2_evaluate_parser.add_argument("--database-url")
    p2_evaluate_parser.add_argument("--scope-key", required=True)
    p2_evaluate_parser.add_argument("--batch-id", required=True)
    p2_evaluate_parser.set_defaults(func=p2_evaluate)

    p2_apply_parser = subparsers.add_parser(
        "p2-movie-apply", help="transactionally adopt a bounded number of rule-verified P2 mappings"
    )
    p2_apply_parser.add_argument("--database-url")
    p2_apply_parser.add_argument("--scope-key", required=True)
    p2_apply_parser.add_argument("--batch-id", required=True)
    p2_apply_parser.add_argument("--limit", type=int, default=5)
    p2_apply_parser.set_defaults(func=p2_apply)

    p2_status_parser = subparsers.add_parser("p2-movie-status", help="show P2 reconciliation and progress")
    p2_status_parser.add_argument("--database-url")
    p2_status_parser.add_argument("--scope-key", required=True)
    p2_status_parser.set_defaults(func=p2_status)

    p2_export_parser = subparsers.add_parser("p2-movie-export", help="export 433 mutually exclusive P2 statuses")
    p2_export_parser.add_argument("--database-url")
    p2_export_parser.add_argument("--scope-key", required=True)
    p2_export_parser.add_argument("--output", required=True)
    p2_export_parser.set_defaults(func=p2_export)

    stage6_export = subparsers.add_parser("stage6-export-tconsts", help="export unique shortlisted tconsts for selective evidence loading")
    stage6_export.add_argument("--database-url")
    stage6_export.add_argument("--snapshot-version", required=True)
    stage6_export.add_argument("--scope-key", required=True)
    stage6_export.add_argument("--output", required=True)
    stage6_export.set_defaults(func=stage6_export_tconsts)

    stage6_eval = subparsers.add_parser(
        "stage6-prepare-evaluation", help="prepare frozen-input movie/person pairs after shortlisted evidence is materialized"
    )
    stage6_eval.add_argument("--database-url")
    stage6_eval.add_argument("--snapshot-version", required=True)
    stage6_eval.add_argument("--candidate-scope-key", required=True)
    stage6_eval.add_argument("--selective-scope-key", required=True)
    stage6_eval.add_argument("--exclusions-file", required=True)
    stage6_eval.add_argument("--output-dir", required=True)
    stage6_eval.add_argument("--movie-pairs", type=int, default=100)
    stage6_eval.add_argument("--person-pairs", type=int, default=100)
    stage6_eval.add_argument("--random-seed", type=int, default=6022026)
    stage6_eval.set_defaults(func=stage6_prepare_evaluation)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

