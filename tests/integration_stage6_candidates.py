from __future__ import annotations

import gzip
import json
import os
import tempfile
import uuid
from datetime import date
from pathlib import Path

import psycopg

from amazon_movies.imdb_snapshot import REQUIRED_HEADERS, inspect_snapshot, register_snapshot, write_manifest
from amazon_movies.stage6_candidates import candidate_status, export_shortlist_tconsts, scan_candidate_range
from amazon_movies.evaluation_material import create_dataset, export_material


def _write(root: Path, name: str, rows: list[list[str]]) -> None:
    with gzip.open(root / name, "wt", encoding="utf-8", newline="") as stream:
        stream.write("\t".join(REQUIRED_HEADERS[name]) + "\n")
        for row in rows:
            stream.write("\t".join(row) + "\n")


def _fixture(root: Path) -> None:
    _write(root, "title.basics.tsv.gz", [
        ["tt6000001", "movie", "Fixture Film", "Fixture Film", "0", r"\N", r"\N", "90", r"\N"],
        ["tt6000002", "tvSeries", "Fixture Film", "Fixture Film", "0", "2020", r"\N", "45", "Drama"],
        ["tt6000003", "movie", "Unrelated", "Unrelated", "0", "2010", r"\N", "80", "Drama"],
    ])
    _write(root, "title.akas.tsv.gz", [
        ["tt6000001", "1", "Fixture Alias", "US", "en", r"\N", r"\N", "0"],
        ["tt6000003", "1", "Other", "US", "en", r"\N", r"\N", "0"],
    ])
    _write(root, "title.crew.tsv.gz", [["tt6000001", "nm6000001", r"\N"]])
    _write(root, "title.principals.tsv.gz", [["tt6000001", "1", "nm6000002", "actor", r"\N", '["Lead"]']])
    _write(root, "name.basics.tsv.gz", [
        ["nm6000001", "Director Fixture", "1970", r"\N", "director", "tt6000001"],
        ["nm6000002", "Actor Fixture", "1980", r"\N", "actor", "tt6000001"],
    ])


def main() -> None:
    database_url = os.environ["AMAZON_MOVIES_DATABASE_URL"]
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        _fixture(root)
        manifest = inspect_snapshot(root, date(2099, 6, 1), full_verify=True, progress_every=0)
        manifest["snapshot_version"] = "imdb-stage6-constructed-" + uuid.uuid4().hex[:8]
        manifest_path = root / "manifest.json"
        write_manifest(manifest, manifest_path)
        registered = register_snapshot(database_url, manifest_path)
        with psycopg.connect(database_url) as conn:
            run_id = conn.execute(
                """INSERT INTO meta.etl_run(pipeline_name,batch_id,input_version,rule_version,status,stats)
                   VALUES ('stage6_fixture_setup',%s,'constructed','fixture','succeeded','{}') RETURNING run_id""",
                (uuid.uuid4().hex,),
            ).fetchone()[0]
            source_file_id = conn.execute(
                """INSERT INTO meta.source_file(source_name,source_uri,sha256,size_bytes,parser_version)
                   VALUES ('stage6_fixture_product','file:///constructed-products',%s,1,'fixture')
                   RETURNING source_file_id""", (uuid.uuid4().hex * 2,)
            ).fetchone()[0]
            source_record_id = conn.execute(
                """INSERT INTO raw.source_record(source_kind,source_file_id,source_uri,record_number,
                     content_sha256,raw_payload,access_status)
                   VALUES ('third_party_file',%s,'file:///constructed-products#1',1,%s,
                     '{"title":"Fixture Alias"}','success') RETURNING source_record_id""",
                (source_file_id, "a" * 64),
            ).fetchone()[0]
            conn.execute(
                """INSERT INTO target.product_edition(asin,classification,created_run_id)
                   VALUES ('S600000001','movie',%s) ON CONFLICT DO NOTHING""", (run_id,)
            )
            conn.execute(
                """INSERT INTO staging.product_metadata_record(
                     asin,source_name,source_version,source_record_id,source_record_number,
                     title_raw,title_normalized,proposed_classification,classification_basis,run_id)
                   VALUES ('S600000001','fixture','constructed',%s,1,'Fixture Alias','fixture alias',
                     'movie','{}',%s) ON CONFLICT DO NOTHING""", (source_record_id, run_id)
            )
        exclusions = root / "exclusions.json"
        exclusions.write_text(json.dumps({"development_asins": [], "manifest_sha256": "b" * 64}), encoding="utf-8")
        first = scan_candidate_range(
            database_url, snapshot_dir=root, snapshot_version=registered["snapshot_version"],
            exclusions_file=exclusions, scope_key="stage6-constructed", batch_id="stage6-constructed",
            batch_size=2, progress_every=0, minimum_products=1,
        )
        repeated = scan_candidate_range(
            database_url, snapshot_dir=root, snapshot_version=registered["snapshot_version"],
            exclusions_file=exclusions, scope_key="stage6-constructed", batch_id="stage6-constructed-repeat",
            batch_size=2, progress_every=0, minimum_products=1,
        )
        output = root / "tconsts.txt"
        exported = export_shortlist_tconsts(database_url, registered["snapshot_version"], "stage6-constructed", output)
        status = candidate_status(database_url, registered["snapshot_version"], "stage6-constructed")
        evaluation_input = root / "evaluation.json"
        evaluation_input.write_text(json.dumps({
            "dataset_key": "stage6-constructed-evaluation-" + uuid.uuid4().hex[:8],
            "entity_type": "movie", "snapshot_id": registered["snapshot_id"],
            "sampling_rule_version": "constructed", "prediction_rule_version": "constructed-prediction",
            "development_exclusion": {"entity_keys": []},
            "pairs": [{
                "left_entity_key": "S600000001", "right_entity_key": "tt6000001",
                "stratum": "constructed", "left_display": {"title": "Fixture Alias"},
                "right_display": {"title": "Fixture Film"}, "evidence_display": {"alias": "Fixture Alias"},
                "source_links": [{"label": "IMDb", "url": "https://www.imdb.com/title/tt6000001/"}],
                "algorithm_prediction": {"prediction": "same_entity", "evidence": {"constructed": True}},
            }],
        }), encoding="utf-8")
        frozen = create_dataset(database_url, evaluation_input)
        review_dir = root / "review"
        review = export_material(database_url, json.loads(evaluation_input.read_text())["dataset_key"], review_dir)
        html_text = (review_dir / "review_material.html").read_text(encoding="utf-8")
        result = {
            "data_kind": "constructed_stage6_database_acceptance",
            "first": first, "repeat": repeated, "status": status, "export": exported,
            "evaluation_freeze": frozen, "blind_export": review,
            "all_checks_passed": first["candidate_rows"] == repeated["candidate_rows"] == 1
                and status["status"] == "candidates_ready"
                and all(item["reached_eof"] for item in status["progress"])
                and exported["tconst_count"] == 1
                and frozen["predictions_inserted"] == 1
                and review["human_answers_prefilled"] is False
                and "same_entity" not in html_text,
        }
        print(json.dumps(result, indent=2))
        if not result["all_checks_passed"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
