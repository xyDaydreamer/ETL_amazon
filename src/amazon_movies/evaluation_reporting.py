from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

PERSON_NAME_SIMILARITY_THRESHOLD = 0.85


def normalize_person_name(value: str) -> str:
    folded = "".join(
        char
        for char in unicodedata.normalize("NFKD", value).casefold()
        if not unicodedata.combining(char)
    )
    return " ".join(re.findall(r"[^\W_]+", folded, flags=re.UNICODE))


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    human_cannot = sum(row["human_label"] == "cannot_determine" for row in rows)
    algorithm_deferred = sum(row["prediction"] == "needs_review" for row in rows)
    defer_with_determinate_human = sum(
        row["prediction"] == "needs_review" and row["human_label"] != "cannot_determine"
        for row in rows
    )
    scored = [
        row for row in rows
        if row["human_label"] in {"same_entity", "different_entity"}
        and row["prediction"] in {"same_entity", "different_entity"}
    ]
    tp = sum(row["prediction"] == row["human_label"] == "same_entity" for row in scored)
    fp = sum(row["prediction"] == "same_entity" and row["human_label"] == "different_entity" for row in scored)
    fn = sum(row["prediction"] == "different_entity" and row["human_label"] == "same_entity" for row in scored)
    tn = sum(row["prediction"] == row["human_label"] == "different_entity" for row in scored)
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    f1 = 2 * precision * recall / (precision + recall) if precision is not None and recall is not None and precision + recall else None
    determinate = [row for row in rows if row["human_label"] in {"same_entity", "different_entity"}]
    operational_tp = sum(row["prediction"] == "same_entity" and row["human_label"] == "same_entity" for row in determinate)
    operational_fp = sum(row["prediction"] == "same_entity" and row["human_label"] == "different_entity" for row in determinate)
    operational_fn = sum(row["prediction"] != "same_entity" and row["human_label"] == "same_entity" for row in determinate)
    operational_tn = sum(row["prediction"] != "same_entity" and row["human_label"] == "different_entity" for row in determinate)
    operational_precision = operational_tp / (operational_tp + operational_fp) if operational_tp + operational_fp else None
    operational_recall = operational_tp / (operational_tp + operational_fn) if operational_tp + operational_fn else None
    operational_f1 = (
        2 * operational_precision * operational_recall / (operational_precision + operational_recall)
        if operational_precision is not None and operational_recall is not None
        and operational_precision + operational_recall else None
    )
    deferred_label_breakdown = Counter(
        row["human_label"] for row in rows if row["prediction"] == "needs_review"
    )
    return {
        "total_pairs": len(rows),
        "human_cannot_determine": human_cannot,
        "algorithm_deferred": algorithm_deferred,
        "algorithm_deferred_with_determinate_human": defer_with_determinate_human,
        "scored_samples": len(scored),
        "coverage_of_human_determinate": len(scored) / (len(rows) - human_cannot) if len(rows) > human_cannot else None,
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "scoring_policy": "exclude human cannot_determine and algorithm needs_review from the binary confusion matrix; report both as abstentions",
        "deferred_human_label_breakdown": dict(sorted(deferred_label_breakdown.items())),
        "defer_as_not_auto_merged": {
            "scored_samples": len(determinate),
            "TP": operational_tp,
            "FP": operational_fp,
            "FN": operational_fn,
            "TN": operational_tn,
            "precision": operational_precision,
            "recall": operational_recall,
            "f1": operational_f1,
            "scoring_policy": "exclude human cannot_determine; treat algorithm needs_review as not automatically merged (negative)",
        },
    }


def _name_features(row: dict[str, Any]) -> dict[str, Any]:
    left_raw = str(row["left_display"].get("raw_name") or "")
    right_raw = str(row["right_display"].get("primary_name") or "")
    left = normalize_person_name(left_raw)
    right = normalize_person_name(right_raw)
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    shared = sorted(left_tokens & right_tokens)
    exact = bool(left) and left == right
    similarity = SequenceMatcher(None, left, right).ratio() if left and right else 0.0
    one_shared = len(shared) == 1 and not exact
    spelling_near = not exact and similarity >= PERSON_NAME_SIMILARITY_THRESHOLD
    categories = []
    if one_shared:
        categories.append("one_shared_name_token_only")
    if spelling_near:
        categories.append("spelling_near")
    if exact:
        categories.append("normalized_full_name_same")
    return {
        "review_item_id": row["review_item_id"],
        "display_order": row["display_order"],
        "left_name": left_raw,
        "right_name": right_raw,
        "left_normalized": left,
        "right_normalized": right,
        "shared_tokens": shared,
        "name_similarity": similarity,
        "one_shared_name_token_only": one_shared,
        "spelling_near": spelling_near,
        "normalized_full_name_same": exact,
        "category_combination": "+".join(categories) if categories else "none",
        "human_label": row["human_label"],
        "human_reason": row["reason"],
    }


def build_evaluation_report(database_url: str, dataset_keys: list[str], output_dir: Path) -> dict[str, Any]:
    import psycopg
    from psycopg.rows import dict_row

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    datasets: dict[str, dict[str, Any]] = {}
    all_rows: dict[str, list[dict[str, Any]]] = {}
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        for dataset_key in dataset_keys:
            dataset = conn.execute(
                """SELECT evaluation_dataset_id,dataset_key,entity_type,status,manifest_sha256,
                          sampling_rule_version
                   FROM evaluation.dataset WHERE dataset_key=%s""",
                (dataset_key,),
            ).fetchone()
            if not dataset:
                raise ValueError(f"unknown evaluation dataset: {dataset_key}")
            rows = conn.execute(
                """WITH current_label AS (
                       SELECT DISTINCT ON (hl.review_item_id)
                              hl.review_item_id,hl.human_label,hl.reason,hl.annotator,
                              hl.labeled_at,hl.submission_channel
                       FROM evaluation.human_label hl
                       JOIN evaluation.entity_pair ep USING(review_item_id)
                       WHERE ep.evaluation_dataset_id=%s
                       ORDER BY hl.review_item_id,hl.labeled_at DESC,hl.imported_at DESC,hl.label_hash DESC
                   )
                   SELECT ep.display_order,ep.review_item_id,ep.left_entity_key,ep.right_entity_key,
                          ep.stratum,ep.left_display,ep.right_display,ep.evidence_display,ep.source_links,
                          ap.rule_version,ap.prediction,ap.evidence AS prediction_evidence,
                          cl.human_label,cl.reason,cl.annotator,cl.labeled_at,cl.submission_channel
                   FROM evaluation.entity_pair ep
                   LEFT JOIN evaluation.algorithm_prediction ap
                     ON ap.evaluation_dataset_id=ep.evaluation_dataset_id
                    AND ap.review_item_id=ep.review_item_id
                   LEFT JOIN current_label cl ON cl.review_item_id=ep.review_item_id
                   WHERE ep.evaluation_dataset_id=%s
                   ORDER BY ep.display_order""",
                (dataset["evaluation_dataset_id"], dataset["evaluation_dataset_id"]),
            ).fetchall()
            if not rows or any(row["human_label"] is None for row in rows):
                missing = [row["review_item_id"] for row in rows if row["human_label"] is None]
                raise ValueError(f"dataset is not fully labeled: {dataset_key}; missing={missing}")
            if any(row["prediction"] is None for row in rows):
                raise ValueError(f"dataset is missing frozen predictions: {dataset_key}")
            conflicts = conn.execute(
                """SELECT hl.review_item_id,array_agg(DISTINCT hl.human_label ORDER BY hl.human_label) labels
                   FROM evaluation.human_label hl
                   JOIN evaluation.entity_pair ep USING(review_item_id)
                   WHERE ep.evaluation_dataset_id=%s
                   GROUP BY hl.review_item_id HAVING count(DISTINCT hl.human_label)>1""",
                (dataset["evaluation_dataset_id"],),
            ).fetchall()
            if conflicts:
                raise ValueError(
                    f"dataset has conflicting human-label history and cannot be scored: {dataset_key}; "
                    f"items={[row['review_item_id'] for row in conflicts]}"
                )
            datasets[dataset["entity_type"]] = {
                "dataset_key": dataset_key,
                "manifest_sha256": dataset["manifest_sha256"],
                "sampling_rule_version": dataset["sampling_rule_version"],
                "prediction_rule_version": rows[0]["rule_version"],
                "human_label_conflicts": [dict(row) for row in conflicts],
                "metrics": _metrics(rows),
            }
            all_rows[dataset["entity_type"]] = rows
            conn.execute(
                "UPDATE evaluation.dataset SET status='evaluated' WHERE evaluation_dataset_id=%s AND status='labeled'",
                (dataset["evaluation_dataset_id"],),
            )

    person_features = [_name_features(row) for row in all_rows.get("person", [])]
    name_summary: dict[str, Any] = {
        "normalization": "Unicode NFKD, casefold, remove combining marks, keep Unicode word characters, replace punctuation/underscores with separators, collapse whitespace",
        "spelling_similarity": "difflib.SequenceMatcher on normalized full names",
        "spelling_near_threshold": PERSON_NAME_SIMILARITY_THRESHOLD,
        "categories_can_overlap": True,
        "categories": {},
        "overlap_combinations": dict(sorted(Counter(row["category_combination"] for row in person_features).items())),
    }
    for key in ("one_shared_name_token_only", "spelling_near", "normalized_full_name_same"):
        selected = [row for row in person_features if row[key]]
        name_summary["categories"][key] = {
            "pairs": len(selected),
            "human_different_entity": sum(row["human_label"] == "different_entity" for row in selected),
        }
    earl = [row for row in person_features if {row["left_name"], row["right_name"]} == {"Earl Vickers", "Hugo Vickers"}]
    name_summary["earl_vickers_example"] = earl
    exact_name_different = name_summary["categories"]["normalized_full_name_same"]["human_different_entity"]
    name_summary["identical_name_limitation"] = (
        "Only " + str(exact_name_different)
        + " normalized-identical-name pair(s) were labeled different_entity; the result therefore has limited support for disambiguating distinct people with exactly identical full names."
        if exact_name_different < 10 else None
    )

    result = {
        "datasets": datasets,
        "person_name_composition": name_summary,
    }
    (output_dir / "evaluation_metrics.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
    )
    with (output_dir / "person_name_composition.tsv").open("w", encoding="utf-8-sig", newline="") as stream:
        fields = [
            "display_order", "review_item_id", "left_name", "right_name", "left_normalized",
            "right_normalized", "shared_tokens", "name_similarity", "one_shared_name_token_only",
            "spelling_near", "normalized_full_name_same", "category_combination", "human_label", "human_reason",
        ]
        writer = csv.DictWriter(stream, fields, delimiter="\t", lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for row in person_features:
            writer.writerow({**row, "shared_tokens": json.dumps(row["shared_tokens"], ensure_ascii=False)})

    error_fields = [
        "entity_type", "display_order", "review_item_id", "left_entity_key", "right_entity_key",
        "prediction", "human_label", "reason", "stratum", "left_display", "right_display",
        "evidence_display", "source_links",
    ]
    with (output_dir / "representative_errors.tsv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, error_fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for entity_type, rows in all_rows.items():
            errors = [
                row for row in rows
                if row["human_label"] in {"same_entity", "different_entity"}
                and row["prediction"] in {"same_entity", "different_entity"}
                and row["prediction"] != row["human_label"]
            ]
            for row in errors:
                writer.writerow({
                    **{key: row.get(key) for key in error_fields},
                    "entity_type": entity_type,
                    "left_display": json.dumps(row["left_display"], ensure_ascii=False),
                    "right_display": json.dumps(row["right_display"], ensure_ascii=False),
                    "evidence_display": json.dumps(row["evidence_display"], ensure_ascii=False),
                    "source_links": json.dumps(row["source_links"], ensure_ascii=False),
                })

    scored_fields = [
        "entity_type", "dataset_key", "display_order", "review_item_id",
        "left_entity_key", "right_entity_key", "stratum", "rule_version",
        "prediction", "human_label", "reason",
    ]
    for entity_type, rows in all_rows.items():
        dataset_key = datasets[entity_type]["dataset_key"]
        path = output_dir / f"{entity_type}_frozen_predictions_and_labels.tsv"
        with path.open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, scored_fields, delimiter="\t", lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    "entity_type": entity_type,
                    "dataset_key": dataset_key,
                    **{key: row.get(key) for key in scored_fields},
                })
    return result
