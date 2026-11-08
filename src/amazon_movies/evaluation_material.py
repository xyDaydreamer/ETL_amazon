from __future__ import annotations

import csv
import hashlib
import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg


LABEL_ALIASES = {
    "same_entity": "same_entity",
    "同一实体": "same_entity",
    "different_entity": "different_entity",
    "不同实体": "different_entity",
    "cannot_determine": "cannot_determine",
    "无法确定": "cannot_determine",
}


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def create_dataset(database_url: str, input_path: Path) -> dict[str, Any]:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    required = {
        "dataset_key", "entity_type", "sampling_rule_version", "development_exclusion", "pairs"
    }
    missing = sorted(required - payload.keys())
    if missing:
        raise ValueError(f"dataset input missing fields: {missing}")
    if payload["entity_type"] not in {"movie", "person"}:
        raise ValueError("entity_type must be movie or person")
    if not payload["pairs"]:
        raise ValueError("evaluation dataset must contain at least one pair")
    if len(payload["pairs"]) != len({(p["left_entity_key"], p["right_entity_key"]) for p in payload["pairs"]}):
        raise ValueError("duplicate entity pair in evaluation input")
    manifest_sha = _digest(payload)
    with psycopg.connect(database_url) as conn:
        if payload["entity_type"] == "movie":
            left_keys = [pair["left_entity_key"] for pair in payload["pairs"]]
            overlapping = conn.execute(
                "SELECT asin FROM integration.movie_match_development_sample WHERE asin = ANY(%s)",
                (left_keys,),
            ).fetchall()
            if overlapping:
                raise ValueError(f"independent movie evaluation overlaps Stage 4 development ASINs: {[r[0] for r in overlapping]}")
        exclusion = payload["development_exclusion"]
        excluded_left = set(exclusion.get("left_entity_keys", []))
        excluded_right = set(exclusion.get("right_entity_keys", []))
        excluded_entities = set(exclusion.get("entity_keys", []))
        overlap = sorted(
            excluded_left.intersection(pair["left_entity_key"] for pair in payload["pairs"])
            | excluded_right.intersection(pair["right_entity_key"] for pair in payload["pairs"])
            | excluded_entities.intersection(
                key for pair in payload["pairs"] for key in (pair["left_entity_key"], pair["right_entity_key"])
            )
        )
        if overlap:
            raise ValueError(f"evaluation input contains declared development exclusions: {overlap}")
        dataset_id = conn.execute(
            """
            INSERT INTO evaluation.dataset(
                dataset_key,entity_type,snapshot_id,sampling_rule_version,
                development_exclusion,status,manifest_sha256,frozen_at
            ) VALUES (%s,%s,%s,%s,%s::jsonb,'frozen',%s,clock_timestamp())
            ON CONFLICT (dataset_key) DO UPDATE SET
                manifest_sha256 = CASE
                    WHEN evaluation.dataset.manifest_sha256 = EXCLUDED.manifest_sha256
                    THEN evaluation.dataset.manifest_sha256
                    ELSE NULL
                END
            RETURNING evaluation_dataset_id,manifest_sha256
            """,
            (
                payload["dataset_key"], payload["entity_type"], payload.get("snapshot_id"),
                payload["sampling_rule_version"], _canonical(payload["development_exclusion"]), manifest_sha,
            ),
        ).fetchone()
        if dataset_id[1] != manifest_sha:
            raise ValueError("dataset_key already exists with different frozen content")
        inserted = 0
        predictions_inserted = 0
        for order, pair in enumerate(payload["pairs"], start=1):
            pair_payload = {
                key: pair[key]
                for key in (
                    "left_entity_key", "right_entity_key", "stratum", "left_display",
                    "right_display", "evidence_display", "source_links",
                )
            }
            pair_sha = _digest(pair_payload)
            review_item_id = hashlib.sha256(
                f"{payload['dataset_key']}\0{pair['left_entity_key']}\0{pair['right_entity_key']}".encode("utf-8")
            ).hexdigest()
            result = conn.execute(
                """
                INSERT INTO evaluation.entity_pair(
                    review_item_id,evaluation_dataset_id,display_order,left_entity_key,
                    right_entity_key,stratum,left_display,right_display,evidence_display,
                    source_links,pair_payload_sha256
                ) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s)
                ON CONFLICT (review_item_id) DO NOTHING
                RETURNING review_item_id
                """,
                (
                    review_item_id, dataset_id[0], order, pair["left_entity_key"],
                    pair["right_entity_key"], pair["stratum"], _canonical(pair["left_display"]),
                    _canonical(pair["right_display"]), _canonical(pair["evidence_display"]),
                    _canonical(pair["source_links"]), pair_sha,
                ),
            ).fetchone()
            inserted += int(result is not None)
            existing = conn.execute(
                "SELECT pair_payload_sha256 FROM evaluation.entity_pair WHERE review_item_id=%s",
                (review_item_id,),
            ).fetchone()[0]
            if existing != pair_sha:
                raise ValueError(f"review item collision with different evidence: {review_item_id}")
            prediction = pair.get("algorithm_prediction")
            if prediction is not None:
                rule_version = payload.get("prediction_rule_version")
                if not rule_version:
                    raise ValueError("prediction_rule_version is required when pairs contain predictions")
                prediction_value = str(prediction.get("prediction") or "").strip()
                if prediction_value not in {"same_entity", "different_entity", "needs_review"}:
                    raise ValueError(f"unsupported algorithm prediction: {prediction_value!r}")
                saved = conn.execute(
                    """INSERT INTO evaluation.algorithm_prediction(
                         evaluation_dataset_id,review_item_id,rule_version,prediction,evidence)
                       VALUES (%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING RETURNING review_item_id""",
                    (dataset_id[0], review_item_id, rule_version, prediction_value,
                     _canonical(prediction.get("evidence", {}))),
                ).fetchone()
                predictions_inserted += int(saved is not None)
    return {
        "dataset_key": payload["dataset_key"], "dataset_id": dataset_id[0],
        "pairs": len(payload["pairs"]), "inserted": inserted,
        "predictions_inserted": predictions_inserted, "manifest_sha256": manifest_sha,
    }


def _safe_link(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _display_block(title: str, value: Any) -> str:
    pretty = json.dumps(value, ensure_ascii=False, indent=2)
    return f"<section><h3>{html.escape(title)}</h3><pre>{html.escape(pretty)}</pre></section>"


def export_material(database_url: str, dataset_key: str, output_dir: Path) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(database_url) as conn:
        dataset = conn.execute(
            """SELECT evaluation_dataset_id,entity_type,sampling_rule_version,manifest_sha256,status
               FROM evaluation.dataset WHERE dataset_key=%s""",
            (dataset_key,),
        ).fetchone()
        if not dataset:
            raise ValueError(f"unknown evaluation dataset: {dataset_key}")
        if dataset[4] not in {"frozen", "labeling", "labeled", "evaluated"}:
            raise ValueError("evaluation material can only be exported from a frozen dataset")
        rows = conn.execute(
            """SELECT review_item_id,display_order,left_display,right_display,evidence_display,source_links,stratum
               FROM evaluation.entity_pair WHERE evaluation_dataset_id=%s ORDER BY display_order""",
            (dataset[0],),
        ).fetchall()
    cards: list[str] = []
    answer_rows: list[tuple[str, str, str]] = []
    for row in rows:
        links = row[5]
        anchors = []
        for link in links:
            url = str(link.get("url", ""))
            label = str(link.get("label", url))
            if _safe_link(url):
                anchors.append(f'<li><a href="{html.escape(url, quote=True)}" target="_blank" rel="noreferrer">{html.escape(label)}</a></li>')
        cards.append(
            f'<article class="pair"><h2>#{row[1]} · {html.escape(row[6])}</h2>'
            f'<p class="item">review_item_id: <code>{row[0]}</code></p>'
            f'<div class="columns">{_display_block("左侧实体", row[2])}{_display_block("右侧实体", row[3])}</div>'
            f'{_display_block("必要证据与冲突", row[4])}'
            f'<section><h3>可点击来源</h3><ul>{"".join(anchors) or "<li>无可点击来源；不得强行判断</li>"}</ul></section>'
            '<p><strong>请仅在答案TSV中填写：</strong>同一实体／不同实体／无法确定，以及简短理由。</p></article>'
        )
        answer_rows.append((row[0], "", ""))
    document = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>{html.escape(dataset_key)} 人工盲评材料</title>
<style>body{{font-family:system-ui,sans-serif;max-width:1200px;margin:auto;padding:24px;line-height:1.5}}.notice{{background:#fff4ce;padding:16px;border-left:5px solid #d69e00}}.pair{{border:1px solid #bbb;border-radius:8px;padding:18px;margin:22px 0}}.columns{{display:grid;grid-template-columns:1fr 1fr;gap:18px}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#f6f8fa;padding:12px}}code{{overflow-wrap:anywhere}}@media(max-width:800px){{.columns{{grid-template-columns:1fr}}}}</style></head>
<body><h1>{html.escape(dataset_key)} 人工盲评材料</h1>
<div class="notice">本材料不显示程序预测，也不预填人工答案。只依据双方信息、来源链接和证据判断；证据不足时选择“无法确定”。</div>
<p>实体类型：{html.escape(dataset[1])}；抽样规则：{html.escape(dataset[2])}；冻结清单SHA-256：<code>{dataset[3]}</code>；共{len(rows)}对。</p>
{"".join(cards)}</body></html>"""
    html_path = output_dir / "review_material.html"
    answers_path = output_dir / "answers.tsv"
    manifest_path = output_dir / "review_manifest.json"
    html_path.write_text(document, encoding="utf-8")
    with answers_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(("review_item_id", "human_label", "reason"))
        writer.writerows(answer_rows)
    review_manifest = {
        "dataset_key": dataset_key, "entity_type": dataset[1], "sampling_rule_version": dataset[2],
        "frozen_manifest_sha256": dataset[3], "pair_count": len(rows),
        "html_sha256": hashlib.sha256(html_path.read_bytes()).hexdigest(),
        "answers_template_sha256": hashlib.sha256(answers_path.read_bytes()).hexdigest(),
        "allowed_labels": ["same_entity", "different_entity", "cannot_determine"],
        "human_answers_prefilled": False,
        "algorithm_predictions_in_material": False,
    }
    manifest_path.write_text(json.dumps(review_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"output_dir": str(output_dir), **review_manifest}


def import_labels(
    database_url: str,
    dataset_key: str,
    input_path: Path,
    *,
    annotator: str,
    labeled_at: datetime | None = None,
    submission_channel: str = "evaluation_tsv",
) -> dict[str, Any]:
    when = labeled_at or datetime.now(timezone.utc)
    inserted = 0
    skipped_blank = 0
    with input_path.open("r", encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream, delimiter="\t"))
    with psycopg.connect(database_url) as conn:
        dataset = conn.execute(
            "SELECT evaluation_dataset_id FROM evaluation.dataset WHERE dataset_key=%s",
            (dataset_key,),
        ).fetchone()
        if not dataset:
            raise ValueError(f"unknown evaluation dataset: {dataset_key}")
        for row in rows:
            raw_label = (row.get("human_label") or "").strip()
            if not raw_label:
                skipped_blank += 1
                continue
            label = LABEL_ALIASES.get(raw_label)
            if label is None:
                raise ValueError(f"unsupported label {raw_label!r}")
            # Preserve the annotator's wording byte-for-byte after TSV decoding.
            # Whitespace is only used for the required-value check; it is not
            # silently normalized before the evidence is stored.
            reason = row.get("reason") or ""
            if not reason.strip():
                raise ValueError("every nonblank human label requires a reason")
            review_item_id = (row.get("review_item_id") or "").strip()
            exists = conn.execute(
                """SELECT 1 FROM evaluation.entity_pair
                   WHERE review_item_id=%s AND evaluation_dataset_id=%s""",
                (review_item_id, dataset[0]),
            ).fetchone()
            if not exists:
                raise ValueError(f"review item does not belong to dataset: {review_item_id}")
            label_payload = {
                "review_item_id": review_item_id, "human_label": label, "reason": reason,
                "annotator": annotator, "labeled_at": when.isoformat(),
                "submission_channel": submission_channel,
            }
            result = conn.execute(
                """INSERT INTO evaluation.human_label(
                       label_hash,review_item_id,human_label,reason,annotator,labeled_at,submission_channel)
                   VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (label_hash) DO NOTHING
                   RETURNING label_hash""",
                (
                    _digest(label_payload), review_item_id, label, reason, annotator, when,
                    submission_channel,
                ),
            ).fetchone()
            inserted += int(result is not None)
        pair_count = conn.execute(
            "SELECT count(*) FROM evaluation.entity_pair WHERE evaluation_dataset_id=%s",
            (dataset[0],),
        ).fetchone()[0]
        labeled_count = conn.execute(
            """SELECT count(DISTINCT hl.review_item_id)
               FROM evaluation.human_label hl
               JOIN evaluation.entity_pair ep USING(review_item_id)
               WHERE ep.evaluation_dataset_id=%s""",
            (dataset[0],),
        ).fetchone()[0]
        next_status = "labeled" if labeled_count == pair_count and pair_count else "labeling"
        conn.execute(
            """UPDATE evaluation.dataset
               SET status=CASE
                   WHEN status='evaluated' AND %s=0 THEN status
                   ELSE %s
               END
               WHERE evaluation_dataset_id=%s
                 AND status IN ('frozen','labeling','labeled','evaluated')""",
            (inserted, next_status, dataset[0]),
        )
        actual_status = conn.execute(
            "SELECT status FROM evaluation.dataset WHERE evaluation_dataset_id=%s",
            (dataset[0],),
        ).fetchone()[0]
    return {
        "dataset_key": dataset_key,
        "rows": len(rows),
        "inserted": inserted,
        "skipped_blank": skipped_blank,
        "labeled_count": labeled_count,
        "pair_count": pair_count,
        "dataset_status": actual_status,
    }
