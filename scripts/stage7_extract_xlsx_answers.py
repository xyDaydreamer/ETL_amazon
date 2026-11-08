from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import zipfile
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree as ET


NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
DOC_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
LABELS = {
    "同一实体": "same_entity",
    "不同实体": "different_entity",
    "无法确定": "cannot_determine",
    "same_entity": "same_entity",
    "different_entity": "different_entity",
    "cannot_determine": "cannot_determine",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _column_index(cell_ref: str) -> int:
    letters = re.match(r"[A-Z]+", cell_ref.upper())
    if not letters:
        raise ValueError(f"invalid cell reference: {cell_ref}")
    value = 0
    for char in letters.group(0):
        value = value * 26 + ord(char) - 64
    return value - 1


def read_sheet(path: Path, sheet_name: str) -> list[list[str]]:
    with zipfile.ZipFile(path) as archive:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in root.findall("x:si", NS):
                shared.append("".join(node.text or "" for node in item.findall(".//x:t", NS)))

        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relation_id = None
        for sheet in workbook.findall("x:sheets/x:sheet", NS):
            if sheet.attrib.get("name") == sheet_name:
                relation_id = sheet.attrib.get(f"{{{DOC_REL_NS}}}id")
                break
        if not relation_id:
            raise ValueError(f"worksheet not found: {sheet_name}")
        relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        target = None
        for relation in relationships.findall("r:Relationship", REL_NS):
            if relation.attrib.get("Id") == relation_id:
                target = relation.attrib["Target"]
                break
        if not target:
            raise ValueError(f"worksheet relationship missing: {sheet_name}")
        sheet_path = "xl/" + target.lstrip("/") if not target.startswith("xl/") else target
        root = ET.fromstring(archive.read(sheet_path))
        result: list[list[str]] = []
        for row in root.findall("x:sheetData/x:row", NS):
            values: dict[int, str] = {}
            for cell in row.findall("x:c", NS):
                index = _column_index(cell.attrib["r"])
                cell_type = cell.attrib.get("t")
                if cell_type == "inlineStr":
                    value = "".join(node.text or "" for node in cell.findall(".//x:t", NS))
                else:
                    node = cell.find("x:v", NS)
                    value = node.text if node is not None and node.text is not None else ""
                    if cell_type == "s" and value:
                        value = shared[int(value)]
                values[index] = value
            width = max(values, default=-1) + 1
            result.append([values.get(index, "") for index in range(width)])
        return result


def expected_ids(frozen_input: Path) -> tuple[str, list[str]]:
    payload = json.loads(frozen_input.read_text(encoding="utf-8"))
    dataset_key = payload["dataset_key"]
    ids = [
        hashlib.sha256(
            f"{dataset_key}\0{pair['left_entity_key']}\0{pair['right_entity_key']}".encode("utf-8")
        ).hexdigest()
        for pair in payload["pairs"]
    ]
    return dataset_key, ids


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate an XLSX answer sheet against one frozen evaluation manifest.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--frozen-input", type=Path, required=True)
    parser.add_argument("--output-tsv", type=Path, required=True)
    parser.add_argument("--output-manifest", type=Path, required=True)
    parser.add_argument("--archive-xlsx", type=Path)
    parser.add_argument("--sheet", default="answers")
    args = parser.parse_args()

    rows = read_sheet(args.input, args.sheet)
    if not rows:
        raise ValueError("answer worksheet is empty")
    headers = rows[0]
    required = ["review_item_id", "human_label", "reason"]
    if headers[:3] != required:
        raise ValueError(f"unexpected headers: {headers!r}")
    records = []
    for row_number, values in enumerate(rows[1:], start=2):
        values = values + [""] * (3 - len(values))
        if not any(values[:3]):
            continue
        records.append({"row_number": row_number, **dict(zip(required, values[:3]))})

    dataset_key, expected = expected_ids(args.frozen_input)
    ids = [record["review_item_id"].strip() for record in records]
    duplicates = sorted(key for key, count in Counter(ids).items() if count > 1)
    missing = sorted(set(expected) - set(ids))
    unexpected = sorted(set(ids) - set(expected))
    blank_labels = [record["review_item_id"] for record in records if not record["human_label"].strip()]
    blank_reasons = [record["review_item_id"] for record in records if not record["reason"].strip()]
    unsupported = sorted({record["human_label"].strip() for record in records if record["human_label"].strip() not in LABELS})
    issues = {
        "duplicate_ids": duplicates,
        "missing_ids": missing,
        "unexpected_ids": unexpected,
        "blank_labels": blank_labels,
        "blank_reasons": blank_reasons,
        "unsupported_labels": unsupported,
    }
    if len(records) != len(expected) or any(issues.values()):
        print(json.dumps({"dataset_key": dataset_key, "rows": len(records), "expected": len(expected), **issues}, ensure_ascii=False, indent=2))
        return 2

    args.output_tsv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_tsv.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(required)
        for record in records:
            writer.writerow((record["review_item_id"].strip(), LABELS[record["human_label"].strip()], record["reason"]))

    source_hash = sha256(args.input)
    archive_hash = None
    if args.archive_xlsx:
        args.archive_xlsx.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.input, args.archive_xlsx)
        archive_hash = sha256(args.archive_xlsx)
        if archive_hash != source_hash:
            raise RuntimeError("archived XLSX hash differs from source")
    manifest = {
        "dataset_key": dataset_key,
        "source_xlsx": str(args.input.resolve()),
        "source_xlsx_sha256": source_hash,
        "archived_xlsx": str(args.archive_xlsx.resolve()) if args.archive_xlsx else None,
        "archived_xlsx_sha256": archive_hash,
        "sheet": args.sheet,
        "row_count": len(records),
        "expected_count": len(expected),
        "label_counts": dict(sorted(Counter(LABELS[record["human_label"].strip()] for record in records).items())),
        "validation": {key: len(value) for key, value in issues.items()},
        "canonical_tsv": str(args.output_tsv.resolve()),
        "canonical_tsv_sha256": sha256(args.output_tsv),
        "reason_policy": "preserved exactly as stored in the XLSX cell",
    }
    args.output_manifest.parent.mkdir(parents=True, exist_ok=True)
    args.output_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
