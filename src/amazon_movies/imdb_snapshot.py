from __future__ import annotations

import gzip
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

import psycopg


SOURCE_NAME = "imdb_noncommercial"
SOURCE_URI = "https://datasets.imdbws.com/"
LICENSE_URI = "https://help.imdb.com/article/imdb/general-information/can-i-use-imdb-data-in-my-software/"

REQUIRED_HEADERS: dict[str, tuple[str, ...]] = {
    "title.basics.tsv.gz": (
        "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
        "startYear", "endYear", "runtimeMinutes", "genres",
    ),
    "title.akas.tsv.gz": (
        "titleId", "ordering", "title", "region", "language", "types",
        "attributes", "isOriginalTitle",
    ),
    "title.crew.tsv.gz": ("tconst", "directors", "writers"),
    "title.principals.tsv.gz": (
        "tconst", "ordering", "nconst", "category", "job", "characters",
    ),
    "name.basics.tsv.gz": (
        "nconst", "primaryName", "birthYear", "deathYear", "primaryProfession",
        "knownForTitles",
    ),
}


def _sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _read_header_and_sample(path: Path) -> tuple[list[str], list[str]]:
    with gzip.open(path, "rt", encoding="utf-8", errors="strict", newline="") as stream:
        header_line = stream.readline()
        sample_line = stream.readline()
    if not header_line:
        raise ValueError(f"empty gzip TSV: {path}")
    header = header_line.rstrip("\r\n").split("\t")
    sample = sample_line.rstrip("\r\n").split("\t") if sample_line else []
    return header, sample


def _verify_gzip_stream(path: Path, progress_every: int = 5_000_000) -> int:
    line_count = 0
    with gzip.open(path, "rt", encoding="utf-8", errors="strict", newline="") as stream:
        for line_count, _ in enumerate(stream, start=1):
            if progress_every and line_count % progress_every == 0:
                print(f"verified {path.name}: {line_count:,} lines", file=sys.stderr, flush=True)
    return max(0, line_count - 1)


def inspect_snapshot(
    snapshot_dir: Path,
    snapshot_date: date,
    *,
    full_verify: bool = False,
    progress_every: int = 5_000_000,
) -> dict[str, Any]:
    snapshot_dir = snapshot_dir.resolve()
    files: list[dict[str, Any]] = []
    missing: list[str] = []
    for file_name, expected in REQUIRED_HEADERS.items():
        path = snapshot_dir / file_name
        if not path.is_file():
            missing.append(file_name)
            continue
        header, sample = _read_header_and_sample(path)
        header_verified = tuple(header) == expected
        sample_width_valid = not sample or len(sample) == len(header)
        if full_verify:
            file_sha256 = _sha256_file(path)
            record_count = _verify_gzip_stream(path, progress_every)
        else:
            file_sha256 = None
            record_count = None
        files.append(
            {
                "file_name": file_name,
                "path_hint": str(path),
                "size_bytes": path.stat().st_size,
                "file_sha256": file_sha256,
                "expected_header": list(expected),
                "observed_header": header,
                "header_verified": header_verified,
                "sample_width_valid": sample_width_valid,
                "gzip_stream_verified": full_verify,
                "uncompressed_record_count": record_count,
            }
        )
    complete = not missing and len(files) == len(REQUIRED_HEADERS)
    headers_verified = complete and all(
        item["header_verified"] and item["sample_width_valid"] for item in files
    )
    streams_verified = full_verify and complete and all(item["gzip_stream_verified"] for item in files)
    manifest: dict[str, Any] = {
        "manifest_schema": "amazon-movies-imdb-snapshot-v1",
        "source_name": SOURCE_NAME,
        "source_uri": SOURCE_URI,
        "license_uri": LICENSE_URI,
        "snapshot_date": snapshot_date.isoformat(),
        "snapshot_version": f"imdb-noncommercial-{snapshot_date.isoformat()}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "verification_mode": "full" if full_verify else "bounded_header_sample",
        "complete_file_set": complete,
        "missing_files": missing,
        "all_headers_verified": headers_verified,
        "all_gzip_streams_verified": streams_verified,
        "files": files,
    }
    canonical = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    manifest["manifest_sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return manifest


def write_manifest(manifest: dict[str, Any], output: Path) -> None:
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output)


def register_snapshot(database_url: str, manifest_path: Path) -> dict[str, Any]:
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    required = {
        "source_name", "source_uri", "license_uri", "snapshot_date", "snapshot_version", "files"
    }
    missing = sorted(required - manifest.keys())
    if missing:
        raise ValueError(f"manifest missing fields: {missing}")
    if manifest["source_name"] != SOURCE_NAME:
        raise ValueError("manifest is not an IMDb non-commercial snapshot")
    status = (
        "fully_verified" if manifest.get("all_gzip_streams_verified")
        else "headers_verified" if manifest.get("all_headers_verified")
        else "registered"
    )
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    with psycopg.connect(database_url) as conn:
        snapshot_id = conn.execute(
            """
            INSERT INTO meta.external_snapshot(
                source_name,snapshot_version,snapshot_date,source_uri,license_uri,
                manifest_sha256,verification_status,verified_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,
                CASE WHEN %s IN ('headers_verified','fully_verified') THEN clock_timestamp() END)
            ON CONFLICT (source_name,snapshot_version) DO UPDATE SET
                manifest_sha256=EXCLUDED.manifest_sha256,
                verification_status=EXCLUDED.verification_status,
                verified_at=EXCLUDED.verified_at
            RETURNING snapshot_id
            """,
            (
                manifest["source_name"], manifest["snapshot_version"], manifest["snapshot_date"],
                manifest["source_uri"], manifest["license_uri"], manifest_sha, status, status,
            ),
        ).fetchone()[0]
        for item in manifest["files"]:
            conn.execute(
                """
                INSERT INTO meta.external_snapshot_file(
                    snapshot_id,file_name,path_hint,size_bytes,file_sha256,expected_header,
                    observed_header,header_verified,gzip_stream_verified,uncompressed_record_count
                ) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s)
                ON CONFLICT (snapshot_id,file_name) DO UPDATE SET
                    path_hint=EXCLUDED.path_hint,size_bytes=EXCLUDED.size_bytes,
                    file_sha256=EXCLUDED.file_sha256,expected_header=EXCLUDED.expected_header,
                    observed_header=EXCLUDED.observed_header,header_verified=EXCLUDED.header_verified,
                    gzip_stream_verified=EXCLUDED.gzip_stream_verified,
                    uncompressed_record_count=EXCLUDED.uncompressed_record_count
                """,
                (
                    snapshot_id, item["file_name"], item["path_hint"], item["size_bytes"],
                    item.get("file_sha256"), json.dumps(item["expected_header"]),
                    json.dumps(item["observed_header"]), item["header_verified"],
                    item.get("gzip_stream_verified", False), item.get("uncompressed_record_count"),
                ),
            )
    return {"snapshot_id": snapshot_id, "snapshot_version": manifest["snapshot_version"], "status": status}


def _iter_rows(path: Path, progress_every: int = 0) -> Iterator[tuple[int, dict[str, str], str]]:
    with gzip.open(path, "rt", encoding="utf-8", errors="strict", newline="") as stream:
        header_line = stream.readline()
        if not header_line:
            raise ValueError(f"empty gzip TSV: {path}")
        field_names = header_line.rstrip("\r\n").split("\t")
        for line_number, raw_line in enumerate(stream, start=2):
            if progress_every and line_number % progress_every == 0:
                print(f"scanned {path.name}: {line_number - 1:,} records", file=sys.stderr, flush=True)
            content = raw_line.rstrip("\r\n")
            values = content.split("\t")
            if len(values) != len(field_names):
                raise ValueError(
                    f"TSV column count mismatch in {path.name} line {line_number}: "
                    f"expected {len(field_names)}, got {len(values)}"
                )
            yield line_number, dict(zip(field_names, values)), hashlib.sha256(content.encode("utf-8")).hexdigest()


def _null(value: str | None) -> str | None:
    return None if value in (None, r"\N") else value


def _split(value: str | None) -> list[str]:
    normalized = _null(value)
    return [] if not normalized else normalized.split(",")


def title_evidence(snapshot_dir: Path, tconst: str, progress_every: int = 0) -> dict[str, Any]:
    if not tconst.startswith("tt") or not tconst[2:].isdigit():
        raise ValueError("tconst must match tt followed by digits")
    snapshot_dir = snapshot_dir.resolve()
    title: dict[str, Any] | None = None
    crew: dict[str, Any] | None = None
    aliases: list[dict[str, Any]] = []
    principals: list[dict[str, Any]] = []
    needed_names: set[str] = set()

    for line, row, digest in _iter_rows(snapshot_dir / "title.basics.tsv.gz", progress_every):
        if row["tconst"] == tconst:
            title = {**row, "source_file": "title.basics.tsv.gz", "source_line": line, "raw_line_sha256": digest}
            break
    for line, row, digest in _iter_rows(snapshot_dir / "title.crew.tsv.gz", progress_every):
        if row["tconst"] == tconst:
            crew = {**row, "source_file": "title.crew.tsv.gz", "source_line": line, "raw_line_sha256": digest}
            needed_names.update(_split(row.get("directors")))
            needed_names.update(_split(row.get("writers")))
            break
    for line, row, digest in _iter_rows(snapshot_dir / "title.akas.tsv.gz", progress_every):
        if row["titleId"] == tconst:
            aliases.append({**row, "source_file": "title.akas.tsv.gz", "source_line": line, "raw_line_sha256": digest})
    for line, row, digest in _iter_rows(snapshot_dir / "title.principals.tsv.gz", progress_every):
        if row["tconst"] == tconst:
            principals.append({**row, "source_file": "title.principals.tsv.gz", "source_line": line, "raw_line_sha256": digest})
            needed_names.add(row["nconst"])

    names: dict[str, dict[str, Any]] = {}
    if needed_names:
        for line, row, digest in _iter_rows(snapshot_dir / "name.basics.tsv.gz", progress_every):
            if row["nconst"] in needed_names:
                names[row["nconst"]] = {
                    **row, "source_file": "name.basics.tsv.gz", "source_line": line,
                    "raw_line_sha256": digest,
                    "source_url": f"https://www.imdb.com/name/{row['nconst']}/",
                }
                if len(names) == len(needed_names):
                    break
    return {
        "tconst": tconst,
        "source_url": f"https://www.imdb.com/title/{tconst}/",
        "title": title,
        "aliases": aliases,
        "crew": crew,
        "principals": principals,
        "names": names,
        "missing_name_ids": sorted(needed_names - names.keys()),
        "evidence_semantics": {
            "title_principals": "principal credits only; not a complete cast list",
            "title_crew": "director/writer nconst arrays from the fixed snapshot",
            "name_basics": "primary name, profession and known-for fields; not identity proof by name alone",
        },
    }
