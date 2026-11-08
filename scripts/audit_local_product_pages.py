from __future__ import annotations

import argparse
import ast
import gzip
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from amazon_movies.stage3_crawler import assess_product_response


ASIN_FROM_URL = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})(?:[/?]|$)", re.I)
ASIN_FROM_SNAPSHOT = re.compile(r"amazon-([A-Z0-9]{10})-[0-9a-f]{16}\.(?:html|bin)$", re.I)


def decoded_body(raw: bytes, headers: bytes = b"") -> tuple[bytes, str]:
    if raw.startswith(b"\x1f\x8b") or b"content-encoding: gzip" in headers.lower():
        return gzip.decompress(raw), "gzip"
    return raw, "identity"


def audit_cache(cache_root: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not cache_root.exists():
        return results
    for meta_path in cache_root.rglob("meta"):
        directory = meta_path.parent
        body_path = directory / "response_body"
        if not body_path.exists():
            continue
        meta = ast.literal_eval(meta_path.read_text(encoding="utf-8"))
        url = str(meta.get("url", ""))
        match = ASIN_FROM_URL.search(url)
        if not match:
            results.append({"kind": "non_product_cache", "url": url, "cache_directory": str(directory)})
            continue
        target = match.group(1).upper()
        raw = body_path.read_bytes()
        header_path = directory / "response_headers"
        headers = header_path.read_bytes() if header_path.exists() else b""
        body, encoding = decoded_body(raw, headers)
        final_url = str(meta.get("response_url") or url)
        assessment = assess_product_response(
            target_asin=target, status=int(meta.get("status", 0)), final_url=final_url, body=body,
        )
        results.append({
            "kind": "product_cache", "target_asin": target, "request_url": url,
            "final_url": final_url, "http_status": int(meta.get("status", 0)),
            "cache_timestamp": meta.get("timestamp"), "cache_directory": str(directory),
            "stored_bytes": len(raw), "decoded_bytes": len(body), "content_encoding": encoding,
            "stored_sha256": hashlib.sha256(raw).hexdigest(),
            "decoded_sha256": hashlib.sha256(body).hexdigest(),
            "assessment": {
                "outcome": assessment.outcome, "error_code": assessment.error_code,
                "observed_asins": assessment.observed_asins,
                "product_markers": assessment.product_markers,
                "restricted_marker": assessment.restricted_marker,
                "final_url_asin": assessment.final_url_asin,
            },
        })
    return results


def audit_snapshots(snapshot_root: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not snapshot_root.exists():
        return results
    for path in snapshot_root.iterdir():
        if not path.is_file():
            continue
        match = ASIN_FROM_SNAPSHOT.fullmatch(path.name)
        if not match:
            continue
        target = match.group(1).upper()
        body = path.read_bytes()
        assessment = assess_product_response(
            target_asin=target, status=200,
            final_url=f"https://www.amazon.com/dp/{target}", body=body,
        )
        results.append({
            "kind": "saved_snapshot", "target_asin": target, "path": str(path),
            "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
            "assessment": {
                "outcome": assessment.outcome, "error_code": assessment.error_code,
                "observed_asins": assessment.observed_asins,
                "product_markers": assessment.product_markers,
                "restricted_marker": assessment.restricted_marker,
                "final_url_asin": assessment.final_url_asin,
            },
        })
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = {
        "snapshot_root": str(args.snapshot_root),
        "cache_entries": audit_cache(args.snapshot_root / "httpcache") +
                         audit_cache(args.snapshot_root / "httpcache-v2"),
        "saved_snapshots": audit_snapshots(args.snapshot_root),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
