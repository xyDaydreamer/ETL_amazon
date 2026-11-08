from __future__ import annotations

import base64
import gzip
import hashlib
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import BinaryIO, Iterator


PARSER_VERSION = "snap-movies-review-v1"

FIELD_PREFIXES = {
    b"product/productId:": "asin",
    b"review/userId:": "reviewer_id",
    b"review/profileName:": "profile_name",
    b"review/helpfulness:": "helpfulness",
    b"review/score:": "score",
    b"review/time:": "review_time",
    b"review/summary:": "summary",
    b"review/text:": "review_text",
}

CRITICAL_FIELDS = {"asin", "helpfulness", "score", "review_time", "review_text"}
HELPFULNESS_RE = re.compile(r"^(?:\[\s*(\d+)\s*,\s*(\d+)\s*\]|(\d+)\s*/\s*(\d+))$")
ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")


@dataclass(frozen=True)
class ParseIssue:
    code: str
    severity: str
    field_name: str | None = None
    details: dict[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        result: dict[str, object] = {"code": self.code, "severity": self.severity}
        if self.field_name is not None:
            result["field_name"] = self.field_name
        if self.details:
            result["details"] = self.details
        return result


@dataclass(frozen=True)
class ParsedReview:
    record_number: int
    start_line: int
    end_line: int
    uncompressed_start: int
    uncompressed_end: int
    content_sha256: str
    raw_payload: dict[str, object]
    source_review_id: str
    asin_raw: str | None
    asin: str | None
    reviewer_id_raw: str | None
    reviewer_id: str | None
    profile_name_raw: str | None
    profile_name_normalized: str | None
    helpfulness_raw: str | None
    helpful_votes: int | None
    total_votes: int | None
    helpful_ratio: Decimal | None
    score_raw: str | None
    score: Decimal | None
    review_time_raw: str | None
    review_unix_seconds: int | None
    review_time_utc: datetime | None
    summary_raw: str | None
    summary_normalized: str | None
    review_text_raw: str | None
    review_text_normalized: str | None
    parse_status: str
    issues: tuple[ParseIssue, ...]


def normalize_text(value: str | None, *, collapse_whitespace: bool = False) -> str | None:
    if value is None:
        return None
    normalized = unicodedata.normalize("NFKC", value).strip()
    if collapse_whitespace:
        normalized = " ".join(normalized.split())
    return normalized or None


def stable_source_review_id(source_file_sha256: str, record_number: int) -> str:
    material = f"{source_file_sha256.lower()}:{record_number}".encode("ascii")
    return hashlib.sha256(material).hexdigest()


def _decode_value(raw_value: bytes, field_name: str, issues: list[ParseIssue]) -> str:
    try:
        return raw_value.decode("utf-8")
    except UnicodeDecodeError as exc:
        issues.append(
            ParseIssue(
                code="invalid_utf8",
                severity="error",
                field_name=field_name,
                details={"start": exc.start, "end": exc.end},
            )
        )
        return raw_value.decode("utf-8", errors="replace")


def _build_review(
    *,
    source_file_sha256: str,
    record_number: int,
    start_line: int,
    end_line: int,
    uncompressed_start: int,
    uncompressed_end: int,
    raw_lines: list[bytes],
) -> ParsedReview:
    issues: list[ParseIssue] = []
    values: dict[str, list[str]] = {}
    unknown_lines: list[str] = []
    encoding_error = False

    for raw_line in raw_lines:
        line = raw_line.rstrip(b"\r\n")
        matched = False
        for prefix, field_name in FIELD_PREFIXES.items():
            if line.startswith(prefix):
                matched = True
                before = len(issues)
                value = _decode_value(line[len(prefix) :].lstrip(b" "), field_name, issues)
                encoding_error = encoding_error or len(issues) > before
                values.setdefault(field_name, []).append(value)
                break
        if not matched:
            decoded = _decode_value(line, "unknown", issues)
            encoding_error = encoding_error or any(issue.code == "invalid_utf8" for issue in issues)
            unknown_lines.append(decoded)
            issues.append(ParseIssue("unknown_field", "warning", details={"line": decoded[:200]}))

    for field_name, field_values in values.items():
        if len(field_values) > 1:
            issues.append(
                ParseIssue(
                    "duplicate_field",
                    "error",
                    field_name=field_name,
                    details={"count": len(field_values)},
                )
            )

    for field_name in sorted(CRITICAL_FIELDS):
        if field_name not in values:
            issues.append(ParseIssue("missing_required_field", "error", field_name=field_name))

    def first(name: str) -> str | None:
        candidates = values.get(name)
        return candidates[0] if candidates else None

    asin_raw = first("asin")
    asin = normalize_text(asin_raw, collapse_whitespace=True)
    asin = asin.upper() if asin is not None else None
    if asin is not None and not ASIN_RE.fullmatch(asin):
        issues.append(ParseIssue("invalid_asin", "error", "asin", {"value": asin}))

    reviewer_id_raw = first("reviewer_id")
    reviewer_id = normalize_text(reviewer_id_raw, collapse_whitespace=True)
    if reviewer_id_raw is None or reviewer_id is None:
        issues.append(ParseIssue("missing_reviewer_id", "warning", "reviewer_id"))

    profile_name_raw = first("profile_name")
    profile_name_normalized = normalize_text(profile_name_raw, collapse_whitespace=True)
    if profile_name_raw is not None and profile_name_normalized is None:
        issues.append(ParseIssue("empty_profile_name", "warning", "profile_name"))

    helpfulness_raw = first("helpfulness")
    helpful_votes: int | None = None
    total_votes: int | None = None
    helpful_ratio: Decimal | None = None
    if helpfulness_raw is not None:
        helpful_match = HELPFULNESS_RE.fullmatch(helpfulness_raw.strip())
        if helpful_match is None:
            issues.append(ParseIssue("invalid_helpfulness", "error", "helpfulness"))
        else:
            helpful_votes = int(helpful_match.group(1) or helpful_match.group(3))
            total_votes = int(helpful_match.group(2) or helpful_match.group(4))
            if helpful_votes > total_votes:
                issues.append(ParseIssue("helpful_votes_exceed_total", "error", "helpfulness"))
            elif total_votes > 0:
                helpful_ratio = Decimal(helpful_votes) / Decimal(total_votes)

    score_raw = first("score")
    score: Decimal | None = None
    if score_raw is not None:
        try:
            score = Decimal(score_raw.strip())
            if score < 1 or score > 5:
                issues.append(ParseIssue("score_out_of_range", "error", "score", {"value": str(score)}))
        except InvalidOperation:
            issues.append(ParseIssue("invalid_score", "error", "score"))

    review_time_raw = first("review_time")
    review_unix_seconds: int | None = None
    review_time_utc: datetime | None = None
    if review_time_raw is not None:
        try:
            review_unix_seconds = int(review_time_raw.strip())
            review_time_utc = datetime.fromtimestamp(review_unix_seconds, tz=timezone.utc)
        except (ValueError, OverflowError, OSError):
            issues.append(ParseIssue("invalid_review_time", "error", "review_time"))

    summary_raw = first("summary")
    summary_normalized = normalize_text(summary_raw)
    if summary_raw is not None and summary_normalized is None:
        issues.append(ParseIssue("empty_summary", "warning", "summary"))

    review_text_raw = first("review_text")
    review_text_normalized = normalize_text(review_text_raw)
    if review_text_raw is not None and review_text_normalized is None:
        issues.append(ParseIssue("empty_review_text", "warning", "review_text"))

    content = b"".join(raw_lines)
    raw_payload: dict[str, object] = {
        "raw_lines": [line.rstrip(b"\r\n").decode("utf-8", errors="replace") for line in raw_lines],
        "fields": values,
        "unknown_lines": unknown_lines,
    }
    if encoding_error:
        raw_payload["raw_bytes_base64"] = base64.b64encode(content).decode("ascii")

    parse_status = "quarantined" if any(issue.severity == "error" for issue in issues) else "valid"
    return ParsedReview(
        record_number=record_number,
        start_line=start_line,
        end_line=end_line,
        uncompressed_start=uncompressed_start,
        uncompressed_end=uncompressed_end,
        content_sha256=hashlib.sha256(content).hexdigest(),
        raw_payload=raw_payload,
        source_review_id=stable_source_review_id(source_file_sha256, record_number),
        asin_raw=asin_raw,
        asin=asin,
        reviewer_id_raw=reviewer_id_raw,
        reviewer_id=reviewer_id,
        profile_name_raw=profile_name_raw,
        profile_name_normalized=profile_name_normalized,
        helpfulness_raw=helpfulness_raw,
        helpful_votes=helpful_votes,
        total_votes=total_votes,
        helpful_ratio=helpful_ratio,
        score_raw=score_raw,
        score=score,
        review_time_raw=review_time_raw,
        review_unix_seconds=review_unix_seconds,
        review_time_utc=review_time_utc,
        summary_raw=summary_raw,
        summary_normalized=summary_normalized,
        review_text_raw=review_text_raw,
        review_text_normalized=review_text_normalized,
        parse_status=parse_status,
        issues=tuple(issues),
    )


def iter_review_stream(
    stream: BinaryIO,
    *,
    source_file_sha256: str,
    max_records: int | None = None,
) -> Iterator[ParsedReview]:
    record_number = 0
    line_number = 0
    record_lines: list[bytes] = []
    record_start_line = 0
    record_start_offset = 0
    uncompressed_offset = 0

    def emit(end_offset: int) -> ParsedReview:
        nonlocal record_number
        record_number += 1
        return _build_review(
            source_file_sha256=source_file_sha256,
            record_number=record_number,
            start_line=record_start_line,
            end_line=line_number,
            uncompressed_start=record_start_offset,
            uncompressed_end=end_offset,
            raw_lines=record_lines,
        )

    while True:
        raw_line = stream.readline()
        if raw_line == b"":
            if record_lines and (max_records is None or record_number < max_records):
                yield emit(uncompressed_offset)
            break

        line_number += 1
        line_start_offset = uncompressed_offset
        uncompressed_offset += len(raw_line)
        if raw_line.rstrip(b"\r\n") == b"":
            if record_lines:
                yield emit(line_start_offset)
                record_lines = []
                if max_records is not None and record_number >= max_records:
                    break
            continue

        if not record_lines:
            record_start_line = line_number
            record_start_offset = line_start_offset
        record_lines.append(raw_line)


def iter_reviews(
    path: Path,
    *,
    source_file_sha256: str,
    max_records: int | None = None,
) -> Iterator[ParsedReview]:
    with gzip.open(path, "rb") as stream:
        yield from iter_review_stream(
            stream,
            source_file_sha256=source_file_sha256,
            max_records=max_records,
        )


def restore_record_bytes(
    path: Path,
    *,
    uncompressed_start: int,
    uncompressed_end: int,
    expected_sha256: str | None = None,
) -> tuple[bytes, str]:
    """Restore one record by uncompressed offsets.

    ``gzip.GzipFile.seek`` addresses the uncompressed stream and may need to
    decompress from the beginning. It is not a compressed-byte checkpoint.
    """
    if uncompressed_start < 0 or uncompressed_end < uncompressed_start:
        raise ValueError("invalid uncompressed offset range")
    with gzip.open(path, "rb") as stream:
        stream.seek(uncompressed_start)
        payload = stream.read(uncompressed_end - uncompressed_start)
    digest = hashlib.sha256(payload).hexdigest()
    if expected_sha256 is not None and digest != expected_sha256.lower():
        raise ValueError(f"restored record hash mismatch: expected={expected_sha256} actual={digest}")
    return payload, digest


def summarize_reviews(reviews: Iterator[ParsedReview]) -> dict[str, object]:
    total = 0
    valid = 0
    quarantined = 0
    issue_counts: dict[str, int] = {}
    examples: list[dict[str, object]] = []
    for review in reviews:
        total += 1
        if review.parse_status == "valid":
            valid += 1
        else:
            quarantined += 1
        for issue in review.issues:
            issue_counts[issue.code] = issue_counts.get(issue.code, 0) + 1
        if len(examples) < 3:
            examples.append(
                {
                    "record_number": review.record_number,
                    "source_review_id": review.source_review_id,
                    "asin": review.asin,
                    "reviewer_id": review.reviewer_id,
                    "score": str(review.score) if review.score is not None else None,
                    "review_time_utc": review.review_time_utc.isoformat() if review.review_time_utc else None,
                    "parse_status": review.parse_status,
                    "issue_codes": [issue.code for issue in review.issues],
                    "locator": {
                        "start_line": review.start_line,
                        "end_line": review.end_line,
                        "uncompressed_start": review.uncompressed_start,
                        "uncompressed_end": review.uncompressed_end,
                    },
                }
            )
    return {
        "records_read": total,
        "valid_records": valid,
        "quarantined_records": quarantined,
        "classification_balanced": total == valid + quarantined,
        "issue_counts": dict(sorted(issue_counts.items())),
        "examples": examples,
    }
