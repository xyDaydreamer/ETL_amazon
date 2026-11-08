from __future__ import annotations

import base64
import gzip
import hashlib
import math
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import BinaryIO, Iterator


PARSER_VERSION = "snap-movies-review-v3"
BIGINT_MIN = -(2**63)
BIGINT_MAX = 2**63 - 1

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
class DecodedValue:
    text: str
    reliable: bool


@dataclass
class StreamReadState:
    reached_eof: bool = False
    limit_reached: bool = False
    records_emitted: int = 0
    last_record_number: int = 0
    uncompressed_bytes_read: int = 0


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
    asin_status: str
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


def stable_source_review_id(source_file_sha256: str, record_number: int) -> str:
    return hashlib.sha256(f"{source_file_sha256.lower()}:{record_number}".encode("ascii")).hexdigest()


def normalize_display(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(unicodedata.normalize("NFKC", value).split())
    return normalized or None


def _safe_display(raw: bytes) -> str:
    return raw.decode("utf-8", errors="replace").replace("\x00", "\ufffd")


def _decode_value(raw: bytes, field_name: str, issues: list[ParseIssue]) -> DecodedValue:
    reliable = True
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        reliable = False
        context_start = max(0, exc.start - 8)
        context_end = min(len(raw), exc.end + 8)
        issues.append(
            ParseIssue(
                "invalid_utf8",
                "error",
                field_name,
                {
                    "error_start": exc.start,
                    "error_end": exc.end,
                    "context_start": context_start,
                    "hex_context": raw[context_start:context_end].hex(),
                },
            )
        )
        text = raw.decode("utf-8", errors="replace")
    if "\x00" in text:
        reliable = False
        positions = [index for index, char in enumerate(text) if char == "\x00"]
        issues.append(ParseIssue("nul_byte", "error", field_name, {"character_positions": positions[:20]}))
        text = text.replace("\x00", "\ufffd")
    return DecodedValue(text=text, reliable=reliable)


def _delimiter_value(line: bytes, prefix: bytes) -> bytes:
    value = line[len(prefix) :]
    return value[1:] if value.startswith(b" ") else value


def _bounded_bigint(raw: str, field_name: str, issues: list[ParseIssue]) -> int | None:
    try:
        value = int(raw)
    except ValueError:
        issues.append(ParseIssue(f"invalid_{field_name}", "error", field_name))
        return None
    if not BIGINT_MIN <= value <= BIGINT_MAX:
        issues.append(ParseIssue(f"{field_name}_out_of_bigint_range", "error", field_name))
        return None
    return value


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
    decoded: dict[str, list[DecodedValue]] = {}
    unknown_display: list[str] = []

    for raw_line in raw_lines:
        line = raw_line.rstrip(b"\r\n")
        for prefix, field_name in FIELD_PREFIXES.items():
            if line.startswith(prefix):
                decoded.setdefault(field_name, []).append(
                    _decode_value(_delimiter_value(line, prefix), field_name, issues)
                )
                break
        else:
            value = _decode_value(line, "unknown", issues)
            unknown_display.append(value.text[:200])
            issues.append(ParseIssue("unknown_field", "warning", details={"line_display": value.text[:200]}))

    for field_name, values in decoded.items():
        if len(values) > 1 and field_name not in {"asin", "reviewer_id"}:
            issues.append(ParseIssue("duplicate_field", "error", field_name, {"count": len(values)}))
    for field_name in sorted(CRITICAL_FIELDS):
        if field_name not in decoded:
            issues.append(ParseIssue("missing_required_field", "error", field_name))

    def first(name: str) -> DecodedValue | None:
        values = decoded.get(name)
        return values[0] if values else None

    asin_values = decoded.get("asin", [])
    asin_raw = asin_values[0].text if asin_values else None
    asin: str | None = None
    if not asin_values:
        asin_status = "missing"
    elif any(not value.reliable for value in asin_values):
        asin_status = "decode_error"
        issues.append(ParseIssue("asin_decode_error", "error", "asin"))
    elif any(value.text == "" for value in asin_values):
        asin_status = "empty"
        issues.append(ParseIssue("empty_asin", "error", "asin"))
    elif len({value.text for value in asin_values}) > 1:
        asin_status = "conflict"
        issues.append(
            ParseIssue(
                "conflicting_asin_fields",
                "error",
                "asin",
                {"values": [value.text for value in asin_values]},
            )
        )
    elif not ASIN_RE.fullmatch(asin_values[0].text):
        asin_status = "invalid"
        issues.append(ParseIssue("invalid_asin", "error", "asin", {"value": asin_values[0].text}))
    else:
        asin_status = "usable"
        asin = asin_values[0].text
        if len(asin_values) > 1:
            issues.append(ParseIssue("duplicate_asin_same", "warning", "asin", {"count": len(asin_values)}))

    reviewer_values = decoded.get("reviewer_id", [])
    reviewer_value = reviewer_values[0] if reviewer_values else None
    reviewer_id_raw = reviewer_value.text if reviewer_value else None
    reviewer_id = None
    if not reviewer_values:
        pass
    elif any(not value.reliable for value in reviewer_values) or any(value.text == "" for value in reviewer_values):
        pass
    elif len({value.text for value in reviewer_values}) > 1:
        issues.append(
            ParseIssue(
                "conflicting_reviewer_id_fields",
                "error",
                "reviewer_id",
                {"count": len(reviewer_values), "values": [value.text for value in reviewer_values]},
            )
        )
    else:
        reviewer_id = reviewer_values[0].text
        if len(reviewer_values) > 1:
            issues.append(
                ParseIssue("duplicate_reviewer_id_same", "warning", "reviewer_id", {"count": len(reviewer_values)})
            )
    if reviewer_id is None:
        issues.append(ParseIssue("missing_or_unreliable_reviewer_id", "warning", "reviewer_id"))

    profile_value = first("profile_name")
    profile_name_raw = profile_value.text if profile_value else None
    profile_name_normalized = normalize_display(profile_name_raw) if profile_value and profile_value.reliable else None

    helpful_value = first("helpfulness")
    helpfulness_raw = helpful_value.text if helpful_value else None
    helpful_votes = total_votes = None
    helpful_ratio: Decimal | None = None
    if helpful_value and helpful_value.reliable:
        match = HELPFULNESS_RE.fullmatch(helpfulness_raw)
        if match is None:
            issues.append(ParseIssue("invalid_helpfulness", "error", "helpfulness"))
        else:
            helpful_votes = _bounded_bigint(match.group(1) or match.group(3), "helpful_votes", issues)
            total_votes = _bounded_bigint(match.group(2) or match.group(4), "total_votes", issues)
            if helpful_votes is not None and total_votes is not None:
                if helpful_votes > total_votes:
                    issues.append(ParseIssue("helpful_votes_exceed_total", "error", "helpfulness"))
                    helpful_votes = total_votes = None
                elif total_votes > 0:
                    helpful_ratio = Decimal(helpful_votes) / Decimal(total_votes)

    score_value = first("score")
    score_raw = score_value.text if score_value else None
    score: Decimal | None = None
    if score_value and score_value.reliable:
        try:
            candidate = Decimal(score_raw)
            if not candidate.is_finite():
                issues.append(ParseIssue("non_finite_score", "error", "score"))
            elif candidate.as_tuple().exponent < -1:
                issues.append(ParseIssue("score_precision_mismatch", "error", "score"))
            elif candidate < 1 or candidate > 5:
                issues.append(ParseIssue("score_out_of_range", "error", "score", {"value": str(candidate)}))
            else:
                score = candidate
        except InvalidOperation:
            issues.append(ParseIssue("invalid_score", "error", "score"))

    time_value = first("review_time")
    review_time_raw = time_value.text if time_value else None
    review_unix_seconds = None
    review_time_utc = None
    if time_value and time_value.reliable:
        candidate = _bounded_bigint(review_time_raw, "review_time", issues)
        if candidate is not None:
            try:
                review_time_utc = datetime.fromtimestamp(candidate, tz=timezone.utc)
                review_unix_seconds = candidate
            except (ValueError, OverflowError, OSError):
                issues.append(ParseIssue("review_time_out_of_datetime_range", "error", "review_time"))

    summary_value = first("summary")
    summary_raw = summary_value.text if summary_value else None
    summary_normalized = normalize_display(summary_raw) if summary_value and summary_value.reliable else None
    text_value = first("review_text")
    review_text_raw = text_value.text if text_value else None
    review_text_normalized = normalize_display(review_text_raw) if text_value and text_value.reliable else None

    content = b"".join(raw_lines)
    raw_payload: dict[str, object] = {
        "encoding": "base64",
        "raw_record_base64": base64.b64encode(content).decode("ascii"),
        "raw_lines_display": [_safe_display(line.rstrip(b"\r\n")) for line in raw_lines],
        "unknown_lines_display": unknown_display,
    }
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
        asin_status=asin_status,
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
    state: StreamReadState | None = None,
) -> Iterator[ParsedReview]:
    read_state = state if state is not None else StreamReadState()
    record_number = line_number = uncompressed_offset = 0
    record_lines: list[bytes] = []
    record_start_line = record_start_offset = 0

    def emit(end_line: int, end_offset: int) -> ParsedReview:
        nonlocal record_number
        record_number += 1
        read_state.records_emitted = record_number
        read_state.last_record_number = record_number
        return _build_review(
            source_file_sha256=source_file_sha256,
            record_number=record_number,
            start_line=record_start_line,
            end_line=end_line,
            uncompressed_start=record_start_offset,
            uncompressed_end=end_offset,
            raw_lines=record_lines,
        )

    while True:
        raw_line = stream.readline()
        if raw_line == b"":
            read_state.reached_eof = True
            read_state.uncompressed_bytes_read = uncompressed_offset
            if record_lines and (max_records is None or record_number < max_records):
                yield emit(line_number, uncompressed_offset)
            break
        line_number += 1
        line_start_offset = uncompressed_offset
        uncompressed_offset += len(raw_line)
        if raw_line.rstrip(b"\r\n") == b"":
            if record_lines:
                yield emit(line_number - 1, line_start_offset)
                record_lines = []
                if max_records is not None and record_number >= max_records:
                    read_state.limit_reached = True
                    read_state.uncompressed_bytes_read = uncompressed_offset
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
    state: StreamReadState | None = None,
) -> Iterator[ParsedReview]:
    with gzip.open(path, "rb") as stream:
        yield from iter_review_stream(
            stream, source_file_sha256=source_file_sha256, max_records=max_records, state=state
        )
