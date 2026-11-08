from __future__ import annotations

import io
import unittest

from amazon_movies.review_parser_v2 import BIGINT_MAX, iter_review_stream


SOURCE_HASH = "b" * 64


def payload(overrides: dict[bytes, bytes] | None = None, extras: list[bytes] | None = None) -> bytes:
    fields = [
        (b"product/productId:", b"B000000001"),
        (b"review/userId:", b" USER  ID "),
        (b"review/profileName:", b"Name"),
        (b"review/helpfulness:", b"0/0"),
        (b"review/score:", b"5.0"),
        (b"review/time:", b"1000000000"),
        (b"review/summary:", b"Summary"),
        (b"review/text:", b"Body"),
    ]
    replacements = overrides or {}
    rows = [prefix + b" " + replacements.get(prefix, value) for prefix, value in fields]
    rows.extend(extras or [])
    return b"\n".join(rows)


def parse(raw: bytes):
    return list(iter_review_stream(io.BytesIO(raw), source_file_sha256=SOURCE_HASH))


class ParserV2Tests(unittest.TestCase):
    def test_end_line_excludes_blank_separator_and_consecutive_blanks(self) -> None:
        reviews = parse(payload() + b"\n\n\n" + payload() + b"\n\n")
        self.assertEqual([(r.start_line, r.end_line) for r in reviews], [(1, 8), (11, 18)])

    def test_eof_end_line_is_last_content_line(self) -> None:
        review = parse(payload())[0]
        self.assertEqual((review.start_line, review.end_line), (1, 8))

    def test_body_invalid_utf8_still_has_usable_asin(self) -> None:
        review = parse(payload({b"review/text:": b"bad\xfftext"}))[0]
        self.assertEqual(review.parse_status, "quarantined")
        self.assertEqual((review.asin_status, review.asin), ("usable", "B000000001"))
        self.assertIn("raw_record_base64", review.raw_payload)

    def test_duplicate_same_and_conflicting_asin(self) -> None:
        same = parse(payload(extras=[b"product/productId: B000000001"]))[0]
        self.assertEqual((same.asin_status, same.asin), ("usable", "B000000001"))
        conflict = parse(payload(extras=[b"product/productId: B000000002"]))[0]
        self.assertEqual((conflict.asin_status, conflict.asin), ("conflict", None))

    def test_invalid_asin_variants(self) -> None:
        for value, status in [(b"", "empty"), (b"TOO-LONG-ASIN", "invalid"), (b"badlower01", "invalid")]:
            with self.subTest(value=value):
                review = parse(payload({b"product/productId:": value}))[0]
                self.assertEqual((review.asin_status, review.asin), (status, None))

    def test_reviewer_id_is_opaque(self) -> None:
        review = parse(payload())[0]
        self.assertEqual(review.reviewer_id, " USER  ID ")

    def test_duplicate_reviewer_id_same_is_usable_but_conflict_is_not(self) -> None:
        same = parse(payload(extras=[b"review/userId:  USER  ID "]))[0]
        self.assertEqual(same.reviewer_id, " USER  ID ")
        self.assertIn("duplicate_reviewer_id_same", {issue.code for issue in same.issues})
        conflict = parse(payload(extras=[b"review/userId: OTHER-ID"]))[0]
        self.assertIsNone(conflict.reviewer_id)
        self.assertIn("conflicting_reviewer_id_fields", {issue.code for issue in conflict.issues})

    def test_nul_is_safe_in_all_text_paths(self) -> None:
        for prefix in [b"review/text:", b"review/summary:", b"review/profileName:"]:
            review = parse(payload({prefix: b"a\x00b"}))[0]
            self.assertEqual(review.parse_status, "quarantined")
            self.assertIn("nul_byte", {issue.code for issue in review.issues})
        unknown = parse(payload(extras=[b"unknown: a\x00b"]))[0]
        self.assertIn("nul_byte", {issue.code for issue in unknown.issues})

    def test_nonfinite_precision_and_range_scores_become_null(self) -> None:
        for value, code in [
            (b"NaN", "non_finite_score"),
            (b"Infinity", "non_finite_score"),
            (b"-Infinity", "non_finite_score"),
            (b"4.25", "score_precision_mismatch"),
            (b"7.0", "score_out_of_range"),
        ]:
            with self.subTest(value=value):
                review = parse(payload({b"review/score:": value}))[0]
                self.assertIsNone(review.score)
                self.assertIn(code, {issue.code for issue in review.issues})

    def test_bigint_boundaries(self) -> None:
        integer_edge = parse(payload({b"review/helpfulness:": b"2147483648/2147483648"}))[0]
        self.assertEqual(integer_edge.total_votes, 2**31)
        valid = parse(payload({b"review/helpfulness:": f"{BIGINT_MAX}/{BIGINT_MAX}".encode()}))[0]
        self.assertEqual(valid.total_votes, BIGINT_MAX)
        overflow = parse(payload({b"review/helpfulness:": f"{BIGINT_MAX + 1}/{BIGINT_MAX + 1}".encode()}))[0]
        self.assertIsNone(overflow.total_votes)
        huge_time = parse(payload({b"review/time:": str(BIGINT_MAX + 1).encode()}))[0]
        self.assertIsNone(huge_time.review_unix_seconds)
        too_small_time = parse(payload({b"review/time:": str(-(2**63) - 1).encode()}))[0]
        self.assertIsNone(too_small_time.review_unix_seconds)

    def test_missing_asin(self) -> None:
        raw = b"\n".join(payload().splitlines()[1:])
        review = parse(raw)[0]
        self.assertEqual((review.asin_status, review.asin), ("missing", None))


if __name__ == "__main__":
    unittest.main()
