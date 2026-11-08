from __future__ import annotations

import hashlib
import io
import unittest

from amazon_movies.review_parser import iter_review_stream


SOURCE_HASH = "a" * 64


def record_bytes(overrides: dict[bytes, bytes] | None = None, *, final_newline: bool = True) -> bytes:
    fields = {
        b"product/productId:": b"B000000001",
        b"review/userId:": b"USER1",
        b"review/profileName:": b" Test User ",
        b"review/helpfulness:": b"0/0",
        b"review/score:": b"5.0",
        b"review/time:": b"1000000000",
        b"review/summary:": b"Summary: with colon",
        b"review/text:": b"Body: keeps: every colon",
    }
    fields.update(overrides or {})
    payload = b"\n".join(prefix + b" " + value for prefix, value in fields.items())
    return payload + (b"\n" if final_newline else b"")


class ReviewParserTests(unittest.TestCase):
    def parse(self, payload: bytes):
        return list(iter_review_stream(io.BytesIO(payload), source_file_sha256=SOURCE_HASH))

    def test_colons_empty_fields_and_zero_zero_ratio(self) -> None:
        review = self.parse(record_bytes({b"review/profileName:": b""}) + b"\n")[0]
        self.assertEqual(review.review_text_raw, "Body: keeps: every colon")
        self.assertEqual(review.summary_raw, "Summary: with colon")
        self.assertIsNone(review.helpful_ratio)
        self.assertEqual(review.parse_status, "valid")
        self.assertIn("empty_profile_name", {issue.code for issue in review.issues})

    def test_eof_without_blank_line_or_final_newline(self) -> None:
        reviews = self.parse(record_bytes(final_newline=False))
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0].record_number, 1)

    def test_invalid_utf8_is_quarantined_and_preserved(self) -> None:
        review = self.parse(record_bytes({b"review/text:": b"bad\xffbytes"}) + b"\n")[0]
        self.assertEqual(review.parse_status, "quarantined")
        self.assertIn("invalid_utf8", {issue.code for issue in review.issues})
        self.assertIn("raw_bytes_base64", review.raw_payload)

    def test_duplicate_and_unknown_fields_are_reported(self) -> None:
        payload = record_bytes() + b"review/score: 4.0\nunknown/prefix: value\n\n"
        review = self.parse(payload)[0]
        codes = {issue.code for issue in review.issues}
        self.assertEqual(review.parse_status, "quarantined")
        self.assertIn("duplicate_field", codes)
        self.assertIn("unknown_field", codes)

    def test_abnormal_score_and_votes_are_quarantined(self) -> None:
        review = self.parse(
            record_bytes({b"review/helpfulness:": b"[3, 2]", b"review/score:": b"7.0"}) + b"\n"
        )[0]
        codes = {issue.code for issue in review.issues}
        self.assertEqual(review.parse_status, "quarantined")
        self.assertIn("helpful_votes_exceed_total", codes)
        self.assertIn("score_out_of_range", codes)

    def test_bracket_helpfulness_variant_is_supported(self) -> None:
        review = self.parse(record_bytes({b"review/helpfulness:": b"[1, 2]"}) + b"\n")[0]
        self.assertEqual(review.helpful_votes, 1)
        self.assertEqual(review.total_votes, 2)
        self.assertEqual(str(review.helpful_ratio), "0.5")

    def test_stable_id_uses_source_hash_and_record_number(self) -> None:
        review = self.parse(record_bytes() + b"\n")[0]
        expected = hashlib.sha256(f"{SOURCE_HASH}:1".encode("ascii")).hexdigest()
        self.assertEqual(review.source_review_id, expected)


if __name__ == "__main__":
    unittest.main()
