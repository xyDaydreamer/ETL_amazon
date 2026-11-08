from decimal import Decimal
import unittest

from amazon_movies.logical_review_dedupe import stable_logical_key


class LogicalReviewKeyTests(unittest.TestCase):
    def row(self, **updates):
        value = {
            "source_review_id": "a" * 64, "asin": "B000000001", "reviewer_id": "user-1",
            "review_time_raw": "100", "review_time_utc": "1970-01-01T00:01:40Z",
            "score": Decimal("5.0"), "summary_raw": "same", "review_text_raw": "body",
        }
        value.update(updates)
        return value

    def test_helpfulness_does_not_change_exact_identity_key(self):
        one = self.row(helpful_votes=1, total_votes=2)
        two = self.row(helpful_votes=2, total_votes=3)
        self.assertEqual(stable_logical_key(one), stable_logical_key(two))

    def test_cross_asin_and_similar_text_remain_distinct(self):
        base, _ = stable_logical_key(self.row())
        other_asin, _ = stable_logical_key(self.row(asin="B000000002"))
        similar, _ = stable_logical_key(self.row(review_text_raw="body!"))
        self.assertNotEqual(base, other_asin)
        self.assertNotEqual(base, similar)

    def test_missing_identity_is_source_specific(self):
        one, eligibility = stable_logical_key(self.row(reviewer_id=None))
        two, _ = stable_logical_key(self.row(reviewer_id=None, source_review_id="b" * 64))
        self.assertEqual(eligibility, "preserved_missing_key")
        self.assertNotEqual(one, two)


if __name__ == "__main__":
    unittest.main()
