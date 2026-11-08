import unittest

from amazon_movies.stage6_candidates import _match_level, _tokens, candidate_matches


class Stage6CandidateTests(unittest.TestCase):
    def test_exact_and_fuzzy_candidates_use_shared_index(self) -> None:
        products = [
            {"asin": "B000000001", "normalized": "the lost city"},
            {"asin": "B000000002", "normalized": "resident evil afterlife"},
        ]
        exact = {item["normalized"]: [item] for item in products}
        token = {}
        for item in products:
            for value in _tokens(item["normalized"]):
                token.setdefault(value, []).append(item)
        result = candidate_matches("Resident Evil: Afterlife", exact, token)
        self.assertEqual(result[0][0]["asin"], "B000000002")
        self.assertEqual(result[0][2], "exact")

    def test_packaging_and_generic_tokens_do_not_create_a_match_alone(self) -> None:
        product = {"asin": "B000000001", "normalized": "knockout movie"}
        self.assertEqual(_tokens(product["normalized"]), {"knockout"})
        self.assertEqual(candidate_matches("A Movie", {}, {"movie": [product]}), [])

    def test_threshold_labels_are_not_probabilities(self) -> None:
        self.assertEqual(_match_level(0.96, False), "strong")
        self.assertEqual(_match_level(0.86, False), "close")
        self.assertEqual(_match_level(0.83, False), "blocked")


if __name__ == "__main__":
    unittest.main()
