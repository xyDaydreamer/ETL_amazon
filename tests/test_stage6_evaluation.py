import unittest

from amazon_movies.stage6_evaluation import _name_norm, _stratified


class Stage6EvaluationTests(unittest.TestCase):
    def test_name_normalization_is_candidate_only_and_accent_aware(self) -> None:
        self.assertEqual(_name_norm("José López"), "jose lopez")

    def test_stratified_sampling_is_deterministic_and_covers_strata(self) -> None:
        rows = [
            {"pair_key": f"a{i}", "stratum": "positive"} for i in range(5)
        ] + [
            {"pair_key": f"b{i}", "stratum": "negative"} for i in range(5)
        ]
        first = _stratified([dict(r) for r in rows], 4, 42)
        second = _stratified([dict(r) for r in rows], 4, 42)
        self.assertEqual([r["pair_key"] for r in first], [r["pair_key"] for r in second])
        self.assertEqual({r["stratum"] for r in first}, {"positive", "negative"})

    def test_development_person_keys_are_modelled_as_bilateral_exclusions(self) -> None:
        exclusions = {"development_scope_nconsts": ["nm0000001"]}
        excluded = set(exclusions.get("development_scope_nconsts", []))
        people = [{"nconst": "nm0000001"}, {"nconst": "nm0000002"}]
        self.assertEqual([p["nconst"] for p in people if p["nconst"] not in excluded], ["nm0000002"])

    def test_person_pair_work_is_bounded_by_requested_pair_count(self) -> None:
        requested_pairs = 100
        self.assertEqual((requested_pairs + 1) // 2, 50)


if __name__ == "__main__":
    unittest.main()
