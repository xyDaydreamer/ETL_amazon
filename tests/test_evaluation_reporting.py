from __future__ import annotations

import unittest

from amazon_movies.evaluation_reporting import _metrics, _name_features, normalize_person_name


class EvaluationReportingTests(unittest.TestCase):
    def test_metrics_exclude_human_unknown_and_algorithm_abstention(self) -> None:
        rows = [
            {"prediction": "same_entity", "human_label": "same_entity"},
            {"prediction": "same_entity", "human_label": "different_entity"},
            {"prediction": "different_entity", "human_label": "same_entity"},
            {"prediction": "different_entity", "human_label": "different_entity"},
            {"prediction": "needs_review", "human_label": "same_entity"},
            {"prediction": "same_entity", "human_label": "cannot_determine"},
        ]
        result = _metrics(rows)
        self.assertEqual((result["TP"], result["FP"], result["FN"], result["TN"]), (1, 1, 1, 1))
        self.assertEqual(result["scored_samples"], 4)
        self.assertEqual(result["human_cannot_determine"], 1)
        self.assertEqual(result["algorithm_deferred"], 1)
        self.assertEqual(result["deferred_human_label_breakdown"], {"same_entity": 1})
        operational = result["defer_as_not_auto_merged"]
        self.assertEqual((operational["TP"], operational["FP"], operational["FN"], operational["TN"]), (1, 1, 2, 1))
        self.assertEqual(operational["scored_samples"], 5)

    def test_name_categories_use_normalized_names_and_can_overlap(self) -> None:
        row = {
            "review_item_id": "x", "display_order": 1,
            "left_display": {"raw_name": "Earl Vickers"},
            "right_display": {"primary_name": "Hugo Vickers"},
            "human_label": "different_entity", "reason": "没有关联",
        }
        result = _name_features(row)
        self.assertTrue(result["one_shared_name_token_only"])
        self.assertFalse(result["normalized_full_name_same"])
        self.assertEqual(result["shared_tokens"], ["vickers"])
        self.assertEqual(normalize_person_name("José  D'Ávila"), "jose d avila")


if __name__ == "__main__":
    unittest.main()
