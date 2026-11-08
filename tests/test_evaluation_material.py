from __future__ import annotations

import unittest

from amazon_movies.evaluation_material import LABEL_ALIASES, _safe_link


class EvaluationMaterialTests(unittest.TestCase):
    def test_only_http_sources_become_clickable(self) -> None:
        self.assertTrue(_safe_link("https://www.imdb.com/title/tt0000001/"))
        self.assertTrue(_safe_link("http://example.test/evidence"))
        self.assertFalse(_safe_link("javascript:alert(1)"))
        self.assertFalse(_safe_link("file:///D:/secret"))

    def test_chinese_labels_map_to_stored_technical_values(self) -> None:
        self.assertEqual(LABEL_ALIASES["同一实体"], "same_entity")
        self.assertEqual(LABEL_ALIASES["不同实体"], "different_entity")
        self.assertEqual(LABEL_ALIASES["无法确定"], "cannot_determine")


if __name__ == "__main__":
    unittest.main()
