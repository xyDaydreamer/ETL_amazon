import unittest

from amazon_movies.stage5_case_prep import _name_key


class Stage5CasePrepTests(unittest.TestCase):
    def test_name_key_is_only_for_candidate_generation(self) -> None:
        self.assertEqual(_name_key("José  López"), "jose lopez")
        self.assertNotEqual(_name_key("Ann Lee"), _name_key("Joann Lee"))


if __name__ == "__main__":
    unittest.main()
