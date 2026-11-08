from amazon_movies.p2_movie_expansion import RULE_VERSION, _matched_title
from amazon_movies.stage4_v3 import classify_product_type, decide, evaluate_candidate


def test_p2_rule_version_is_separate_from_frozen_evaluation() -> None:
    assert RULE_VERSION == "p2-movie-expansion-v1-stage4-evidence"
    assert "stage6-evidence-baseline" not in RULE_VERSION


def test_best_located_alias_is_used_for_title_comparison() -> None:
    title, source_id = _matched_title({
        "matched_titles": [
            {"source_title_raw": "Weak title", "similarity": 0.86, "record_number": 20, "source_record_id": 2},
            {"source_title_raw": "Exact Title", "similarity": 1.0, "record_number": 10, "source_record_id": 1},
        ],
        "primary_title": "Canonical title", "original_title": "Canonical title",
        "basics_source_record_id": 3,
    })
    assert title == "Exact Title"
    assert source_id == 1


def test_exact_title_and_located_credit_follow_frozen_path_b() -> None:
    product_type = classify_product_type(
        "Exact Title DVD", ["This film is directed by Jane Example and follows the central character."],
    )
    evaluated = evaluate_candidate(
        {"id": "tt1234567", "title": "Exact Title", "year": 2001, "type": "feature",
         "rank": 1, "principals": ["Jane Example"]},
        product_title="Exact Title DVD", product_type=product_type, brand=None,
        descriptions=["This film is directed by Jane Example and follows the central character."],
        source_record_id=10, reliable_product_years=[],
    )
    assert evaluated["auto_confirmation_eligible"] is True
    assert "B_T_exact_plus_explicit_credit" in evaluated["confirmation_paths"]
    assert decide([evaluated])["status"] == "rule_verified"
